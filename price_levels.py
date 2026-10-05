# -*- coding: utf-8 -*-
"""목표가·손절가 검증 — AI 가 낸 가격을 시트·가상계좌·알림에 쓰기 전에 한 번 거른다.

🔴 2026-10-04 — 두 종류의 사고가 확인됐다.
  ① 10/2 장기 리포트 1건: 콤마 없는 7자리 확정 현재가를 1/10 로 읽고
     목표가·손절가를 둘 다 1/10 크기로 냈다. 현재가보다 목표가가 낮아
     가상계좌는 다음 실행에서 '목표가 도달' 로 즉시 닫히고, 장기 채널 알림은
     '목표가 도달' 을 잘못 보낸다. [DATA] 파서에 검증이 하나도 없었다.
  ② 간단 브리핑의 JSON 예시값(150000 / 135000)을 그대로 베낀 행이 백테스트_로그에
     6건 들어갔다. 시스템 채널에는 기준가 대비 +50% 를 넘는 목표가가 31건 있었다.

규칙 (한 곳에서만 정의한다 — 딥리포트 프롬프트 문구도 이 표에서 만든다):
  · 구조: 손절가 < 현재가 < 목표가. 어기면 무효.
  · 리포트 밴드: PRICE_BANDS ± BAND_TOLERANCE_PCT. 밖이면 경고(값은 보존).
  · 상식 범위: 목표 +SANE_MAX_UP 이하, 손절 -SANE_MAX_DOWN 이내. 밖이면 무효.
  · 단위 착오(×10 / ÷10): 보정한 쌍이 **밴드 안에 들어가고 그런 보정이 하나뿐일 때만**
    보정한다. 애매하면 보정하지 않고 무효로 둔다.
  · 손익비 하한 미달(소수 둘째 자리 반올림 기준)은 **무효** — 사용자 결정 2026-10-05.
    그 픽은 가상계좌·백테스트_로그 목표/손절에 쓰지 않는다(리포트 발송과 선정 기록은 그대로).
    밴드 안에서도 손익비를 못 맞추는 조합(예: 단기 +7%/-8%)이 있어 둘 다 지켜야 한다.
    밴드가 없는 경로(간단 브리핑)에는 손익비 하한이 없다.

밴드 수치 자체는 2026-10-04 이전 프롬프트의 수치를 그대로 옮긴 것이다. 바꾸지 않았다.
"""

LEVELS_VERSION = "price-levels-v2"   # v2: 손익비 미달 무효 (2026-10-05)

# 보유기간별 (목표 상승률 % 범위, 손절 하락률 % 범위, 최소 손익비)
PRICE_BANDS = {
    "short": ((7, 12), (6, 8), 1.2),
    "mid": ((12, 20), (7, 11), 1.8),
    "long": ((20, 40), (10, 18), 2.2),
}
BAND_TOLERANCE_PCT = 2.0
# 밴드가 없는 경로(간단 브리핑·시트 동기화)에도 쓰는 상식 범위
SANE_MAX_UP = 0.50
SANE_MAX_DOWN = 0.25
_SCALES = (1, 10, 0.1)


def to_int(v):
    """'1,350,000원' · 1350000 · '1350000.0' → 1350000. 숫자가 아니면 0."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v) if v > 0 else 0
    s = str(v).strip().replace(",", "").replace("원", "")
    try:
        n = float(s)
    except ValueError:
        return 0
    return int(n) if n > 0 else 0


def pct(curr, target, stop):
    """(목표 상승률 %, 손절 하락률 %, 손익비). 손익비 = 상승률 / 하락률."""
    up = (target / curr - 1) * 100
    down = (1 - stop / curr) * 100
    rr = up / down if down > 0 else None
    return up, down, rr


def is_sane(base, target, stop):
    """구조 + 상식 범위. 밴드는 보지 않는다. base 는 가격을 정한 시점의 기준가."""
    base, target, stop = to_int(base), to_int(target), to_int(stop)
    if not (base and target and stop):
        return False
    return (base * (1 - SANE_MAX_DOWN) <= stop < base < target <= base * (1 + SANE_MAX_UP))


def sane_target(base, target):
    base, target = to_int(base), to_int(target)
    return bool(base and target) and base < target <= base * (1 + SANE_MAX_UP)


def sane_stop(base, stop):
    base, stop = to_int(base), to_int(stop)
    return bool(base and stop) and base * (1 - SANE_MAX_DOWN) <= stop < base


def in_band(curr, target, stop, st_type, tol=BAND_TOLERANCE_PCT):
    band = PRICE_BANDS.get(st_type)
    if not band:
        return True
    (ulo, uhi), (dlo, dhi), _ = band
    up, down, _ = pct(curr, target, stop)
    return (ulo - tol <= up <= uhi + tol) and (dlo - tol <= down <= dhi + tol)


def validate_levels(curr, target, stop, st_type=None):
    """AI 가 낸 목표가·손절가를 검사한다.

    반환 dict:
      ok       — 써도 되는가 (구조·상식 범위 통과, 필요하면 단위 보정 후)
      target, stop — 쓸 값 (보정됐으면 보정값, 무효면 None)
      scale    — (목표 배율, 손절 배율). 보정이 없으면 (1, 1)
      issues   — 무효 사유 (구조·상식 범위·손익비 미달)
      warnings — 값은 쓰되 사람이 봐야 할 것 (밴드 이탈·단위 보정)
      up, down, rr — 최종 값 기준 비율 (무효면 None)
    """
    out = {"ok": False, "target": None, "stop": None, "scale": (1, 1),
           "issues": [], "warnings": [], "up": None, "down": None, "rr": None}
    c, t, s = to_int(curr), to_int(target), to_int(stop)
    if not c:
        out["issues"].append("현재가 없음")
        return out
    if not (t and s):
        out["issues"].append("목표가/손절가 없음")
        return out

    if is_sane(c, t, s):
        ft, fs = t, s
    else:
        fixes = []
        for kt in _SCALES:
            for ks in _SCALES:
                if (kt, ks) == (1, 1):
                    continue
                ct, cs = int(round(t * kt)), int(round(s * ks))
                if is_sane(c, ct, cs) and in_band(c, ct, cs, st_type):
                    fixes.append((kt, ks, ct, cs))
        if len(fixes) != 1:
            if s >= c:
                out["issues"].append(f"손절가 {s:,} ≥ 현재가 {c:,}")
            if t <= c:
                out["issues"].append(f"목표가 {t:,} ≤ 현재가 {c:,}")
            if not out["issues"]:
                out["issues"].append("상식 범위 밖 (목표 +{:.0f}% 초과 또는 손절 -{:.0f}% 초과)"
                                     .format(SANE_MAX_UP * 100, SANE_MAX_DOWN * 100))
            if len(fixes) > 1:
                out["issues"].append("단위 보정 후보가 여러 개라 보정하지 않음")
            return out
        kt, ks, ft, fs = fixes[0]
        out["scale"] = (kt, ks)
        out["warnings"].append(f"단위 보정: 목표 {t:,}→{ft:,}, 손절 {s:,}→{fs:,}")

    up, down, rr = pct(c, ft, fs)
    out.update(ok=True, target=ft, stop=fs, up=up, down=down, rr=rr)
    band = PRICE_BANDS.get(st_type)
    if band:
        (ulo, uhi), (dlo, dhi), min_rr = band
        if not in_band(c, ft, fs, st_type):
            out["warnings"].append(
                f"밴드 이탈: 목표 +{up:.1f}% (기준 +{ulo}~{uhi}%), 손절 -{down:.1f}% (기준 -{dlo}~{dhi}%)")
        if rr is None or round(rr, 2) < min_rr:
            out.update(ok=False, target=None, stop=None)
            out["issues"].append(f"손익비 {rr:.2f} < 기준 {min_rr} (목표 +{up:.1f}% / 손절 -{down:.1f}%)")
    return out


def brief_cells(curr, raw_target, raw_stop):
    """간단 브리핑 JSON 의 목표/손절 → 시트 칸 문자열.

    · 둘 다 명시적 0 → ("관망", "관망")  — AI 의 매수 보류 표시(기존 동작 유지)
    · 검증 통과(구조·상식 범위, 필요하면 단위 보정) → ("61,000원", "57,000원")
    · 그 밖(못 읽음·한쪽만 0·역전·범위 밖) → (None, None) — 호출부는 칸을 건드리지 않는다
    세 번째 값은 validate_levels 결과.
    """
    def _zero(v):
        return v is not None and str(v).strip().replace(",", "").replace("원", "") in ("0", "0.0")

    if _zero(raw_target) and _zero(raw_stop):
        return "관망", "관망", {"ok": True, "issues": [], "warnings": ["매수 보류(0)"]}
    lv = validate_levels(curr, raw_target, raw_stop, None)
    if not lv["ok"]:
        return None, None, lv
    return f"{lv['target']:,}원", f"{lv['stop']:,}원", lv


def band_prompt_lines(st_type, curr):
    """딥리포트 프롬프트용 밴드 문장. 같은 표(PRICE_BANDS)에서 만들고 원 단위 범위도 함께 준다."""
    band = PRICE_BANDS.get(st_type)
    c = to_int(curr)
    if not band or not c:
        return ""
    (ulo, uhi), (dlo, dhi), min_rr = band
    t_lo, t_hi = int(c * (1 + ulo / 100)), int(c * (1 + uhi / 100))
    s_hi, s_lo = int(c * (1 - dlo / 100)), int(c * (1 - dhi / 100))
    return (f"   · 목표가: 확정 현재가 {c:,}원 대비 +{ulo}~{uhi}% → {t_lo:,}~{t_hi:,}원\n"
            f"   · 손절가: -{dlo}~{dhi}% → {s_lo:,}~{s_hi:,}원\n"
            f"   · 손익비 = (목표가 − 현재가) ÷ (현재가 − 손절가), 최소 {min_rr} 이상. "
            f"밴드 안의 조합이라도 손익비가 모자라면 목표를 올리거나 손절을 좁히십시오.\n"
            f"   · 손익비가 {min_rr} 에 못 미치면 시스템이 이 픽의 목표가·손절가를 무효로 처리해 가상계좌·백테스트에 기록하지 않습니다.")


def report_note(result):
    """리포트 본문 끝에 붙일 시스템 검증 문구. 문제가 없으면 빈 문자열."""
    if result["ok"] and not result["warnings"]:
        return ""
    lines = ["", "---", "", "**⚠️ 시스템 검증 (목표가·손절가)**", ""]
    if not result["ok"]:
        lines.append("- 리포트의 목표가·손절가가 시스템 검증을 통과하지 못해 **가상계좌·백테스트 목표/손절에 쓰지 않았습니다**: "
                     + "; ".join(result["issues"]))
        lines.append("- 본문의 가격 수치는 참고하지 마십시오.")
    else:
        for w in result["warnings"]:
            lines.append(f"- {w}")
        if result["scale"] != (1, 1):
            lines.append(f"- 기록 값: 목표가 {result['target']:,}원 / 손절가 {result['stop']:,}원 "
                         "(본문 수치는 단위가 잘못됐을 수 있습니다)")
    return "\n".join(lines) + "\n"
