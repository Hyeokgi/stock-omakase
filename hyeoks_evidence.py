# -*- coding: utf-8 -*-
"""거래일·가격의 **독립 근거**를 대조한다 — 스스로 인증하지 않는다.

왜 만드나 (2026-09-16)
----------------------
`account-nav-v3` 의 두 게이트가 계속 막혀 있다:

    calendar_verification.status == 'unverified'
    price_verification.status   == 'unverified'

원래 지적은 **순환논리**였다 — "지수 일봉으로 만든 거래일 목록을 같은 목록으로
검증했다고 하지 않는다"(`account_source_adapter` 설계 주석).
그 지적은 옳고, 그래서 어댑터는 이 둘을 절대 채우지 않는다.

풀 수 있는 것과 없는 것이 **다르다.** 이 모듈은 그 둘을 갈라놓는다.

거래일 — 풀린다
~~~~~~~~~~~~~~~
저장소에 이미 **사람이 외부 자료로 확인한** 휴장 목록이 있다:

    data/market_snapshot/nontrading.txt   + calendar_scope.json
    출처: 한국천문연구원 월력요항, KRX 시장 개폐 규정 (URL 이 파일에 박혀 있다)
    checked_at: 2026-09-13 · 범위: 2026-08-01 ~ 2026-12-31

이건 네이버 일봉과 **출처가 다르다.** 예정 휴장 목록에서 거래일을 유도하고
실제 관측과 맞춰보면 순환이 깨진다. 그게 이 모듈의 `calendar_agreement` 다.

가격 — 지금 자료로는 안 풀린다
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
우리가 가진 두 가격 계열은 **같은 벤더의 다른 엔드포인트**다:

    일봉   fchart.stock.naver.com/sise.nhn      (수정주가)
    스냅샷 stock.naver.com/api/domestic/...     (그 시각 체결가)

둘을 맞춰보면 **불일치는 잡히지만 일치해도 독립 확인이 아니다.**
네이버의 조정 기준이 통째로 틀렸다면 둘 다 같이 틀린다.
그러므로 `price_agreement` 는 대조 결과만 돌려주고
**`verified` 를 만들지 않는다.** 그게 정직한 상태다.

다만 이 대조에는 값이 있다 — 일봉은 **수정주가**이고 스냅샷은 **그날 체결가**라,
기업행사가 있었으면 과거 일봉이 소급 조정돼 둘이 어긋난다.
"가격 조정 기준과 기업행사 대조"가 바로 그것이고, 이게 그걸 **잰다**.
"""
import argparse
import datetime
import json
import sys

from hyeoks_trading_calendar import load_nontrading

KST = datetime.timezone(datetime.timedelta(hours=9))
EVIDENCE_VERSION = "evidence-v1"

# 일봉과 스냅샷이 같은 값을 말한다고 볼 허용 오차.
# 호가단위 반올림·표시 반올림 때문에 0 을 요구할 수 없다. 상대오차로 본다.
PRICE_TOL = 0.005      # 0.5%


# ── 거래일 ────────────────────────────────────────────────────────────
def scheduled_sessions(start, end, nontrading):
    """예정 휴장 목록에서 **유도한** 거래일. 관측이 아니라 달력이다.

    범위 밖이면 `ClosureCalendar.check` 가 막는다 — 주말 규칙으로 추정하지 않는다.
    """
    a, b = datetime.date.fromisoformat(start), datetime.date.fromisoformat(end)
    if a > b:
        raise ValueError(f'범위가 뒤집혔다: {start} ~ {end}')
    out, d = [], a
    while d <= b:
        iso = d.isoformat()
        nontrading.check(iso)               # 검토 범위 밖이면 여기서 막힌다
        if d.weekday() < 5 and iso not in nontrading:
            out.append(iso)
        d += datetime.timedelta(days=1)
    return out


def calendar_agreement(observed, start, end, nontrading):
    """예정 달력과 실제 관측을 맞춰본다. **자동으로 해소하지 않는다.**

    두 어긋남은 뜻이 전혀 다르다:
      · `expected_only` — 예정 거래일인데 관측이 없다.
        임시휴장이거나 **우리 수집이 실패**했다. 파일이 없다고 휴장일을 만들지 않는다.
      · `observed_only` — 관측은 있는데 예정 휴장일이다.
        **달력이 틀렸다.** 이쪽이 더 심각하다 — 등록 자료를 고쳐야 한다.
    """
    exp = scheduled_sessions(start, end, nontrading)
    obs = sorted({d for d in observed if start <= d <= end})
    es, os_ = set(exp), set(obs)
    return {
        "start": start, "end": end,
        "expected": exp, "observed": obs,
        "expected_only": sorted(es - os_),
        "observed_only": sorted(os_ - es),
        "agree": es == os_,
    }


def calendar_evidence(agreement, scope, as_of):
    """게이트에 넣을 `calendar_verification` 블록. **일치할 때만 verified.**

    `status` 를 무조건 verified 로 쓰지 않는다 — 어긋나면 어긋난 채로 돌려준다.
    """
    sessions = [d for d in agreement["expected"] if d <= as_of]
    if not agreement["agree"]:
        return {"status": "unverified",
                "evidence": "",
                "sessions": [],
                "reason": (f"예정 달력과 관측이 어긋난다 — "
                           f"관측없음 {len(agreement['expected_only'])}일 / "
                           f"달력밖관측 {len(agreement['observed_only'])}일")}
    srcs = "; ".join(scope.get("sources", []))
    return {
        "status": "verified",
        "evidence": (f"{scope.get('version')} (checked_at={scope.get('checked_at')}, "
                     f"scope={scope.get('start')}~{scope.get('end')}) "
                     f"예정 휴장 목록과 관측 거래일이 {agreement['start']}~{agreement['end']} "
                     f"전 구간 일치. 출처: {srcs}"),
        "sessions": sessions,
        # 무엇을 확인했고 무엇을 안 했는지 같이 박는다. 이걸 빼면 과장이 된다.
        "establishes": "예정 휴장 달력(외부 출처)과 관측 거래일이 일치한다",
        "does_not_establish": ("실제 장 운영 인증이 아니다. 임시휴장·시스템 장애로 "
                               "예정과 실제가 다를 수 있고, 등록 달력 자체가 "
                               "'scheduled_closures_only' 라고 밝히고 있다"),
    }


# ── 가격 ──────────────────────────────────────────────────────────────
def price_agreement(bars, snaps, tol=PRICE_TOL):
    """일봉(수정주가)과 스냅샷(그날 체결가)을 맞춰본다.

    bars  : {code: {date: {open, high, low, close}}}
    snaps : {date: {code: {open_price, prev_close_derived, ...}}}

    두 가지를 본다:
      ① 같은 날 시가       — 일봉 open   vs 스냅샷 open_price
      ② 전일 종가          — 일봉 close(d-1) vs 스냅샷(d) prev_close_derived
         ②가 핵심이다. 일봉은 **소급 조정**되고 스냅샷은 그날 값이라,
         기업행사가 있었으면 여기서 어긋난다.

    ⚠️ 일치해도 **독립 확인이 아니다.** 같은 벤더의 다른 엔드포인트다(모듈 설명 참조).
    """
    dates = sorted(snaps)
    prev = {d: dates[i - 1] for i, d in enumerate(dates) if i}
    res = {"checked_open": 0, "mismatch_open": 0,
           "checked_prev_close": 0, "mismatch_prev_close": 0,
           "worst_open": [], "worst_prev_close": [], "tol": tol}

    def rel(a, b):
        return abs(a / b - 1.0) if b else None

    for d in dates:
        for code, s in snaps[d].items():
            bar = bars.get(code, {}).get(d)
            if bar:
                o_s, o_b = s.get("open_price"), bar.get("open")
                if _pos(o_s) and _pos(o_b):
                    res["checked_open"] += 1
                    r = rel(o_s, o_b)
                    if r > tol:
                        res["mismatch_open"] += 1
                        res["worst_open"].append((code, d, o_b, o_s, r))
            pd = prev.get(d)
            pbar = bars.get(code, {}).get(pd) if pd else None
            if pbar:
                pc_s, pc_b = s.get("prev_close_derived"), pbar.get("close")
                if _pos(pc_s) and _pos(pc_b):
                    res["checked_prev_close"] += 1
                    r = rel(pc_s, pc_b)
                    if r > tol:
                        res["mismatch_prev_close"] += 1
                        res["worst_prev_close"].append((code, d, pc_b, pc_s, r))
    for k in ("worst_open", "worst_prev_close"):
        res[k] = sorted(res[k], key=lambda x: -x[4])[:5]
    return res


def price_evidence(agreement):
    """가격 게이트 블록. **언제나 unverified 다.** 이유를 같이 적는다."""
    return {
        "status": "unverified",
        "evidence": "",
        "reason": ("일봉(fchart.stock.naver.com)과 스냅샷(stock.naver.com)은 "
                   "**같은 벤더의 다른 엔드포인트**다. 대조로 불일치는 잡히지만 "
                   "일치해도 조정 기준의 독립 확인이 아니다 — 네이버의 기준이 "
                   "틀렸다면 둘 다 같이 틀린다."),
        "crosscheck": {k: v for k, v in agreement.items()
                       if not k.startswith("worst")},
        "needs": ("KRX 공식 시세 또는 다른 벤더의 수정주가/기업행사 이력. "
                  "사람이 넣기 전까지 이 게이트는 열리지 않는다."),
    }


def _pos(v):
    return isinstance(v, (int, float)) and v == v and v not in (float('inf'), float('-inf')) and v > 0


# ── 리포트 / CLI ──────────────────────────────────────────────────────
def report(agreement, evidence, scope):
    L = [f"# 거래일 독립 근거 대조 — {agreement['start']} ~ {agreement['end']}", "",
         "지수 일봉으로 만든 목록을 같은 목록으로 검증하지 않는다.",
         "**출처가 다른** 예정 휴장 목록에서 거래일을 유도해 실제 관측과 맞춘다.", "",
         "| 항목 | 값 |", "|---|---|",
         f"| 등록 달력 | `{scope.get('version')}` |",
         f"| 사람이 확인한 날 | {scope.get('checked_at')} |",
         f"| 검토 범위 | {scope.get('start')} ~ {scope.get('end')} |",
         f"| 예정 거래일 | {len(agreement['expected'])} |",
         f"| 관측 거래일 | {len(agreement['observed'])} |",
         f"| 예정인데 관측 없음 | {len(agreement['expected_only'])} |",
         f"| 달력 밖인데 관측 있음 | {len(agreement['observed_only'])} |",
         f"| 판정 | {'✅ 전 구간 일치' if agreement['agree'] else '❌ 어긋남'} |", ""]
    if agreement["expected_only"]:
        L += ["## 예정 거래일인데 관측이 없다", "",
              "임시휴장이거나 **우리 수집이 실패**한 것이다. "
              "파일이 없다는 이유로 휴장일을 새로 만들지 않는다.", "",
              "- " + ", ".join(f"`{d}`" for d in agreement["expected_only"]), ""]
    if agreement["observed_only"]:
        L += ["## 예정 휴장일인데 관측이 있다", "",
              "**등록 달력이 틀렸다.** 이쪽이 더 심각하다 — 공식 자료로 고쳐야 한다.", "",
              "- " + ", ".join(f"`{d}`" for d in agreement["observed_only"]), ""]
    L += ["## 이 대조가 세우는 것과 세우지 못하는 것", "",
          f"- **세운다**: {evidence.get('establishes', '없음')}",
          f"- **세우지 못한다**: {evidence.get('does_not_establish', '없음')}", "",
          "출처: " + "; ".join(scope.get("sources", [])), ""]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snap-dir", default="data/market_snapshot")
    ap.add_argument("--start", default=None, help="기본: 첫 15:05 스냅샷")
    ap.add_argument("--end", default=None, help="기본: 마지막 15:05 스냅샷")
    ap.add_argument("--out", default=None)
    ap.add_argument("--emit", default=None,
                    help="게이트에 넣을 calendar_verification 블록을 JSON 으로 저장")
    a = ap.parse_args(argv)

    import account_source_adapter as adapter
    obs = adapter.snapshot_dates(a.snap_dir)
    if not obs:
        print("❌ 15:05 스냅샷이 없다 — 대조할 관측이 없다")
        return 2
    start, end = a.start or obs[0], a.end or obs[-1]
    scope = json.loads(open(f"{a.snap_dir}/calendar_scope.json",
                            encoding="utf-8").read())
    nt = load_nontrading(a.snap_dir)

    ag = calendar_agreement(obs, start, end, nt)
    ev = calendar_evidence(ag, scope, end)
    md = report(ag, ev, scope)
    path = a.out or f"data/account/{datetime.datetime.now(KST):%Y-%m-%d}_calendar_evidence.md"
    import os
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w", encoding="utf-8").write(md)
    print(md)
    print(f"💾 저장: {path}")

    if a.emit:
        open(a.emit, "w", encoding="utf-8").write(
            json.dumps(ev, ensure_ascii=False, indent=2, allow_nan=False))
        print(f"💾 근거 블록: {a.emit} (status={ev['status']})")

    # fail-closed: 어긋나면 초록으로 끝내지 않는다
    if not ag["agree"]:
        print("\n❌ 예정 달력과 관측이 어긋난다 — 사람이 확인해야 한다")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
