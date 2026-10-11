# -*- coding: utf-8 -*-
# 대장 정의 v2 후보(B_v2H·B_v2M·B_v2S)를 현행 정의(B_현행)와 매일 나란히 '구조만' 기록하는 연구 스크립트 — 수익률은 계산하지 않는다
# ==========================================================================
# 근거: docs/사전등록_2026-10-11_대장정의_v2_후보.md (§2 정의 · §3 기록)
#
# 🔒 이 파일이 지키는 것
#   1. 진입일 t 의 구조는 t 의 15:05 종목·테마 파일과, 급증 기준용으로 t **이전** 15:05 종목 파일의
#      거래량만 본다. t 이후 파일은 읽지 않는다(시험이 확인).
#      가격 수신 시각(`priceReceivedAt`)과 테마 값 시각(`updatedAt`)이 판단 마감(15:05:00) 뒤이거나
#      확인되지 않으면 관측일로 세지 않는다. 테마 등락률·소속 조회가 실패한 날도 세지 않는다.
#   2. 가격 결과(익일 시가·이후 가격·수익률·상승 여부)를 계산하는 경로가 없다.
#      비교 지표(M1·M2) 계산은 평가 조건(§6) 전에 별도로 만든다.
#   3. 공개 출력은 건수와 날짜별 집합 지문뿐이다. 종목 단위 목록은 쓰지 않는다
#      (입력이 공개 스냅샷이라 같은 코드로 다시 계산하면 같은 지문이 나온다).
#   4. 잠긴 두 연구의 코드(`hyeoks_theme_abc.py`·`hyeoks_leader_firstday.py`)는 가져다 읽기만 한다.
#
# 무엇을 하지 않나: 판정·문턱·채택을 말하지 않는다. 시트·네트워크를 건드리지 않는다. 지문 대상이 아니다.
# 실행: `python hyeoks_leader_def_v2.py --record` (research_daily.yml 이 평일 저녁 부른다) · `--status`(쓰지 않고 출력만)
# ==========================================================================
import argparse
import collections
import csv
import datetime
import gzip
import hashlib
import io
import os
import sys

import hyeoks_theme_abc as abc
from hyeoks_closing_bet import SNAP_DIR, read_snapshot, _f

STUDY_ID = "leader-def-v2"
START = "2026-10-12"     # 기록 시작일 S — 등록 커밋(2026-10-11) 다음 거래일. LOCKED 가 이날 15:05 뒤면 잠금 다음 거래일로 옮긴다
N_T = 20                 # 후보 `B_v2M` 의 테마 범위 — 운영 `omakase.py:569` 의 `[:20]`
N_T_H = 3                # 사용자 기준 `B_v2H` 의 테마 범위 — 사용자 정의 '그날 테마 등락률 상위 3' (10/11)
H_MIN_RATE = 15.0        # 사용자 기준 대장의 15:05 등락률 하한(%) — 사용자 정의 '15% 이상 상승한 종목만 대장' (10/11)
H_DAYS_TARGET = 60       # 표본 끝 — 공통 제외 뒤 `B_v2H` 가 1건 이상인 관측일이 이 수에 이르는 날까지
CAP = "2027-06-30"       # 표본 상한 — 이날 진입분까지 위 수에 못 이르면 자료 부족
SURGE_WINDOW = 10        # 운영 `vol_ratio_10d` 의 창 (`omakase.py:1846`)
SURGE_MULT = 2.0         # 운영 'V.거래과열' `vol_ratio_10d >= 200` (`omakase.py:2080`)
CUTOFF = "15:05:00"      # 판단 마감(KST) — 가격·테마 값이 이 시각까지 수신·갱신된 날만 관측일이다 (운영대전제 §2 15:05 슬롯)
SLOT_CLOSE = (15, 25)    # 수집기 15:05 슬롯 허용 창의 끝(`hyeoks_market_snapshot.SLOTS`) — 이 뒤면 그날 파일 유무가 정해진다
KST = datetime.timezone(datetime.timedelta(hours=9))
OUT = os.path.join("data", "research_runs", "leader_def_v2_structure.csv")

FIELDS = ["date", "status", "in_calendar", "captured_at", "price_at", "theme_at", "written_at",
          "theme_rate_fail", "member_unknown", "themes", "A",
          "cur_pre", "cur", "cur_excl",
          "v2h_pre", "v2h", "v2h_excl", "v2h_below", "v2h_overlap",
          "v2m_pre", "v2m", "v2m_excl", "v2m_slot_small", "v2m_slot_dominance", "v2m_slot_dup",
          "v2s_pre", "v2s", "v2s_short", "v2s_excl",
          "overlap", "v2m_only", "cur_only", "jaccard",
          "top3_v2m_pre", "top3_cur_slots", "topN_cur_slots",
          "fp"]


def _num(v):
    """숫자 칸 → float. 비었거나 읽을 수 없으면 None — 0 으로 바꾸지 않는다(Codex 의견 ②)."""
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def read_themes(path):
    """15:05 테마 파일 → [{code, rate, at, n}]. 첫 줄은 메타다.

    rate — `changeRate`(읽을 수 없으면 None). at — 네이버 값 시각 `updatedAt`.
    n — 네이버가 센 구성종목 수(`risingCount + fallingCount + unchangedCount`, 하나라도 없으면 None).
    """
    with gzip.open(path, "rt", encoding="utf-8") as fp:
        lines = fp.read().splitlines()
    out = []
    for r in csv.DictReader(io.StringIO("\n".join(lines[1:]))):
        code = (r.get("code") or "").strip()
        if code:
            cnt = [_num(r.get(k)) for k in ("risingCount", "fallingCount", "unchangedCount")]
            out.append({"code": code, "rate": _num(r.get("changeRate")), "at": (r.get("updatedAt") or "").strip(),
                        "n": int(sum(cnt)) if all(c is not None for c in cnt) else None})
    return out


def top_themes(themes, n):
    """`changeRate` 내림차순 상위 n. 동률은 테마 code 문자열 오름차순. 파일의 `ranking` 열은 쓰지 않는다.
    등락률을 읽지 못한 테마는 순위에 넣지 않는다 — 그런 날은 `day_quality` 가 관측일에서 뺀다."""
    ok = [t for t in themes if t["rate"] is not None]
    return [t["code"] for t in sorted(ok, key=lambda t: (-t["rate"], t["code"]))[:n]]


def _at(s, d):
    """ISO 시각 문자열 → KST 시각. 읽을 수 없거나 진입일 d 의 시각이 아니면 None (확인 불가)."""
    try:
        t = datetime.datetime.fromisoformat(str(s).strip())
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=KST)
    t = t.astimezone(KST)
    return t if t.date().isoformat() == d else None


def day_quality(d, meta, themes, rows1505, top):
    """(상태, 기록할 값). 상태가 '기록' 인 날만 관측일이다.

    - 시각 — 가격 수신 시각(종목 파일 메타 `priceReceivedAt`)과 테마 값 시각(모든 테마 `updatedAt` 의 최댓값)이
      둘 다 d 의 15:05:00 이하여야 한다. 확인할 수 없으면 '시각불명', 넘으면 '시각초과'.
      테마 소속 목록은 가격 값이 아니라 시각 조건을 두지 않는다(그 뒤에 조회된다 — 사전등록 §9).
    - 완전성 — 등락률을 읽지 못한 테마가 있으면 '불완전(테마등락률)'(순위를 정할 수 없다).
      상위 N_T 테마 중 네이버 구성종목 수가 1 이상인데 종목 파일에 소속 종목이 하나도 없으면
      소속 조회 실패로 보고 '불완전(소속)'(‘구성 부족’ 으로 세지 않는다).
    """
    cutoff = datetime.datetime.fromisoformat(f"{d}T{CUTOFF}+09:00")
    price_at = _at(meta.get("priceReceivedAt", ""), d)
    t_ats = [_at(t["at"], d) for t in themes]
    theme_at = max(t_ats) if themes and all(t_ats) else None
    rate_fail = sum(1 for t in themes if t["rate"] is None)
    found = collections.Counter(c for r in rows1505.values() for c in theme_nos(r))
    tops = set(top)
    unknown = sum(1 for t in themes if t["code"] in tops and (t["n"] or 0) > 0 and found[t["code"]] == 0)
    if price_at is None or theme_at is None:
        status = "시각불명"
    elif price_at > cutoff or theme_at > cutoff:
        status = "시각초과"
    elif rate_fail:
        status = "불완전(테마등락률)"
    elif unknown:
        status = "불완전(소속)"
    else:
        status = "기록"
    return status, {"price_at": price_at.isoformat() if price_at else "",
                    "theme_at": theme_at.isoformat() if theme_at else "",
                    "theme_rate_fail": rate_fail, "member_unknown": unknown}


def theme_nos(row):
    """종목의 소속 테마 전부(`themeNos`, 파이프 구분). 빈 칸은 버린다."""
    return {x.strip() for x in str(row.get("themeNos") or "").replace(",", "|").split("|") if x.strip()}


def build_v2m(items, rows1505, top_codes):
    """후보 1 `B_v2M` — `themeNos` 다중 소속 · 상위 테마. 나머지는 `build_B_current` 와 같은 규칙.

    테마 순위 순서로 처리하고, 앞 순위에서 이미 대장이 된 종목은 '중복' 으로 센다(하루 한 번).
    돌려주는 것: (대장 목록, 자리 집계). 공통 제외(`risk_filter`)는 부르는 쪽이 한다.
    """
    nos = {it["code"]: theme_nos(rows1505.get(it["code"], {})) for it in items}
    leaders, seen = [], set()
    slots = {"small": 0, "dominance": 0, "dup": 0}
    for rank, tc in enumerate(top_codes, 1):
        members = [it for it in items if tc in nos[it["code"]]]
        if len(members) < abc.MIN_THEME_SIZE:
            slots["small"] += 1
            continue
        pool = sorted(members, key=lambda x: (-x["amt"], x["code"]))[:abc.LEADER_POOL]
        if len(pool) >= 2 and pool[0]["amt"] >= pool[1]["amt"] * abc.DOMINANCE_MULT:
            slots["dominance"] += 1
            continue
        best = sorted(pool, key=lambda x: (-x["rate"], x["code"]))[0]
        if best["code"] in seen:
            slots["dup"] += 1
            continue
        seen.add(best["code"])
        leaders.append(dict(best, theme=tc, theme_rank=rank))
    return leaders, slots


def surge_split(leaders, rows1505, prior_vols):
    """후보 2 `B_v2S` — 15:05 거래량 ≥ 2 × 직전 10 관측일 15:05 거래량 평균.

    `prior_vols` 는 t 이전 15:05 종목 파일 10개의 {코드: 거래량} (오래된 순). 10개가 안 되거나
    그중 하루라도 값이 없거나 0 이면 '기준 부족' 으로 빼고 센다. 나눗셈 없이 정수 비교한다.
    """
    passed, short = [], 0
    for it in leaders:
        base = [v.get(it["code"], 0.0) for v in prior_vols]
        if len(base) < SURGE_WINDOW or any(b <= 0 for b in base):
            short += 1
            continue
        today = _f(rows1505.get(it["code"], {}).get("tradeVolume"))
        if today * len(base) >= SURGE_MULT * sum(base):
            passed.append(it)
    return passed, short


def _excl(d):
    return ";".join(f"{k}={d[k]}" for k in abc.RISK_REASONS)


def build_v2h(items, rows1505, top3):
    """사용자 기준 `B_v2H` — 상위 3 테마에서 `build_v2m` 과 같은 규칙으로 뽑은 대장 중 15:05 등락률 ≥ 15%.

    대장을 먼저 뽑고 그 대장에만 하한을 건다. 하한에 못 미친 대장을 같은 테마의 다른 종목으로 채우지 않는다.
    돌려주는 것: (하한 통과 대장, 상위 3 대장 전체, 하한 미달 수). 공통 제외는 부르는 쪽이 한다.
    """
    leaders, _ = build_v2m(items, rows1505, top3)
    passed = [x for x in leaders if x["rate"] >= H_MIN_RATE]
    return passed, leaders, len(leaders) - len(passed)


def day_structure(rows1505, themes, prior_vols):
    """하루 구조 집계. 입력은 그날 15:05 자료와 그 이전 거래량뿐이다. 가격 결과는 다루지 않는다."""
    A, _ = abc.build_A(rows1505)
    cur, _ = abc.build_B_current(A)
    curc, cur_ex = abc.risk_filter(cur)
    top = top_themes(themes, N_T)
    v2h, top3, below = build_v2h(A, rows1505, top[:N_T_H])
    v2hc, h_ex = abc.risk_filter(v2h)
    v2m, slots = build_v2m(A, rows1505, top)
    v2mc, m_ex = abc.risk_filter(v2m)
    v2s, short = surge_split(v2m, rows1505, prior_vols)
    v2sc, s_ex = abc.risk_filter(v2s)
    cur_themes = {x["theme"] for x in cur}

    s_cur, s_m, s_h = {x["code"] for x in curc}, {x["code"] for x in v2mc}, {x["code"] for x in v2hc}
    union = s_cur | s_m
    lines = sorted(f"{name}\t{x['code']}" for name, xs in (("cur", curc), ("v2h", v2hc), ("v2m", v2mc), ("v2s", v2sc))
                   for x in xs)
    return {
        "themes": len(themes), "A": len(A),
        "cur_pre": len(cur), "cur": len(curc), "cur_excl": _excl(cur_ex),
        "v2h_pre": len(v2h), "v2h": len(v2hc), "v2h_excl": _excl(h_ex), "v2h_below": below,
        "v2h_overlap": len(s_cur & s_h),
        "v2m_pre": len(v2m), "v2m": len(v2mc), "v2m_excl": _excl(m_ex),
        "v2m_slot_small": slots["small"], "v2m_slot_dominance": slots["dominance"], "v2m_slot_dup": slots["dup"],
        "v2s_pre": len(v2s), "v2s": len(v2sc), "v2s_short": short, "v2s_excl": _excl(s_ex),
        "overlap": len(s_cur & s_m), "v2m_only": len(s_m - s_cur), "cur_only": len(s_cur - s_m),
        "jaccard": f"{len(s_cur & s_m) / len(union):.4f}" if union else "",
        "top3_v2m_pre": len(top3),
        "top3_cur_slots": sum(1 for tc in top[:N_T_H] if tc in cur_themes),
        "topN_cur_slots": sum(1 for tc in top if tc in cur_themes),
        "fp": hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()[:16],
    }


def _stock_path(snap_dir, d):
    return os.path.join(snap_dir, f"{d}_1505.csv.gz")


def _theme_path(snap_dir, d):
    return os.path.join(snap_dir, f"{d}_1505_theme.csv.gz")


def _in_calendar(d, cal):
    return "Y" if (cal.start is None or cal.start <= d <= cal.end) else "N"


def default_as_of(now=None):
    """기록 기준일 — 15:05 슬롯 허용 창이 닫힌(15:25 뒤) 가장 최근 날짜. 그날까지의 예정 거래일을 모두 행으로 남긴다."""
    now = (now or datetime.datetime.now(KST)).astimezone(KST)
    d = now.date() if (now.hour, now.minute) >= SLOT_CLOSE else now.date() - datetime.timedelta(days=1)
    return d.isoformat()


def collect(snap_dir=SNAP_DIR, start=START, as_of=None):
    """`start` ~ `as_of` 의 구조 행. 예정 거래일인데 파일이 없으면 결측 행을 남긴다(조용히 빼지 않는다).

    예정 거래일은 마지막 파일 날짜가 아니라 `as_of` 까지 센다 — 마지막 파일 뒤로 계속 빠진 날도 행이 된다
    (Codex 의견 ②). 달력 범위를 넘는 날은 예정 거래일을 셀 수 없어, 파일이 있는 날만 `in_calendar=N` 으로 남긴다.
    파일이 있는 날은 등록 휴장일·주말이 아니면 기록한다.
    급증 기준을 위해 `start` 직전 10개의 15:05 종목 파일은 거래량만 읽는다.
    """
    as_of = as_of or default_as_of()
    cal = abc._calendar(snap_dir)
    file_days = sorted(fn[:10] for fn in os.listdir(snap_dir)
                       if fn.endswith("_1505.csv.gz") and fn[:4].isdigit() and fn[:10] <= as_of
                       and not abc._is_closed(fn[:10], cal))
    theme_days = {fn[:10] for fn in os.listdir(snap_dir) if fn.endswith("_1505_theme.csv.gz") and fn[:10] <= as_of}
    after = [d for d in file_days if d >= start]
    scheduled = abc.expected_trading_days(start, as_of, cal) if start <= as_of else []
    days = sorted(set(scheduled) | set(after) | {d for d in theme_days if d >= start and not abc._is_closed(d, cal)})

    i0 = next((i for i, d in enumerate(file_days) if d >= start), len(file_days))
    vols = []                                        # 직전 15:05 종목 파일들의 거래량 (오래된 순)
    for d in file_days[max(0, i0 - SURGE_WINDOW):i0]:
        _, rows, _ = read_snapshot(_stock_path(snap_dir, d))
        vols.append({c: _f(r.get("tradeVolume")) for c, r in rows.items()})

    out = []
    for d in days:
        has_s, has_t = d in file_days, d in theme_days
        row = {"date": d, "in_calendar": _in_calendar(d, cal)}
        rows = None
        if has_s:
            meta, rows, _ = read_snapshot(_stock_path(snap_dir, d))
            row["captured_at"] = meta.get("capturedAt", "")
            row["written_at"] = meta.get("writtenAt", "")
        if has_s and has_t:
            themes = read_themes(_theme_path(snap_dir, d))
            status, q = day_quality(d, meta, themes, rows, top_themes(themes, N_T))
            row.update(q)
            row.update(day_structure(rows, themes, vols[-SURGE_WINDOW:]))
            row["status"] = status
        else:
            row["status"] = "결측(" + ("둘다" if not (has_s or has_t) else "종목" if not has_s else "테마") + ")"
        if rows is not None:                         # 다음 날의 급증 기준 — 테마 파일 유무와 무관하다
            vols.append({c: _f(r.get("tradeVolume")) for c, r in rows.items()})
        out.append(row)
    return out


def write(rows, path=OUT):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})


def summary(rows):
    rec = [r for r in rows if r["status"] == "기록"]
    h_days = sum(1 for r in rec if r["v2h"] > 0)
    other = collections.Counter(r["status"] for r in rows if r["status"] != "기록")
    lines = [f"[{STUDY_ID}] 구조 기록 — 시작 {START} · 관측일 {len(rec)}일 · 그 밖 "
             + (" · ".join(f"{k} {v}일" for k, v in sorted(other.items())) or "0일"),
             f"  표본 진행: 사용자 기준 대장이 있는 관측일 {h_days}/{H_DAYS_TARGET} (상한 {CAP} 진입분)"]
    if rec:
        r = rec[-1]
        lines.append(f"  최근 {r['date']}: 현행 {r['cur']} · v2H {r['v2h']} · v2M {r['v2m']} · v2S {r['v2s']} (공통 제외 뒤) · "
                     f"겹침 {r['overlap']} · 상위 3 테마 중 현행 대장이 있는 자리 {r['top3_cur_slots']}/{N_T_H}")
    lines.append("  수익률·이후 가격은 계산하지 않는다 (평가는 잠긴 두 연구 평가 뒤 — 사전등록 §6)")
    return "\n".join(lines)


def main(argv=None):
    ap = argparse.ArgumentParser(description="대장 정의 v2 후보 — 구조만 기록")
    ap.add_argument("--record", action="store_true", help=f"{OUT} 를 다시 쓴다")
    ap.add_argument("--status", action="store_true", help="쓰지 않고 요약만 출력")
    ap.add_argument("--snap-dir", default=SNAP_DIR)
    ap.add_argument("--out", default=OUT)
    a = ap.parse_args(argv)
    rows = collect(a.snap_dir)
    if a.record:
        write(rows, a.out)
    print(summary(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
