# -*- coding: utf-8 -*-
# ==========================================================================
# 🔒 첫날 대장 부진 — 사전등록 `leader-firstday-v1` (LOCKED 2026-10-05)
# --------------------------------------------------------------------------
# 사전등록: docs/사전등록_2026-10-05_첫날대장부진_v1.md (이 코드와 같은 커밋).
# 질문: 처음 대장으로 잡힌 종목의 15:05 → 3관측일 뒤 15:05 실제 상승 비율이
#       같은 날·같은 층(등락률 구간 × 거래대금 3분위) 비대장보다 낮은가 (단측 D < 0).
#
# 🔒 결과 열람 통제(§6): 평가 조건 전에는 수·상태만 낸다. 상승 비율·차이·수익률을 내지 않는다.
#    평가 조건 = ① 확증 표본(10/6 이후 첫 60 관측일)의 결과 모두 성숙 ② 잠긴 leader-hold-v1 주 평가가 열렸거나 종료.
#    탐색 구간(10/6 전, 이미 열람한 구간)은 `--explore` 로 따로 계산할 수 있다.
# 운영 선정·주문과 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import os
import random
import sys

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, read_snapshot

VERSION = "leader-firstday-v1"
CONFIRM_FROM = "2026-10-06"
LOOKBACK = 20
HORIZON = 3
SAMPLE_DAYS = 60
RATE_CAP = 29.5
RATE_BINS = [("≤0", None, 0.0), ("0~5%", 0.0, 5.0), ("5~10%", 5.0, 10.0), ("10~20%", 10.0, 20.0), ("20~29.5%", 20.0, RATE_CAP)]
BOOT_N, BOOT_BLOCK, SEED = 10_000, 3, 20261005
BREADTH_BINS = [("< 0.4", None, 0.4), ("0.4~0.6", 0.4, 0.6), ("≥ 0.6", 0.6, None)]


def _f(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def rate_bin(rate):
    """≤0 · (0, 5) · [5, 10) · [10, 20) · [20, 29.5). 29.5 이상은 공통 제외라 None."""
    if rate <= 0:
        return "≤0"
    for name, lo, hi in RATE_BINS[1:]:
        if lo <= rate < hi:
            return name
    return None


def load_obs(snap_dir=SNAP_DIR):
    """관측일(15:05 스냅샷이 있는 날) 오름차순. 미래 날짜도 읽지만, 가격은 평가·탐색 함수만 쓴다."""
    out = []
    for name in sorted(os.listdir(snap_dir)):
        if not name.endswith("_1505.csv.gz") or not name[:4].isdigit():
            continue
        _, rows, _ = read_snapshot(os.path.join(snap_dir, name))
        A, _ = T.build_A(rows)
        B, _ = T.build_B_current(A)
        rates = [_f(r.get("prevChangeRate")) for r in rows.values()]
        rates = [x for x in rates if x is not None]
        out.append({"date": name[:10], "rows": rows, "A": A, "B": {x["code"] for x in B},
                    "breadth": (sum(1 for x in rates if x > 0) / len(rates)) if rates else None})
    return out


def registry(days):
    """관측일마다 처리군(새 대장)·대조군과 층. 가격 결과는 계산하지 않는다."""
    reg = []
    for i, day in enumerate(days):
        past = set().union(*[days[q]["B"] for q in range(max(0, i - LOOKBACK), i)]) if i else set()
        pool = [x for x in day["A"] if x["rate"] < RATE_CAP]
        if not pool:
            continue
        amts = sorted(x["amt"] for x in pool)
        c1, c2 = amts[len(amts) // 3], amts[2 * len(amts) // 3]
        for x in pool:
            if x["code"] in day["B"]:
                if x["code"] in past:
                    continue                 # 최근 20 관측일 안에 대장이었다 — 새 대장 아님, 대조군도 아님
                group = "T"
            else:
                group = "C"
            tercile = 0 if x["amt"] < c1 else (1 if x["amt"] < c2 else 2)
            reg.append({"i": i, "date": day["date"], "code": x["code"], "group": group,
                        "stratum": (day["date"], rate_bin(x["rate"]), tercile), "rate_bin": rate_bin(x["rate"]),
                        "breadth": day["breadth"]})
    return reg


def outcome(days, i, code, k=HORIZON):
    """1 = 실제 상승, 0 = 아님, None = 결측/미성숙."""
    if i + k >= len(days):
        return None
    a, b = days[i]["rows"].get(code), days[i + k]["rows"].get(code)
    pa, pb = (_f(a.get("nowPrice")) if a else None), (_f(b.get("nowPrice")) if b else None)
    if not pa or not pb:
        return None
    return 1 if pb > pa else 0


def stratified_D(rows):
    """[{stratum, group, y}] → (D, 처리군 수, 대조 없는 층에서 빠진 처리군 수)."""
    st = {}
    for r in rows:
        s = st.setdefault(r["stratum"], {"T": [], "C": []})
        s[r["group"]].append(r["y"])
    num, N, dropped = 0.0, 0, 0
    for s in st.values():
        if not s["T"]:
            continue
        if not s["C"]:
            dropped += len(s["T"])
            continue
        n = len(s["T"])
        num += n * (sum(s["T"]) / n - sum(s["C"]) / len(s["C"]))
        N += n
    return (num / N if N else None), N, dropped


def block_boot(rows, n=BOOT_N, block=BOOT_BLOCK, seed=SEED):
    """관측일 단위 이동 블록 부트스트랩. (D, 하한 2.5%, 상한 97.5%, P(D* ≥ 0))."""
    by_day = {}
    for r in rows:
        by_day.setdefault(r["date"], []).append(r)
    dates = sorted(by_day)
    D, _, _ = stratified_D(rows)
    if D is None or len(dates) < block:
        return D, None, None, None
    rng = random.Random(seed)
    nb = -(-len(dates) // block)
    sims = []
    for _ in range(n):
        picked = []
        for _ in range(nb):
            s = rng.randint(0, len(dates) - block)
            picked.extend(dates[s:s + block])
        picked = picked[:len(dates)]
        sample = []
        for rep, d in enumerate(picked):
            for r in by_day[d]:
                sample.append(dict(r, stratum=(rep,) + tuple(r["stratum"][1:])))   # 같은 날을 두 번 뽑으면 다른 층으로
        v = stratified_D(sample)[0]
        if v is not None:
            sims.append(v)
    sims.sort()
    return D, sims[int(0.025 * len(sims))], sims[min(len(sims) - 1, int(0.975 * len(sims)))], \
        sum(1 for v in sims if v >= 0) / len(sims)


# ── 표본 ─────────────────────────────────────────────────────────────
def confirm_window(days):
    """확증 표본의 관측일 인덱스: 10/6 이후 첫 60 관측일."""
    idx = [i for i, d in enumerate(days) if d["date"] >= CONFIRM_FROM]
    return idx[:SAMPLE_DAYS]


def status(days=None):
    """🔒 수·상태만. 가격 결과 값(상승 여부)은 계산하지 않는다 — 성숙 여부(가격 존재)만 센다."""
    days = days if days is not None else load_obs()
    win = set(confirm_window(days))
    reg = [r for r in registry(days) if r["i"] in win]
    T_ = [r for r in reg if r["group"] == "T"]
    matured = sum(1 for r in T_ if r["i"] + HORIZON < len(days))
    return {"version": VERSION, "observationDays": len(win), "targetDays": SAMPLE_DAYS, "newLeaders": len(T_),
            "controls": sum(1 for r in reg if r["group"] == "C"), "matured": matured,
            "lastDay": days[max(win)]["date"] if win else None, "gate": gate(days)}


def leader_hold_concluded(snap_dir=SNAP_DIR):
    """잠긴 leader-hold-v1 의 주 평가가 열렸거나(유효 60) 종료(상한 초과)됐는가. 수만 본다."""
    try:
        hdays, _ = T.collect(snap_dir, with_returns=True)
        cap, _ = T.trading_day_n(T.CONFIRM_FROM, T.CAP_TRADING_DAYS, T._calendar(snap_dir))
        valid, _ = T.valid_hold_days(hdays, T.CONFIRM_FROM, cap)
        last = max((d["date"] for d in hdays), default="")
        return len(valid) >= T.EVAL_VALID_DAYS or bool(cap and last > cap)
    except Exception:
        return False


def gate(days, hold_done=None):
    win = confirm_window(days)
    full = len(win) == SAMPLE_DAYS and win[-1] + HORIZON < len(days)
    hold = leader_hold_concluded() if hold_done is None else hold_done
    return {"sampleMatured": full, "leaderHoldConcluded": hold, "open": bool(full and hold)}


# ── 평가 / 탐색 ────────────────────────────────────────────────────────
def compute(days, idx_set, max_outcome_date=None):
    reg = [r for r in registry(days) if r["i"] in idx_set]
    rows, missing = [], {"T": 0, "C": 0}
    for r in reg:
        if max_outcome_date and (r["i"] + HORIZON >= len(days) or days[r["i"] + HORIZON]["date"] >= max_outcome_date):
            continue
        y = outcome(days, r["i"], r["code"])
        if y is None:
            missing[r["group"]] += 1
            continue
        rows.append(dict(r, y=y))
    D, lo, hi, p = block_boot(rows)
    _, N, dropped = stratified_D(rows)
    res = {"D": D, "ci": (lo, hi), "p": p, "N": N, "dropped": dropped, "missing": missing,
           "T_rate": _rate([r for r in rows if r["group"] == "T"]), "C_rate": _rate([r for r in rows if r["group"] == "C"]),
           "days": len({r["date"] for r in rows}), "by_breadth": [], "by_rate": []}
    for name, lo_, hi_ in BREADTH_BINS:
        sub = [r for r in rows if r["breadth"] is not None and (lo_ is None or r["breadth"] >= lo_) and (hi_ is None or r["breadth"] < hi_)]
        d, n, _ = stratified_D(sub)
        res["by_breadth"].append((name, d, n, len({r["date"] for r in sub})))
    for name, _, _ in RATE_BINS:
        d, n, _ = stratified_D([r for r in rows if r["rate_bin"] == name])
        res["by_rate"].append((name, d, n))
    return res


def _rate(rows):
    return (sum(r["y"] for r in rows) / len(rows), len(rows)) if rows else (None, 0)


def explore_past(days=None):
    """탐색 구간(10/6 전 t0, 결과일도 10/6 전) — 이미 열람한 구간. 확증 판정이 아니다."""
    days = days if days is not None else load_obs()
    idx = {i for i, d in enumerate(days) if d["date"] < CONFIRM_FROM}
    return compute(days, idx, max_outcome_date=CONFIRM_FROM)


def evaluate(days=None, hold_done=None):
    days = days if days is not None else load_obs()
    g = gate(days, hold_done)
    if not g["open"]:
        raise PermissionError(f"🔒 평가 조건 미충족 — {g}. 수·상태만 볼 수 있다(status).")
    return compute(days, set(confirm_window(days)))


def fmt(res, title):
    def pct(x):
        return "—" if x is None else f"{x * 100:+.1f}%p"
    def rt(t):
        return "—" if t[0] is None else f"{t[0] * 100:.1f}% (n={t[1]})"
    pv = "—" if res["p"] is None else f"{res['p']:.4f}"
    L = [f"### {title}", "",
         f"- 처리군(새 대장) 상승 비율 {rt(res['T_rate'])} · 대조군 {rt(res['C_rate'])} · 관측일 {res['days']}",
         f"- **층 가중 차이 D = {pct(res['D'])}** · 95% 블록 부트스트랩 {pct(res['ci'][0])} ~ {pct(res['ci'][1])} · "
         f"단측 p(D ≥ 0) = {pv}",
         f"- 층에 넣은 처리군 {res['N']} · 대조 없는 층이라 빠진 처리군 {res['dropped']} · 결측 처리 {res['missing']['T']} / 대조 {res['missing']['C']}",
         "- (검정 안 함) 시장 확산별 D: " + " · ".join(f"{n} {pct(d)} (처리 {k}, {nd}일)" for n, d, k, nd in res["by_breadth"]),
         "- (검정 안 함) 등락률 구간별 D: " + " · ".join(f"{n} {pct(d)} (처리 {k})" for n, d, k in res["by_rate"])]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--explore", action="store_true", help="탐색 구간(10/6 전)만 계산")
    ap.add_argument("--evaluate", action="store_true", help="평가 조건 충족 시에만")
    a = ap.parse_args(argv)
    days = load_obs()
    if a.explore:
        print(fmt(explore_past(days), f"{VERSION} 정의로 본 탐색 구간 (10/6 전 · 확증 아님)"))
    elif a.evaluate:
        print(fmt(evaluate(days), f"{VERSION} 확증 평가"))
    else:
        s = status(days)
        print(f"🔒 {VERSION} 상태 — 관측일 {s['observationDays']}/{s['targetDays']} · 새 대장 {s['newLeaders']} · "
              f"대조 {s['controls']} · 결과 성숙 {s['matured']} · 평가 조건 {s['gate']} (성과 비공개)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
