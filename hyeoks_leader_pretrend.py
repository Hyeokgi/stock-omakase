# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 대장 방향 탐색 3회차 — t0 이전 추세 변수 (탐색 · 검증 아님)
# --------------------------------------------------------------------------
# 사전 고정: docs/사전등록_2026-10-05_대장경로방향_탐색v1.md §10 (3c7eba5, 계산 전 커밋).
# 일봉은 드라이브 비공개 파일의 스크래치 사본에서만 읽는다(저장소에 두지 않는다). 경로는 인자로 받는다.
# 🔒 방화벽: 상태일·결과일·봉 날짜 모두 2026-10-06 전. 봉 파일의 최대 날짜를 확인하고, 결과일을 다시 확인한다.
# 출력은 구간별 집계뿐 — 종목 코드·이름·종목별 결과를 찍지 않는다. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import gzip
import sys

import hyeoks_leader_direction as D

CUTOFF = D.CUTOFF
DISCOVERY_END = D.DISCOVERY_END
K = 3
MISMATCH = 0.01
VARS = {
    "pos60": [("≥−5%", -0.05, None), ("−5~−15%", -0.15, -0.05), ("−15~−30%", -0.30, -0.15), ("<−30%", None, -0.30)],
    "run5": [("<0", None, 0.0), ("0~10%", 0.0, 0.10), ("10~30%", 0.10, 0.30), ("≥30%", 0.30, None)],
    "run20": [("<0", None, 0.0), ("0~20%", 0.0, 0.20), ("20~50%", 0.20, 0.50), ("≥50%", 0.50, None)],
    "pos52w": [("≥−5%", -0.05, None), ("−5~−20%", -0.20, -0.05), ("−20~−40%", -0.40, -0.20), ("<−40%", None, -0.40)],
    "disp20": [("<0", None, 0.0), ("0~10%", 0.0, 0.10), ("10~25%", 0.10, 0.25), ("≥25%", 0.25, None)],
}
LABELS = {"pos60": "60봉 고점 대비 위치", "run5": "직전 5봉 상승폭", "run20": "직전 20봉 상승폭",
          "pos52w": "250봉(52주) 고점 대비", "disp20": "20봉 평균 이격"}


def load_bars(paths):
    bars = {}
    for p in paths:
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                bars.setdefault(r["code"], []).append((r["date"], float(r["high"]), float(r["close"])))
    for v in bars.values():
        v.sort()
    last = max(b[-1][0] for b in bars.values()) if bars else ""
    assert last < CUTOFF, f"방화벽 위반 — 봉 파일에 {last} 봉이 있다"
    return bars


def bin_of(var, x):
    if x is None:
        return None
    for name, lo, hi in VARS[var]:
        if (lo is None or x >= lo) and (hi is None or x < hi):
            return name
    return None


def features(series, date, P, rate):
    """t0 날짜보다 앞선 봉만. (변수 dict, '계열 불일치' 여부)."""
    b = [x for x in series if x[0] < date]
    if not b or not P:
        return {}, False
    prev_close = P / (1 + rate / 100.0) if rate is not None and rate > -100 else None
    if prev_close and abs(b[-1][2] / prev_close - 1) > MISMATCH:
        return {}, True
    c = [x[2] for x in b]
    h = [x[1] for x in b]
    f = {}
    if len(b) >= 60:
        f["pos60"] = P / max(h[-60:]) - 1
    if len(b) >= 6:
        f["run5"] = c[-1] / c[-6] - 1
    if len(b) >= 21:
        f["run20"] = c[-1] / c[-21] - 1
    if len(b) >= 250:
        f["pos52w"] = P / max(h[-250:]) - 1
    if len(b) >= 20:
        f["disp20"] = P / (sum(c[-20:]) / 20) - 1
    return f, False


def _label(days, j, code, P):
    """(x3, r3) — 시장 보정·보정 없음. 결과일 < CUTOFF 확인."""
    if j + K >= len(days):
        return None, None
    fut = days[j + K]
    assert fut["date"] < CUTOFF, "방화벽 위반 — 결과일"
    rf = fut["rows"].get(code)
    Pk = D._f(rf.get("nowPrice")) if rf else None
    if not (P and Pk):
        return None, None
    mkt = days[j]["rows"][code].get("sosok", "1")
    I0, Ik = days[j]["idx"].get(mkt), fut["idx"].get(mkt)
    r = Pk / P - 1
    return ((r - (Ik / I0 - 1)) if I0 and Ik else None), r


def build_rows(days, bars):
    """t0 대장 행과 같은 날 A 비대장 행. 변수·라벨만, 종목 정보는 집계 뒤 버린다."""
    t0_of = {(e["i0"], e["code"]) for e in D.episodes(days)}
    rows, mism, nobars = [], {"T": 0, "C": 0}, {"T": 0, "C": 0}
    for j, day in enumerate(days):
        if j + K >= len(days):
            continue
        for code, x in day["A"].items():
            g = "T" if (j, code) in t0_of else ("C" if code not in day["B"] else None)
            if g is None:
                continue                      # 이미 대장이었던 종목의 재등장 — 이번 표본 밖
            r = day["rows"][code]
            P, rate = D._f(r.get("nowPrice")), D._f(r.get("prevChangeRate"), 0.0)
            if code not in bars:
                nobars[g] += 1
                continue
            f, bad = features(bars[code], day["date"], P, rate)
            if bad:
                mism[g] += 1
                continue
            x3, r3 = _label(days, j, code, P)
            rows.append(dict(f, group=g, date=day["date"], x3=x3, r3=r3))
    return rows, mism, nobars


def analyze(rows):
    out = {}
    for var in VARS:
        for name, _, _ in VARS[var]:
            sel = [r for r in rows if bin_of(var, r.get(var)) == name]
            T_, C_ = [r for r in sel if r["group"] == "T"], [r for r in sel if r["group"] == "C"]
            cell = {"nT": len([r for r in T_ if r["x3"] is not None]), "nC": len([r for r in C_ if r["x3"] is not None])}
            cell["rateT"], _ = D.up_rate(T_, "x3")
            cell["rateC"], _ = D.up_rate(C_, "x3")
            bd = D.boot_diff(T_, C_, "x3")
            cell["diff"], cell["lo"], cell["hi"] = (bd[0], bd[1], bd[2]) if bd else (None, None, None)
            br = D.boot_diff(T_, C_, "r3")
            cell["diff_r3"] = br[0] if br else None
            for half, cond in (("disc", lambda d: d <= DISCOVERY_END), ("conf", lambda d: d > DISCOVERY_END)):
                th = [r for r in T_ if cond(r["date"])]
                ch = [r for r in C_ if cond(r["date"])]
                a, na = D.up_rate(th, "x3")
                b, nb = D.up_rate(ch, "x3")
                cell[f"{half}_diff"] = (a - b) if a is not None and b is not None else None
                cell[f"{half}_nT"] = na
            cell["candidate"] = (cell["disc_diff"] is not None and cell["conf_diff"] is not None
                                 and cell["disc_nT"] >= 30 and cell["conf_nT"] >= 30
                                 and (cell["disc_diff"] > 0) == (cell["conf_diff"] > 0))
            out[(var, name)] = cell
    return out


def _p(x):
    return "—" if x is None else f"{x * 100:+.1f}%p"


def _r(x):
    return "—" if x is None else f"{x * 100:.1f}%"


def report(rows, cells, mism, nobars, days):
    nT = sum(1 for r in rows if r["group"] == "T")
    nC = sum(1 for r in rows if r["group"] == "C")
    last_t0 = max((r["date"] for r in rows), default="")
    L = ["### 3회차 결과 — t0 이전 추세 (탐색 · 같은 구간 네 번째 열람 · 검증 아님)", "",
         f"t0 대장 {nT} · 같은 날 A 비대장 {nC} 행 · 상태일 {days[0]['date']}~{last_t0} · 결과일 ≤ {days[-1]['date']}",
         f"계열 불일치로 뺀 행: 대장 {mism['T']} · 비대장 {mism['C']} · 봉 없음: 대장 {nobars['T']} · 비대장 {nobars['C']}", "",
         "| 변수 | 구간 | 대장 n | 대장 x3>0 | 비대장 x3>0 | 차이 (95%) | 발견 / 확인 차이 | r3 차이 | 후보 |",
         "|---|---|--:|--:|--:|--:|--:|--:|:-:|"]
    for (var, name), c in cells.items():
        small = " (표본 부족)" if c["nT"] < 30 else ""
        ci = f" ({_p(c['lo'])}~{_p(c['hi'])})" if c["lo"] is not None else ""
        L.append(f"| {LABELS[var]} | {name} | {c['nT']}{small} | {_r(c['rateT'])} | {_r(c['rateC'])} | "
                 f"{_p(c['diff'])}{ci} | {_p(c['disc_diff'])} / {_p(c['conf_diff'])} | {_p(c['diff_r3'])} | "
                 f"{'✓' if c['candidate'] else ''} |")
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--bars", nargs="+", required=True, help="스크래치 사본 경로(저장소 밖)")
    a = ap.parse_args(argv)
    bars = load_bars(a.bars)
    days = D.load_days()
    rows, mism, nobars = build_rows(days, bars)
    print(report(rows, analyze(rows), mism, nobars, days))
    return 0


if __name__ == "__main__":
    sys.exit(main())
