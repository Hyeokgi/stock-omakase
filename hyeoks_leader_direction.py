# -*- coding: utf-8 -*-
# ==========================================================================
# 🧭 대장 이후 방향 예측 — 탐색 v1 (`leader-direction-explore-v1`)
# --------------------------------------------------------------------------
# 사전 고정: docs/사전등록_2026-10-05_대장경로방향_탐색v1.md (커밋 d596f5b — 이 코드보다 먼저).
# 사용자 지시(2026-10-05): "방향예측을 위한 전략을 발견하기 위한 최대한의 노력을 해줘."
#
# 🔒 방화벽: 상태일·결과일 모두 2026-10-06(잠긴 leader-hold-v1 확증 시작) **전** 자료만 쓴다.
#    `load_days()` 가 그 뒤 날짜를 읽으면 예외를 낸다 — 미래 스냅샷이 쌓여도 이 탐색은 자동으로 섞지 않는다.
# 탐색이다. 숫자는 검증이 아니다. 운영 선정·주문과 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import gzip
import io
import math
import os
import random
import sys

import numpy as np

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, read_snapshot

VERSION = "leader-direction-explore-v1"
CUTOFF = T.CONFIRM_FROM            # 이 날짜 이상은 상태일·결과일 모두 금지
DISCOVERY_END = "2026-09-16"       # 발견 구간: 상태일 ≤ 이 날
KS = (1, 3, 5)
PRIMARY_K = 3
DD_REST = -0.03                    # 대장 쉼: t0 이후 고점 대비 ≤ −3%
FLAT_3 = 0.03                      # 횡보: 최근 3관측일 수익 절대값 < 3%
ALIVE_VAL = 0.8                    # 테마 살아 있음: 테마 거래대금 / t0 ≥ 0.8
ALIVE_BREADTH = 0.5                #               그리고 테마 상승 비율 ≥ 0.5
PATH_T = 0.03                      # 경로 분류 문턱
BOOT_N, SEED = 2000, 20261005
TOP_SCREEN = 5
MIN_CELL = 30


# ── 읽기 ─────────────────────────────────────────────────────────────
def _f(x, default=None):
    try:
        v = float(str(x).replace(",", ""))
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _index_level(s):
    """'6,983.70(0.18)' → (6983.70, 0.18)."""
    if not s:
        return None, None
    s = str(s)
    lvl = _f(s.split("(")[0])
    rate = _f(s.split("(")[1].rstrip(")")) if "(" in s else None
    return lvl, rate


def read_theme_file(path):
    """테마 파일 → {테마코드: {rate, rising, falling, unchanged, value}}."""
    if not os.path.exists(path):
        return {}
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    body = [ln for ln in lines if not ln.startswith("#meta")]
    out = {}
    for r in csv.DictReader(io.StringIO("\n".join(body))):
        out[r["code"]] = {"rate": _f(r.get("changeRate"), 0.0), "rising": _f(r.get("risingCount"), 0.0),
                          "falling": _f(r.get("fallingCount"), 0.0), "unchanged": _f(r.get("unchangedCount"), 0.0),
                          "value": _f(r.get("totalTradingValue"), 0.0)}
    return out


def load_days(snap_dir=SNAP_DIR, cutoff=CUTOFF):
    """15:05 스냅샷이 있는 관측일 목록(오름차순). 각 원소는 그날 자료 dict."""
    days = []
    for name in sorted(os.listdir(snap_dir)):
        if not name.endswith("_1505.csv.gz") or not name[:4].isdigit():
            continue
        d = name[:10]
        if d >= cutoff:
            continue                       # 🔒 확증 구간 이후는 읽지 않는다
        meta, rows, _ = read_snapshot(os.path.join(snap_dir, name))
        p13 = os.path.join(snap_dir, f"{d}_1300.csv.gz")
        rows13 = read_snapshot(p13)[1] if os.path.exists(p13) else {}
        A, _ = T.build_A(rows)
        B, _ = T.build_B_current(A)
        themes = read_theme_file(os.path.join(snap_dir, f"{d}_1505_theme.csv.gz"))
        ranked = sorted(themes, key=lambda c: (-themes[c]["rate"], c))
        rank = {c: i + 1 for i, c in enumerate(ranked)}
        kospi, kospi_rate = _index_level(meta.get("KOSPI"))
        kosdaq, kosdaq_rate = _index_level(meta.get("KOSDAQ"))
        rates = [_f(r.get("prevChangeRate")) for r in rows.values()]
        rates = [x for x in rates if x is not None]
        days.append({"date": d, "rows": rows, "rows13": rows13, "A": {x["code"]: x for x in A},
                     "B": {x["code"]: x for x in B}, "themes": themes, "theme_rank": rank,
                     "idx": {"0": kospi, "1": kosdaq}, "idx_rate": {"0": kospi_rate, "1": kosdaq_rate},
                     "mkt_breadth": (sum(1 for x in rates if x > 0) / len(rates)) if rates else None,
                     "mkt_amt": sum(_f(r.get("tradeAmount"), 0.0) for r in rows.values())})
    assert all(x["date"] < cutoff for x in days), "방화벽 위반"
    return days


# ── 에피소드와 상태일 ───────────────────────────────────────────────────
def episodes(days):
    """종목이 현행 대장 정의에 **처음** 나온 날 = t0. 테마는 t0 의 테마로 고정."""
    seen, out = set(), []
    for i, day in enumerate(days):
        for code, x in sorted(day["B"].items()):
            if code not in seen:
                seen.add(code)
                out.append({"code": code, "i0": i, "t0": day["date"], "theme": x["theme"]})
    return out


def _price(day, code):
    r = day["rows"].get(code)
    return _f(r.get("nowPrice")) if r else None


def path_class(rs):
    """t0 이후 r_1..r_5 (15:05 점) → 경로 분류. 하나라도 없으면 None."""
    if len(rs) < 5 or any(x is None for x in rs):
        return None
    r1, r2, r5 = rs[0], rs[1], rs[4]
    if r1 >= PATH_T and r5 >= PATH_T:
        return "즉시상승"
    if abs(r1) < PATH_T and abs(r2) < PATH_T and r5 >= PATH_T:
        return "횡보후상승"
    if min(rs[:4]) <= -PATH_T and r5 >= 0:
        return "조정후회복"
    if r5 <= -PATH_T and max(rs) < PATH_T:
        return "지속약화"
    return "기타"


def state_rows(days, eps):
    """에피소드 × 상태일(t0 와 그 이후) 표. 그날 15:05 까지의 정보만 쓴다."""
    out = []
    for ep in eps:
        code, i0, th = ep["code"], ep["i0"], ep["theme"]
        p0 = _price(days[i0], code)
        t0_amt = _f(days[i0]["rows"][code].get("tradeAmount"))
        t0_tval = days[i0]["themes"].get(th, {}).get("value")
        peak, lead_n = None, 0
        for j in range(i0, len(days)):
            day = days[j]
            r = day["rows"].get(code)
            if not r:
                continue                    # 그날 종목 행이 없으면 상태 결측 — 에피소드는 끝내지 않는다
            P = _f(r.get("nowPrice"))
            if not P or not p0:
                continue
            peak = P if peak is None else max(peak, P)
            lead_n += code in day["B"]
            mkt = r.get("sosok", "1")
            hi, lo, op = _f(r.get("highPrice")), _f(r.get("lowPrice")), _f(r.get("openPrice"))
            rate = _f(r.get("prevChangeRate"), 0.0)
            prevclose = P / (1 + rate / 100.0) if rate > -100 else None
            amt = _f(r.get("tradeAmount"), 0.0)
            r13 = day["rows13"].get(code)
            P13 = _f(r13.get("nowPrice")) if r13 else None
            a13 = _f(r13.get("tradeAmount")) if r13 else None
            tm = day["themes"].get(th, {})
            tval = tm.get("value")
            tcount = (tm.get("rising", 0) + tm.get("falling", 0) + tm.get("unchanged", 0)) if tm else 0
            prevP = _price(days[j - 1], code) if j >= 1 else None
            p3 = _price(days[j - 3], code) if j >= 3 else None
            prev_tval = days[j - 1]["themes"].get(th, {}).get("value") if j >= 1 else None
            top20 = sum(1 for q in range(max(0, j - 4), j + 1) if days[q]["theme_rank"].get(th, 999) <= 20)
            buy, sell = _f(r.get("totalBuyVolume")), _f(r.get("totalSellVolume"))
            row = {
                "code": code, "t0": ep["t0"], "date": day["date"], "j": j, "is_t0": j == i0, "theme": th,
                "f_rate": rate,
                "f_ret_since_t0": P / p0 - 1,
                "f_drawdown": P / peak - 1,
                "f_ret_prev1": (P / prevP - 1) if prevP else None,
                "f_ret_prev3": (P / p3 - 1) if p3 else None,
                "f_hold": (P / hi) if hi else None,
                "f_range_pos": ((P - lo) / (hi - lo)) if hi and lo is not None and hi > lo else None,
                "f_gap": (op / prevclose - 1) if op and prevclose else None,
                "f_pm_mom": (P / P13 - 1) if P13 else None,
                "f_late_amt": (1 - a13 / amt) if a13 is not None and amt else None,
                "f_log_amt": math.log10(amt) if amt > 0 else None,
                "f_amt_vs_t0": (amt / t0_amt) if t0_amt else None,
                "f_is_leader": float(code in day["B"]),
                "f_in_A": float(code in day["A"]),
                "f_age": float(j - i0),
                "f_lead_count": float(lead_n),
                "f_log_mcap": math.log10(_f(r.get("marketSum"), 0.0)) if _f(r.get("marketSum"), 0.0) > 0 else None,
                "f_frgn": _f(r.get("frgnHoldRate")),
                "f_buy_sell": (buy / sell) if buy and sell else None,
                "f_theme_rate": tm.get("rate") if tm else None,
                "f_theme_rank": float(day["theme_rank"].get(th)) if th in day["theme_rank"] else None,
                "f_theme_breadth": (tm["rising"] / tcount) if tcount else None,
                "f_theme_val_vs_t0": (tval / t0_tval) if tval and t0_tval else None,
                "f_theme_val_chg1": (tval / prev_tval) if tval and prev_tval else None,
                "f_theme_top20_5d": float(top20),
                "f_leader_share": (amt / tval) if tval else None,
                "f_idx_rate": day["idx_rate"].get(mkt),
                "f_mkt_breadth": day["mkt_breadth"],
                "f_mkt_amt_chg1": (day["mkt_amt"] / days[j - 1]["mkt_amt"]) if j >= 1 and days[j - 1]["mkt_amt"] else None,
            }
            for k in KS:
                if j + k < len(days):
                    Pk = _price(days[j + k], code)
                    I0, Ik = day["idx"].get(mkt), days[j + k]["idx"].get(mkt)
                    if Pk:
                        rk = Pk / P - 1
                        row[f"r{k}"] = rk
                        row[f"x{k}"] = (rk - (Ik / I0 - 1)) if I0 and Ik else None
                        row[f"r{k}_date"] = days[j + k]["date"]
            if j == i0:
                row["path"] = path_class([(_price(days[i0 + q], code) / P - 1) if i0 + q < len(days)
                                          and _price(days[i0 + q], code) else None for q in range(1, 6)])
            out.append(row)
    for r in out:
        for k in KS:
            assert r.get(f"r{k}_date", "") < CUTOFF, "방화벽 위반 — 결과일"
    return out


def a_nonleader_rows(days, k=PRIMARY_K):
    """기준선 (b): 같은 날 A 안 비대장 종목의 x_k."""
    out = []
    for j, day in enumerate(days):
        if j + k >= len(days):
            continue
        fut = days[j + k]
        for code, x in day["A"].items():
            if code in day["B"]:
                continue
            r, rf = day["rows"].get(code), fut["rows"].get(code)
            P, Pk = _f(r.get("nowPrice")) if r else None, _f(rf.get("nowPrice")) if rf else None
            mkt = (r or {}).get("sosok", "1")
            I0, Ik = day["idx"].get(mkt), fut["idx"].get(mkt)
            if P and Pk and I0 and Ik:
                out.append({"date": day["date"], f"x{k}": (Pk / P - 1) - (Ik / I0 - 1)})
    return out


# ── 통계 ─────────────────────────────────────────────────────────────
def up_rate(rows, key):
    v = [r[key] for r in rows if r.get(key) is not None]
    return (sum(1 for x in v if x > 0) / len(v), len(v)) if v else (None, 0)


def _by_date(rows, key):
    g = {}
    for r in rows:
        if r.get(key) is not None:
            g.setdefault(r["date"], []).append(1.0 if r[key] > 0 else 0.0)
    return g


def boot_diff(g1, g2, key, n=BOOT_N, seed=SEED):
    """두 집단 상승 비율 차이(1−2)의 날짜 부트스트랩. (차이, 하한, 상한, P(차이≤0))."""
    a, b = _by_date(g1, key), _by_date(g2, key)
    dates = sorted(set(a) | set(b))
    if not dates or not a or not b:
        return None
    def diff(ds):
        x = [v for d in ds for v in a.get(d, [])]
        y = [v for d in ds for v in b.get(d, [])]
        return (sum(x) / len(x) - sum(y) / len(y)) if x and y else None
    obs = diff(dates)
    rng = random.Random(seed)
    sims = []
    for _ in range(n):
        s = diff([rng.choice(dates) for _ in dates])
        if s is not None:
            sims.append(s)
    sims.sort()
    if not sims:
        return obs, None, None, None
    lo, hi = sims[int(0.025 * len(sims))], sims[min(len(sims) - 1, int(0.975 * len(sims)))]
    return obs, lo, hi, sum(1 for s in sims if s <= 0) / len(sims)


def holm(ps):
    """[(이름, p)] → {이름: 통과 여부} (α=0.05)."""
    order = sorted([x for x in ps if x[1] is not None], key=lambda x: x[1])
    m, out, stop = len(order), {}, False
    for i, (name, p) in enumerate(order):
        ok = (not stop) and p <= 0.05 / (m - i)
        stop = stop or not ok
        out[name] = ok
    return out


def _rank(v):
    order = np.argsort(v, kind="mergesort")
    r = np.empty(len(v))
    r[order] = np.arange(len(v))
    # 동률 평균 순위
    vs = np.asarray(v)[order]
    i = 0
    while i < len(vs):
        j = i
        while j + 1 < len(vs) and vs[j + 1] == vs[i]:
            j += 1
        r[order[i:j + 1]] = (i + j) / 2.0
        i = j + 1
    return r


def spearman(x, y):
    if len(x) < 5:
        return None
    rx, ry = _rank(np.asarray(x, float)), _rank(np.asarray(y, float))
    if rx.std() == 0 or ry.std() == 0:
        return None
    return float(np.corrcoef(rx, ry)[0, 1])


def auc(score, label):
    s, l = np.asarray(score, float), np.asarray(label, float)
    pos, neg = s[l == 1], s[l == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    r = _rank(np.concatenate([pos, neg])) + 1
    return float((r[:len(pos)].sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def logistic_fit(X, y, l2=1.0, iters=500, lr=0.1):
    X1 = np.hstack([np.ones((len(X), 1)), X])
    w = np.zeros(X1.shape[1])
    for _ in range(iters):
        p = 1 / (1 + np.exp(-X1 @ w))
        g = X1.T @ (p - y) / len(y) + l2 * np.r_[0, w[1:]] / len(y)
        w -= lr * g
    return w


def feature_names(rows):
    return sorted(k for k in rows[0] if k.startswith("f_")) if rows else []


# ── 분석 ─────────────────────────────────────────────────────────────
def alive(r):
    v, b = r.get("f_theme_val_vs_t0"), r.get("f_theme_breadth")
    return v is not None and b is not None and v >= ALIVE_VAL and b >= ALIVE_BREADTH


HYPOTHESES = {
    "H1 대장 쉼: 테마 살아 있음 vs 약화": (
        lambda r: r.get("f_drawdown") is not None and r["f_drawdown"] <= DD_REST,
        alive, "greater"),
    "H2 횡보: 테마 살아 있음 vs 약화": (
        lambda r: r.get("f_ret_prev3") is not None and abs(r["f_ret_prev3"]) < FLAT_3,
        alive, "greater"),
    "H3 상승 지속: 테마 동반 vs 비동반": (
        lambda r: r.get("f_ret_prev1") is not None and r["f_ret_prev1"] > 0 and r.get("f_theme_rate") is not None,
        lambda r: r["f_theme_rate"] > 0, "greater"),
    "H4 테마 살아 있음: 오늘도 대장 vs 배지 잃음": (
        alive, lambda r: r.get("f_is_leader") == 1.0, "two-sided"),
}


def run_hypothesis(rows, cond, split, side, key):
    sub = [r for r in rows if not r["is_t0"] and cond(r) and r.get(key) is not None]
    g1 = [r for r in sub if split(r)]
    g2 = [r for r in sub if not split(r)]
    (p1, n1), (p2, n2) = up_rate(g1, key), up_rate(g2, key)
    b = boot_diff(g1, g2, key)
    p = None
    if b and b[3] is not None:
        p = b[3] if side == "greater" else min(1.0, 2 * min(b[3], 1 - b[3]))
    return {"g1": (p1, n1), "g2": (p2, n2), "diff": b[0] if b else None, "ci": (b[1], b[2]) if b else (None, None),
            "p": p, "small": min(n1, n2) < MIN_CELL, "_g1": g1, "_g2": g2}


def split_period(rows):
    disc = [r for r in rows if r["date"] <= DISCOVERY_END]
    hold = [r for r in rows if r["date"] > DISCOVERY_END]
    return disc, hold


def screen_t0(rows, key):
    """H5 — t0 변수 선별(발견) → 확인 구간 상위 3분위."""
    t0 = [r for r in rows if r["is_t0"] and r.get(key) is not None]
    disc, hold = split_period(t0)
    res = []
    for f in feature_names(t0):
        xs = [(r[f], 1.0 if r[key] > 0 else 0.0) for r in disc if r.get(f) is not None]
        rho = spearman([a for a, _ in xs], [b for _, b in xs]) if xs else None
        res.append((f, rho, len(xs)))
    res = [x for x in res if x[1] is not None]
    res.sort(key=lambda x: -abs(x[1]))
    picked = res[:TOP_SCREEN]
    out = []
    for f, rho, n in picked:
        vals = sorted(r[f] for r in disc if r.get(f) is not None)
        if len(vals) < 3:
            continue
        cut = vals[int(len(vals) * 2 / 3)] if rho > 0 else vals[int(len(vals) / 3)]
        top = (lambda r, f=f, cut=cut: r.get(f) is not None and r[f] >= cut) if rho > 0 else \
              (lambda r, f=f, cut=cut: r.get(f) is not None and r[f] <= cut)
        h_top = [r for r in hold if top(r)]
        h_rest = [r for r in hold if not top(r)]
        d_top = [r for r in disc if top(r)]
        b = boot_diff(h_top, h_rest, key)
        out.append({"feature": f, "rho_disc": rho, "n_disc": n, "dir": "높을수록" if rho > 0 else "낮을수록",
                    "disc_top": up_rate(d_top, key), "disc_all": up_rate(disc, key),
                    "hold_top": up_rate(h_top, key), "hold_all": up_rate(hold, key),
                    "diff": b[0] if b else None, "ci": (b[1], b[2]) if b else (None, None),
                    "p": b[3] if b else None})
    dropped = res[TOP_SCREEN:]
    return out, dropped, (len(disc), len(hold))


def model_auc(rows, key, only_t0):
    data = [r for r in rows if (r["is_t0"] if only_t0 else True) and r.get(key) is not None]
    feats = feature_names(data)
    disc, hold = split_period(data)
    def mat(rs, mu=None, sd=None):
        X = np.array([[r.get(f) if r.get(f) is not None else np.nan for f in feats] for r in rs], float)
        if mu is None:
            mu = np.nanmean(X, axis=0)
            sd = np.nanstd(X, axis=0)
            sd[sd == 0] = 1.0
        X = np.where(np.isnan(X), mu, X)
        return (X - mu) / sd, mu, sd
    if len(disc) < 20 or len(hold) < 20:
        return None
    Xd, mu, sd = mat(disc)
    yd = np.array([1.0 if r[key] > 0 else 0.0 for r in disc])
    w = logistic_fit(Xd, yd)
    Xh, _, _ = mat(hold, mu, sd)
    yh = np.array([1.0 if r[key] > 0 else 0.0 for r in hold])
    score = np.hstack([np.ones((len(Xh), 1)), Xh]) @ w
    best_f, best_rho = None, 0
    for i, f in enumerate(feats):
        rho = spearman(Xd[:, i], yd)
        if rho is not None and abs(rho) > abs(best_rho):
            best_f, best_rho = f, rho
    single = auc(Xh[:, feats.index(best_f)] * (1 if best_rho > 0 else -1), yh) if best_f else None
    top_w = sorted(zip(feats, w[1:]), key=lambda x: -abs(x[1]))[:5]
    return {"n_disc": len(disc), "n_hold": len(hold), "auc_model": auc(score, yh), "auc_in_sample": auc(Xd @ w[1:], yd),
            "best_single": best_f, "best_rho_disc": best_rho, "auc_single": single, "top_weights": top_w}


def analyze(days):
    eps = episodes(days)
    rows = state_rows(days, eps)
    key = f"x{PRIMARY_K}"
    rep = {"days": (days[0]["date"], days[-1]["date"], len(days)), "episodes": len(eps), "state_rows": len(rows)}
    t0 = [r for r in rows if r["is_t0"]]
    rep["base"] = {k: up_rate(t0, k) for k in ("r1", "r3", "r5", "x1", "x3", "x5")}
    rep["base_track"] = {k: up_rate([r for r in rows if not r["is_t0"]], k) for k in ("r1", "r3", "x1", "x3")}
    nl = a_nonleader_rows(days)
    rep["a_nonleader"] = up_rate(nl, key)
    rep["leader_vs_A"] = boot_diff(t0, nl, key)
    paths = {}
    for r in t0:
        if r.get("path"):
            paths[r["path"]] = paths.get(r["path"], 0) + 1
    rep["paths"] = paths
    rep["mean_x"] = {k: (float(np.mean([r[k] for r in t0 if r.get(k) is not None])) if any(r.get(k) is not None for r in t0) else None)
                     for k in ("x1", "x3", "x5")}
    disc, hold = split_period(rows)
    rep["hyp"] = {}
    for name, (cond, split, side) in HYPOTHESES.items():
        rep["hyp"][name] = {"disc": run_hypothesis(disc, cond, split, side, key),
                            "hold": run_hypothesis(hold, cond, split, side, key)}
    rep["hyp_holm"] = holm([(n, v["hold"]["p"]) for n, v in rep["hyp"].items()])
    # 조합 — 확인 구간에서 방향이 맞은 가설끼리
    ok = [n for n, v in rep["hyp"].items() if v["hold"]["diff"] is not None and
          (v["hold"]["diff"] > 0 or HYPOTHESES[n][2] == "two-sided")]
    combos = []
    for i in range(len(ok)):
        for jx in range(i + 1, len(ok)):
            a, b = ok[i], ok[jx]
            ca, sa, _ = HYPOTHESES[a]
            cb, sb, _ = HYPOTHESES[b]
            fav_a = [r for r in hold if not r["is_t0"] and ca(r) and sa(r)]
            both = [r for r in fav_a if cb(r) and sb(r)]
            only = [r for r in fav_a if not (cb(r) and sb(r))]
            bb = boot_diff(both, only, key)
            combos.append({"pair": (a, b), "both": up_rate(both, key), "only_a": up_rate(only, key),
                           "diff": bb[0] if bb else None, "ci": (bb[1], bb[2]) if bb else (None, None)})
    rep["combos"] = combos
    rep["screen"], rep["screen_dropped"], rep["screen_n"] = screen_t0(rows, key)
    rep["screen_holm"] = holm([(s["feature"], s["p"]) for s in rep["screen"]])
    rep["model_t0"] = model_auc(rows, key, True)
    rep["model_all"] = model_auc(rows, key, False)
    return rep


# ── 보고 ─────────────────────────────────────────────────────────────
def _pct(x):
    return "—" if x is None else f"{x * 100:.1f}%"


def _rate(t):
    p, n = t
    return "—" if p is None else f"{p * 100:.1f}% (n={n})"


def report_md(rep):
    L = [f"## 결과 — `{VERSION}` (탐색 · 검증 아님)", "",
         f"- 자료: 관측일 {rep['days'][2]}일 ({rep['days'][0]} ~ {rep['days'][1]}) · 에피소드 {rep['episodes']} · 상태일 행 {rep['state_rows']}",
         f"- 주 라벨: 시장 보정 {PRIMARY_K}관측일 방향 `x_{PRIMARY_K} > 0`. 발견 ≤ {DISCOVERY_END} / 확인 그 뒤(결과일 10/2 이내)", "",
         "### 1. 기본 빈도 (예측 정확도 아님)", "",
         "| 라벨 | 에피소드 시작(t0) | 추적일(t0 이후) |", "|---|---|---|"]
    for k in ("r1", "r3", "r5", "x1", "x3", "x5"):
        L.append(f"| `{k} > 0` | {_rate(rep['base'][k])} | {_rate(rep['base_track'].get(k, (None, 0)))} |")
    lv = rep["leader_vs_A"]
    L += ["", f"- 같은 날 A 안 비대장 종목 `x_3 > 0`: {_rate(rep['a_nonleader'])} · 대장 t0 − 비대장 = "
          f"{_pct(lv[0]) if lv else '—'} (95% 구간 {_pct(lv[1]) if lv else '—'} ~ {_pct(lv[2]) if lv else '—'})",
          "- t0 평균 시장 보정 수익: " + " · ".join(f"x_{k[1:]} {_pct(v)}" for k, v in rep["mean_x"].items()),
          "- t0 경로 분류(5관측일, 15:05 점 기준): " + (" · ".join(f"{k} {v}" for k, v in sorted(rep["paths"].items(), key=lambda x: -x[1])) or "—"),
          "", "### 2. 상태 가설 H1~H4 (문턱 사전 고정)", "",
          "| 가설 | 구간 | 집단1 | 집단2 | 차이 | 95% 구간 | p | Holm(확인) |", "|---|---|---|---|--:|---|--:|---|"]
    for n, v in rep["hyp"].items():
        for per in ("disc", "hold"):
            x = v[per]
            flag = (" ⚠️표본부족" if x["small"] else "")
            hol = ("통과" if rep["hyp_holm"].get(n) else "미통과") if per == "hold" else ""
            pv = "—" if x["p"] is None else f"{x['p']:.3f}"
            L.append(f"| {n} | {'발견' if per == 'disc' else '확인'} | {_rate(x['g1'])} | {_rate(x['g2'])} | {_pct(x['diff'])}{flag} | "
                     f"{_pct(x['ci'][0])} ~ {_pct(x['ci'][1])} | {pv} | {hol} |")
    L += ["", "### 3. 조합 (확인 구간, 방향이 맞은 가설끼리 — 둘 다 유리 vs 앞 조건만)", ""]
    if rep["combos"]:
        L += ["| 조합 | 둘 다 | 앞 조건만 | 차이 | 95% 구간 |", "|---|---|---|--:|---|"]
        for c in rep["combos"]:
            L.append(f"| {c['pair'][0][:2]} × {c['pair'][1][:2]} | {_rate(c['both'])} | {_rate(c['only_a'])} | {_pct(c['diff'])} | "
                     f"{_pct(c['ci'][0])} ~ {_pct(c['ci'][1])} |")
    else:
        L.append("- 확인 구간에서 방향이 맞은 가설이 둘 이상 없어 조합을 만들지 않았다")
    sd, sh = rep["screen_n"]
    L += ["", f"### 4. H5 — 에피소드 시작 변수 선별 (발견 t0 {sd}건 → 확인 t0 {sh}건)", "",
          "| 변수 | 발견 순위상관 | 방향 | 발견 상위3분위 | 확인 상위3분위 | 확인 전체 | 차이 | 95% 구간 | Holm |",
          "|---|--:|---|---|---|---|--:|---|---|"]
    for s in rep["screen"]:
        L.append(f"| `{s['feature']}` | {s['rho_disc']:+.3f} | {s['dir']} | {_rate(s['disc_top'])} | {_rate(s['hold_top'])} | "
                 f"{_rate(s['hold_all'])} | {_pct(s['diff'])} | {_pct(s['ci'][0])} ~ {_pct(s['ci'][1])} | "
                 f"{'통과' if rep['screen_holm'].get(s['feature']) else '미통과'} |")
    L.append("- 선별 탈락(발견 순위상관): " + ", ".join(f"`{f}` {r:+.2f}" for f, r, _ in rep["screen_dropped"]))
    L += ["", "### 5. 보조 모형 (로지스틱, 발견 적합 → 확인 AUC)", ""]
    for name, m in (("t0 만", rep["model_t0"]), ("t0 + 추적일", rep["model_all"])):
        if not m:
            L.append(f"- {name}: 표본 부족")
            continue
        single = "—" if m["auc_single"] is None else f"{m['auc_single']:.3f}"
        L.append(f"- {name}: 발견 {m['n_disc']} · 확인 {m['n_hold']} · **확인 AUC {m['auc_model']:.3f}** "
                 f"(발견 자체 {m['auc_in_sample']:.3f}) · 단일 최강 `{m['best_single']}` 확인 AUC {single} · "
                 "큰 가중치 " + ", ".join(f"`{f}` {w:+.2f}" for f, w in m["top_weights"]))
    L += ["", "> 모든 수치는 탐색이다. 확인 구간도 같은 24관측일 안의 뒤쪽 날들일 뿐 독립 재현이 아니다. "
          "상태일 표본은 같은 에피소드가 여러 날 나오고 결과 기간이 겹쳐 독립이 아니다 — 구간은 참고값이다."]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="")
    a = ap.parse_args(argv)
    rep = analyze(load_days())
    md = report_md(rep)
    print(md)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write(md + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
