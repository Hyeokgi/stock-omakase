# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 호가 필드 감사 — 매수 가능성 조사의 첫 범위 (읽기 전용)
# --------------------------------------------------------------------------
# 묻는 것: 15:05 스냅샷의 `askBuy`·`askSell`·`totalBuyVolume`·`totalSellVolume` 은
#   1. 언제부터 어떤 형태로 들어 있나(존재율·빈칸·0·음수),
#   2. 방향과 단위가 맞아 보이나(교차 호가, 상한가의 매도잔량 0, 하한가의 매수잔량 0),
#   3. 현재가와 같은 순간의 값으로 볼 수 있나(현재가가 호가대 밖인 비율),
#   4. 상한가 근접 집단은 일반 집단과 호가 상태가 어떻게 다른가.
#
# 하지 않는 것
#   · 수익률을 계산하지 않는다. 익일 시가·종가를 읽지 않는다 → 확증 구간 결과를 열람하지 않는다.
#   · 실제 15:05 가격이 없으면 가격 차이를 만들어 추정하지 않는다. 현재가는 스냅샷 값 그대로다.
#   · 총잔량(`totalBuyVolume`·`totalSellVolume`)을 최우선 호가의 매수 가능 수량이나 체결률로 해석하지 않는다.
#   · 29.5% 이상을 무조건 매수 불가로 단정하지 않는다 — 매도 호가가 남은 종목 수를 센다.
#   · 아무 파일도 쓰지 않는다. 시트·네트워크를 건드리지 않는다.
#
# 호가 방향은 제공처 문서가 아니라 **데이터로 확인한 정황**이다: 상한가 근처에서 매도 쪽이 0 이고
#   하한가 근처에서 매수 쪽이 0 이면 askSell/totalSellVolume 이 매도 쪽, askBuy/totalBuyVolume 이 매수 쪽과 일치한다.
# ==========================================================================
import argparse
import glob
import os
import statistics
import sys
from collections import Counter

from hyeoks_closing_bet import SNAP_DIR, MIN_TURNOVER, read_snapshot

QUOTE_COLS = ("askBuy", "askSell", "totalBuyVolume", "totalSellVolume")
LIMIT_NEAR = 29.5            # 등락률(%) — 사전등록 `leader-hold-v1` 의 상한가 근접 대용과 같은 값. 판단 기준이 아니라 집단 구분용


def num(v):
    """숫자면 float, 빈칸·비수치면 None."""
    try:
        s = str(v).replace(",", "").strip()
        return float(s) if s not in ("", "None") else None
    except (TypeError, ValueError):
        return None


def quote_state(row):
    """한 종목의 호가 상태 플래그. 가격을 만들어 내지 않는다."""
    ab, asl = num(row.get("askBuy")), num(row.get("askSell"))
    tb, ts = num(row.get("totalBuyVolume")), num(row.get("totalSellVolume"))
    px = num(row.get("nowPrice"))
    out = {"flags": [], "spread_pct": None, "where": None}
    for name, v in (("askBuy", ab), ("askSell", asl), ("totalBuyVolume", tb), ("totalSellVolume", ts)):
        if v is None:
            out["flags"].append(f"{name}_빈칸")
        elif v == 0:
            out["flags"].append(f"{name}_0")
        elif v < 0:
            out["flags"].append(f"{name}_음수")
    if ab and asl and ab > 0 and asl > 0:
        if ab > asl:
            out["flags"].append("교차")
        elif ab == asl:
            out["flags"].append("동가")
        elif px and px > 0:
            out["spread_pct"] = (asl - ab) / px * 100.0
            out["where"] = "위" if px > asl else ("아래" if px < ab else "안")
    return out


def audit_rows(rows, min_turnover=MIN_TURNOVER, select=lambda r: True):
    """한 스냅샷의 행들 → (Counter, 스프레드 목록, 호가대 밖 거리%(위), 거리%(아래))."""
    cnt, spreads, above, below = Counter(), [], [], []
    for r in rows.values():
        if (num(r.get("tradeAmount")) or 0) < min_turnover or not select(r):
            continue
        st = quote_state(r)
        cnt["n"] += 1
        for f in st["flags"]:
            cnt[f] += 1
        if st["where"]:
            cnt["유효호가"] += 1
            cnt["현재가_" + st["where"]] += 1
            px, ab, asl = num(r["nowPrice"]), num(r["askBuy"]), num(r["askSell"])
            if st["where"] == "위":
                above.append((px - asl) / px * 100.0)
            elif st["where"] == "아래":
                below.append((ab - px) / px * 100.0)
        if st["spread_pct"] is not None:
            spreads.append(st["spread_pct"])
    return cnt, spreads, above, below


def side_check(rows, near):
    """상한가 근처에서 매도 쪽이 0, 하한가 근처에서 매수 쪽이 0 인지(방향 정황). 전 종목 대상."""
    up = dn = up_sell0 = dn_buy0 = up_buy0 = dn_sell0 = 0
    for r in rows.values():
        rate = num(r.get("prevChangeRate")) or 0.0
        if rate >= near:
            up += 1
            up_sell0 += num(r.get("totalSellVolume")) == 0
            up_buy0 += num(r.get("totalBuyVolume")) == 0
        elif rate <= -near:
            dn += 1
            dn_buy0 += num(r.get("totalBuyVolume")) == 0
            dn_sell0 += num(r.get("totalSellVolume")) == 0
    return {"상한가근처": up, "상한가_매도잔량0": up_sell0, "상한가_매수잔량0": up_buy0,
            "하한가근처": dn, "하한가_매수잔량0": dn_buy0, "하한가_매도잔량0": dn_sell0}


def has_quote_columns(rows):
    first = next(iter(rows.values()), None)
    return bool(first) and all(c in first for c in QUOTE_COLS)


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def collect(snap_dir=SNAP_DIR, slot="1505"):
    """(날짜, 행) 목록과 호가 열이 없는 날짜."""
    ok, missing = [], []
    for p in sorted(glob.glob(os.path.join(snap_dir, f"2026-*_{slot}.csv.gz"))):
        day = os.path.basename(p)[:10]
        _, rows, _ = read_snapshot(p)
        if rows and has_quote_columns(rows):
            ok.append((day, rows))
        else:
            missing.append(day)
    return ok, missing


def _line(name, cnt, spreads, above, below):
    n = cnt["n"]
    if not n:
        return f"| {name} | 0 | | | | | | |"
    flags = ", ".join(f"{k} {v}" for k, v in sorted(cnt.items()) if k not in ("n", "유효호가") and not k.startswith("현재가_"))
    v = cnt["유효호가"]
    out_ = cnt["현재가_위"] + cnt["현재가_아래"]
    sp = f"{statistics.median(spreads):.3f} / {pctl(spreads, 0.9):.3f} / {max(spreads):.3f}" if spreads else "—"
    return (f"| {name} | {n} | {flags or '없음'} | {v} | "
            f"{out_} ({out_ / v * 100:.0f}%) | {sp} |" if v else f"| {name} | {n} | {flags or '없음'} | 0 | — | — |")


def report(snap_dir=SNAP_DIR):
    L = ["# 🔎 호가 필드 감사 (읽기 전용 · 수익률 없음)", ""]
    for slot in ("1505", "1300"):
        ok, missing = collect(snap_dir, slot)
        L += [f"## {slot} 슬롯", "",
              f"- 호가 4열이 있는 날 **{len(ok)}일** ({ok[0][0] if ok else '—'} ~ {ok[-1][0] if ok else '—'}) · "
              f"열이 없는 날 {len(missing)}일 {missing if missing else ''}", ""]
        pops = (("전체 종목", 0.0, lambda r: True),
                (f"거래대금 ≥ {MIN_TURNOVER // 100_000_000}억", MIN_TURNOVER, lambda r: True),
                (f"≥{MIN_TURNOVER // 100_000_000}억 · 등락률 ≥ {LIMIT_NEAR}%", MIN_TURNOVER, lambda r: (num(r.get("prevChangeRate")) or 0) >= LIMIT_NEAR),
                (f"≥{MIN_TURNOVER // 100_000_000}억 · 20% ≤ 등락률 < {LIMIT_NEAR}%", MIN_TURNOVER,
                 lambda r: 20 <= (num(r.get("prevChangeRate")) or 0) < LIMIT_NEAR),
                (f"≥{MIN_TURNOVER // 100_000_000}억 · 등락률 < 20%", MIN_TURNOVER, lambda r: (num(r.get("prevChangeRate")) or 0) < 20))
        L += ["| 집단 | 종목-일 | 0·빈칸·음수·교차 | 유효 호가 | 현재가가 호가대 밖 | 스프레드% 중앙/p90/최대 |",
              "|---|--:|---|--:|--:|---|"]
        for name, mt, sel in pops:
            tot = Counter(); sp = []; ab = []; be = []
            for _, rows in ok:
                c, s, a, b = audit_rows(rows, mt, sel)
                tot.update(c); sp += s; ab += a; be += b
            L.append(_line(name, tot, sp, ab, be))
        L.append("")
        if slot == "1505":
            agg = Counter()
            for _, rows in ok:
                agg.update(side_check(rows, LIMIT_NEAR))
            L += ["### 방향 정황 (전 종목, 1505)", "",
                  f"- 등락률 ≥ {LIMIT_NEAR}%: {agg['상한가근처']}종목-일 중 매도잔량 0 **{agg['상한가_매도잔량0']}** · 매수잔량 0 {agg['상한가_매수잔량0']}",
                  f"- 등락률 ≤ −{LIMIT_NEAR}%: {agg['하한가근처']}종목-일 중 매수잔량 0 **{agg['하한가_매수잔량0']}** · 매도잔량 0 {agg['하한가_매도잔량0']}",
                  "- 상한가 쪽에서 매도 쪽이, 하한가 쪽에서 매수 쪽이 0 이면 `askSell`/`totalSellVolume` 이 매도, "
                  "`askBuy`/`totalBuyVolume` 이 매수와 일치한다. **제공처 문서 확인이 아니라 정황이다.**", ""]
            above, below = [], []
            for _, rows in ok:
                _, _, a, b = audit_rows(rows, MIN_TURNOVER)
                above += a; below += b
            if above or below:
                L += ["### 현재가가 호가대 밖일 때의 거리 (거래대금 ≥ 기준, 1505)", "",
                      f"- 매도호가 위: {len(above)}건 · 중앙 {statistics.median(above):.3f}% · p90 {pctl(above, 0.9):.3f}%" if above else "- 매도호가 위: 0건",
                      f"- 매수호가 아래: {len(below)}건 · 중앙 {statistics.median(below):.3f}% · p90 {pctl(below, 0.9):.3f}%" if below else "- 매수호가 아래: 0건",
                      "- 위·아래가 비슷하면 방향이 뒤집힌 것이 아니라 **현재가와 호가가 같은 순간의 값이 아닐 가능성**을 뜻한다 (이 감사로는 원인을 확정하지 못한다).", ""]
    L += ["> 이 감사는 수익률을 계산하지 않고 익일 가격을 읽지 않는다. 실제 15:05 가격이 없으므로 가격 차이를 추정하지 않았다.",
          "> 총잔량은 최우선 호가의 수량이 아니다. 스프레드는 `(askSell − askBuy) ÷ nowPrice` 이며 **현재가와 호가가 같은 순간일 때만** 비용 하한으로 읽을 수 있다."]
    return "\n".join(L)


def self_test():
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    r = lambda **kw: {"askBuy": "99", "askSell": "101", "totalBuyVolume": "10", "totalSellVolume": "20",
                      "nowPrice": "100", "tradeAmount": str(MIN_TURNOVER), "prevChangeRate": "1.0", **kw}
    s = quote_state(r())
    chk("정상 호가는 스프레드를 계산한다", abs(s["spread_pct"] - 2.0) < 1e-9 and s["where"] == "안" and not s["flags"])
    chk("현재가가 매도호가 위면 '위'", quote_state(r(nowPrice="105"))["where"] == "위")
    chk("현재가가 매수호가 아래면 '아래'", quote_state(r(nowPrice="95"))["where"] == "아래")
    chk("경계(호가와 같음)는 안", quote_state(r(nowPrice="101"))["where"] == "안")
    chk("교차 호가를 표시하고 스프레드를 만들지 않는다",
        quote_state(r(askBuy="102"))["flags"] == ["교차"] and quote_state(r(askBuy="102"))["spread_pct"] is None)
    chk("동가를 표시한다", "동가" in quote_state(r(askBuy="101"))["flags"])
    chk("0·빈칸·음수를 가른다",
        quote_state(r(askSell="0"))["flags"] == ["askSell_0"]
        and quote_state(r(totalBuyVolume=""))["flags"] == ["totalBuyVolume_빈칸"]
        and quote_state(r(totalSellVolume="-1"))["flags"] == ["totalSellVolume_음수"])
    chk("매도 호가가 0 이면 스프레드를 만들지 않는다 (가격을 지어내지 않음)", quote_state(r(askSell="0"))["spread_pct"] is None)
    chk("현재가가 없으면 스프레드를 만들지 않는다", quote_state(r(nowPrice=""))["spread_pct"] is None)
    rows = {"a": r(), "b": r(nowPrice="120"), "c": r(tradeAmount="1"), "d": r(prevChangeRate="29.8", askSell="0", totalSellVolume="0")}
    cnt, sp, ab, be = audit_rows(rows)
    chk("거래대금 미달은 센다에서 뺀다", cnt["n"] == 3, str(dict(cnt)))
    chk("호가대 밖과 거리", cnt["현재가_위"] == 1 and len(ab) == 1 and abs(ab[0] - (120 - 101) / 120 * 100) < 1e-9)
    chk("선택 함수", audit_rows(rows, select=lambda x: num(x["prevChangeRate"]) >= 29.5)[0]["n"] == 1)
    sc = side_check({"u": r(prevChangeRate="29.9", totalSellVolume="0"), "d": r(prevChangeRate="-29.9", totalBuyVolume="0"),
                     "m": r(prevChangeRate="1")}, 29.5)
    chk("방향 정황 집계", sc["상한가근처"] == 1 and sc["상한가_매도잔량0"] == 1 and sc["하한가_매수잔량0"] == 1, str(sc))
    chk("열 존재 판정", has_quote_columns({"x": r()}) and not has_quote_columns({"x": {"nowPrice": "1"}}) and not has_quote_columns({}))
    with open(__file__, encoding="utf-8") as fp:
        body = fp.read().split("def self_test")[0]
    for banned in ("exit_open", "next_trading_day", "openPrice", "closePrice", "COST"):
        chk(f"수익률·익일 가격 경로가 코드에 없다: {banned}", banned not in body)
    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="합성 자료로 감사 로직만 검증 (unittest 가 호출한다)")
    ap.add_argument("--snap-dir", default=SNAP_DIR)
    a = ap.parse_args()
    if a.check:
        return self_test()
    print(report(a.snap_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
