# -*- coding: utf-8 -*-
# ==========================================================================
# 📋 HYEOKS §6-4 모의 집행 — 슬리피지·체결 가능성 실측 (주문은 내지 않는다)
# --------------------------------------------------------------------------
# 왜 만들었나
#   §4-4 의 가장 뼈아픈 교훈이 이것이었다 —
#     "상한가 안착 7건이 수익의 대부분(+6.07%)을 만들었는데, 그 종목들은 **살 수가 없다.**
#      신호일 종가가 상한가면 매수 잔량이 없다. 체결 가능성은 수익률 표에 안 보인다."
#   그래서 로드맵은 §6-4 모의 집행을 요구했다 —
#     "주문을 내지 않고 '냈다고 가정한' 기록만 남긴다. 15:20 시점 호가를 찍어두고
#      실제 종가와 비교해 슬리피지를 추정한다."
#   그동안 못 한 이유는 **호가 데이터가 없어서**였다(§6-9 '여전히 못 모으는 것' 표).
#   2026-08-28 에 시장 스냅샷이 askBuy·askSell·totalBuyVolume·totalSellVolume 를
#   담기 시작했다. 그 호가가 바로 §6-4 가 요구한 그 호가다. 이제 잴 수 있다.
#
# 무엇을 재나
#   1. **스프레드** = (매도호가 − 매수호가) / 현재가
#      → 즉시 체결의 비용 **하한**. 이것조차 못 넘으면 그 종목은 단타 대상이 아니다.
#   2. **잔량 대비 주문 크기** = 주문금액 / (매도잔량 × 매도호가)
#      → §6-8 이 말한 "1억 원을 세팅했을 때의 주수가 호가에 깔려 있는가".
#         1.0 을 넘으면 최우선 호가만으로는 못 채운다 = 호가를 올려 사야 한다.
#   3. **15:05 → 종가 이동** = (종가 − 15:05 현재가) / 15:05 현재가
#      → 스냅샷 시점에 '샀다고 가정'했을 때 종가 대비 얼마나 유불리했는가.
#         §6-2 가 리포트 채널의 오버나이트 갭이 0 임을 보였는데, 그 앞단인
#         '장 막판 이동'은 아직 안 쟀다. 그걸 여기서 잰다.
#
# 무엇을 하지 않나
#   · 주문을 내지 않는다. 계좌에 접근하지 않는다. 이름 그대로 **모의**다.
#   · 구글시트를 건드리지 않는다(픽 조인은 선택 기능이고 그것도 읽기 전용).
#   · 선정·판정에 영향을 주지 않는다. 관측이다.
#
# 사용법
#   python hyeoks_paper_fill.py                    → 최신 15:05 스냅샷으로 분석
#   python hyeoks_paper_fill.py --date 2026-08-29  → 특정 날짜
#   python hyeoks_paper_fill.py --order 100000000  → 주문 가정 금액(기본 1억)
#   python hyeoks_paper_fill.py --self-test        → 데이터 없이 계산식만 검증
# ==========================================================================
import os
import io
import sys
import csv
import gzip
import time
import argparse
import datetime
import statistics
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
SNAP_DIR = "data/market_snapshot"
OUT_DIR = "data/paper_fill"
DEFAULT_ORDER_KRW = 100_000_000     # §6-8 자막이 말한 '1억 원 세팅' 기준

# 이 열들이 없으면 분석 자체가 불가능하다(2026-08-29 이전 스냅샷에는 없다).
NEED = ("askBuy", "askSell", "totalBuyVolume", "totalSellVolume", "nowPrice", "tradeAmount")

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
})


def _f(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


# ── 계산식 (self-test 대상) ────────────────────────────────────────────────

def spread_pct(ask_buy, ask_sell, now_price):
    """호가 스프레드(%). 즉시 체결 비용의 하한이다."""
    if ask_buy <= 0 or ask_sell <= 0 or now_price <= 0 or ask_sell < ask_buy:
        return None
    return (ask_sell - ask_buy) / now_price * 100.0


def depth_ratio(order_krw, sell_volume, ask_sell):
    """주문금액 ÷ 최우선 매도호가에 깔린 금액.
    1.0 초과 = 최우선 호가만으로는 못 채운다 → 호가를 올려 사야 한다(추가 슬리피지)."""
    depth_krw = sell_volume * ask_sell
    if depth_krw <= 0:
        return None            # 매도 잔량 0 = 살 수 없다(상한가 등)
    return order_krw / depth_krw


def close_drift_pct(now_price, close_price):
    """스냅샷 시점(15:05) 대비 종가 이동(%). 양수면 15:05 에 산 쪽이 유리했다."""
    if now_price <= 0 or close_price <= 0:
        return None
    return (close_price - now_price) / now_price * 100.0


# ── 데이터 ────────────────────────────────────────────────────────────────

def read_snapshot(path):
    with gzip.open(path, "rt", encoding="utf-8") as fp:
        rows = list(csv.reader(fp))
    if len(rows) < 3:
        return None, []
    meta = {}
    for kv in rows[0][1:]:
        if "=" in kv:
            k, v = kv.split("=", 1)
            meta[k] = v
    hdr = rows[1]
    return meta, [dict(zip(hdr, r)) for r in rows[2:]]


def get_close(code, date_str):
    """그날 일봉 종가. 15:05 스냅샷과 비교해 '장 막판 이동'을 낸다."""
    try:
        r = SESSION.get(f"https://fchart.stock.naver.com/sise.nhn"
                        f"?symbol={code}&timeframe=day&count=10&requestType=0",
                        verify=False, timeout=8)
        want = date_str.replace("-", "")
        for item in ET.fromstring(r.text).findall(".//item"):
            d = (item.get("data") or "").split("|")
            if len(d) >= 5 and d[0] == want:
                return float(d[4])
    except Exception:
        pass
    return 0.0


def pick_snapshot(date):
    """분석할 15:05 스냅샷을 고른다. 날짜 미지정이면 가장 최근 것."""
    try:
        files = sorted(f for f in os.listdir(SNAP_DIR) if f.endswith("_1505.csv.gz"))
    except Exception:
        return None
    if not files:
        return None
    if date:
        want = f"{date}_1505.csv.gz"
        return f"{SNAP_DIR}/{want}" if want in files else None
    return f"{SNAP_DIR}/{files[-1]}"


# ── 분석 ──────────────────────────────────────────────────────────────────

def analyze(rows, date_str, order_krw, with_close, top_n):
    """거래대금 상위 top_n 종목에 대해 스프레드·체결가능성·종가이동을 잰다.

    ⚠️ 전 종목을 다 보지 않는 이유 — 종가 조회가 종목마다 1회씩 필요하고,
       애초에 우리가 살 만한 크기의 종목이 아니면 슬리피지를 재는 의미가 없다.
    """
    ranked = sorted(rows, key=lambda x: -_f(x.get("tradeAmount")))[:top_n]
    out = []
    for i, x in enumerate(ranked, 1):
        code = str(x.get("itemcode", "")).strip()
        now = _f(x.get("nowPrice"))
        ab, asl = _f(x.get("askBuy")), _f(x.get("askSell"))
        sv = _f(x.get("totalSellVolume"))
        rec = {
            "rank": i, "code": code, "name": str(x.get("itemname", "")).strip(),
            "tradeAmount": _f(x.get("tradeAmount")), "nowPrice": now,
            "spread": spread_pct(ab, asl, now),
            "depth": depth_ratio(order_krw, sv, asl),
            "sellVol": sv,
            "buyVol": _f(x.get("totalBuyVolume")),
            "drift": None,
        }
        if with_close and code:
            c = get_close(code, date_str)
            rec["drift"] = close_drift_pct(now, c)
            rec["close"] = c
            time.sleep(0.05)
        out.append(rec)
    return out


def summarize(recs, order_krw):
    sp = [r["spread"] for r in recs if r["spread"] is not None]
    dp = [r["depth"] for r in recs if r["depth"] is not None]
    dr = [r["drift"] for r in recs if r["drift"] is not None]
    nofill = [r for r in recs if r["depth"] is None]
    heavy = [r for r in recs if r["depth"] is not None and r["depth"] > 1.0]
    return {"n": len(recs), "spread": sp, "depth": dp, "drift": dr,
            "nofill": nofill, "heavy": heavy, "order": order_krw}


def build_report(recs, s, date_str, order_krw):
    def stat(v, unit="%"):
        if not v:
            return "—"
        return (f"중앙값 **{statistics.median(v):.3f}{unit}** · "
                f"평균 {statistics.mean(v):.3f}{unit} · "
                f"최대 {max(v):.3f}{unit}")

    L = [f"# 📋 §6-4 모의 집행 — {date_str}", "",
         f"_15:05 스냅샷 기준 · 거래대금 상위 {s['n']}종목 · 주문 가정 {order_krw / 1e8:.0f}억_", "",
         "> 로드맵 §6-4 가 요구한 산출물이다. **주문은 내지 않는다.**",
         "> 호가로 슬리피지의 하한과 체결 가능성을 재고, 15:05 대비 종가 이동을 본다.", "",
         "## 요약", "",
         "| 지표 | 값 | 읽는 법 |", "|---|---|---|",
         f"| 호가 스프레드 | {stat(s['spread'])} | 즉시 체결 비용의 **하한**. "
         f"§3-4-2 의 왕복 0.35% 에 **더해지는** 몫이다 |",
         f"| 잔량 대비 주문 | {stat(s['depth'], '배')} | 1.0 초과 = 최우선 호가로 못 채움 |",
         f"| 15:05→종가 이동 | {stat(s['drift'])} | 양수면 15:05 에 산 쪽이 유리했다 |", ""]

    if s["spread"]:
        med = statistics.median(s["spread"])
        L += [f"> **스프레드 중앙값 {med:.3f}%** 를 §3-4-2 의 0.35% 에 더하면 "
              f"왕복 실질 비용은 약 **{0.35 + med:.2f}%** 가 된다. "
              "단기 채널(T+5)은 연 50회전이므로 연 부담이 "
              f"약 **{(0.35 + med) * 50:.0f}%** 다.", ""]

    L += ["## 체결 가능성", "",
          f"- **살 수 없음**(매도 잔량 0) — **{len(s['nofill'])}종목**"
          + (f" : {', '.join(r['name'] for r in s['nofill'][:8])}" if s['nofill'] else ""),
          f"- **최우선 호가로 부족**(잔량 대비 1.0 초과) — **{len(s['heavy'])}종목**", ""]
    if s["nofill"]:
        L += ["> ⚠️ 매도 잔량이 0 인 종목은 **상한가일 가능성이 높다.** §4-4 에서 수익의 "
              "대부분을 만들었던 '상한가 안착' 7건이 정확히 이 상태였다. "
              "수익률 표에는 좋아 보이지만 **실제로는 살 수 없다.**", ""]

    L += ["## 종목별 (거래대금 상위 20)", "",
          "| # | 종목 | 거래대금 | 스프레드 | 잔량대비 | 15:05→종가 |",
          "|---:|---|---:|---:|---:|---:|"]
    for r in recs[:20]:
        sp = f"{r['spread']:.3f}%" if r["spread"] is not None else "—"
        dp = ("**살 수 없음**" if r["depth"] is None
              else (f"**{r['depth']:.2f}배**" if r["depth"] > 1.0 else f"{r['depth']:.2f}배"))
        dr = f"{r['drift']:+.2f}%" if r["drift"] is not None else "—"
        L.append(f"| {r['rank']} | {r['name']} | {r['tradeAmount'] / 1e8:,.0f}억 | {sp} | {dp} | {dr} |")

    L += ["", "## 읽는 법", "",
          "- 스프레드는 **비용의 하한**이다. 실제 체결은 이보다 나쁠 수 있고 좋을 수는 없다.",
          "- 잔량은 **15:05 그 순간**의 값이다. 동시호가(15:20~15:30)에는 판이 다시 짜인다.",
          "- '살 수 없음'이 곧 상한가는 아니다. 다만 그 방향의 강한 신호다.",
          f"- N={s['n']} 은 하루치다. §3-5 탐색적 항목이므로 **표본이 쌓이기 전에는 판정하지 않는다.**", ""]
    return "\n".join(L) + "\n"


def self_test():
    print("🧪 계산식 자기검증")
    ok = True

    def chk(label, got, want):
        nonlocal ok
        hit = (got is None and want is None) or (got is not None and want is not None
                                                and abs(got - want) < 1e-9)
        ok = ok and hit
        print(f"  {'✅' if hit else '❌'} {label}: {got}" + ("" if hit else f" (기대 {want})"))

    # 스프레드 — 1000/1001, 현재가 1000 → 0.1%
    chk("스프레드 정상", spread_pct(1000, 1001, 1000), 0.1)
    chk("스프레드 호가 역전(비정상)", spread_pct(1001, 1000, 1000), None)
    chk("스프레드 호가 없음", spread_pct(0, 1000, 1000), None)
    # 잔량 — 1억 주문, 매도잔량 100주 × 호가 1,000,000 = 1억 → 정확히 1.0
    chk("잔량 딱 맞음", depth_ratio(100_000_000, 100, 1_000_000), 1.0)
    chk("잔량 절반 → 2배 필요", depth_ratio(100_000_000, 50, 1_000_000), 2.0)
    chk("매도 잔량 0 → 살 수 없음", depth_ratio(100_000_000, 0, 1_000_000), None)
    # 종가 이동
    chk("종가 상승", close_drift_pct(1000, 1010), 1.0)
    chk("종가 하락", close_drift_pct(1000, 990), -1.0)
    chk("종가 미확보", close_drift_pct(1000, 0), None)

    print("\n✅ 전부 통과" if ok else "\n❌ 실패 있음")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="데이터 없이 계산식만 검증")
    ap.add_argument("--date", help="분석할 날짜 YYYY-MM-DD (기본: 최신 15:05 스냅샷)")
    ap.add_argument("--order", type=int, default=DEFAULT_ORDER_KRW, help="주문 가정 금액(원)")
    ap.add_argument("--top", type=int, default=100, help="거래대금 상위 N종목 (기본 100)")
    ap.add_argument("--no-close", action="store_true", help="종가 조회를 건너뛴다(빠름)")
    a = ap.parse_args()

    if a.self_test:
        return self_test()

    path = pick_snapshot(a.date)
    if not path:
        print(f"❌ 15:05 스냅샷을 찾지 못했습니다 ({a.date or '최신'}) — {SNAP_DIR} 확인")
        return 1
    date_str = os.path.basename(path)[:10]
    print(f"📋 [§6-4 모의 집행] {date_str} · 주문 가정 {a.order / 1e8:.0f}억 · 상위 {a.top}종목")

    meta, rows = read_snapshot(path)
    if not rows:
        print(f"❌ 스냅샷이 비었습니다: {path}")
        return 1

    missing = [c for c in NEED if c not in rows[0]]
    if missing:
        # 2026-08-29 이전 스냅샷에는 호가 4열이 없다. 조용히 0 을 내지 말고 분명히 멈춘다.
        print(f"❌ 이 스냅샷에는 필요한 열이 없습니다: {missing}")
        print("   호가 4종은 2026-08-28 커밋 이후 스냅샷부터 담긴다. 그 이후 날짜로 다시 시도할 것.")
        return 1

    print(f"📖 {len(rows)}종목 · capturedAt={meta.get('capturedAt', '?')}")
    recs = analyze(rows, date_str, a.order, not a.no_close, a.top)
    s = summarize(recs, a.order)

    os.makedirs(OUT_DIR, exist_ok=True)
    out = f"{OUT_DIR}/{date_str}_paper_fill.md"
    io.open(out, "w", encoding="utf-8", newline="\n").write(
        build_report(recs, s, date_str, a.order))
    print(f"📝 리포트 {out}")

    if s["spread"]:
        med = statistics.median(s["spread"])
        print(f"   스프레드 중앙값 {med:.3f}% → 왕복 실질비용 약 {0.35 + med:.2f}% "
              f"(단기 T+5 연 50회전이면 연 {(0.35 + med) * 50:.0f}%)")
    print(f"   살 수 없음 {len(s['nofill'])}종목 · 최우선 호가로 부족 {len(s['heavy'])}종목")
    if s["drift"]:
        print(f"   15:05→종가 이동 중앙값 {statistics.median(s['drift']):+.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
