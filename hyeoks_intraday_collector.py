# -*- coding: utf-8 -*-
# ==========================================================================
# ⏱️ HYEOKS 장중 분봉 프로파일 수집기 (관측 전용)
# --------------------------------------------------------------------------
# 왜 만들었나
#   서로 정반대인 두 주장을 판정하기 위해서다. 둘 다 "종가가 고가 근처에서 끝난다"는
#   같은 현상을 말하는데, 오후 거래량 예측이 정면으로 충돌한다 —
#
#     ① '오후 수급' (더트레이딩TV 유아트, §6-8 자막)
#        "오전에도 한번 들어오고 오후에도 한번 들어오는 게 제일 좋다."
#        → 오후에 거래량이 **늘어야** 익일 상승 확률이 오른다.
#
#     ② '거자름 = 에너지 응축' (제미나이 정리본, §6-11)
#        "오후장 분봉 거래량이 오전 피크 대비 10~20% 수준으로 마르면서도
#         주가는 고가 부근에서 안 밀리는 상태."
#        → 오후에 거래량이 **말라야** 세력이 물량을 쥐고 있다는 뜻이다.
#
#   우리 시장 스냅샷(13:00 / 15:05)은 두 시점의 누적 거래대금 차이만 안다.
#   '오전 피크 대비 오후가 얼마나 말랐는가'는 분봉이 있어야 잰다.
#
# ⚠️ 지금 안 모으면 못 만든다
#   네이버 분봉은 **최근 6거래일치만** 조회된다(실측: count=2000 을 줘도 6일 = 2,283봉).
#   그래서 매일 받아 쌓아야 한다. 다만 6일 유예가 있어 하루 이틀 실패해도 --date 로 메울 수 있다.
#
# ✅ 지연에 강하다 — 시장 스냅샷과 다른 점
#   분봉은 장이 끝난 뒤엔 확정된 과거 데이터다. 15:40 에 받든 23:00 에 받든 값이 같다.
#   그래서 이 워크플로는 GAS 트리거가 필요 없고 깃허브 cron 으로 충분하다.
#   (시장 스냅샷은 '그 시각의 값'이 핵심이라 GAS 로 정시에 쏴야 했다 — §6-7)
#
# 분봉 데이터 형식 (fchart, 실측)
#   "202608271518|null|null|null|211000|771473"
#    └ 시각        └ OHL 은 전부 null  └ 종가  └ **누적** 거래량 (분당 아님)
#   · 분당 거래량 = 누적의 차분
#   · 09:00~15:19 까지 1분봉, 그 뒤 **15:30 봉 하나**가 더 온다.
#     15:20~15:29(동시호가) 구간은 봉이 없으므로
#     **15:30 누적 − 15:19 누적 = 종가 동시호가 거래량** 이다. 공짜로 얻는 신호다.
#
# 무엇을 하지 않는가
#   · 구글시트를 건드리지 않는다. 선정·점수·채널에 어떤 영향도 없다.
#   · 판정하지 않는다. 지표를 계산해 저장할 뿐, 어느 가설이 맞는지는 9/17 에 본다.
#
# 사용법
#   python hyeoks_intraday_collector.py                 # 오늘
#   python hyeoks_intraday_collector.py --date 2026-08-25   # 소급 (6거래일 이내만)
#   python hyeoks_intraday_collector.py --top 500
# ==========================================================================
import os
import io
import sys
import csv
import gzip
import time
import glob
import argparse
import datetime
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
OUT_DIR = "data/intraday_profile"
SNAP_DIR = "data/market_snapshot"
TOP_N = 300          # 분봉은 종목마다 1회씩 불러야 해서 전 종목(2,877)은 불가능하다.
                     # 6일 유예가 있으니 나중에 상위 300 밖 종목이 필요해지면 그때 메우면 된다.

RANK_SRC = "?"       # 대상 종목 순위를 어디서 얻었는지 (메타에 기록 — 소급분은 대용일 수 있다)

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://stock.naver.com/',
})

MARKET_URL = ("https://stock.naver.com/api/domestic/market/stock/default"
              "?tradeType=KRX&marketType=ALL&orderType=priceTop&startIdx=0&pageSize=3000")
MINUTE_URL = ("https://fchart.stock.naver.com/sise.nhn"
              "?symbol={code}&timeframe=minute&count=2000&requestType=0")

# 30분 버킷 13개. 마지막 1500 은 15:00~15:19 이고, 동시호가(15:30)는 따로 뺀다.
BUCKETS = ["0900", "0930", "1000", "1030", "1100", "1130", "1200",
           "1230", "1300", "1330", "1400", "1430", "1500"]

FIELDS = (["code", "name", "dayVolume", "openPrice", "closePrice", "highClose",
           "peak1mAM", "avg1mPM", "closingAuctionVol", "bars"]
          + [f"v{b}" for b in BUCKETS]
          + [f"c{b}" for b in BUCKETS])


def _i(v):
    try:
        return int(float(str(v).replace(",", "").strip() or 0))
    except Exception:
        return 0


def trading_days(n=8):
    """지수 일봉에서 최근 거래일 목록을 얻는다(휴장일 판정 + --date 검증용)."""
    try:
        r = SESSION.get(f"https://fchart.stock.naver.com/sise.nhn"
                        f"?symbol=KOSPI&timeframe=day&count={n}&requestType=0",
                        verify=False, timeout=10)
        out = []
        for it in ET.fromstring(r.text).findall(".//item"):
            raw = (it.get("data") or "").split("|")[0]
            if len(raw) == 8:
                out.append(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
        return sorted(out)
    except Exception as e:
        print(f"⚠️ 거래일 조회 실패: {e}")
        return []


def pick_targets(date_str, top_n):
    """그날 거래대금 상위 종목을 고른다.
    오늘이면 실시간 시장 API, 소급이면 그날 저장해 둔 시장 스냅샷을 쓴다."""
    global RANK_SRC
    today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
    if date_str == today:
        RANK_SRC = "live"
        try:
            rows = SESSION.get(MARKET_URL, verify=False, timeout=30).json()
        except Exception as e:
            print(f"❌ 시장 조회 실패: {e}")
            return []
        if not isinstance(rows, list) or len(rows) < 1500:
            print(f"❌ 스캔 {len(rows) if isinstance(rows, list) else '?'}건 — 응답이 잘렸다")
            return []
        rows.sort(key=lambda x: -_i(x.get("tradeAmount")))
        return [(str(x.get("itemcode", "")).strip(), str(x.get("itemname", "")).strip())
                for x in rows[:top_n] if str(x.get("itemcode", "")).strip().isdigit()]

    # 소급 — 그날 시장 스냅샷에서 순위를 복원한다 (15:05 슬롯 우선)
    for slot in ("1505", "1300"):
        p = f"{SNAP_DIR}/{date_str}_{slot}.csv.gz"
        if not os.path.exists(p):
            continue
        try:
            with gzip.open(p, "rt", encoding="utf-8") as fp:
                rows = list(csv.reader(fp))
            hdr = rows[1]
            recs = [dict(zip(hdr, r)) for r in rows[2:]]
            recs.sort(key=lambda x: -_i(x.get("tradeAmount")))
            RANK_SRC = f"snapshot_{slot}"
            print(f"   ({date_str} 스냅샷 {slot} 슬롯에서 순위 복원)")
            return [(x["itemcode"].strip(), x["itemname"].strip())
                    for x in recs[:top_n] if x["itemcode"].strip().isdigit()]
        except Exception as e:
            print(f"⚠️ 스냅샷 읽기 실패 {p}: {e}")

    # 스냅샷이 없는 날(수집 개시 8/28 이전)은 오늘 순위를 대용으로 쓴다.
    # 분봉이 6거래일 남아 있어 개시 이전 며칠을 소급해 건질 수 있는데, 그러려면
    # 그날 순위를 알 수 없으니 어쩔 수 없다. 상위 300 구성은 며칠 사이 크게 안 바뀐다.
    # ⚠️ 그날 급등해 상위로 올라온 종목이 오늘 순위에는 없을 수 있다 = 누락 가능.
    RANK_SRC = "proxy_today"
    print(f"⚠️ {date_str} 시장 스냅샷이 없다 — **오늘 거래대금 순위를 대용**으로 쓴다")
    print("   (그날 순위가 아니므로 일부 종목이 빠질 수 있다. 분석 시 이 파일의 meta 를 확인할 것)")
    try:
        rows = SESSION.get(MARKET_URL, verify=False, timeout=30).json()
    except Exception as e:
        print(f"❌ 시장 조회도 실패: {e}")
        return []
    if not isinstance(rows, list) or len(rows) < 1500:
        # ⚠️ 장 시작 전에는 이 API 가 **빈 배열**을 준다(실측 2026-08-28 08:05 → 0건).
        #    순위를 만들 수 없으니 소급 수집은 장중이나 장 마감 이후에 돌려야 한다.
        print(f"❌ 시장 응답 {len(rows) if isinstance(rows, list) else '?'}건 — "
              "장 시작 전이면 빈 배열이 온다. 09:00 이후에 다시 실행할 것")
        return []
    rows.sort(key=lambda x: -_i(x.get("tradeAmount")))
    return [(str(x.get("itemcode", "")).strip(), str(x.get("itemname", "")).strip())
            for x in rows[:top_n] if str(x.get("itemcode", "")).strip().isdigit()]


def profile(code, date_str):
    """한 종목의 그날 분봉을 30분 버킷 프로파일로 압축한다. 실패하면 None."""
    ymd = date_str.replace("-", "")
    try:
        r = SESSION.get(MINUTE_URL.format(code=code), verify=False, timeout=12)
        items = ET.fromstring(r.text).findall(".//item")
    except Exception:
        return None

    # (HHMM, 종가, 누적거래량) — 누적이므로 차분해야 분당 거래량이 된다
    seq = []
    for it in items:
        d = (it.get("data") or "").split("|")
        if len(d) < 6 or not d[0].startswith(ymd):
            continue
        try:
            seq.append((d[0][8:12], float(d[4]), float(d[5])))
        except ValueError:
            continue
    if len(seq) < 60:            # 반나절도 안 되는 데이터면 버린다
        return None
    seq.sort(key=lambda x: x[0])

    # 분당 거래량 = 누적 차분. 첫 봉은 그 자체가 시초 누적이다.
    per, prev = [], 0.0
    for hhmm, close, cum in seq:
        per.append((hhmm, close, max(0.0, cum - prev)))
        prev = cum

    reg = [x for x in per if x[0] < "1520"]          # 정규장 (동시호가 이전)
    auction = sum(v for h, c, v in per if h >= "1520")  # 15:30 봉 = 종가 동시호가

    def plus30(hhmm):
        m = int(hhmm[:2]) * 60 + int(hhmm[2:]) + 30
        return f"{m // 60:02d}{m % 60:02d}"

    vol, cls = {}, {}
    for b in BUCKETS:
        hi = "1520" if b == "1500" else plus30(b)   # 마지막 버킷은 동시호가 직전까지
        seg = [x for x in reg if b <= x[0] < hi]
        vol[b] = int(sum(v for _, _, v in seg))
        cls[b] = int(seg[-1][1]) if seg else 0

    am = [v for h, c, v in reg if "0900" <= h < "1000"]      # 오전 1시간
    pm = [v for h, c, v in reg if "1430" <= h < "1520"]      # 장 막판 50분
    return {
        "code": code, "dayVolume": int(prev), "bars": len(reg),
        "openPrice": int(reg[0][1]), "closePrice": int(per[-1][1]),
        "highClose": int(max(c for _, c, _ in reg)),
        "peak1mAM": int(max(am)) if am else 0,
        "avg1mPM": int(sum(pm) / len(pm)) if pm else 0,
        "closingAuctionVol": int(auction),
        **{f"v{b}": vol[b] for b in BUCKETS},
        **{f"c{b}": cls[b] for b in BUCKETS},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD (기본: 오늘). 분봉은 6거래일치만 남으므로 그 이전은 불가")
    ap.add_argument("--top", type=int, default=TOP_N, help=f"거래대금 상위 N종목 (기본 {TOP_N})")
    ap.add_argument("--force", action="store_true", help="기존 파일이 있어도 다시 만든다")
    a = ap.parse_args()

    now = datetime.datetime.now(KST)
    date_str = a.date or now.strftime('%Y-%m-%d')
    print(f"⏱️ [분봉 프로파일] {date_str}  (실행 {now.strftime('%Y-%m-%d %H:%M:%S')} KST)")

    days = trading_days(8)
    if days and date_str not in days:
        print(f"🚫 {date_str} 는 거래일이 아니거나 6거래일 범위 밖 — 종료")
        print(f"   조회 가능 구간: {days[0]} ~ {days[-1]}")
        return 0

    path = f"{OUT_DIR}/{date_str}.csv.gz"
    if os.path.exists(path) and not a.force:
        print(f"ℹ️ 이미 존재 — {path} (종료)")
        return 0

    # 장중에 돌면 그날 프로파일이 미완성이 된다. 15:30 이후에만 오늘치를 만든다.
    if date_str == now.strftime('%Y-%m-%d') and now.hour * 60 + now.minute < 15 * 60 + 35:
        print(f"🚫 아직 장중({now.strftime('%H:%M')}) — 15:35 이후에 실행할 것")
        print("   (미완성 프로파일이 저장되면 오전/오후 비교가 통째로 왜곡된다)")
        return 0

    targets = pick_targets(date_str, a.top)
    if not targets:
        return 1
    print(f"   대상 {len(targets)}종목 — 분봉 수집 시작")

    out, fails, t0 = [], 0, time.time()
    for i, (code, name) in enumerate(targets, 1):
        p = profile(code, date_str)
        if p:
            p["name"] = name
            out.append(p)
        else:
            fails += 1
        if i % 100 == 0:
            print(f"   {i}/{len(targets)}  ({time.time() - t0:.0f}초, 실패 {fails})")
        time.sleep(0.05)

    if not out:
        print("❌ 수집된 프로파일이 없다 — 저장하지 않는다")
        return 1

    os.makedirs(OUT_DIR, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(["#meta", f"date={date_str}", f"collectedAt={now.isoformat()}",
                    f"targets={len(targets)}", f"saved={len(out)}", f"failed={fails}",
                    f"rankSource={RANK_SRC}",
                    "volumeUnit=주", "note=v*/c* 는 30분 버킷, 1500 은 15:00~15:19"])
        w.writerow(FIELDS)
        for p in out:
            w.writerow([p.get(k, "") for k in FIELDS])

    print(f"✅ 저장 {path}  ({os.path.getsize(path):,}바이트, {time.time() - t0:.0f}초)")
    print(f"   {len(out)}종목 저장 · 실패 {fails}건")

    # 두 가설이 실제로 갈리는지 한 줄로 보여준다 (판정이 아니라 눈으로 보는 용도)
    ok = [p for p in out if p["peak1mAM"] > 0]
    if ok:
        comp = sorted(p["avg1mPM"] / p["peak1mAM"] for p in ok)
        med = comp[len(comp) // 2]
        dry = sum(1 for c in comp if c <= 0.20)
        print(f"   오후/오전 분봉 비율 중앙값 {med:.2f} · "
              f"거자름(≤0.20) {dry}종목 / {len(ok)} ({dry / len(ok) * 100:.0f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
