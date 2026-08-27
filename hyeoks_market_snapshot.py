# -*- coding: utf-8 -*-
# ==========================================================================
# 📸 HYEOKS 장중 시장 스냅샷 수집기 (관측 전용 — 선정 로직에 일절 관여하지 않음)
# --------------------------------------------------------------------------
# 왜 만들었나
#   "고수의 종가베팅" 조건(당일 수급이 몰리고, 오후에도 그 수급이 꺾이지 않으며,
#    평소보다 거래량이 급증해 시장을 주도하는 종목)을 우리 데이터로 검증하려 했는데
#   막혔다. 이유는 방법이 아니라 데이터였다 —
#     ① 우리 백테스트_로그에는 '우리 스캐너가 고른 종목'만 있다. 그날 시장 전체에서
#        진짜 주도주가 무엇이었는지는 기록이 없다. 유니버스 자체를 검증할 수 없다.
#     ② '오후에 수급이 유지되는가'는 일봉으로 못 잰다. 종가/고가 비율로 근사해봤지만
#        오전에 다 오르고 횡보한 것과 오후에 계속 붙은 것을 구분하지 못한다.
#   → 그래서 하루 두 번(13:00 / 15:10) 시장 전체를 찍어 둔다. 두 스냅샷의 차이가
#     곧 '오후 수급'이다.
#
# 무엇을 하지 않는가 (중요)
#   · 구글시트를 건드리지 않는다. 읽지도 쓰지도 않는다 → 시트 락 불필요, 9/7 기준선 무영향.
#   · 선정·점수·채널에 어떤 영향도 주지 않는다. 순수 관측이다.
#
# 저장
#   data/market_snapshot/YYYY-MM-DD_{tag}.csv.gz  (거래대금 상위 TOP_N)
#   주도주는 언제나 거래대금 상위권에 있으므로 전 종목을 보관할 이유가 없다.
#
# 사용법
#   python hyeoks_market_snapshot.py --slot 1300
#   python hyeoks_market_snapshot.py --slot 1505
#   (GAS 시간 트리거가 깃허브 워크플로를 dispatch 하고, 워크플로가 이 스크립트를 슬롯과 함께 부른다.
#    깃허브 cron 은 지연이 커서 정밀 타이밍을 맡길 수 없다 — GAS 가 정시에 쏜다.)
# ==========================================================================
import os
import io
import sys
import csv
import gzip
import argparse
import datetime
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
OUT_DIR = "data/market_snapshot"
TOP_N = 300          # 거래대금 상위 N종목만 보관
MIN_ROWS = 1500      # 전체 스캔이 이보다 적으면 응답이 잘린 것으로 보고 저장하지 않음

# ⏰ [시간 창 가드] 슬롯별로 '이 시각 안에 찍힌 것만 유효'하다.
#    깃허브 cron 은 지연이 크다(2026-08-27 실측: 정적데이터 4시간 54분, 캘린더 4시간 45분 지연).
#    늦게 도착한 실행이 그대로 저장되면 13:00 스냅샷도 15:05 스냅샷도 아닌 '그냥 종가 스냅샷'이
#    표본에 섞여 분석이 오염된다. 창 밖이면 저장하지 않고 종료한다.
#    창 폭은 GAS 시간 기반 트리거의 지터(±15분)를 감안해 잡았다.
SLOTS = {
    "1300": ((12, 40), (13, 40)),   # 오전장 마감 시점
    "1505": ((14, 50), (15, 25)),   # 종가 동시호가(15:20) 직전
}

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://stock.naver.com/',
})

MARKET_URL = ("https://stock.naver.com/api/domestic/market/stock/default"
              "?tradeType=KRX&marketType=ALL&orderType=priceTop&startIdx=0&pageSize=3000")

# 보관 필드 — 나중에 무엇을 물어볼지 다 알 수 없으므로 판단 근거가 될 만한 원자료를 넓게 남긴다.
FIELDS = ["itemcode", "itemname", "sosok", "nowPrice", "openPrice", "highPrice", "lowPrice",
          "prevChangeRate", "tradeVolume", "tradeAmount", "marketSum", "listedStockCnt",
          "frgnHoldRate", "manageStatusGb", "tradeStopYn", "marketAlertType", "marketStatus"]


def is_trading_day(today_str):
    """지수 일봉에 오늘 날짜가 있는지로 개장 여부를 판정한다.
    (omakase 의 휴장일 가드와 같은 방식 — 임시공휴일에도 GAS/cron 은 그대로 돌기 때문)"""
    try:
        r = SESSION.get("https://fchart.stock.naver.com/sise.nhn"
                        "?symbol=KOSPI&timeframe=day&count=5&requestType=0",
                        verify=False, timeout=10)
        days = set()
        for it in ET.fromstring(r.text).findall(".//item"):
            raw = (it.get("data") or "").split("|")[0]
            if len(raw) == 8:
                days.add(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
        return today_str in days
    except Exception as e:
        print(f"⚠️ 개장 여부 판정 실패: {e}")
        return False


def index_snapshot():
    """지수도 같이 남긴다 — 그날이 어떤 장이었는지 없이는 종목 해석이 안 된다."""
    out = {}
    for name in ("KOSPI", "KOSDAQ"):
        try:
            j = SESSION.get(f"https://m.stock.naver.com/api/index/{name}/basic",
                            verify=False, timeout=8).json()
            out[name] = (str(j.get("closePrice", "")), str(j.get("fluctuationsRatio", "")))
        except Exception:
            out[name] = ("", "")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", required=True, choices=sorted(SLOTS) + ["manual"],
                    help="스냅샷 슬롯 (1300 | 1505 | manual)")
    ap.add_argument("--force", action="store_true",
                    help="휴장일·시간창 판정을 모두 무시하고 저장 (테스트용)")
    a = ap.parse_args()

    now = datetime.datetime.now(KST)
    today = now.strftime('%Y-%m-%d')
    print(f"📸 [시장 스냅샷] slot={a.slot}  {now.strftime('%Y-%m-%d %H:%M:%S')} KST")

    if not a.force and not is_trading_day(today):
        print(f"🚫 {today} 는 거래일이 아님 — 저장하지 않고 종료")
        return 0

    # ⏰ 시간 창 검사 — 늦게 도착한 실행은 조용히 버린다(빈손이 오염보다 낫다).
    if not a.force and a.slot in SLOTS:
        (sh, sm), (eh, em) = SLOTS[a.slot]
        cur = now.hour * 60 + now.minute
        if not (sh * 60 + sm <= cur <= eh * 60 + em):
            print(f"🚫 시간 창 밖 — slot {a.slot} 유효구간 "
                  f"{sh:02d}:{sm:02d}~{eh:02d}:{em:02d}, 현재 {now.strftime('%H:%M')}")
            print("   (늦게 도착한 실행은 저장하지 않는다. 오염된 표본보다 결측이 낫다)")
            return 0

    # 같은 슬롯을 이미 찍었으면 덮어쓰지 않는다 (트리거 중복 발사 대비)
    dup = f"{OUT_DIR}/{today}_{a.slot}.csv.gz"
    if not a.force and os.path.exists(dup):
        print(f"ℹ️ 이미 존재 — {dup} (중복 실행으로 보고 종료)")
        return 0

    try:
        r = SESSION.get(MARKET_URL, verify=False, timeout=30)
        rows = r.json() if r.status_code == 200 else []
    except Exception as e:
        print(f"❌ 시장 조회 실패: {e}")
        return 1

    if not isinstance(rows, list) or len(rows) < MIN_ROWS:
        # ⚠️ 이 API 계열은 잘린 응답에도 200 을 준다. 건수로 판정해야 한다.
        print(f"❌ 스캔 {len(rows) if isinstance(rows, list) else '?'}건 — 임계 {MIN_ROWS} 미달, 저장 중단")
        return 1

    def amt(x):
        try:
            return float(x.get("tradeAmount") or 0)
        except Exception:
            return 0.0

    top = sorted(rows, key=amt, reverse=True)[:TOP_N]

    os.makedirs(OUT_DIR, exist_ok=True)
    path = f"{OUT_DIR}/{today}_{a.slot}.csv.gz"
    idx = index_snapshot()
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp)
        # 1행: 메타 (실제 캡처 시각이 중요하다 — 깃허브 cron 은 10분 이상 늦게 도는 일이 흔하다)
        # capturedAt 이 진짜 기준이다 — 파일명의 슬롯은 '의도한 시각'일 뿐이다.
        w.writerow(["#meta", f"capturedAt={now.isoformat()}", f"slot={a.slot}",
                    f"total={len(rows)}", f"kept={len(top)}",
                    f"KOSPI={idx['KOSPI'][0]}({idx['KOSPI'][1]})",
                    f"KOSDAQ={idx['KOSDAQ'][0]}({idx['KOSDAQ'][1]})"])
        w.writerow(FIELDS)
        for x in top:
            w.writerow([str(x.get(k, "")) for k in FIELDS])

    size = os.path.getsize(path)
    lead = top[0] if top else {}
    print(f"✅ 저장 {path}  ({size:,}바이트)")
    print(f"   전체 {len(rows)}종목 중 거래대금 상위 {len(top)}종목 보관")
    print(f"   지수 KOSPI {idx['KOSPI'][0]}({idx['KOSPI'][1]}) / KOSDAQ {idx['KOSDAQ'][0]}({idx['KOSDAQ'][1]})")
    print(f"   거래대금 1위: {lead.get('itemname')} {amt(lead)/1e8:,.0f}억 "
          f"({lead.get('prevChangeRate')}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
