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
#   data/market_snapshot/YYYY-MM-DD_{tag}.csv.gz        (전 종목)
#   data/market_snapshot/YYYY-MM-DD_{tag}_theme.csv.gz  (네이버 테마 전량)
#
#   ⚠️ 처음에는 거래대금 상위 300종목만 남겼다가 전 종목으로 바꿨다(2026-08-27).
#      거래대금으로 자르면 삼성전자·SK하이닉스가 언제나 위에 있어, 정작 찾으려는
#      '평소보다 급증해 시장을 주도하기 시작한 종목'이 잘려 나간다. 대장 판정 기준을
#      아직 정하지 못했으므로 후보를 미리 좁히지 않는다.
#
# 테마를 왜 같이 찍나
#   "자금이 어디로 쏠렸나"는 종목 단위로만 보면 안 보인다. 고수가 말하는 대장주는
#   '테마를 이루고 동반 상승하는 무리의 선두'이지 혼자 뛰는 종목이 아니다. 테마 집계에는
#   totalAccAmount(테마 전체 거래대금)와 leadingItem(네이버가 지정한 대장주)이 들어 있어,
#   종목 쏠림과 테마 쏠림을 같은 시각에 나란히 볼 수 있다.
#   ⚠️ 테마끼리 종목이 겹친다(한 종목이 여러 테마에 속함). 테마 거래대금을 전부 더하면
#      중복 합산이 되므로 총량이 아니라 '테마 간 순위·비중'으로만 해석할 것.
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
import time
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
OUT_DIR = "data/market_snapshot"
TOP_N = 0            # 0 = 전 종목 보관. 대장주 후보를 사전에 좁히지 않기 위함이다 —
                     # 거래대금 상위만 남기면 삼성전자·SK하이닉스가 늘 1등이라 그 아래에서
                     # 무슨 일이 있었는지가 통째로 사라진다. 전체 약 2,880종목 / 회당 약 120KB.
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

# 🏷️ 테마 — rankings v2. 구 `market/theme/list` 보다 이쪽이 낫다.
#    구 API 는 leadingItem 하나(등락률 상위 2종목)만 주는데, v2 는 대장 후보를
#    **네 기준으로 따로** 준다 — topByChangeRate / topByTradingValue /
#    topByMarketCap / topByTradingVolume. 어떤 정의가 맞는지 아직 모르므로
#    네 가지를 다 받아 두고 판단은 나중에 한다.
#    cursor 페이지네이션으로 전량(약 266개)을 받는다. 구 API 는 200개에서 잘렸다.
THEME_V2_URL = ("https://stock.naver.com/api/stockSecurity/rankings/v2/domestic/themes"
                "?sortType=changeRate&size=100&period=daily")
# 구성종목(코드→테마) 매핑은 v2 에 없어 구 엔드포인트를 계속 쓴다. 테마 번호 체계는 동일하다.
THEME_STOCK_URL = ("https://stock.naver.com/api/domestic/market/theme/{no}/stocklist"
                   "?marketType=ALL&orderType=priceTop&startIdx=0&pageSize=100")

# 보관 필드 — 나중에 무엇을 물어볼지 다 알 수 없으므로 판단 근거가 될 만한 원자료를 넓게 남긴다.
FIELDS = ["itemcode", "itemname", "sosok", "nowPrice", "openPrice", "highPrice", "lowPrice",
          "prevChangeRate", "tradeVolume", "tradeAmount", "marketSum", "listedStockCnt",
          "frgnHoldRate", "manageStatusGb", "tradeStopYn", "marketAlertType", "marketStatus"]

# 테마 집계 필드 (v2). 단위는 전부 **원** 이다 — 종목 tradeAmount 와 같다.
#   topBy* 4종은 각각 상위 3종목이며 "코드:종목명:값|..." 으로 눌러 담는다.
THEME_FIELDS = ["ranking", "code", "name", "changeRate",
                "risingCount", "fallingCount", "unchangedCount",
                "totalMarketCap", "totalTradingVolume", "totalTradingValue",
                "topByChangeRate", "topByTradingValue", "topByMarketCap", "topByTradingVolume",
                "updatedAt"]
THEME_TOPS = ["topByChangeRate", "topByTradingValue", "topByMarketCap", "topByTradingVolume"]

# 종목 행에 덧붙이는 테마 열 — 판정에 쓰는 값이 아니라 조인 키다.
#   themeNos   — 이 종목이 속한 테마 번호 전부(파이프 구분). 테마 파일의 code 와 조인한다.
#   topThemeNo — 그중 테마 거래대금이 가장 큰 테마. '주 소속'으로 볼 만한 것.
#
# 🔻 대장 판정은 수집 단계에서 하지 않는다. 왜인지가 중요하다 —
#    · 거래대금만 보면 **어느 테마든 1등이 SK하이닉스·삼성전자**다. 2026-08-27 실측에서
#      거래대금 상위 8개 테마의 topByTradingValue 가 전부 같은 두 종목이었다. 판별력 0.
#    · 등락률만 보면 **죽은 테마에서 우연히 오른 대형주**가 대장이 된다. 같은 날
#      '공기청정기'(테마 -0.53%)와 '제습기'(-0.60%)의 등락률 1위가 둘 다 삼성전자(+1.72%)였다.
#    · 신호는 둘의 교집합에 있어 보인다 — 테마 자체가 살아있고(등락률·거래대금 급증)
#      그 안에서 자금을 끄는 종목. 전선 테마(+8.72%)는 등락률 1위도 거래대금 1위도
#      가온전선이었다.
#    어느 정의가 맞는지는 표본이 쌓인 뒤 정한다. 그래서 지금은 네 기준을 다 저장만 한다.
THEME_COLS = ["themeNos", "topThemeNo"]


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


def _flat_top(v):
    """topBy* 는 [{code,name,value,itemLogoUrl}, ...] 다. 로고 URL 은 버리고 눌러 담는다."""
    if not isinstance(v, list):
        return ""
    return "|".join(f"{x.get('code','')}:{x.get('name','')}:{x.get('value','')}"
                    for x in v if isinstance(x, dict))


def fetch_theme_data():
    """테마 전량(v2, 커서 페이지네이션) + 각 테마 구성종목을 받아
    (테마행, 코드→테마번호들) 로 만든다.

    ⚠️ 이 함수는 예외를 밖으로 내지 않는다. 테마가 실패해도 가격 스냅샷은 저장돼야 한다 —
       관측이 통째로 끊기는 것이 최악이다. 실패 시 빈 값을 주고 종목 행의 테마 열은 공란이 된다.
    구성종목 조회가 테마 수만큼(약 266회) 있어 45초 남짓 걸린다(타임아웃 10분 안에서 충분).
    """
    themes, cursor, page = [], None, 0
    try:
        while page < 15:                       # 안전장치 — 커서가 안 끝나는 경우 대비
            url = THEME_V2_URL + (f"&cursor={cursor}" if cursor else "")
            d = SESSION.get(url, verify=False, timeout=20).json()
            themes += [x for x in d.get("items", []) if isinstance(x, dict) and x.get("code")]
            page += 1
            cursor = d.get("cursor")
            if not d.get("hasNext") or not cursor:
                break
            time.sleep(0.1)
    except Exception as e:
        print(f"⚠️ 테마 목록 조회 실패: {e} — 테마 없이 진행한다")
        return [], {}
    if not themes:
        print("⚠️ 테마 목록이 비었다 — 테마 없이 진행한다")
        return [], {}

    # 커서가 겹쳐 같은 테마가 두 번 올 수 있다
    uniq = {}
    for t in themes:
        uniq[str(t["code"])] = t
    themes = list(uniq.values())

    belong, fails = {}, 0
    for t in themes:
        try:
            rr = SESSION.get(THEME_STOCK_URL.format(no=t["code"]), verify=False, timeout=10)
            for x in (rr.json() if rr.status_code == 200 else []):
                c = str(x.get("itemcode", "")).strip()
                if c.isdigit():
                    belong.setdefault(c, []).append(str(t["code"]))
        except Exception:
            fails += 1
        time.sleep(0.08)

    print(f"🏷️ 테마 {len(themes)}개 ({page}페이지) · 매핑 {len(belong)}종목"
          + (f" · 구성종목 조회 실패 {fails}개" if fails else ""))
    return themes, belong


def _read_theme(path):
    """테마 스냅샷 1개를 행 dict 리스트로 읽는다. 없거나 깨졌으면 빈 리스트."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fp:
            rows = list(csv.reader(fp))
        return [dict(zip(rows[1], r)) for r in rows[2:]]
    except Exception:
        return []


def _read(path):
    """스냅샷 1개를 읽어 (메타dict, {코드: 행dict}) 로 돌려준다."""
    with gzip.open(path, "rt", encoding="utf-8") as fp:
        rows = list(csv.reader(fp))
    meta = {}
    for kv in rows[0][1:]:
        if "=" in kv:
            k, v = kv.split("=", 1)
            meta[k] = v
    hdr = rows[1]
    out = {}
    for r in rows[2:]:
        d = dict(zip(hdr, r))
        out[d["itemcode"]] = d
    return meta, out


def _f(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def write_readme():
    """폰에서도 읽히는 요약을 만든다.
    .csv.gz 는 gzip 바이너리라 깃허브가 미리보기를 못 한다 — 파일명·크기만 보이고 내용은 안 보인다.
    그래서 마크다운 요약을 같이 두어, 브라우저만으로 '오늘 무엇이 잡혔는지'를 확인할 수 있게 한다.
    (분석은 여전히 원본 .csv.gz 로 한다. 이 파일은 사람이 보는 용도다.)"""
    try:
        files = sorted(f for f in os.listdir(OUT_DIR)
                       if f.endswith(".csv.gz") and not f.endswith("_theme.csv.gz"))
    except Exception:
        return
    days = {}
    for f in files:
        d, slot = f[:-7].rsplit("_", 1)
        days.setdefault(d, {})[slot] = f"{OUT_DIR}/{f}"
    if not days:
        return

    L = ["# 📸 장중 시장 스냅샷", "",
         "**전 종목**을 하루 두 번(13:00 / 15:05 KST) 기록한다. "
         "두 스냅샷의 **거래대금 차이가 곧 '오후 수급'** 이다.", "",
         "> 이 파일은 사람이 보는 요약이다. 분석은 같은 폴더의 `.csv.gz` 원본으로 한다.",
         "> 파일명의 슬롯은 '의도한 시각'일 뿐이니, 정확한 시각은 원본 첫 줄의 `capturedAt` 을 볼 것.", ""]

    latest = sorted(days)[-1]
    slots = days[latest]
    m13, s13 = _read(slots["1300"]) if "1300" in slots else ({}, {})
    m15, s15 = _read(slots["1505"]) if "1505" in slots else ({}, {})
    base = s15 or s13
    meta = m15 or m13

    L += [f"## 최신: {latest}", "",
          f"- 캡처 — 13:00슬롯 `{m13.get('capturedAt', '없음')[11:19] or '없음'}` / "
          f"15:05슬롯 `{m15.get('capturedAt', '없음')[11:19] or '없음'}`",
          f"- 지수 — KOSPI {meta.get('KOSPI', '?')} · KOSDAQ {meta.get('KOSDAQ', '?')}",
          f"- 스캔 {meta.get('total', '?')}종목 중 상위 {meta.get('kept', '?')}종목 보관", ""]

    def risk(x):
        f = []
        if str(x.get("manageStatusGb") or "0") != "0":
            f.append("관리")
        if x.get("tradeStopYn") == "Y":
            f.append("정지")
        f.append({"01": "주의", "02": "경고", "03": "위험"}.get(x.get("marketAlertType"), ""))
        return "·".join(y for y in f if y)

    if base:
        top = sorted(base.values(), key=lambda x: -_f(x["tradeAmount"]))[:15]
        L += ["### 거래대금 TOP 15", "",
              "| # | 종목 | 등락률 | 거래대금 | 오후증가 | 비고 |", "|---:|---|---:|---:|---:|---|"]
        for i, x in enumerate(top, 1):
            a15 = _f(x["tradeAmount"])
            prev = s13.get(x["itemcode"])
            grow = f"+{(a15 - _f(prev['tradeAmount'])) / _f(prev['tradeAmount']) * 100:.0f}%"                 if prev and _f(prev["tradeAmount"]) > 0 and s15 else "—"
            L.append(f"| {i} | {x['itemname']} | {x['prevChangeRate']}% | "
                     f"{a15 / 1e8:,.0f}억 | {grow} | {risk(x)} |")
        L.append("")

    # 🔥 오후 수급 급증 — 우리가 3주 뒤 검증하려는 바로 그 값이다.
    if s13 and s15:
        cand = []
        for c, x in s15.items():
            p = s13.get(c)
            if not p:
                continue
            a0, a1 = _f(p["tradeAmount"]), _f(x["tradeAmount"])
            if a0 <= 0 or a1 < 3e10:          # 오후 거래대금 300억 미만은 노이즈로 제외
                continue
            cand.append(((a1 - a0) / a0 * 100, x))
        cand.sort(key=lambda t: -t[0])
        if cand:
            L += ["### 🔥 오후 수급 급증 TOP 10", "",
                  "13:00 → 15:05 거래대금 증가율. 오후 거래대금 300억 이상만.", "",
                  "| # | 종목 | 등락률 | 증가율 | 오후 거래대금 | 비고 |", "|---:|---|---:|---:|---:|---|"]
            for i, (g, x) in enumerate(cand[:10], 1):
                L.append(f"| {i} | {x['itemname']} | {x['prevChangeRate']}% | "
                         f"**+{g:.0f}%** | {_f(x['tradeAmount']) / 1e8:,.0f}억 | {risk(x)} |")
            L.append("")

    # 🏷️ 테마 자금 쏠림 — 종목 단위로는 안 보이는 '무리 단위' 쏠림
    _tslot = next((k for k in ("1505", "1300") if k in slots), sorted(slots)[-1] if slots else "")
    th = _read_theme(f"{OUT_DIR}/{latest}_{_tslot}_theme.csv.gz")
    if th:
        def tops(t, key, n=2):
            out = []
            for p in str(t.get(key, "")).split("|"):
                b = p.split(":")
                if len(b) >= 2 and b[1]:
                    out.append(b[1])
            return " · ".join(out[:n])

        hot = sorted(th, key=lambda z: -_f(z.get("changeRate")))[:10]
        L += ["### 🏷️ 테마 자금 쏠림 — 등락률 상위 10", "",
              "테마가 **살아있는지 먼저 보고**, 그 안에서 자금을 끄는 종목을 본다.", "",
              "| # | 테마 | 등락률 | 거래대금 | 상승/전체 | 등락 1위 | 대금 1위 |",
              "|---:|---|---:|---:|:---:|---|---|"]
        for i, t in enumerate(hot, 1):
            tot = _f(t.get("risingCount")) + _f(t.get("fallingCount")) + _f(t.get("unchangedCount"))
            L.append(f"| {i} | {t.get('name', '')[:18]} | **{t.get('changeRate', '')}%** | "
                     f"{_f(t.get('totalTradingValue')) / 1e8:,.0f}억 | "
                     f"{t.get('risingCount', '')}/{tot:.0f} | "
                     f"{tops(t, 'topByChangeRate')} | {tops(t, 'topByTradingValue')} |")
        L += ["",
              "> ⚠️ **거래대금 상위 테마는 일부러 안 싣는다.** 어느 테마든 대금 1위가 "
              "SK하이닉스·삼성전자로 나와 판별력이 없다(2026-08-27 실측: 대금 상위 8개 테마 전부 동일).",
              "> 반대로 등락률만 보면 죽은 테마에서 우연히 오른 대형주가 1위가 된다"
              "(같은 날 공기청정기 −0.53%·제습기 −0.60% 의 등락 1위가 둘 다 삼성전자).",
              "> 그래서 **테마 강도와 종목 자금을 나란히** 둔다. 대장 정의는 표본이 쌓인 뒤 정한다.", ""]

    L += ["### 수집 이력", "",
          f"총 **{len(days)}일 / {len(files)}개** 파일", "",
          "| 날짜 | 13:00 | 15:05 |", "|---|:---:|:---:|"]
    for d in sorted(days, reverse=True)[:25]:
        L.append(f"| {d} | {'✅' if '1300' in days[d] else '—'} | {'✅' if '1505' in days[d] else '—'} |")
    L += ["", f"_자동 생성: {datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} KST_"]

    io.open(f"{OUT_DIR}/README.md", "w", encoding="utf-8", newline="\n").write("\n".join(L) + "\n")
    print(f"📝 요약 갱신 — {OUT_DIR}/README.md ({len(days)}일치)")


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

    top = sorted(rows, key=amt, reverse=True)
    if TOP_N:
        top = top[:TOP_N]

    # 🏷️ 테마 — 실패해도 가격 스냅샷 저장은 계속한다.
    themes, belong = [], {}
    try:
        themes, belong = fetch_theme_data()
    except Exception as e:
        print(f"⚠️ 테마 수집 전체 실패: {e} — 가격 스냅샷은 그대로 저장한다")
    tamt = {str(t.get("code")): _f(t.get("totalTradingValue")) for t in themes}

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
        w.writerow(FIELDS + THEME_COLS)
        for x in top:
            c = str(x.get("itemcode", "")).strip()
            nos = belong.get(c, [])
            topno = max(nos, key=lambda n: tamt.get(n, 0.0)) if nos else ""
            w.writerow([str(x.get(k, "")) for k in FIELDS] + ["|".join(nos), topno])

    # 테마 집계는 별도 파일 — 스키마가 다르고, 이 파일이 없어도 종목 분석은 그대로 된다.
    if themes:
        tpath = f"{OUT_DIR}/{today}_{a.slot}_theme.csv.gz"
        with gzip.open(tpath, "wt", encoding="utf-8", newline="") as fp:
            tw = csv.writer(fp)
            tw.writerow(["#meta", f"capturedAt={now.isoformat()}", f"slot={a.slot}",
                         f"themes={len(themes)}", "amountUnit=원"])
            tw.writerow(THEME_FIELDS)
            for t in sorted(themes, key=lambda z: -_f(z.get("totalTradingValue"))):
                tw.writerow([_flat_top(t.get(k)) if k in THEME_TOPS else str(t.get(k, ""))
                             for k in THEME_FIELDS])
        print(f"✅ 테마 저장 {tpath}  ({os.path.getsize(tpath):,}바이트)")
        hotc = sorted(themes, key=lambda z: -_f(z.get("changeRate")))[0]
        print(f"   테마 등락률 1위: {hotc.get('name')} {hotc.get('changeRate')}% "
              f"(거래대금 {_f(hotc.get('totalTradingValue')) / 1e8:,.0f}억)")

    size = os.path.getsize(path)
    lead = top[0] if top else {}
    print(f"✅ 저장 {path}  ({size:,}바이트)")
    print(f"   전체 {len(rows)}종목 중 {len(top)}종목 보관"
          + (f" (거래대금 상위 {TOP_N})" if TOP_N else " (전 종목)"))
    print(f"   지수 KOSPI {idx['KOSPI'][0]}({idx['KOSPI'][1]}) / KOSDAQ {idx['KOSDAQ'][0]}({idx['KOSDAQ'][1]})")
    print(f"   거래대금 1위: {lead.get('itemname')} {amt(lead)/1e8:,.0f}억 "
          f"({lead.get('prevChangeRate')}%)")

    write_readme()   # 폰에서 읽을 수 있는 요약 갱신
    return 0


if __name__ == "__main__":
    sys.exit(main())
