# -*- coding: utf-8 -*-
# ==========================================================================
# 🩺 HYEOKS 네이버 의존 지점 진단기 (개편 전/후 비교용)
# --------------------------------------------------------------------------
# 목적: 시스템이 네이버에서 긁어오는 모든 지점을 한 번에 호출해보고, 단순 HTTP 200이
#       아니라 "우리가 실제로 파싱해 쓰는 값이 나오는가"까지 확인한다.
#       (200인데 껍데기만 오는 경우가 가장 위험하다 — 조용한 실패)
# 사용:
#   python hyeoks_naver_healthcheck.py                 → 진단 실행 후 결과 출력
#   python hyeoks_naver_healthcheck.py --save-baseline → 현재 상태를 정상 기준선으로 저장
#   python hyeoks_naver_healthcheck.py --compare       → 저장된 기준선과 비교(개편 후 회귀 탐지)
# 종료코드: 치명적 항목이 하나라도 깨지면 1, 아니면 0 (CI/워크플로에서 활용 가능)
# ==========================================================================
import sys, io, json, time, argparse, datetime
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
BASELINE_PATH = "naver_healthcheck_baseline.json"

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://finance.naver.com/',
})

# 심각도 — 깨졌을 때 시스템에 미치는 영향
FATAL = "치명적"    # 이게 깨지면 채널/게이트가 통째로 무력화됨
MAJOR = "중요"      # 핵심 판단 근거가 비어버림
MINOR = "보통"      # 표시·부가정보가 비는 수준


# ── 개별 점검 함수: (성공여부, 측정치 문자열) 을 반환 ──────────────────────

def _get(url, encoding=None, timeout=10):
    r = SESSION.get(url, verify=False, timeout=timeout)
    r.raise_for_status()
    if encoding:
        return BeautifulSoup(r.content, 'html.parser', from_encoding=encoding)
    return r


def chk_frgn():
    """외국인·기관 수급 (V2 수급점수의 근간)"""
    soup = _get("https://finance.naver.com/item/frgn.naver?code=005930", 'euc-kr')
    rows = [tr for tr in soup.select("table.type2 tr")
            if len(tr.select("td")) >= 7 and tr.select("td")[0].text.strip().replace('.', '').isdigit()]
    return len(rows) >= 5, f"일자행 {len(rows)}개"


def chk_frgn_json():
    """외국인·기관 수급 JSON 대체재 (개편 후 이관 대상)"""
    d = _get("https://m.stock.naver.com/api/stock/005930/trend").json()
    need = {'foreignerPureBuyQuant', 'organPureBuyQuant', 'closePrice'}
    ok = len(d) >= 5 and need.issubset(set(d[0].keys()))
    return ok, f"{len(d)}일치"


def chk_theme():
    """테마 순위 — 주도테마·대장주 판별"""
    soup = _get("https://finance.naver.com/sise/theme.naver", 'cp949')
    table = soup.find('table', {'class': 'theme_area'}) or soup.find('table', {'class': 'type_1'})
    if not table:
        return False, "테마 테이블 없음"
    names = [a.text.strip() for tr in table.find_all('tr')
             for tds in [tr.find_all('td')] if len(tds) > 1
             for a in [tds[0].find('a')] if a]
    return len(names) >= 10, f"테마 {len(names)}개"


def chk_theme_json():
    """테마 순위 JSON 대체재 (신규 PC 사이트가 실제로 쓰는 API)"""
    d = _get("https://stock.naver.com/api/domestic/market/theme/list").json()
    if not isinstance(d, list) or not d:
        return False, "빈 배열(캐치올 주의)"
    need = {'no', 'name', 'changeRate', 'totalAccAmount', 'leadingItem'}
    return need.issubset(set(d[0].keys())), f"테마 {len(d)}개"


def chk_theme_detail():
    """테마 구성종목 — 현재 코드는 테마 상세 HTML의 type_5 표를 5종목까지 읽는다"""
    soup = _get("https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no=205", 'cp949')
    t = soup.find('table', {'class': 'type_5'})
    if not t:
        return False, "type_5 테이블 없음"
    n = sum(1 for tr in t.find_all('tr') if len(tr.find_all('td')) > 8 and tr.find_all('td')[0].find('a'))
    return n >= 3, f"구성종목 {n}개"


def chk_risk_pages():
    """위험종목 필터 — 관리/거래정지/투자경고 (조용한 실패 최대 위험 지점)"""
    srcs = [("관리종목", "https://finance.naver.com/sise/management.naver"),
            ("거래정지", "https://finance.naver.com/sise/trading_halt.naver"),
            ("투자경고", "https://finance.naver.com/sise/investment_alert.naver?type=warning")]
    counts = {}
    for label, url in srcs:
        try:
            soup = _get(url, 'cp949')
            codes = {a['href'].split('code=')[-1][:6] for a in soup.select("a[href*='code=']")
                     if 'code=' in a.get('href', '')}
            counts[label] = len(codes)
        except Exception:
            counts[label] = -1
    ok = counts.get("관리종목", 0) >= 20
    return ok, str(counts)


def chk_lastsearch():
    """검색상위"""
    soup = _get("https://finance.naver.com/sise/lastsearch2.naver", 'euc-kr')
    t = soup.find('table', {'class': 'type_5'})
    n = 0 if not t else sum(1 for tr in t.find_all('tr')
                            if len(tr.find_all('td')) >= 6 and tr.find_all('td')[0].text.strip().isdigit())
    return n >= 5, f"{n}종목"


def chk_mainnews():
    """주요뉴스"""
    soup = _get("https://finance.naver.com/news/mainnews.naver", 'cp949')
    n = len(soup.select('.articleSubject a'))
    return n >= 5, f"{n}건"


def chk_newslist():
    """뉴스 키워드 원천 (시황 뉴스 목록)"""
    soup = _get("https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258", 'cp949')
    n = len(soup.select('.articleSubject a'))
    return n >= 5, f"{n}건"


def chk_itemnews():
    """종목별 뉴스 (AI 리포트 근거)"""
    soup = _get("https://finance.naver.com/item/news_news.naver?code=005930&page=1", 'euc-kr')
    n = len(soup.select('.title a'))
    return n >= 3, f"{n}건"


def chk_market_sum():
    """시가총액 (#_market_sum)"""
    soup = _get("https://finance.naver.com/item/main.naver?code=005930", 'cp949')
    em = soup.find('em', id='_market_sum')
    return em is not None and any(c.isdigit() for c in em.text), (em.text.strip()[:20] if em else "없음")


def chk_stock_basic():
    """현재가 API (스캐너 실시간 가격)"""
    j = _get("https://m.stock.naver.com/api/stock/005930/basic").json()
    return bool(j.get('closePrice')), f"종가 {j.get('closePrice')}"


def chk_index_basic():
    """지수 API"""
    j = _get("https://m.stock.naver.com/api/index/KOSPI/basic").json()
    return bool(j.get('closePrice')), f"KOSPI {j.get('closePrice')}"


def chk_index_price():
    """지수 일별 시세 (하락장 판정)"""
    d = _get("https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1").json()
    return len(d) >= 20, f"{len(d)}일치"


def chk_fchart_stock():
    """일봉 XML — 백테스트 추적·거래일 캘린더의 뿌리"""
    root = ET.fromstring(_get("https://fchart.stock.naver.com/sise.nhn?symbol=005930&timeframe=day&count=10&requestType=0").text)
    n = len(root.findall(".//item"))
    return n >= 5, f"{n}봉"


def chk_fchart_index():
    """지수 일봉 XML — 휴장일 가드가 이걸 쓴다"""
    root = ET.fromstring(_get("https://fchart.stock.naver.com/sise.nhn?symbol=KOSPI&timeframe=day&count=10&requestType=0").text)
    n = len(root.findall(".//item"))
    return n >= 5, f"{n}봉"


def chk_search_code():
    """종목명 → 코드 검색 (기존 /api/search/all 은 2026-08 개편으로 사망)"""
    j = _get("https://ac.stock.naver.com/ac?q=삼성전자&target=stock").json()
    items = j.get('items') or []
    return any(str(i.get('code')) == '005930' for i in items), f"{len(items)}건"


def chk_overtime_json():
    """시간외 단일가 (KRX/NXT 표기)"""
    j = _get("https://m.stock.naver.com/api/stock/005930/basic").json()
    has = any(k in j for k in ('nightMarketPriceInfo', 'overMarketPriceInfo', 'overTimePriceInfo'))
    return has, "필드 있음" if has else "필드 없음"


def chk_sise_bulk():
    """전종목 벌크 시세 (대체 소스 후보)"""
    j = _get("https://m.stock.naver.com/api/json/sise/siseListJson.nhn?menu=market_sum&pageSize=100&page=1").json()
    n = len(j.get("result", {}).get("itemList", []))
    return n >= 50, f"{n}종목"


def chk_risk_json():
    """위험종목 JSON 대체재 — 전종목 스캔 1회로 관리/거래정지/투자경보를 모두 읽는다"""
    d = _get("https://stock.naver.com/api/domestic/market/stock/default"
             "?tradeType=KRX&marketType=ALL&orderType=priceTop&startIdx=0&pageSize=3000").json()
    if not isinstance(d, list) or len(d) < 1500:
        return False, f"스캔 {len(d) if isinstance(d, list) else '?'}건(임계 1500)"
    risk = [x for x in d if (str(x.get('manageStatusGb') or '0') != '0'
                             or x.get('tradeStopYn') == 'Y'
                             or str(x.get('marketAlertType') or '00') != '00')]
    return len(risk) >= 100, f"전체{len(d)}/위험{len(risk)}"


def chk_theme_stocks_json():
    """테마 구성종목 JSON 대체재 (HTML td[8]과 단위 동일: 백만원)"""
    tl = _get("https://stock.naver.com/api/domestic/market/theme/list").json()
    if not isinstance(tl, list) or not tl:
        return False, "테마목록 비어있음"
    no = tl[0].get('no')
    d = _get(f"https://stock.naver.com/api/domestic/market/theme/{no}/stocklist"
             f"?marketType=ALL&orderType=priceTop&startIdx=0&pageSize=100").json()
    return isinstance(d, list) and len(d) >= 2, f"테마{no} 구성 {len(d) if isinstance(d, list) else 0}종목"


def chk_news_json():
    """주요뉴스 JSON 대체재"""
    d = _get("https://stock.naver.com/api/domestic/news/list?category=MAINNEWS&page=1&pageSize=15").json()
    arts = (d or {}).get('articles') if isinstance(d, dict) else None
    return bool(arts) and len(arts) >= 5, f"{len(arts) if arts else 0}건"


def chk_ranking_json():
    """검색상위 JSON 대체재 (orderType=searchTop)"""
    d = _get("https://stock.naver.com/api/domestic/market/stock/default"
             "?tradeType=KRX&marketType=ALL&orderType=searchTop&startIdx=0&pageSize=10").json()
    return isinstance(d, list) and len(d) >= 5, f"{len(d) if isinstance(d, list) else 0}종목"


CHECKS = [
    # (키, 심각도, 설명, 함수, 담당 기능)
    ("frgn_html",     FATAL, "외국인·기관 수급 HTML",   chk_frgn,          "V2 수급점수 / 수급TOP2"),
    ("risk_pages",    FATAL, "위험종목 3종 HTML",       chk_risk_pages,    "관리·정지·경고 필터"),
    ("fchart_stock",  FATAL, "종목 일봉 XML",           chk_fchart_stock,  "백테스트 추적"),
    ("fchart_index",  FATAL, "지수 일봉 XML",           chk_fchart_index,  "휴장일 가드 / 벤치마크"),
    ("stock_basic",   FATAL, "종목 현재가 API",         chk_stock_basic,   "스캐너 전 종목"),
    ("theme_html",    MAJOR, "테마 순위 HTML",          chk_theme,         "주도테마 / 대장주"),
    ("theme_detail",  MAJOR, "테마 구성종목 HTML",      chk_theme_detail,  "테마 내 상위 5종목"),
    ("index_basic",   MAJOR, "지수 현재가 API",         chk_index_basic,   "코스피 등락률"),
    ("index_price",   MAJOR, "지수 일별시세 API",       chk_index_price,   "하락장 판정"),
    ("market_sum",    MAJOR, "시가총액 HTML",           chk_market_sum,    "시총 필터"),
    ("search_code",   MAJOR, "종목명→코드 검색",        chk_search_code,   "종목 코드 해석"),
    # ── 대체재(폴백) 계열 — 개편으로 HTML이 죽으면 이쪽이 받아야 하므로 '중요' 등급 ──
    ("risk_json",     MAJOR, "위험종목 JSON(폴백)",     chk_risk_json,     "static_collector 이중소스"),
    ("theme_json",    MAJOR, "테마 순위 JSON(폴백)",    chk_theme_json,    "omakase 테마 폴백"),
    ("themestk_json", MAJOR, "테마 구성종목 JSON(폴백)", chk_theme_stocks_json, "omakase 테마상세 폴백"),
    ("frgn_json",     MINOR, "수급 JSON(대체재)",       chk_frgn_json,     "미이관 — 9/7 이후"),
    ("news_json",     MINOR, "주요뉴스 JSON(폴백)",     chk_news_json,     "omakase 뉴스 폴백"),
    ("rank_json",     MINOR, "검색상위 JSON(폴백)",     chk_ranking_json,  "omakase 검색상위 폴백"),
    ("mainnews",      MINOR, "주요뉴스",                chk_mainnews,      "앱 뉴스 탭"),
    ("newslist",      MINOR, "시황 뉴스목록",           chk_newslist,      "뉴스 키워드"),
    ("itemnews",      MINOR, "종목별 뉴스",             chk_itemnews,      "AI 리포트 근거"),
    ("lastsearch",    MINOR, "검색상위",                chk_lastsearch,    "네이버 검색상위 탭"),
    ("overtime",      MINOR, "시간외 단일가 필드",      chk_overtime_json, "KRX/NXT 표기"),
    ("sise_bulk",     MINOR, "전종목 벌크 시세",        chk_sise_bulk,     "대체 소스 후보"),
]


def run():
    now = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    print("=" * 78)
    print(f"🩺 HYEOKS 네이버 의존 지점 진단  ({now} KST)")
    print("=" * 78)
    results = {}
    for sev in (FATAL, MAJOR, MINOR):
        print(f"\n──── {sev} ────")
        for key, s, desc, fn, owner in CHECKS:
            if s != sev:
                continue
            t0 = time.time()
            try:
                ok, detail = fn()
                err = ""
            except Exception as e:
                ok, detail, err = False, "", f"{type(e).__name__}: {str(e)[:70]}"
            ms = int((time.time() - t0) * 1000)
            mark = "✅" if ok else "❌"
            print(f"  {mark} {desc:<22} {detail or err:<28} {ms:>5}ms   [{owner}]")
            results[key] = {"ok": ok, "detail": detail, "error": err, "severity": s, "desc": desc}
    return results


def summarize(results):
    fatal_bad = [k for k, v in results.items() if not v["ok"] and v["severity"] == FATAL]
    major_bad = [k for k, v in results.items() if not v["ok"] and v["severity"] == MAJOR]
    minor_bad = [k for k, v in results.items() if not v["ok"] and v["severity"] == MINOR]
    print("\n" + "=" * 78)
    total_bad = len(fatal_bad) + len(major_bad) + len(minor_bad)
    if total_bad == 0:
        print("🟢 전 항목 정상")
    else:
        if fatal_bad:
            print(f"🔴 치명적 파손 {len(fatal_bad)}건: {', '.join(results[k]['desc'] for k in fatal_bad)}")
        if major_bad:
            print(f"🟠 중요 파손 {len(major_bad)}건: {', '.join(results[k]['desc'] for k in major_bad)}")
        if minor_bad:
            print(f"🟡 보통 파손 {len(minor_bad)}건: {', '.join(results[k]['desc'] for k in minor_bad)}")
    print("=" * 78)
    return len(fatal_bad)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--save-baseline", action="store_true", help="현재 상태를 정상 기준선으로 저장")
    ap.add_argument("--compare", action="store_true", help="저장된 기준선과 비교")
    a = ap.parse_args()

    results = run()
    fatal_count = summarize(results)

    if a.save_baseline:
        payload = {"savedAt": datetime.datetime.now(KST).isoformat(), "results": results}
        io.open(BASELINE_PATH, "w", encoding="utf-8").write(json.dumps(payload, ensure_ascii=False, indent=1))
        print(f"\n💾 기준선 저장: {BASELINE_PATH}")

    if a.compare:
        try:
            base = json.loads(io.open(BASELINE_PATH, encoding="utf-8").read())
        except Exception as e:
            print(f"\n⚠️ 기준선을 읽지 못했습니다({e}). 먼저 --save-baseline 으로 저장하세요.")
            return fatal_count and 1 or 0
        print(f"\n📊 기준선({base['savedAt'][:19]}) 대비 회귀 비교")
        regressed = []
        for k, v in results.items():
            was = base["results"].get(k, {}).get("ok")
            if was is True and not v["ok"]:
                regressed.append(v["desc"])
                print(f"  🔻 회귀: {v['desc']}  ({v['error'] or v['detail']})")
            elif was is False and v["ok"]:
                print(f"  🔺 복구: {v['desc']}")
        if not regressed:
            print("  변화 없음 — 기준선 대비 새로 깨진 항목 없음")

    return 1 if fatal_count else 0


if __name__ == "__main__":
    sys.exit(main())
