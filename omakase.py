import os, re, time, datetime, requests, gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
import concurrent.futures
import urllib3
import pandas as pd
import random
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 🆕 [트레일링 스탑 알림용] hyeoks_analyst.py와 동일한 텔레그램 봇/채널을 재사용 —
#    PDF 리포트가 아니라 목표가 도달·트레일링 손절 같은 실시간 짧은 알림 전용으로 sendMessage만 씀.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"

def send_telegram_alert(text):
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      data={'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}, timeout=5)
    except Exception as e:
        print(f"⚠️ [텔레그램 알림 전송 실패] {e}")

# ==========================================
# ⚙️ 글로벌 설정 및 세션/Set 최적화 레이어
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
TARGET_PERCENT = 3.0
KST = datetime.timezone(datetime.timedelta(hours=9))
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_URL_BASE = "https://openapi.koreainvestment.com:9443"  # 👑 [교정 완료]: inwestment -> investment 오타 수정
MAX_WORKERS = int(os.environ.get("OMAKASE_MAX_WORKERS", "12"))

# requests.Session 공용화로 연결 비용 절감 및 Keep-Alive 활성화
# requests.Session 공용화로 연결 비용 절감 및 Keep-Alive 활성화
GLOBAL_SESSION = requests.Session()
GLOBAL_SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://finance.naver.com/'
})

# 👑 [보호 레이어 주입]: 멀티스레드 동시 요청 시 커넥션 풀 고갈 및 한투 서버 강제 차단(RemoteDisconnected) 차단
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

retry_strategy = Retry(
    total=3,                # 한투 서버가 연결을 끊으면 최대 3회 자동 재시도
    backoff_factor=0.3,     # 재시도 간의 미세한 시간 차 조정 (서버 부하 분산)
    status_forcelist=[429, 500, 502, 503, 504],
    raise_on_status=False
)
# 스레드 개수(12개)보다 훨씬 넉넉하게 커넥션 풀 용량을 50으로 확장
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry_strategy)
GLOBAL_SESSION.mount("https://", adapter)
GLOBAL_SESSION.mount("http://", adapter)

stock_alias_map = {
    "삼성화재": "삼성화재해상보험",
    "IPARK현대산업개발": "HDC현대산업개발",
    "NC": "엔씨소프트",
    "한국전력": "한국전력공사",
    "KCC": "KCC",
    "LS ELECTRIC": "LS ELECTRIC"
}

def bounded_workers(item_count):
    return max(1, min(MAX_WORKERS, item_count or 1))

now_kst_check = datetime.datetime.now(KST)
if 4 <= now_kst_check.hour < 7:
    print(f"🌙 현재 시간({now_kst_check.strftime('%H:%M')}): 시스템을 휴식 모드로 전환합니다. (04시~07시)")
    sys.exit(0)

# O(1) 초고속 해시 탐색을 위한 불용어/블랙리스트 Set 변환
STOPWORDS = set(['코스피', '코스닥', '증시', '주식', '투자', '종목', '시장', '지수', '대형주', '중소형주', '외인', '기관', '개인', '외국인', '매수', '매도', '순매수', '순매도', '거래', '대금', '주가', '펀드', '사모', '상장', '상폐', '공모', '특징주', '테마', '테마주', '관련', '관련주', '수혜', '수혜주', '장세', '개장', '출발', '마감', '초반', '후반', '오전', '오후', '장중', '증권', '증권사', '운용', '자사', '괴리', '프리미어', '가치', '밸류', '공시', '병합', '분할', '상승', '하락', '급등', '급락', '강세', '약세', '폭락', '반등', '조정', '랠리', '위축', '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '돌파', '경신', '연속', '최고', '최저', '신고가', '신저가', '최고치', '최저치', '최고가', '최저가', '급증', '급감', '확산', '진정', '완화', '악화', '개선', '회복', '최대', '사상', '역대', '최초', '최신', '규모', '수준', '가격', '목표가', '상향', '하향', '박살', '킬러', '대규모', '변동', '오픈', '호재', '연계', '대비', '경제', '금융', '기업', '정부', '자산', '머니', '한국', '미국', '국내', '글로벌', '뉴욕', '회장', '대표', '임원', '주주', '총회', '이유', '때문', '달러', '금리', '인상', '인하', '동결', '연준', '파월', '물가', '지표', '고용', '기름값', '주유소', '석유', '신용', '수익', '매출', '적자', '흑자', '배당', '지분', '인수', '합병', '사업', '추진', '공급', '계약', '체결', '실적', '발표', '이익', '반사이익', '현금', '자회사', '계열사', '지주사', '관계사', '기내식', '서비스', '오늘', '내일', '이번', '주간', '월간', '분기', '시간', '하루', '하루만', '올해', '내년', '지난해', '전일', '전주', '전월', '동기', '내달', '연말', '연초', '이날', '당일', '최근', '현재', '이후', '이전', '상반기', '하반기', '당분간', '예상', '전망', '기대', '우려', '경고', '목표', '분석', '평가', '결정', '검토', '참여', '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', '타격', '피해', '직격탄', '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', '달성', '기준', '행사', '이사', '의결', '개정', '취지', '적극', '개최', '진행', '예정', '상황', '필요', '대응', '마련', '운영', '관리', '적용', '이용', '사용', '활용', '확보', '제공', '구축', '기반', '중심', '노력', '계획', '정밀', '경우', '이상', '이하', '가운데', '가장', '포함', '제외', '기대감', '우려감', '불확실성', '가능성', '움직임', '분위기', '흐름', '국면', '대목', '차원', '입장', '배경', '결과', '모습', '모멘텀', '현상', '차이', '비중', '비율', '단계', '목적', '대상', '조원', '억원', '만원', '천원', '전문', '현지', '사회', '생산자', '제도', '재고', '면제', '속보', '단독', '기자', '특파원', '앵커', '저작권', '무단', '전재', '재배포', '금지', '뉴스', '보도', '자료', '사진', '관계자', '주장', '설명', '강조', '위원회', '법안', '회의', '통과', '정책', '의원', '장관', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인', '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '란', '국민연금', '종전', '전쟁', '트럼프', '제안', '찬성', '대통령', '사내', '협상', '출시', '계좌', '중동', '상품', '체제', '変更', '투자증권', '성장', '시그널', '신규', '정치', '외교', '합의', '수출', '수입', '도입', '본격', '소식', '임박', '부각', '주도'])
AD_FILTER = set(['펀드', '투어', '캠페인', '서비스', '최초', '강화', '고객', '연금', '마스터', '코리아', '정책', '개최', '박람회', '전시회', '프로모션', '할인', '기획전', '페스티벌', '출시', '협약', 'MOU', '체결', '선정', '어워드', '스마트픽', '팔자', '사자', '증가', '감소', '목표', '꺾인', '주석', '전망', '우려', '기대', '연내', '내달', '오늘', '내일', '돌파', '연속', '급락', '투자', '매수', '매도', '수익'])
THEME_BLACKLIST = set(['코로나19', '메르스', '지카바이러스', '우한폐렴', '원숭이두창', '엠폭스', '아프리카돼지열병', '구제역', '광우병', '야놀자(Yanolja)', '리비안(RIVIAN)'])

def cleanup_and_reorder(doc, sheet_name, sort_col_idx):
    try:
        sheet = doc.worksheet(sheet_name)
        data = sheet.get_all_values()
        if len(data) <= 1: return
        header = data[0]
        
        rows = [r for r in data[1:] if len(r) > sort_col_idx and str(r[sort_col_idx]).strip() and r[0] != header[0]]
        
        def parse_date(val):
            val = str(val).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y. %m. %d"):
                try: return datetime.datetime.strptime(val, fmt)
                except Exception: continue
            return datetime.datetime(1900, 1, 1)
            
        rows.sort(key=lambda x: parse_date(x[sort_col_idx]), reverse=True)
        sheet.batch_clear(['A2:Z10000'])
        
        if rows:
            sheet.update(range_name="A2", values=rows, value_input_option="USER_ENTERED")
        print(f"✅ [{sheet_name}] 최신순 정렬 및 오염 데이터 청소 완료")
    except Exception as e:
        print(f"⚠️ [{sheet_name}] 정렬 실패: {e}")

def normalize_date_format(date_str, current_year=None):
    if current_year is None:
        current_year = str(datetime.datetime.now(KST).year)
    m = re.search(r'(?:(\d{4})[.\-\s년]+)?(\d{1,2})[.\-\s월]+(\d{1,2})', str(date_str))
    if m:
        year = m.group(1) if m.group(1) else current_year
        month = int(m.group(2))
        day = int(m.group(3))
        return f"{year}-{month:02d}-{day:02d}"
    return str(date_str).strip()

def is_mega_cap_or_not_earnings(title):
    if not any(kw in title for kw in ['실적', '영업익', '영업이익', '매출', '흑자', '적자', '어닝']):
        return True
    mega_caps = [
        '삼성전자', 'SK하이닉스', '현대차', '기아', 'LG에너지솔루션', '네이버', '카카오', '셀트리온',
        '엔비디아', 'NVIDIA', '애플', '테슬라', '마이크로소프트', 'MS', '구글', '알파벳', '아마존', '메타'
    ]
    if any(cap in title for cap in mega_caps):
        return True
    return False

def get_kis_access_token():
    if not KIS_APP_KEY or not KIS_APP_SECRET: return None
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)
        setting_sheet = doc.worksheet("⚙️설정")
        records = setting_sheet.get_all_values()
        token_row_idx, date_row_idx = -1, -1
        saved_token, saved_date = "", ""
        for i, row in enumerate(records):
            if len(row) >= 2:
                if row[0] == "KIS_TOKEN": token_row_idx, saved_token = i + 1, row[1]
                elif row[0] == "KIS_TOKEN_DATE": date_row_idx, saved_date = i + 1, row[1]
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        if saved_date == now_str and saved_token:
            print("♻️ 구글 시트에서 기존 KIS 토큰을 불러옵니다. (안전)")
            return saved_token
        print("🆕 KIS 토큰을 새로 발급합니다...")
        headers = {"content-type": "application/json"}
        body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
        res = GLOBAL_SESSION.post(f"{KIS_URL_BASE}/oauth2/tokenP", headers=headers, json=body, timeout=5)
        if res.status_code == 200:
            new_token = res.json().get("access_token")
            if token_row_idx != -1: setting_sheet.update_cell(token_row_idx, 2, new_token)
            else: setting_sheet.append_row(["KIS_TOKEN", new_token])
            if date_row_idx != -1: setting_sheet.update_cell(date_row_idx, 2, now_str)
            else: setting_sheet.append_row(["KIS_TOKEN_DATE", now_str])
            return new_token
        else: print(f"❌ KIS API 토큰 발급 에러: {res.text}")
    except Exception as e: print(f"❌ KIS 토큰 관리 에러: {e}")
    return None

print("🔑 한국투자증권 API 접근 토큰을 준비합니다...")
KIS_TOKEN = get_kis_access_token()
if KIS_TOKEN: print("✅ KIS 토큰 준비 완료!")
else: print("⚠️ KIS 토큰 준비 실패")

def check_warning_market():
    warning_count = 0
    try:
        url = f"https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1&_={int(time.time() * 1000)}"
        res = GLOBAL_SESSION.get(url, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) == 20:
            ma20 = sum(prices) / 20
            ma5  = sum(prices[:5]) / 5
            if prices[0] < ma20: warning_count += 1
            if ma5 < ma20:       warning_count += 1
    except Exception as e:
        print(f"⚠️ [check_warning_market Naver Index Error] {e}")
        warning_count += 1 # 👑 Fail-Closed: 네트워크 터지면 리스크 관리를 위해 위험 장세로 간주

    try:
        kospi_rate = get_kospi_fluctuation_rate()
        if kospi_rate <= -1.0: warning_count += 1
    except Exception as e:
        print(f"⚠️ [check_warning_market Kospi Fluctuation Error] {e}")
        warning_count += 1 # 👑 Fail-Closed: 에러 시 안전하게 위험 카운트 수치 유도

    return warning_count >= 1

def is_index_above_ma5():
    try:
        url = f"https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=5&page=1&_={int(time.time() * 1000)}"
        res = GLOBAL_SESSION.get(url, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) >= 5:
            ma5 = sum(prices[:5]) / 5
            return prices[0] >= ma5
    except Exception as e:
        print(f"⚠️ [is_index_above_ma5 Error] {e}")
    return True

def get_kospi_fluctuation_rate():
    try:
        url = f"https://m.stock.naver.com/api/index/KOSPI/basic?_={int(time.time() * 1000)}"
        res = GLOBAL_SESSION.get(url, verify=False, timeout=3).json()
        rate_str = res.get("fluctuationsRatio", "0")
        return float(str(rate_str).replace(',', ''))
    except Exception as e:
        print(f"⚠️ [get_kospi_fluctuation_rate Error] {e}")
        return 0.0

# ==========================================================================
# 🆕 [네이버 PC 개편 폴백] 신규 사이트가 실제로 쓰는 JSON API 어댑터
# --------------------------------------------------------------------------
# 원칙: 기존 HTML 경로를 '주', JSON을 '보조'로 둔다. HTML이 살아있는 동안은 결과가
#       완전히 동일하므로 9/7 재점검의 기준선이 흔들리지 않고, finance.naver.com이
#       개편으로 죽으면 자동으로 JSON이 받아 시스템이 멈추지 않는다.
# 주의: /api/domestic/market/... 계열은 '없는 값'에도 200 + [] 를 준다(캐치올).
#       따라서 상태코드가 아니라 '건수'로 성공을 판정해야 한다.
# ==========================================================================
NAVER_JSON_BASE = "https://stock.naver.com/api/domestic"
_JSON_HEADERS = {'Accept': 'application/json, text/plain, */*', 'Referer': 'https://stock.naver.com/'}


def _json_get(path, timeout=15):
    try:
        r = GLOBAL_SESSION.get(NAVER_JSON_BASE + path, headers=_JSON_HEADERS, verify=False, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print(f"⚠️ [JSON 폴백 실패] {path} :: {e}")
        return None


def fetch_theme_list_json(limit=20):
    """테마 순위 — [{'no','name'}]. 실패/빈배열이면 빈 리스트."""
    d = _json_get("/market/theme/list")
    if not isinstance(d, list) or not d:
        return []
    return [{'no': str(x.get('no')), 'name': str(x.get('name', '')).strip()} for x in d[:limit] if x.get('no')]


def fetch_theme_stocks_json(theme_no):
    """테마 구성종목 — HTML 파싱 결과와 같은 단위로 맞춘다.
    rate = 등락률(%), value = 거래대금(백만원) — HTML의 td[8]과 같은 단위."""
    d = _json_get(f"/market/theme/{theme_no}/stocklist?marketType=ALL&orderType=priceTop&startIdx=0&pageSize=100")
    if not isinstance(d, list) or not d:
        return []
    out = []
    for x in d:
        code = str(x.get('itemcode', '')).strip()
        if not code.isdigit():
            continue
        try:
            rate = float(str(x.get('prevChangeRate') or 0))
            amt = float(str(x.get('tradeAmount') or 0)) / 1_000_000.0   # 원 → 백만원
        except Exception:
            continue
        out.append({'name': str(x.get('itemname', '')).strip(), 'code': f"'{code}",
                    'rate': rate, 'value': int(amt)})
    return out


def fetch_investor_trend_json(code, days=5):
    """외국인·기관 순매수 — 구 frgn.naver HTML 표와 '완전히 동일한 값'을 주는 JSON 대체재.
    반환: [(날짜 'YYYY.MM.DD', 종가, 기관순매수수량, 외국인순매수수량)] 최신순, 최대 days개.

    2026-08-26 실측 검증 — 5종목(005930/000660/005380/035420/034020) × 5일에 대해
    날짜·종가·기관·외국인 네 값이 HTML과 자릿수까지 100% 일치했다. 따라서 앞의 5개만
    잘라 쓰면 V2 수급점수가 비트 단위로 같아, 9/7 기준선이 오염되지 않는다.
    소스는 2단 폴백이다(둘 다 HTML과 동일 검증). 주의할 점 두 가지 —
      · tradeType 은 반드시 KRX. NXT 는 대체거래소 체결분만이라 값이 완전히 다르다.
      · 시장 전체 랭킹인 /market/trend/trendForeignOrg 는 이 용도로 쓸 수 없다.
        종목 지정이 불가능하다(itemCode 파라미터가 무시됨).
    """
    # 소스 우선순위 — 둘 다 HTML과 값이 동일함이 검증됐다.
    #  ① 신규 PC 사이트가 실제로 쓰는 경로. 앞으로도 유지될 가능성이 가장 높다.
    #     ⚠️ tradeType 은 반드시 KRX. NXT 는 대체거래소 체결분만이라 값이 전혀 다르다
    #        (008930 기준 KRX 외국인 -22,404 vs NXT +1,290 — 부호까지 뒤집힌다).
    #  ② 구 모바일 API. 같은 값을 주지만 이 계열은 이미 /api/search/all 이 사망한 전례가 있어 2순위.
    sources = (
        f"https://stock.naver.com/api/domestic/detail/{code}/trend?tradeType=KRX&startIdx=0&pageSize={max(days, 20)}",
        f"https://m.stock.naver.com/api/stock/{code}/trend",
    )

    def _n(v):
        return int(str(v).replace(',', '').replace('+', '').strip() or 0)

    for url in sources:
        try:
            r = GLOBAL_SESSION.get(url, headers=_JSON_HEADERS, verify=False, timeout=5)
            if r.status_code != 200:
                continue
            data = r.json()
            if not isinstance(data, list) or not data:
                continue
            out = []
            for x in data[:days]:
                bd = str(x.get('bizdate', ''))
                if len(bd) != 8:
                    continue
                out.append((f"{bd[:4]}.{bd[4:6]}.{bd[6:8]}", _n(x.get('closePrice')),
                            _n(x.get('organPureBuyQuant')), _n(x.get('foreignerPureBuyQuant'))))
            if out:
                return out
        except Exception as e:
            print(f"⚠️ [수급 JSON 폴백 실패] {code} :: {url.split('/')[2]} :: {e}")
    return []


def fetch_main_news_json(size=20):
    """주요뉴스 — [[시각, 언론사, 제목, 요약, 링크]] (기존 DataFrame 컬럼 순서와 동일)."""
    d = _json_get(f"/news/list?category=MAINNEWS&page=1&pageSize={size}")
    arts = (d or {}).get('articles') if isinstance(d, dict) else None
    if not arts:
        return []
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    rows = []
    for a in arts:
        oid, aid = str(a.get('officeId', '')), str(a.get('articleId', ''))
        link = f"https://n.news.naver.com/mnews/article/{oid}/{aid}" if oid and aid else ""
        rows.append([now_str, str(a.get('officeHname', '')).strip(), str(a.get('title', '')).strip(),
                     str(a.get('subcontent', '')).strip()[:200], link])
    return rows


def fetch_search_ranking_json(size=10):
    """검색상위 — [[순위, 종목명, 현재가, 등락률, 종목코드]] (기존 컬럼 순서와 동일)."""
    d = _json_get(f"/market/stock/default?tradeType=KRX&marketType=ALL&orderType=searchTop&startIdx=0&pageSize={size}")
    if not isinstance(d, list) or not d:
        return []
    rows = []
    for i, x in enumerate(d, 1):
        code = str(x.get('itemcode', '')).strip()
        if not code.isdigit():
            continue
        rows.append([len(rows) + 1, str(x.get('itemname', '')).strip(),
                     str(x.get('nowPrice', '')), f"{x.get('prevChangeRate', '')}%", f"{code:0>6}"])
    return rows


def search_code_from_naver(stock_name):
    # 🔧 [별칭 폴백] 별칭(정식 법인명) → 실패 시 원래 이름 순으로 시도한다.
    #    stock_alias_map 은 구 /api/search/all(정식명 매칭)용이었는데, 신규 자동완성 API는
    #    '상장 표기명'만 매칭한다. 실측 결과 6개 별칭 중 4개가 오히려 0건을 만들었다
    #    (삼성화재해상보험·엔씨소프트·한국전력공사·HDC현대산업개발 → 0건, 원래 이름은 전부 정상).
    #    별칭을 지우는 대신 폴백을 두어, 별칭이 유효한 경우와 아닌 경우 모두 살린다.
    candidates = []
    for nm in (stock_alias_map.get(stock_name), stock_name):
        if nm and nm not in candidates:
            candidates.append(nm)
    try:
        time.sleep(random.uniform(0.05, 0.15))
        # 🔧 [엔드포인트 교체] 기존 m.stock.naver.com/api/search/all 은 PC 개편과 함께 404로 사망했다
        #    (2026-08-26 실측). 같은 JSON 형태를 주는 자동완성 API 두 곳으로 대체한다.
        #    응답 스키마는 둘 다 {"items":[{"code","name","typeCode",...}]} 로 동일하다.
        for lookup_name in candidates:
            for url in (f"https://ac.stock.naver.com/ac?q={lookup_name}&target=stock",
                        f"https://m.stock.naver.com/front-api/search/autoComplete?query={lookup_name}&target=stock"):
                res = GLOBAL_SESSION.get(url, timeout=3, verify=False)
                if res.status_code != 200:
                    continue
                data = res.json()
                items = data.get('items') or (data.get('result') or {}).get('items') or []
                for it in items:
                    code = str(it.get('code', '')).strip()
                    if code.isdigit() and len(code) == 6:
                        return code
    except Exception:
        pass
    return None

def get_news_keywords():
    try:
        now_minute = datetime.datetime.now(KST).minute
        if not (30 <= now_minute < 40): return pd.DataFrame()
        full_text = ""
        theme_phrases = []
        for page in range(1, 10):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = GLOBAL_SESSION.get(url, verify=False, timeout=5)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
            for sub in soup.select('.articleSubject a'):
                title_text = sub.get_text(strip=True)
                full_text += title_text + " \n "
                for m in re.findall(r"['\"‘“](.*?)['\"’”]", title_text):
                    clean = re.sub(r'(수혜|관련주|테마주|대장주|강세|상한가|특징주|급등|주목|부각)', '', m).strip()
                    clean = re.sub(r'[^\w\s]', '', clean).strip()
                    if 1 < len(clean) <= 12 and clean.count(' ') <= 1 and clean not in AD_FILTER:
                        theme_phrases.append(clean)
                for m in re.findall(r'([가-힣a-zA-Z0-9]+)(?:\s+)?(?:관련주|테마주|수혜주|대장주|섹터|주도주)', title_text):
                    m = re.sub(r'[^\w\s]', '', m).strip()
                    if 1 < len(m) <= 10 and m not in AD_FILTER: theme_phrases.append(m)
        core_keywords = ['의료AI', '비만치료제', '전고체', '자율주행', '로봇', '반도체', '바이오시밀러', '원격진료', '탈플라스틱', '신재생', '원전', '우주항공', 'UAM', '메타버스', 'OLED', 'LFP', 'HBM', 'CXL', '온디바이스', 'AI', '초전도체', '양자암호', '저전력', '데이터센터', '웹툰', '비트코인', 'STO', '밸류업', '방산', '조선', '피지컬AI', '전력설비', '유리기판', '액침냉각', '엔터', '화장품', '미용기기', '제약', '바이오', '이차전지', '2차전지', '폐배터리', '수소', '태양광', '마이크로바이옴']
        for word in core_keywords: theme_phrases.extend([word] * full_text.count(word))
        final_keywords = [word for word in theme_phrases if word not in STOPWORDS]
        top_10 = [(word, count) for word, count in Counter(final_keywords).most_common() if count > 1][:10]
        if not top_10: return pd.DataFrame()
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        return pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_10, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
    except Exception as e:
        print(f"❌ [get_news_keywords Exception] {e}")
        return pd.DataFrame()

def fetch_market_cap_json(code):
    """시가총액(억원) JSON 대체재. marketSum 은 '원' 단위라 1억으로 나눈다.
    2026-08-26 실측: 6종목 대조에서 구 HTML(#_market_sum) 파싱값과 오차 0.00%."""
    try:
        r = GLOBAL_SESSION.get(f"https://stock.naver.com/api/domestic/detail/{code}/detail",
                               params={'codeType': 'KRX'}, headers=_JSON_HEADERS, verify=False, timeout=5)
        if r.status_code != 200:
            return 0
        ms = r.json().get('marketSum')
        return int(float(ms) / 100_000_000) if ms else 0
    except Exception as e:
        print(f"⚠️ [시가총액 JSON 폴백 실패] {code} :: {e}")
        return 0


def get_market_cap(code):
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = GLOBAL_SESSION.get(url, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        market_sum_em = soup.find('em', id='_market_sum')
        if market_sum_em:
            text = market_sum_em.text.strip()
            if '조' in text:
                parts = text.split('조')
                jo = int(parts[0].replace(',', '').strip())
                return jo * 10000 + (int(parts[1].replace(',', '').strip()) if len(parts) > 1 and parts[1].strip() else 0)
            else:
                return int(text.replace(',', '').strip())
    except Exception as e:
        print(f"⚠️ [get_market_cap Error code {code}] {e}")
    # 🆕 [개편 폴백] HTML 파싱이 실패했거나 값이 없으면 신규 JSON 경로로 대체
    return fetch_market_cap_json(code)

def get_real_money_themes():
    try:
        now = datetime.datetime.now(KST)
        is_market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 30)
        time_str = now.strftime('%H:%M')
        
        res = GLOBAL_SESSION.get("https://finance.naver.com/sise/theme.naver", verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        table = soup.find('table', {'class': 'theme_area'}) or soup.find('table', {'class': 'type_1'})
        if not table:
            # 🔎 [진단 로그]: 예전엔 여기서 아무 흔적 없이 조용히 빈 값 반환 → 테마복기 정지 원인 추적 불가했음.
            print(f"❌ [get_real_money_themes] 테마 테이블을 찾지 못함 — status={res.status_code}, 응답길이={len(res.text)}자, "
                  f"제목태그={soup.title.text.strip() if soup.title else '없음'}")

        if table:
            raw_themes = [{'name': a.text.strip(), 'url': "https://finance.naver.com" + a['href'], 'no': None}
                          for tds in [tr.find_all('td') for tr in table.find_all('tr')] if len(tds) > 1
                          for a in [tds[0].find('a')] if a]
        else:
            # 🆕 [개편 폴백] 구 HTML이 죽으면 신규 JSON으로 테마 순위를 받는다.
            raw_themes = [{'name': t['name'], 'url': None, 'no': t['no']} for t in fetch_theme_list_json(30)]
            if raw_themes:
                print(f"🔁 [테마 폴백] 구 HTML 실패 → 신규 JSON API로 테마 {len(raw_themes)}개 확보")
            else:
                print("❌ [테마] HTML·JSON 모두 실패 — 이번 회차 테마 수집을 건너뜁니다.")
                return pd.DataFrame(), is_market_closed, {}

        themes = [t for t in raw_themes if t['name'] not in THEME_BLACKLIST][:20]
        
        theme_data_list = []
        print("▶️ 실시간 주도 테마 수집 시작 (1등 독식 5배수 필터 적용)...")
        
        for theme in themes:
            try:
                stocks = []
                type_5_table = None
                if theme.get('url'):
                    soup = BeautifulSoup(GLOBAL_SESSION.get(theme['url'], verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                    type_5_table = soup.find('table', {'class': 'type_5'})

                if not type_5_table:
                    # 🆕 [개편 폴백] 상세 HTML이 없거나 깨졌으면 JSON 구성종목으로 대체.
                    #    단위는 어댑터에서 HTML(td[8], 백만원)과 동일하게 맞춰 두었다.
                    if theme.get('no'):
                        for sj in fetch_theme_stocks_json(theme['no']):
                            if sj['rate'] >= TARGET_PERCENT and sj['value'] >= 5000:
                                stocks.append(sj)
                    if not stocks:
                        continue
                    stocks_val = sorted(stocks, key=lambda x: x['value'], reverse=True)[:5]
                    if len(stocks_val) >= 2:
                        theme_data_list.append({'theme_name': theme['name'],
                                                'stocks': sorted(stocks_val, key=lambda x: x['rate'], reverse=True)})
                    continue

                name_idx, rate_idx, val_idx = 0, 4, 8

                for tr in type_5_table.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) > val_idx:
                        try:
                            a_tag = tds[name_idx].find('a')
                            if not a_tag: continue
                            s_name = a_tag.text.strip()
                            s_code = f"'{a_tag['href'].split('code=')[-1]}"
                            
                            rate_str = tds[rate_idx].text.strip()
                            val_str  = tds[val_idx].text.strip()

                            if '%' not in rate_str or '-' in rate_str or '0.00' in rate_str: continue
                            rate_num = float(rate_str.replace('%', '').replace('+', '').replace(',', '').strip())
                            val_num  = int(val_str.replace(',', '').strip())

                            if rate_num >= TARGET_PERCENT and val_num >= 5000:
                                stocks.append({'name': s_name, 'code': s_code, 'rate': rate_num, 'value': val_num})
                        except Exception: continue
                        
                stocks_val = sorted(stocks, key=lambda x: x['value'], reverse=True)[:5]
                if len(stocks_val) >= 2:
                    stocks_rate = sorted(stocks_val, key=lambda x: x['rate'], reverse=True)
                    theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks_rate})
            except Exception as e:
                print(f"⚠️ [get_real_money_themes Loop Exception for {theme['name']}] {e}")
                continue
            
        if not theme_data_list:
            print("⚠️ 조건을 만족하는 테마 종목이 하나도 없습니다. (시가총액/등락률 필터 확인)")
            return pd.DataFrame(), is_market_closed, {}
            
        grouped_themes = {}
        for t_data in theme_data_list: grouped_themes.setdefault(t_data['stocks'][0]['code'], []).append(t_data)
        
        merged_themes = []
        for top_code, t_list in grouped_themes.items():
            theme_names = list(dict.fromkeys(t['theme_name'] for t in t_list))
            merged_name = " / ".join(theme_names) + f" (대장: {t_list[0]['stocks'][0]['name']})" if len(theme_names) > 1 else theme_names[0]
            
            unique_stocks = {s['code']: s for t in t_list for s in t['stocks']}
            merged_stocks_val = sorted(unique_stocks.values(), key=lambda x: x['value'], reverse=True)[:5]
            
            if len(merged_stocks_val) >= 2:
                if merged_stocks_val[0]['value'] >= merged_stocks_val[1]['value'] * 5:
                    print(f"⚠️ [{merged_name}] 1등 대장주가 2등보다 거래대금이 5배 이상 커서 개별주로 강등(테마 배제)합니다.")
                    continue
            
            merged_stocks_rate = sorted(merged_stocks_val, key=lambda x: x['rate'], reverse=True)
            merged_themes.append({'theme_name': merged_name, 'theme_sum': sum(s['value'] for s in merged_stocks_val), 'stocks': merged_stocks_rate})
            
        merged_themes = sorted(merged_themes, key=lambda x: x['theme_sum'], reverse=True)
        
        all_theme_map = {}
        for m_data in merged_themes:
            for idx, s in enumerate(m_data['stocks']):
                if s['name'] not in all_theme_map:
                    all_theme_map[s['name']] = {'theme_name': m_data['theme_name'], 'is_leader': (idx == 0)}
                    
        final_themes = []
        for m_data in merged_themes:
            if not any(len(set(s['code'] for s in m_data['stocks']).intersection(set(s['code'] for s in f_data['stocks']))) >= 2 for f_data in final_themes):
                final_themes.append(m_data)
            if len(final_themes) >= 10: break
            
        final_rows = [{'날짜': now.strftime('%Y-%m-%d'), '시간': time_str, '순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)} for rank, t_data in enumerate(final_themes, 1) for s in t_data['stocks']]
        
        return pd.DataFrame(final_rows), is_market_closed, all_theme_map
        
    except Exception as e:
        print(f"❌ 테마 수집 에러: {e}")
        return pd.DataFrame(), False, {}

def get_naver_search_ranking():
    try:
        soup = BeautifulSoup(GLOBAL_SESSION.get("https://finance.naver.com/sise/lastsearch2.naver", verify=False).content, 'html.parser', from_encoding='euc-kr')
        data = []
        search_blacklist = []
        table = soup.find('table', {'class': 'type_5'})
        if not table: return pd.DataFrame()
        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) >= 6 and tds[0].text.strip().isdigit():
                name = tds[1].find('a').text.strip()
                if name in search_blacklist: continue
                s_code = tds[1].find('a')['href'].split('code=')[-1]
                if get_market_cap(s_code) >= 1000:
                    data.append([len(data) + 1, name, tds[3].text.strip(), tds[5].text.strip(), f"{s_code:0>6}"])
            if len(data) >= 10: break
        if not data:
            # 🆕 [개편 폴백] 구 검색상위 페이지가 죽으면 신규 랭킹 API(searchTop)로 대체
            data = fetch_search_ranking_json(10)
            if data: print(f"🔁 [검색상위 폴백] 신규 JSON API로 {len(data)}건 확보")
        return pd.DataFrame(data, columns=['순위', '종목명', '현재가', '등락률(%)', '종목코드'])
    except Exception as e:
        print(f"⚠️ [get_naver_search_ranking Error] {e}")
        return pd.DataFrame()

def get_naver_main_news():
    try:
        soup = BeautifulSoup(GLOBAL_SESSION.get("https://finance.naver.com/news/mainnews.naver", verify=False, timeout=5).content, 'html.parser', from_encoding='cp949')
        news_list = []
        for dl in soup.find_all('dl'):
            subject_tag = dl.find(['dt', 'dd'], {'class': 'articleSubject'})
            summary_tag = dl.find('dd', {'class': 'articleSummary'})
            if subject_tag and subject_tag.find('a'):
                a_tag = subject_tag.find('a')
                href = a_tag['href']
                article_match, office_match = re.search(r'article_id=(\d+)', href), re.search(r'office_id=(\d+)', href)
                link = f"https://n.news.naver.com/mnews/article/{office_match.group(1)}/{article_match.group(1)}" if article_match and office_match else "https://finance.naver.com" + href
                press = "언론사"
                if summary_tag:
                    press_tag = summary_tag.find('span', {'class': 'press'})
                    if press_tag: press = press_tag.text.strip()
                    for span in summary_tag.find_all('span'): span.decompose()
                summary = summary_tag.text.strip() if summary_tag else ""
                now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
                news_list.append([now_str, press, a_tag.text.strip(), summary, link])
                if len(news_list) >= 20: break
        if not news_list:
            # 🆕 [개편 폴백] 구 HTML이 비면 신규 JSON 뉴스 API로 대체
            news_list = fetch_main_news_json(20)
            if news_list: print(f"🔁 [주요뉴스 폴백] 신규 JSON API로 {len(news_list)}건 확보")
        return pd.DataFrame(news_list, columns=['업데이트 시간', '언론사', '기사 제목', '요약 내용', '기사 링크'])
    except Exception as e:
        print(f"⚠️ [get_naver_main_news Error] {e}")
        return pd.DataFrame()

def update_google_sheet(doc, df_theme, df_news, df_naver, df_main_news, is_market_closed):
    try:
        if not df_theme.empty:
            try:
                sheet_rt = doc.worksheet("수급_실시간")
                sheet_rt.batch_clear(['A2:Z'])
                sheet_rt.update(range_name="A2", values=df_theme.values.tolist(), value_input_option="USER_ENTERED")
                print("✅ [수급_실시간] 시트 갱신 완료")
            except Exception as e: print(f"❌ [수급_실시간] 업데이트 실패: {e}")
            now_check = datetime.datetime.now(KST)
            # 🛡️ [수정] "now_check.hour < 9" 조건 삭제 — 이게 새벽~아침 9시 전 모든 실행을 "장마감 이후"로
            #    오인시켜서, 원래 하루 한 번(진짜 장마감 15:30 이후)만 돌아야 할 수급_Raw 전체 재기록 로직이
            #    장 시작 전 반복 실행되게 만든 버그였음 (2026-07-17 8:58 수급_Raw 전체 소실 사고의 원인).
            is_real_closing = now_check.hour > 15 or (now_check.hour == 15 and now_check.minute >= 30)
            
            if is_market_closed or is_real_closing:
                try:
                    sheet_raw = doc.worksheet("수급_Raw")
                    today_str = df_theme.iloc[0]['날짜']
                    all_data = sheet_raw.get_all_values()
                    df_raw = df_theme.drop(columns=['시간'])
                    combined_data = df_raw.values.tolist() + [row for row in all_data[1:] if len(row) > 0 and row[0] != today_str]

                    # 🛡️ [신규 안전장치] 기존 데이터가 꽤 있었는데(10행 초과) 새로 쓸 데이터가 절반 미만으로
                    #    급감하면, 정상적인 하루치 갱신이 아니라 이상 상황으로 보고 중단(데이터 보존 최우선).
                    if len(all_data) > 10 and len(combined_data) < (len(all_data) - 1) * 0.5:
                        print(f"🚨 [수급_Raw 안전장치 발동] 기존 {len(all_data) - 1}행 → 새 데이터 {len(combined_data)}행으로 급감 감지, 기록 중단(데이터 보존 우선)")
                    else:
                        combined_data.sort(key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
                        combined_data.sort(key=lambda x: x[0], reverse=True)

                        # 🛡️ [수정] "지우고 나서 쓰기" 순서 자체가 근본 위험이었음 — 쓰기가 중간에 실패하면
                        #    (2026-07-17 8:57 사고: 구글시트 API 503 일시 오류로 clear는 성공, write가 실패)
                        #    원본이 통째로 사라짐. → 먼저 새 데이터를 기존 자리에 "덮어쓰기"로 쓰고
                        #    (이 자체는 지우는 동작이 아니라, 실패해도 원본이 보존됨), 성공을 확인한 뒤에만
                        #    새 데이터보다 길게 남은 꼬리 부분만 지움. 503/429 같은 일시 오류는 잠깐 쉬었다 재시도.
                        write_ok = False
                        for attempt in range(3):
                            try:
                                sheet_raw.update(range_name="A2", values=combined_data, value_input_option="USER_ENTERED")
                                write_ok = True
                                break
                            except Exception as e:
                                print(f"⚠️ [수급_Raw 쓰기 재시도 {attempt + 1}/3] {e}")
                                time.sleep(5 * (attempt + 1))

                        if write_ok:
                            old_row_count = len(all_data) - 1
                            new_row_count = len(combined_data)
                            if old_row_count > new_row_count:
                                start_row = new_row_count + 2  # 헤더(1행) 다음부터, 새 데이터 마지막 다음 행부터
                                sheet_raw.batch_clear([f"A{start_row}:Z{old_row_count + 1}"])
                            print("✅ [수급_Raw] 누적 기록 완료")
                        else:
                            print("❌ [수급_Raw] 3회 재시도 모두 실패 — 원본 데이터는 그대로 보존됨(안전)")
                except Exception as e: print(f"❌ [수급_Raw] 누적 기록 실패: {e}")
        else:
            print("⚠️ 수집된 테마 데이터가 없어 구글 시트 업데이트를 건너뜁니다.")
        for df, target_sheet_name in [(df_news, "뉴스_키워드"), (df_naver, "네이버_검색상위"), (df_main_news, "네이버_주요뉴스")]:
            if not df.empty:
                try:
                    sheet = doc.worksheet(target_sheet_name)
                    sheet.batch_clear(['A2:Z'])
                    sheet.update(range_name="A2", values=df.values.tolist(), value_input_option="USER_ENTERED")
                except Exception as e: print(f"❌ [{target_sheet_name}] 업데이트 에러: {e}")
    except Exception as e:
        print(f"❌ 구글 시트 전체 업데이트 에러: {e}")

def get_market_schedule():
    try:
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        res = GLOBAL_SESSION.get(url, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        schedules = []
        seen_titles = set()
        for dl in soup.find_all('dl')[:15]:
            title_tag = dl.find('dt', {'class': 'articleSubject'})
            if not title_tag:
                title_tag = dl.find('dd', {'class': 'articleSubject'})
            if title_tag and title_tag.find('a'):
                title = title_tag.find('a').text.strip()
                clean_title = title.replace(" ", "").strip()
                if not is_mega_cap_or_not_earnings(title): continue
                include_kws = ['실적', '発表', '만기', '배당', '금통위', 'FOMC', '고용', '학회', '임상', '상장', '개막', '출시']
                exclude_kws = ['주주총회', '주총', '공모', '청약', '전망', '주목', '대기', '반환점', '서프라이즈', '쇼크', '기대감', '우려', '물귀신', '박스권', '코스피', '코스닥', '증시', '마감', '시황', '특징주', '주간']
                if any(kw in title for kw in include_kws) and not any(ex_kw in title for ex_kw in exclude_kws):
                    if "증시전망" not in title and "외환전망" not in title:
                        if clean_title not in seen_titles:
                            clean_date = normalize_date_format(today_str)
                            schedules.append([clean_date, title, "📅 자동수집(당일)"])
                            seen_titles.add(clean_title)
        return schedules
    except Exception as e:
        print(f"❌ 일정 수집 에러: {e}")
        return []

def manage_schedule_sheet(schedules):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)
        sheet = doc.worksheet("주요일정")
        all_data = sheet.get_all_values()
        if not all_data: return
        rows = all_data[1:]
        today = datetime.datetime.now(KST).date()
        three_months_ago = today - datetime.timedelta(days=90)
        valid_rows = []
        for row in rows:
            if not row or not row[0]: continue
            raw_date = str(row[0]).strip().replace('.', '-').replace(' ', '').strip('-')
            try:
                row_date = datetime.datetime.strptime(raw_date, '%Y-%m-%d').date()
                if row_date >= three_months_ago:
                    row[0] = row_date.strftime('%Y-%m-%d')
                    valid_rows.append(row)
            except ValueError:
                valid_rows.append(row)
        existing_titles_clean = [str(r[1]).replace(" ", "").strip() for r in valid_rows if len(r) > 1 and r[0] == today.strftime('%Y-%m-%d')]
        for sch in schedules:
            clean_sch_title = str(sch[1]).replace(" ", "").strip()
            if clean_sch_title not in existing_titles_clean:
                valid_rows.append(sch)
                existing_titles_clean.append(clean_sch_title)
        def sort_key(x):
            try: return datetime.datetime.strptime(x[0], '%Y-%m-%d').date()
            except Exception: return datetime.date(2099, 12, 31)
        valid_rows.sort(key=sort_key)
        sheet.batch_clear(['A2:C'])
        if valid_rows:
            sheet.update(range_name="A2", values=valid_rows, value_input_option="USER_ENTERED")
        requests_list = []
        requests_list.append({"updateDimensionProperties": {"range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": 1, "endIndex": len(valid_rows) + 1}, "properties": {"hiddenByUser": False}, "fields": "hiddenByUser"}})
        hide_start = -1
        hide_end = -1
        for i, row in enumerate(valid_rows):
            try:
                row_date = datetime.datetime.strptime(row[0], '%Y-%m-%d').date()
                if row_date < today:
                    if hide_start == -1: hide_start = i + 1
                    hide_end = i + 2
            except Exception:
                pass
        if hide_start != -1:
            requests_list.append({"updateDimensionProperties": {"range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": hide_start, "endIndex": hide_end}, "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}})
        if requests_list:
            doc.batch_update({"requests": requests_list})
            print(f"📅 HYEOKS 주요일정 관리 완료 (완벽 중복제거 + 서술형 뉴스 차단 + 포맷팅 + 과거숨김)")
    except Exception as e:
        print(f"❌ 주요일정 시트 관리 에러: {e}")

def safe_int(v, default=0):
    try:
        if v in [None, "", "null"]: return default
        return int(str(v).replace(",", "").strip())
    except Exception: return default

def is_excluded_row(row):
    """백테스트_로그에서 '측정 대상이 아닌' 행인지 판정한다.
    Z열(실제캡처거래일, idx 25)에 '제외' 표식이 있으면 집계에서 뺀다. 행을 지우지 않고 표식만
    남기는 이유는 감사 추적을 보존하기 위함이다 — 왜 뺐는지가 시트에 그대로 남는다.
    현재 표식 두 종류
      · '제외:위험종목(게이트버그)' — is_junk 게이트가 열려 있던 동안 통과한 픽 (2026-08-26 소급 표기)
      · '거래정지(일봉 시가=0) — 측정 제외' — 진입일 시가가 없어 수익률 자체를 못 만드는 행
    """
    return "제외" in (str(row[25]) if len(row) > 25 else "")


def _sheet_bool(v):
    """구글시트의 불리언 표기 흔들림을 흡수한다.
    value_input_option='USER_ENTERED'로 "True"를 쓰면 시트가 불리언으로 해석해 'TRUE'로 되돌아온다.
    예전 코드는 row[3] == 'True' 로 비교해서 'TRUE'를 전부 False로 읽었고, 그 결과
    위험종목 게이트가 통째로 열려 있었다(2026-08-26 발견). 대소문자·공백 무관하게 판정한다."""
    return str(v).strip().upper() in ("TRUE", "T", "1", "Y", "YES")


def parse_score_num(value):
    try:
        text = str(value)
        return int(text.split('점')[0]) if '점' in text else int(float(text))
    except Exception:
        return 0

def parse_stock_name(value):
    text = str(value).strip()
    if 'HYPERLINK' in text:
        m = re.search(r',\s*"([^"]+)"\)', text)
        if m:
            return m.group(1).strip()
    return text

def parse_price_num(value):
    cleaned = re.sub(r'[^0-9]', '', str(value))
    return int(cleaned) if cleaned else 0

def find_key(data, key):
    if isinstance(data, dict):
        if key in data: return data[key]
        for v in data.values():
            res = find_key(v, key)
            if res is not None: return res
    elif isinstance(data, list):
        for item in data:
            res = find_key(item, key)
            if res is not None: return res
    return None

def fetch_extra_closing_prices_from_kis(code, session_obj=None):
    if not KIS_TOKEN or not KIS_APP_KEY or not KIS_APP_SECRET:
        return 0, 0
    req = session_obj if session_obj else GLOBAL_SESSION
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {KIS_TOKEN}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P"
    }
    krx_close = 0
    nxt_close = 0
    try:
        headers["tr_id"] = "FHPST02320000"
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        res = req.get(f"{KIS_URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-overtimeprice", headers=headers, params=params, timeout=5)
        if res.status_code == 200:
            data = res.json()
            output_list = data.get("output", [])
            if isinstance(output_list, list) and len(output_list) > 0:
                for row in output_list:
                    price = safe_int(row.get("ovtm_untp_prpr"))
                    if price > 0:
                        krx_close = price
                        break
            else:
                overtime_price = safe_int(find_key(data, "ovtm_untp_prpr"))
                if overtime_price > 0: krx_close = overtime_price
    except Exception as e:
        print(f"⚠️ [fetch_extra_closing_prices_from_kis KRX Error for {code}] {e}")

    try:
        r = req.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3, verify=False)
        if r.status_code == 200:
            j = r.json()
            night_info = j.get("nightMarketPriceInfo") or j.get("overMarketPriceInfo") or {}
            nxt_price = safe_int(night_info.get("closePrice") or night_info.get("price") or night_info.get("overPrice"))
            if nxt_price > 0: nxt_close = nxt_price
    except Exception as e:
        print(f"⚠️ [fetch_extra_closing_prices_from_kis NXT Error for {code}] {e}")

    if krx_close == 0:
        try:
            r = req.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3, verify=False)
            if r.status_code == 200:
                j = r.json()
                ot_info = j.get("overTimePriceInfo") or j.get("overMarketPriceInfo") or {}
                ot_price = safe_int(ot_info.get("closePrice") or ot_info.get("price") or ot_info.get("overPrice"))
                if ot_price > 0: krx_close = ot_price
        except Exception as e:
            print(f"⚠️ [fetch_extra_closing_prices_from_kis Fallback Error for {code}] {e}")

    return krx_close, nxt_close

def get_current_price_for_backtest(code):
    try:
        t_code = str(code).replace("'", "").strip().zfill(6)
        rt_res = GLOBAL_SESSION.get(f"https://m.stock.naver.com/api/stock/{t_code}/basic", verify=False, timeout=3).json()
        return parse_price_num(rt_res.get('closePrice', '0'))
    except Exception as e:
        print(f"⚠️ [Backtest Current Price Error for {code}] {e}")
        return 0

def check_target_alerts_and_trailing_stop(doc, bt_sheet):
    """🆕 [트레일링 스탑 + 목표가 도달 알림] 리포트TOP2_중기/장기 채널의 열린 포지션(아직 손절 터치가
       안 된 것)을 확인해서, 목표가에 처음 닿으면 텔레그램으로 알리고 트레일링손절가를 진입가(본전)
       위로 올려 이익을 보호한다. 이후 주가가 더 오르면 트레일링손절가를 현재가의 92%선까지 같이
       끌어올리고, 반대로 트레일링손절가에 닿으면 "청산 권장" 알림을 한 번만 보낸다.
       ⚠️ 목표가·손절가(연구용 고정값, 채널 비교의 기준)는 여기서 절대 건드리지 않는다 —
       트레일링손절가·목표가알림발송 2개 칸만 별도로 갱신한다."""
    try:
        rows = bt_sheet.get_all_values()
        if len(rows) < 2:
            return
        header = rows[0]
        if len(header) < 38 or header[36] != "트레일링손절가":
            print("⚠️ [트레일링 스탑] 백테스트_로그가 아직 38열 스키마가 아니라 이번 회차는 건너뜁니다.")
            return

        TARGET_CHANNELS = ("리포트TOP2_중기", "리포트TOP2_장기")
        updates = []

        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 34:
                continue
            channel = str(row[2]).strip()
            if channel not in TARGET_CHANNELS:
                continue
            name, code = str(row[3]).strip(), str(row[4]).replace("'", "").strip().zfill(6)
            target_raw, stop_raw = str(row[32]).strip(), str(row[33]).strip()
            if not target_raw or not stop_raw:
                continue
            stop_touched = str(row[35]).strip() if len(row) > 35 else ""
            if stop_touched:  # 이미 원 손절가로 종료 처리된 포지션은 트레일링 대상에서 제외
                continue
            try:
                target_p = float(target_raw.replace(',', ''))
                stop_p = float(stop_raw.replace(',', ''))
                # 🔧 [본전 기준 교정] 예전엔 row[14](기준종가=신호일 종가)를 '본전'으로 썼는데,
                #    이 시스템의 실제 진입가는 row[16](진입가=T+1 시가)이다. 갭이 있으면 둘이 어긋나
                #    아직 손실인데 '본전 확보'로 트레일링을 올리는(또는 그 반대) 일이 생긴다.
                #    실측상 중기/장기 14건 중 5건이 1% 이상 벌어져 있었다(최대 -2.95%).
                #    진입가는 다음 날 아침 추적에서 확정되므로, 미확정 구간에서만 기준종가로 폴백한다.
                entry_raw = str(row[16]).strip() if len(row) > 16 else ""
                entry_p = float(entry_raw.replace(',', '')) if entry_raw else float(str(row[14]).replace(',', ''))
                if entry_p <= 0:
                    entry_p = float(str(row[14]).replace(',', ''))
            except Exception:
                continue

            curr_p = get_current_price_for_backtest(code)
            if curr_p <= 0:
                continue

            trailing_raw = str(row[36]).strip() if len(row) > 36 else ""
            alert_sent = str(row[37]).strip() if len(row) > 37 else ""
            trailing_stop = float(trailing_raw.replace(',', '')) if trailing_raw else stop_p

            new_trailing, new_alert, row_changed = trailing_stop, alert_sent, False

            if not alert_sent and curr_p >= target_p:
                new_alert = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
                new_trailing = max(trailing_stop, entry_p)
                send_telegram_alert(
                    f"🎯 [목표가 도달] {channel}\n{name}({code})\n"
                    f"현재가 {curr_p:,.0f}원 ≥ 목표가 {target_p:,.0f}원\n"
                    f"트레일링손절가를 {new_trailing:,.0f}원(본전)으로 상향합니다."
                )
                row_changed = True
            elif alert_sent and "청산권고발송" not in alert_sent:
                if curr_p > trailing_stop:
                    candidate_trailing = curr_p * 0.92
                    if candidate_trailing > trailing_stop:
                        new_trailing = candidate_trailing
                        row_changed = True
                elif curr_p <= trailing_stop:
                    send_telegram_alert(
                        f"🛑 [트레일링 손절 도달] {channel}\n{name}({code})\n"
                        f"현재가 {curr_p:,.0f}원 ≤ 트레일링손절가 {trailing_stop:,.0f}원\n청산을 권장합니다."
                    )
                    new_alert = alert_sent + "|청산권고발송"
                    row_changed = True

            if row_changed:
                updates.append({'range': f'AK{i}:AL{i}', 'values': [[round(new_trailing), new_alert]]})

        if updates:
            for j in range(0, len(updates), 50):
                bt_sheet.batch_update(updates[j:j + 50], value_input_option="USER_ENTERED")
            print(f"📈 [트레일링 스탑] {len(updates)}건 갱신")
    except Exception as e:
        print(f"⚠️ [트레일링 스탑 체크 실패] {e}")

def update_recommendation_tracking(doc, top_20_results):
    pass

# ── [백테스트 V6] 신 26열 스키마 & 진입 메타 캡처 헬퍼 (Step 1) ──
BT_HEADER = [
    "trade_id", "진입일", "채널", "종목명", "종목코드", "주도테마", "타점유형", "STAGE", "집중도",
    "V1", "V2", "V2게이트", "수급상태", "벤치명", "기준종가", "진입지수", "진입가(T+1시가)",
    "종목T+1", "종목T+3", "종목T+5", "종목T+10", "지수T+1", "지수T+3", "지수T+5", "지수T+10", "실제캡처거래일",
    "종목T+20", "종목T+60", "종목T+120", "지수T+20", "지수T+60", "지수T+120",
    "목표가", "손절가", "익절터치", "손절터치", "트레일링손절가", "목표가알림발송"
    # 🆕 [트레일링 스탑 + 목표가 도달 알림] 목표가·손절가는 진입 시점에 고정된 "연구용" 값(기존 그대로 보존).
    #    여기 2개는 별개로, 실전 대응을 위해 실시간으로 갱신되는 값 — 목표가에 닿으면 텔레그램 알림을 보내고
    #    트레일링손절가를 올려서 이익을 잠가두되, 채널 비교 연구의 기준값(목표가·손절가)은 절대 건드리지 않는다.
    # 🆕 [수정] T+10(약 2주)까지만 추적하던 걸 T+20(약 1개월)·T+60(약 3개월)·T+120(약 6개월)까지 확장.
    #    전략 자체가 6개월~1년짜리 구조적 성장을 겨냥하는데 2주 성과만으로는 검증이 안 됐던 문제 해결.
    #    기존 컬럼 위치는 그대로 두고 끝에만 추가(다른 코드가 기존 인덱스로 읽는 부분이 안 깨지도록).
]

def compute_channel_comparison_dashboard(doc):
    """🆕 [채널 비교 대시보드] 채널별로 T+1~T+120 여러 호라이즌의 평균수익률·초과수익(vs지수)·승률을
       한 화면에서 비교할 수 있게 표로 정리. 초과수익 칸은 양수=초록/음수=빨강으로 색을 입혀서
       한눈에 어느 채널이 지수를 이기고 있는지 바로 보이게 함."""
    try:
        bt_data = doc.worksheet("백테스트_로그").get_all_values()
    except Exception as e:
        print(f"⚠️ [채널비교 대시보드 실패] 백테스트_로그 읽기 실패: {e}")
        return
    if len(bt_data) < 2:
        return
    rows = bt_data[1:]

    horizons = [(1, 17, 21), (3, 18, 22), (5, 19, 23), (10, 20, 24), (20, 26, 29), (60, 27, 30), (120, 28, 31)]
    channels = ["차트TOP2", "수급TOP2", "랜덤2", "리포트TOP2", "리포트TOP2_단기", "리포트TOP2_중기", "리포트TOP2_장기",
                "지수벤치_KOSPI", "지수벤치_KOSDAQ"]  # 🆕 두 지수 이격을 나란히 비교할 수 있도록 추가

    from collections import defaultdict
    by_channel = defaultdict(lambda: defaultdict(list))
    excluded_n = 0
    for r in rows:
        ch = str(r[2]).strip() if len(r) > 2 else ""
        if ch not in channels: continue
        if is_excluded_row(r):   # 🆕 위험종목 게이트 버그로 들어온 픽 등은 표본에서 제외
            excluded_n += 1
            continue
        for h, s_col, i_col in horizons:
            if len(r) <= max(s_col, i_col): continue
            s_str = str(r[s_col]).strip().replace('%', '')
            i_str = str(r[i_col]).strip().replace('%', '')
            if not s_str or not i_str: continue
            try:
                by_channel[ch][h].append((float(s_str), float(i_str)))
            except Exception:
                continue

    header = ["채널"]
    for h, _, _ in horizons:
        # 🔧 [표기 교정] '평균수익률'과 '초과수익'이 이름만으로는 구분이 안 돼 사람이 계속 헷갈렸다.
        #    (지수가 크게 빠진 구간에서는 실수익이 마이너스인데 초과α만 크게 양수로 나온다)
        #    → 실제로 번 돈인지, 지수 대비 상대성과인지를 이름에서 바로 드러나게 한다.
        header += [f"T+{h} 표본", f"T+{h} 실수익(%)", f"T+{h} 초과α(%p·vs지수)", f"T+{h} 승률(%)"]
    out_rows = [header]

    for ch in channels:
        row = [ch]
        for h, _, _ in horizons:
            pairs = by_channel[ch].get(h, [])
            n = len(pairs)
            if n == 0:
                row += ["", "", "", ""]
                continue
            avg_s = sum(p[0] for p in pairs) / n
            avg_i = sum(p[1] for p in pairs) / n
            win = sum(1 for p in pairs if p[0] > 0) / n * 100
            row += [n, round(avg_s, 2), round(avg_s - avg_i, 2), round(win, 1)]
        out_rows.append(row)

    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')

    try:
        try:
            dash_sheet = doc.worksheet("채널비교_대시보드")
        except Exception:
            dash_sheet = doc.add_worksheet(title="채널비교_대시보드", rows="20", cols="30")
        dash_sheet.clear()
        dash_sheet.update(range_name="A1", values=out_rows, value_input_option="RAW")
        dash_sheet.update(range_name=f"A{len(out_rows) + 2}", values=[
            [f"갱신: {now_str}"],
            ["※ 실수익 = 실제 손익. 초과α = 지수 대비 상대성과(%p)."],
            ["※ 지수가 크게 빠진 구간에서는 실수익이 마이너스여도 초과α는 크게 양수로 나온다."
             " 즉 초과α가 양수라고 돈을 번 것이 아니다 — 판단은 '실수익'을 먼저 보고 할 것."],
            [f"※ 집계 제외 {excluded_n}행 (백테스트_로그 Z열 '제외' 표식). "
             f"위험종목 게이트가 열려 있던 동안 통과한 픽 등 — 행은 보존되어 있고 집계에서만 뺀다."],
        ], value_input_option="RAW")

        # 기존 조건부 서식 삭제 후, 초과수익 칸에만 양/음수 색상 규칙 새로 등록(중복 누적 방지)
        sheet_id = dash_sheet.id
        meta = doc.fetch_sheet_metadata()
        existing_count = 0
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == sheet_id:
                existing_count = len(s.get("conditionalFormats", []))
                break
        requests_list = [{"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}} for _ in range(existing_count)]

        n_channels = len(channels)
        for idx, (h, _, _) in enumerate(horizons):
            # 🔧 [표기 교정] 예전엔 '초과α' 칸에만 색을 칠했다. 그래서 지수가 폭락한 구간에서
            #    실수익이 마이너스인데도 초과α만 초록으로 보여 "다 잘하고 있다"는 착시를 만들었다.
            #    → 실수익 칸에도 같은 색 규칙을 걸어, 돈을 잃은 칸은 반드시 빨갛게 보이도록 한다.
            for col_idx in (1 + idx * 4 + 1,      # "T+h 실수익(%)"
                            1 + idx * 4 + 2):     # "T+h 초과α(%p·vs지수)"
                col_range = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1 + n_channels, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1}
                requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [col_range], "booleanRule": {
                    "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                    "format": {"backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}}}}, "index": 0}})
                requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [col_range], "booleanRule": {
                    "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                    "format": {"backgroundColor": {"red": 1.0, "green": 0.88, "blue": 0.88}}}}, "index": 0}})
        doc.batch_update({"requests": requests_list})
        print(f"✅ [채널비교_대시보드] {n_channels}개 채널 × {len(horizons)}개 호라이즌 갱신 완료")
    except Exception as e:
        print(f"⚠️ [채널비교_대시보드 기록 실패] {e}")


def compute_channel_kelly(doc):
    """🆕 [하프켈리 베팅비중 참고자료] 채널별로 승률·손익비를 계산해서 켈리 공식으로 베팅비중을 추정.
       표본이 통계적으로 의미 있는 최소치(30건) 미만이면 숫자를 내지 않고 '데이터 부족'으로 표시함.
       추정 오차에 대한 안전마진으로 풀켈리가 아니라 절반(하프켈리)만 씀. 자동 매매 실행이 아니라
       참고용 표시 목적 — 종목/테마 단위는 반복 표본이 근본적으로 부족해 채널 단위로만 계산."""
    MIN_SAMPLE = 30
    MAX_HALF_KELLY_CAP = 0.25  # 하프켈리라도 25%를 넘지 않도록 안전 상한

    # 채널별로 어느 호라이즌을 기준으로 볼지 — 단기 성격 채널은 T+5, 중기(스윙)는 T+10, 장기(구조적 성장)는 T+60
    CHANNEL_HORIZON = {
        "차트TOP2": 5, "수급TOP2": 5, "랜덤2": 5,
        "리포트TOP2": 5, "리포트TOP2_단기": 5, "리포트TOP2_중기": 10, "리포트TOP2_장기": 60,
    }
    horizon_col = {1: 17, 3: 18, 5: 19, 10: 20, 20: 26, 60: 27, 120: 28}  # 종목 수익률 컬럼(0-based)

    try:
        bt_data = doc.worksheet("백테스트_로그").get_all_values()
    except Exception as e:
        print(f"⚠️ [켈리 계산 실패] 백테스트_로그 읽기 실패: {e}")
        return
    if len(bt_data) < 2:
        return
    rows = bt_data[1:]

    from collections import defaultdict
    by_channel = defaultdict(list)
    for r in rows:
        if len(r) < 29: continue
        ch = str(r[2]).strip()
        if ch not in CHANNEL_HORIZON: continue
        if is_excluded_row(r): continue   # 🆕 제외 표식 행은 승률·손익비 계산에서도 뺀다
        h = CHANNEL_HORIZON[ch]
        col = horizon_col[h]
        val_str = str(r[col]).strip().replace('%', '') if len(r) > col else ""
        if not val_str: continue
        try:
            by_channel[ch].append(float(val_str))
        except Exception:
            continue

    out_rows = [["채널", "기준호라이즌", "표본수", "승률(%)", "손익비", "풀켈리(%)", "하프켈리 추천(%)", "갱신일시"]]
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    for ch, horizon in CHANNEL_HORIZON.items():
        returns = by_channel.get(ch, [])
        n = len(returns)
        if n < MIN_SAMPLE:
            out_rows.append([ch, f"T+{horizon}", n, "", "", "", "데이터 부족(최소 30건 필요)", now_str])
            continue

        wins = [x for x in returns if x > 0]
        losses = [x for x in returns if x <= 0]
        win_rate = len(wins) / n
        avg_win = (sum(wins) / len(wins)) if wins else 0.0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0

        if avg_loss == 0 or win_rate <= 0:
            out_rows.append([ch, f"T+{horizon}", n, round(win_rate * 100, 1), "", "", "권장 없음(손실 표본 없음 등)", now_str])
            continue

        rr = avg_win / avg_loss  # 손익비
        full_kelly = win_rate - (1 - win_rate) / rr
        half_kelly = max(0.0, min(full_kelly / 2, MAX_HALF_KELLY_CAP))  # 음수(마이너스 기대값)면 0으로, 상한 25%
        out_rows.append([
            ch, f"T+{horizon}", n, round(win_rate * 100, 1), round(rr, 2),
            round(full_kelly * 100, 1), round(half_kelly * 100, 1), now_str
        ])

    try:
        try:
            kelly_sheet = doc.worksheet("베팅비중_참고")
        except Exception:
            kelly_sheet = doc.add_worksheet(title="베팅비중_참고", rows="50", cols="8")
        kelly_sheet.clear()
        kelly_sheet.update(range_name="A1", values=out_rows, value_input_option="RAW")
        print(f"✅ [베팅비중_참고] 채널 {len(out_rows) - 1}개 갱신 완료 (하프켈리 기준, 최소표본 {MIN_SAMPLE}건)")
    except Exception as e:
        print(f"⚠️ [베팅비중_참고 기록 실패] {e}")


def _parse_price_cell(v):
    """목표가/손절가 칸을 백테스트_로그에 넣을 정수로 변환.
    같은 값이라도 경로에 따라 형식이 제각각이라 한 곳에서 흡수한다.
      · result_row 계산값          → 61000 (int)
      · DB_스캐너 동기화된 AI 값     → '61,000원' (문자열)
      · 비매수/미산출 상태          → '관망', 'AI 데이터 계산중', '' → 빈 값 유지(설계 의도)
    """
    s = str(v).strip()
    if not s:
        return ""
    if any(k in s for k in ("관망", "계산", "대기", "매매금지", "제외")):
        return ""           # 진짜 비매수 신호는 숫자를 넣지 않는다
    digits = re.sub(r'[^0-9]', '', s)
    if not digits:
        return ""
    n = int(digits)
    return n if n > 0 else ""


def sort_and_format_backtest_log(doc, bt_sheet):
    """🆕 [정렬+서식] 백테스트_로그를 진입일 최신순으로 정렬하고, 채널별 배경색(조건부 서식)과
       날짜가 바뀌는 지점에 실제 구분선(테두리)을 적용해서 한눈에 보기 쉽게 만듦.
       정렬은 '먼저 쓰고 나중에 지우기'와 동일한 안전 원칙(재시도)으로 처리."""
    all_data = bt_sheet.get_all_values()
    if len(all_data) < 3:
        return
    n_rows = len(all_data)

    # 🔒 [레이스 차단] 예전엔 read→sort→update("A1") 전체 재작성이었다. 그러면 그 사이에
    #    analyst(다른 동시성 그룹)나 저녁 추적 스크립트가 쓴 셀을 통째로 덮어써 날린다.
    #    서버측 sortRange는 원자적이라 남의 변경분을 지우지 않는다.
    #    정렬키: 진입일 DESC → 채널 ASC. 채널을 가나다순으로 두는 이유는 analyst 쪽 정렬과
    #    규칙을 일치시켜, 어느 쪽이 마지막으로 돌든 같은 순서가 나오게 하기 위함이다.
    #    (예전 CHANNEL_ORDER 커스텀 우선순위는 두 writer가 서로 다른 순서를 강제해
    #     실행할 때마다 행 순서가 뒤집히는 부작용이 있었다.)
    sort_ok = False
    for attempt in range(3):
        try:
            doc.batch_update({"requests": [{"sortRange": {
                "range": {"sheetId": bt_sheet.id, "startRowIndex": 1, "endRowIndex": n_rows,
                          "startColumnIndex": 0, "endColumnIndex": 38},
                "sortSpecs": [
                    {"dimensionIndex": 1, "sortOrder": "DESCENDING"},   # B 진입일(최신 위)
                    {"dimensionIndex": 2, "sortOrder": "ASCENDING"},    # C 채널(날짜 내 그룹 고정)
                ]}}]})
            sort_ok = True
            break
        except Exception as e:
            print(f"⚠️ [백테스트_로그 정렬 재시도 {attempt + 1}/3] {e}")
            time.sleep(3)
    if not sort_ok:
        print("❌ [백테스트_로그 정렬] 재시도 후에도 실패 — 다음 회차에서 다시 시도")
        return
    print(f"✅ [백테스트_로그 정렬] {n_rows - 1}행 최신순 정렬 완료 (원자적 sortRange)")

    # 테두리(날짜 구분선)는 정렬된 '현재 행 위치'가 필요하므로 정렬 후 다시 읽는다.
    try:
        sorted_rows = bt_sheet.get_all_values()[1:]
        apply_backtest_formatting(doc, bt_sheet, sorted_rows)
    except Exception as e:
        print(f"⚠️ [백테스트_로그 서식 적용 실패] {e}")


def _col_letter(idx):
    """0-based 컬럼 인덱스를 시트 열 문자로 변환(A,B,...,Z,AA,AB,...). 예전엔 26 이상에서 깨지는 버그가 있었음."""
    s = ""
    idx += 1
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        s = chr(65 + rem) + s
    return s


def apply_change_rate_formatting(doc, sheet, num_rows, col_index, extra_numeric_rules=None):
    """🆕 [등락률 색상 + 선택적 숫자 강조] 국내 증시 관행대로 음수=파랑 굵게, 양수=빨강 굵게로 표시.
       "%" 문자열 텍스트라 숫자 비교가 아니라 커스텀 수식(맨 앞 글자가 '-'인지)으로 판정.
       col_index는 정수 하나 또는 여러 컬럼 리스트 모두 가능(백테스트_로그처럼 T+N 칸이 여러 개일 때 한 번에 처리).
       extra_numeric_rules로 다른 숫자 컬럼(예: RS등급)에 대한 임계값 강조도 같은 배치로 같이 적용 가능
       — 별도 함수로 두 번 지우고 추가하면 서로의 규칙을 지워버리는 충돌이 생기므로 한 번에 처리.
       기존 조건부 서식 규칙은 먼저 지우고 새로 등록해서 재실행마다 중복 누적되는 걸 방지."""
    try:
        sheet_id = sheet.id
        meta = doc.fetch_sheet_metadata()
        existing_count = 0
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == sheet_id:
                existing_count = len(s.get("conditionalFormats", []))
                break
        requests_list = [{"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}} for _ in range(existing_count)]

        col_indices = col_index if isinstance(col_index, (list, tuple)) else [col_index]
        for ci in col_indices:
            col_letter = _col_letter(ci)  # 🔧 [수정] 26번째 컬럼(AA) 이상에서 깨지던 chr(ord('A')+idx) 방식 교체
            rng = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": ci, "endColumnIndex": ci + 1}
            requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [rng], "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f'=LEFT({col_letter}2,1)="-"'}]},
                "format": {"textFormat": {"foregroundColor": {"red": 0.15, "green": 0.35, "blue": 0.95}, "bold": True}}}}, "index": 0}})
            requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [rng], "booleanRule": {
                "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f'=AND(LEFT({col_letter}2,1)<>"-",{col_letter}2<>"0.00%",{col_letter}2<>"")'}]},
                "format": {"textFormat": {"foregroundColor": {"red": 0.85, "green": 0.1, "blue": 0.1}, "bold": True}}}}, "index": 0}})

        for extra_col, threshold, rgb in (extra_numeric_rules or []):
            extra_rng = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": num_rows, "startColumnIndex": extra_col, "endColumnIndex": extra_col + 1}
            requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [extra_rng], "booleanRule": {
                "condition": {"type": "NUMBER_GREATER_THAN_EQ", "values": [{"userEnteredValue": str(threshold)}]},
                "format": {"textFormat": {"foregroundColor": rgb, "bold": True}}}}, "index": 0}})

        doc.batch_update({"requests": requests_list})
    except Exception as e:
        print(f"⚠️ [등락률 색상 서식 실패] {e}")


def apply_backtest_formatting(doc, bt_sheet, sorted_rows):
    """채널별 배경색은 조건부 서식으로 등록해 새 행에도 자동 유지되게 하고,
       날짜 구분선은 조건부 서식이 테두리를 지원하지 않아 실제 테두리로 매번 재계산해서 적용."""
    sheet_id = bt_sheet.id
    n_rows = len(sorted_rows)

    # ① 기존 조건부 서식 규칙 전부 삭제(재실행할 때마다 중복 누적되는 것 방지)
    meta = doc.fetch_sheet_metadata()
    existing_count = 0
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("sheetId") == sheet_id:
            existing_count = len(s.get("conditionalFormats", []))
            break
    requests_list = [{"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": 0}} for _ in range(existing_count)]

    # ② 채널별 배경색 규칙 추가 (C열=채널 값으로 행 전체를 물들임)
    channel_colors = {
        "차트TOP2": (0.85, 0.92, 1.0), "수급TOP2": (1.0, 0.93, 0.82),
        "랜덤2": (0.93, 0.93, 0.93), "지수벤치_KOSPI": (0.85, 0.97, 0.87), "지수벤치_KOSDAQ": (0.87, 0.95, 0.97),
        "리포트TOP2_단기": (1.0, 0.87, 0.87), "리포트TOP2_중기": (0.93, 0.87, 1.0), "리포트TOP2_장기": (1.0, 0.95, 0.78),
        "리포트TOP2": (0.95, 0.87, 0.95),
        # 🔧 [누락 보완] 구 채널명 '지수벤치'(_KOSPI/_KOSDAQ 접미사 없는 과거 행)가 빠져 있어
        #    해당 행들만 색이 없는 채로 남아 있었음.
        "지수벤치": (0.85, 0.97, 0.87),
    }
    grid_range = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1 + n_rows, "startColumnIndex": 0, "endColumnIndex": 38}
    # 🔧 [가독성] 채널 배경색은 메타 영역(A~Q)에만 칠한다. 예전엔 36열 전체를 물들여서
    #    T+N 수익률 칸의 빨강/파랑 글자가 채널 배경색 위에 겹쳐 지저분했음(수익률은 흰 바탕이 잘 보임).
    channel_bg_range = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1 + n_rows,
                        "startColumnIndex": 0, "endColumnIndex": 17}
    for ch, (r, g, b) in channel_colors.items():
        requests_list.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [channel_bg_range],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f'=$C2="{ch}"'}]},
                        "format": {"backgroundColor": {"red": r, "green": g, "blue": b}}
                    }
                },
                "index": 0
            }
        })

    # 🆕 [등락률 색상] 종목/지수 T+N 수익률 칸(음수=파랑, 양수=빨강) — apply_backtest_formatting과 같은
    #    requests_list/배치에 같이 실어서 처리. 별도 함수 호출로 나누면 그 함수가 시작할 때 기존 조건부
    #    서식을 전부 지우는 방식이라, 방금 위에서 건 채널 배경색까지 같이 지워버리는 충돌이 생기기 때문.
    t_n_cols = [17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28, 29, 30, 31]  # 종목/지수 T+1,3,5,10,20,60,120
    for ci in t_n_cols:
        col_letter = _col_letter(ci)
        col_rng = {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 1 + n_rows, "startColumnIndex": ci, "endColumnIndex": ci + 1}
        requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [col_rng], "booleanRule": {
            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f'=LEFT({col_letter}2,1)="-"'}]},
            "format": {"textFormat": {"foregroundColor": {"red": 0.15, "green": 0.35, "blue": 0.95}, "bold": True}}}}, "index": 0}})
        requests_list.append({"addConditionalFormatRule": {"rule": {"ranges": [col_rng], "booleanRule": {
            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": f'=AND(LEFT({col_letter}2,1)<>"-",{col_letter}2<>"0.00%",{col_letter}2<>"")'}]},
            "format": {"textFormat": {"foregroundColor": {"red": 0.85, "green": 0.1, "blue": 0.1}, "bold": True}}}}, "index": 0}})

    # ③ 기존 테두리 초기화(재정렬로 행 위치가 바뀌므로, 새로 계산한 구분선만 남도록 먼저 지움)
    requests_list.append({
        "updateBorders": {
            "range": grid_range,
            "top": {"style": "NONE"}, "bottom": {"style": "NONE"},
            "left": {"style": "NONE"}, "right": {"style": "NONE"},
            "innerHorizontal": {"style": "NONE"}, "innerVertical": {"style": "NONE"}
        }
    })

    # ④ 날짜(B열=진입일)가 바뀌는 행 위쪽에 실제 구분선(굵은 테두리) 적용
    border_count = 0
    prev_date = None
    for idx, row in enumerate(sorted_rows):
        cur_date = str(row[1]) if len(row) > 1 else ""
        if prev_date is not None and cur_date != prev_date:
            sheet_row_idx = idx + 1  # 헤더가 0행이므로, 데이터의 idx번째 행은 시트상 (idx+1)번째(0-based) 행
            requests_list.append({
                "updateBorders": {
                    "range": {"sheetId": sheet_id, "startRowIndex": sheet_row_idx, "endRowIndex": sheet_row_idx + 1, "startColumnIndex": 0, "endColumnIndex": 38},
                    "top": {"style": "SOLID_THICK", "color": {"red": 0.12, "green": 0.12, "blue": 0.12}}
                }
            })
            border_count += 1
        prev_date = cur_date

    doc.batch_update({"requests": requests_list})
    print(f"✅ [백테스트_로그 서식] 채널별 색상 {len(channel_colors)}종 + 날짜 구분선 {border_count}개 적용 완료")


def get_market_name(code):
    # 종목 상장시장(KOSPI/KOSDAQ) → 벤치마크 지수 매칭. 실패 시 KOSPI 기본.
    try:
        t = str(code).replace("'", "").strip().zfill(6)
        j = GLOBAL_SESSION.get(f"https://m.stock.naver.com/api/stock/{t}/basic", verify=False, timeout=3).json()
        nm = str(j.get("stockExchangeName", "")).upper()
        return nm if nm in ("KOSPI", "KOSDAQ") else "KOSPI"
    except Exception:
        return "KOSPI"

def get_index_close(index_name):
    # 지수 당일 종가(fchart 마지막 일봉) — 진입지수(P열)용
    try:
        sym = "KOSDAQ" if str(index_name).upper() == "KOSDAQ" else "KOSPI"
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={sym}&timeframe=day&count=3&requestType=0"
        root = ET.fromstring(GLOBAL_SESSION.get(url, verify=False, timeout=4).text)
        items = root.findall(".//item")
        if items:
            return float(items[-1].get("data").split("|")[4])
    except Exception as e:
        print(f"⚠️ [get_index_close {index_name}] {e}")
    return 0.0

def get_daily_bars(symbol, count=80):
    # fchart 일봉 OHLC 시리즈 → [{date, open, high, low, close}]. 종목·지수(KOSPI/KOSDAQ) 동일 포맷.
    # 이 시리즈 자체가 '거래일 달력'(주말·공휴일 자동 제외)이라 거래일수/호라이즌 인덱싱에 그대로 사용.
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={symbol}&timeframe=day&count={count}&requestType=0"
        root = ET.fromstring(GLOBAL_SESSION.get(url, verify=False, timeout=5).text)
        bars = []
        for item in root.findall(".//item"):
            raw = item.get("data")
            if not raw: continue
            d = raw.split("|")
            if len(d) < 5: continue
            rd = d[0]
            bars.append({
                'date': f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}",
                'open': float(d[1]), 'high': float(d[2]), 'low': float(d[3]), 'close': float(d[4])
            })
        return bars
    except Exception as e:
        print(f"⚠️ [get_daily_bars {symbol}] {e}")
        return []

# ==================================================================
# 📊 [핵심 연산 레이어]: 순서 교정 및 선형 구조화 엔진 패치 완료
# ==================================================================
def analyze_single_stock(name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate, past_theme_map, static_db, theme_historical_max, long_term_stocks, index_above_ma5):
    time.sleep(random.uniform(0.1, 0.4))
    
    fail_fallback = [
        name, f"'{code}", 0, "0.00%", 0, 0, "전일비 100%", "⚡ 관망 (데이터 수집 오류)",
        "⏸ 관망 · 조건미달", "AI 브리핑 대기중", 0, 0, 0, 0, "🟡 일반형", "📉 이격 과다",
        "100.0%", "평범(X)", "🟡 [V.평년수준]", "개별주/기타", "⚪ [수급강도 평년] 1.0배", 0,
        "🏦기:0.0억 / 🌎외:0.0억", 0, 0, "NORMAL", "", "", "정규장", 0, "0점 (오류)",
        0, "0점 (V2오류)", "GATE_FAIL"
    ]

    try:
        # 👑 [핵심 수정]: NameError 및 UnboundLocalError 연쇄 차단을 위한 전체 핵심 변수 선방 초기화
        target_price = 0
        stop_loss = 0
        is_upper_limit = False
        is_envelope_over_under = False
        is_foreigner_active_buy = False
        is_strong_dual_buy = False
        is_jongbe_cand = False
        is_accumulation_cand = False
        is_platform_breakout = False
        is_super_leader = False
        is_long_term_pick = False
        is_absolute_protected = False
        is_theme_daejang = False
        is_theme_hubal = False
        is_theme_leader_raw = False
        is_fatal_drop = False
        is_junk = False
        is_financial_risk = False
        is_chronic_loss = False
        is_dual_accumulation = False
        is_dual_outflow = False
        is_overheated_chase = False
        
        i_buy_today = 0
        f_buy_today = 0
        acc_i_buy_eok = 0.0
        acc_f_buy_eok = 0.0
        pgtr_ntby_eok = 0.0
        smi_ratio = 1.0
        turnover_rate = 0.0
        vol_ratio_yest = 100.0
        vol_ratio_10d = 100.0
        trading_value = 0
        
        supply_text = ""
        program_text = "⚪ [수급강도 평년] 1.0배 / 프로그램:0.0억"
        signal = "⚡ 관망 (이격발생)"
        master_tajeom = "⏸ 관망 · 조건미달"
        secret_tajeom = ""
        trend_phase = "NORMAL"
        track_type = "눌림"

        desktop_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        # ── [레이어 1] fchart 일봉 데이터 동기화 ──
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        try:
            res = GLOBAL_SESSION.get(url, verify=False, timeout=3)
            root = ET.fromstring(res.text)
        except Exception as e:
            print(f"⚠️ [analyze_single_stock fchart Network Error for {name}] {e}")
            return fail_fallback, None

        history = []
        high_prices = []
        max_hist_tv_krw = 0

        for item in root.findall(".//item"):
            raw_data = item.get("data")
            if not raw_data: continue
            data = raw_data.split("|")
            if len(data) < 6: continue
            open_p, high_p, low_p, close_p, vol = int(data[1]), int(data[2]), int(data[3]), int(data[4]), int(data[5])
            if vol == 0: continue
            day_tv_krw = close_p * vol
            if day_tv_krw > max_hist_tv_krw: max_hist_tv_krw = day_tv_krw
            history.append({"date": data[0], "open": open_p, "high": high_p, "low": low_p, "close": close_p, "volume": vol, "trading_value": day_tv_krw})
            high_prices.append(high_p)

        if len(history) < 2: return fail_fallback, None

        last_day = history[-1]
        df_hist = pd.DataFrame(history)

        open_price = last_day['open']
        today_high = last_day['high']
        today_low = last_day['low']
        current_price = last_day['close']
        today_vol = last_day['volume']

        today_str_ymd = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        if last_day['date'] == today_str_ymd:
            yest_close = int(df_hist['close'].iloc[-2]) if len(df_hist) >= 2 else current_price
        else:
            yest_close = current_price

        change_rate = (current_price - yest_close) / yest_close if yest_close > 0 else 0.0

        # ── [레이어 2] 네이버 실시간 주가 API 동기화 ──
        live_success = False
        try:
            rt_url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            rt_json = GLOBAL_SESSION.get(rt_url, headers=desktop_headers, verify=False, timeout=2).json()
            if rt_json and rt_json.get('closePrice'):
                live_p = int(str(rt_json['closePrice']).replace(',', '').strip())
                if live_p > 0:
                    current_price = live_p
                    if rt_json.get('accumulatedTradingVolume'): today_vol = int(str(rt_json['accumulatedTradingVolume']).replace(',', '').strip())
                    if rt_json.get('highPrice'): today_high = int(str(rt_json['highPrice']).replace(',', '').strip())
                    if rt_json.get('lowPrice'): today_low = int(str(rt_json['lowPrice']).replace(',', '').strip())
                    if rt_json.get('openPrice'): open_price = int(str(rt_json['openPrice']).replace(',', '').strip())
                    if rt_json.get('fluctuationsRatio'): change_rate = float(str(rt_json['fluctuationsRatio']).replace('%', '').replace('+', '').strip()) / 100.0
                    live_success = True
        except Exception as e:
            print(f"⚠️ [analyze_single_stock Live API 1 Exception for {name}] {e}")

        static_info = static_db.get(code)
        if static_info:
            market_cap, is_junk, is_financial_risk, is_chronic_loss = static_info['market_cap'], static_info['is_junk'], static_info['is_fin_risk'], static_info['is_chronic_loss']
        else:
            market_cap, is_junk, is_financial_risk, is_chronic_loss = get_market_cap(code), False, False, False
            
        is_fatal_drop = is_junk or is_financial_risk

        krx_close, nxt_close = 0, 0
        now_kst_api = datetime.datetime.now(KST)
        is_regular_market = (9 <= now_kst_api.hour < 15) or (now_kst_api.hour == 15 and now_kst_api.minute <= 40)
        market_type = "정규장 진행중" if is_regular_market else "정규장"

        if not is_regular_market:
            try: krx_close, nxt_close = fetch_extra_closing_prices_from_kis(code, session_obj=GLOBAL_SESSION)
            except Exception as e: print(f"⚠️ [analyze_single_stock fetch_extra_closing_prices Exception for {name}] {e}")
            if nxt_close > 0:
                krx_close = 0
                market_type = "NXT"
            elif krx_close > 0:
                market_type = "KRX"

        krx_rate = ((krx_close - current_price) / current_price * 100) if krx_close > 0 and current_price > 0 else 0.0
        nxt_rate = ((nxt_close - current_price) / current_price * 100) if nxt_close > 0 and current_price > 0 else 0.0

        # ── [레이어 3] 기술적 보조 지표 및 수렴 필터 연산 ──
        is_upper_limit = change_rate >= 0.295
        yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) >= 2 else today_vol
        trading_value = current_price * today_vol

        ma5 = int(df_hist['close'].tail(5).mean()) if len(df_hist) >= 5 else current_price
        ma20 = int(df_hist['close'].tail(20).mean()) if len(df_hist) >= 20 else current_price
        ma60 = int(df_hist['close'].tail(60).mean()) if len(df_hist) >= 60 else current_price
        # 🆕 [추세템플릿] 미너비니 스타일 8계명 체크용 — 50일선·150일선·200일선, 그리고 200일선이
        #    한 달 전보다 우상향인지 비교하기 위한 "한 달 전 시점의 200일선"까지 계산
        ma50 = int(df_hist['close'].tail(50).mean()) if len(df_hist) >= 50 else None
        ma150 = int(df_hist['close'].tail(150).mean()) if len(df_hist) >= 150 else None
        ma200 = int(df_hist['close'].tail(200).mean()) if len(df_hist) >= 200 else None
        ma200_1mo_ago = int(df_hist['close'].iloc[-220:-200].mean()) if len(df_hist) >= 220 else None
        low_52w = int(df_hist['low'].min()) if len(df_hist) > 0 else current_price

        # 🆕 [RS등급] 전종목 상대순위를 매기기 위한 원점수 — 최근 분기에 더 큰 가중치를 주는 방식
        #    (3개월 40% + 6·9·12개월 각 20%). 스캔이 다 끝난 뒤 전종목을 놓고 백분위로 변환해서
        #    최종 RS등급(1~99)이 나옴 — 이 시점의 raw_rs_score는 그 전 단계의 원재료일 뿐.
        close_list = df_hist['close'].tolist()

        def _perf(days):
            if len(close_list) <= days: return None
            base = close_list[-1 - days]
            return (close_list[-1] - base) / base if base > 0 else None

        p3, p6, p9, p12 = _perf(63), _perf(126), _perf(189), _perf(250)
        _periods = [(p3, 0.4), (p6, 0.2), (p9, 0.2), (p12, 0.2)]
        _valid = [(p, w) for p, w in _periods if p is not None]
        if _valid and p3 is not None:  # 최근 3개월 데이터는 최소한 있어야 의미 있는 점수로 인정
            _wsum = sum(w for _, w in _valid)
            raw_rs_score = sum(p * w for p, w in _valid) / _wsum
        else:
            raw_rs_score = None

        high_prices_120 = high_prices[-120:] if len(high_prices) >= 120 else high_prices
        low_prices_120 = [h['low'] for h in history[-120:]] if len(history) >= 120 else [h['low'] for h in history]
        highest_120d = max(high_prices_120[:-1]) if len(high_prices_120) > 1 else today_high
        lowest_120d = min(low_prices_120[:-1]) if len(low_prices_120) > 1 else today_low
        ilmok_120_mid = (highest_120d + lowest_120d) / 2
        is_ilmok_sangsang = current_price > ilmok_120_mid

        # ── [준비 완료 / 기본 미적용] 과매도(역배팅) 이격률 임계값 완화 ──────────────────
        # 왜 만들어 뒀나 (2026-08-16 실측 + BNF(小手川隆) 기법 조사):
        #   · 현행 -20% 고정은 문턱이 너무 높다. 스캔된 651종목 중 조건 충족이 2종목(0.3%)뿐이라
        #     과매도 채널이 사실상 죽어 있고, 표본이 안 쌓여 검증 자체가 불가능하다.
        #   · BNF의 원 기법은 고정값이 아니라 국면·업종별 가변이었다(강세장 -20% / 약세장 -35%,
        #     업종별 5~15%). 목표가를 ma20(=이격률 0% 회귀)으로 잡은 현행 로직은 이미 BNF와 같다.
        #   · 즉 바꿔야 할 것은 청산이 아니라 '진입 문턱'이다.
        # 왜 지금 켜지 않나: 켜면 과매도 진입 건수가 늘어 그 시점부터 표본 성격이 달라진다.
        #   3주 뒤 재점검(2026-09-07 예약) 때 다른 항목들과 함께 판단할 것.
        # 켜는 법: ENVELOPE_BAND=on  (문턱 조정은 ENVELOPE_PCT_NORMAL / ENVELOPE_PCT_WARNING, 단위 %)
        #   기본 완화값은 평시 -12%(BNF 전기주 기준대), 경계장 -20%(현행 유지)로 국면 연동한다.
        if os.environ.get("ENVELOPE_BAND", "off").strip().lower() in ("on", "true", "1"):
            try: _env_normal = float(os.environ.get("ENVELOPE_PCT_NORMAL", "12"))
            except Exception: _env_normal = 12.0
            try: _env_warn = float(os.environ.get("ENVELOPE_PCT_WARNING", "20"))
            except Exception: _env_warn = 20.0
            _env_pct = _env_warn if is_warning_market else _env_normal
            envelope_lower_20 = ma20 * (1 - _env_pct / 100.0)
        else:
            envelope_lower_20 = ma20 * 0.80   # 현행: -20% 고정
        min_nulim_tv = 10_000_000_000 if is_warning_market else 5_000_000_000
        min_breakout_tv = 10_000_000_000  
        min_danta_rate = 0.03            
        is_envelope_over_under = (current_price <= envelope_lower_20 and trading_value >= min_nulim_tv and not is_upper_limit and change_rate <= 0.10)

        high_60d_calc = max(high_prices[-60:-1]) if len(high_prices) >= 60 else today_high
        high_250d_calc = max(high_prices[:-1]) if len(high_prices) > 1 else today_high
        display_high_60d = max(high_prices[-60:]) if len(high_prices) >= 60 else today_high
        display_high_250d = max(high_prices) if high_prices else today_high

        recent_20d_min = int(df_hist['low'].tail(20).min())
        recent_60d_min = int(df_hist['low'].tail(60).min())
        is_double_bottom = (current_price <= recent_20d_min * 1.05) and (recent_20d_min >= recent_60d_min * 0.95)

        surge_rate_60d_top = (current_price - high_60d_calc) / high_60d_calc if high_60d_calc > 0 else 0
        is_deep_correction = surge_rate_60d_top <= -0.15

        surge_rate_60d_bottom = (current_price - recent_60d_min) / recent_60d_min if recent_60d_min > 0 else 0
        is_recent_overheated = surge_rate_60d_bottom >= 0.50

        min_250d = int(df_hist['close'].min())
        surge_rate_250d = (current_price - min_250d) / min_250d if min_250d > 0 else 0
        is_true_history_leader = 0.5 <= surge_rate_250d < 2.0

        body_top = max(current_price, open_price)
        body_bottom = min(current_price, open_price)
        upper_shadow = today_high - body_top
        real_body = body_top - body_bottom
        upper_shadow_ratio = upper_shadow / current_price if current_price > 0 else 0
        is_today_yangbong = current_price >= open_price

        is_afternoon_check = (now_kst_api.hour == 15)
        is_shadow_disqualified = False
        if is_afternoon_check:
            daily_range = today_high - today_low
            if daily_range > 0 and (upper_shadow / daily_range) > 0.30: is_shadow_disqualified = True
            if upper_shadow_ratio >= 0.03: is_shadow_disqualified = True

        gap_ratio = (open_price - yest_close) / yest_close if yest_close > 0 else 0
        is_huge_gap = gap_ratio >= 0.04

        avg_vol_10 = df_hist['volume'].tail(11).head(10).mean() if len(df_hist) >= 2 else today_vol
        vol_ratio_10d = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
        vol_ratio_yest = (today_vol / yest_vol) * 100 if yest_vol > 0 else 0
        surge_rate_20d = (current_price - recent_20d_min) / recent_20d_min if recent_20d_min > 0 else 0

        is_near_high = current_price >= (high_60d_calc * 0.90) or yest_close >= (high_60d_calc * 0.90)
        is_near_52w_high = current_price >= (high_250d_calc * 0.90) or yest_close >= (high_250d_calc * 0.90)

        if is_near_52w_high: dist_text = "🎯 52주신고가 턱밑"
        elif is_near_high: dist_text = "🎯 60일전고 턱밑"
        elif current_price >= high_60d_calc * 0.80: dist_text = "🟢 매물대 소화중"
        elif is_deep_correction: dist_text = "📉 고점 대비 큰 폭 조정"
        else: dist_text = "📉 이격 과다"

        # ── [레이어 4] Adaptive 칼만 필터 추세 가속 연산 ──
        try:
            high_low   = df_hist['high'] - df_hist['low']
            high_close = (df_hist['high'] - df_hist['close'].shift()).abs()
            low_close  = (df_hist['low']  - df_hist['close'].shift()).abs()
            tr         = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr_14     = tr.rolling(14).mean().iloc[-1]
            if pd.isna(atr_14) or atr_14 == 0: atr_14 = current_price * 0.03

            volatility_ratio = atr_14 / current_price if current_price > 0 else 0.03
            Q_base = max(1e-5, min(1e-3, volatility_ratio * 0.01))
            R      = max(5e-3, min(5e-2, volatility_ratio * 0.3))

            prices = df_hist['close'].values
            kalman_ma        = []
            innovation_hist  = []
            x_hat, p         = float(prices[0]), 1.0

            for z in prices:
                p_hat = p + Q_base
                K_init = p_hat / (p_hat + R)
                innovation = abs(float(z) - x_hat)
                innovation_hist.append(innovation)

                if len(innovation_hist) >= 5:
                    recent_innov = sum(innovation_hist[-5:]) / 5
                    Q_adaptive   = Q_base * (1 + recent_innov / max(x_hat * 0.01, 1e-6))
                    Q_adaptive   = max(1e-5, min(5e-3, Q_adaptive))
                    p_hat        = p + Q_adaptive
                    K            = p_hat / (p_hat + R)
                else: K = K_init

                x_hat = x_hat + K * (float(z) - x_hat)
                p     = (1 - K) * p_hat
                kalman_ma.append(x_hat)

            if len(kalman_ma) >= 11:
                slope_1  = kalman_ma[-1] - kalman_ma[-2]
                slope_3  = (kalman_ma[-1] - kalman_ma[-4]) / 3
                accel    = slope_1 - slope_3
            else: slope_1 = slope_3 = accel = 0.0

            slope_pct = slope_1 / current_price * 100 if current_price > 0 else 0
            accel_pct = accel   / current_price * 100 if current_price > 0 else 0

            if   slope_pct > 0.3  and accel_pct >  0.05:  trend_phase = "ACCELERATION"
            elif slope_pct > 0.1  and accel_pct >= -0.05: trend_phase = "STEADY"
            elif slope_pct > 0    and accel_pct <  -0.05: trend_phase = "DECELERATION"
            else:                                         trend_phase = "REVERSAL"

            is_kalman_uptrend   = slope_pct > 0.05
            is_kalman_downtrend = slope_pct < -0.05

            min_slope_th = current_price * 0.0008
            if len(kalman_ma) >= 4:
                z_now  = kalman_ma[-1] - kalman_ma[-2]
                z_prev = kalman_ma[-2] - kalman_ma[-3]
                kalman_turned_green = (z_now > min_slope_th and z_prev > 0 and kalman_ma[-2] >= kalman_ma[-3] and kalman_ma[-3] <= kalman_ma[-4])
                kalman_turned_red   = (z_now < -min_slope_th and z_prev < 0 and kalman_ma[-2] <= kalman_ma[-3] and kalman_ma[-3] >= kalman_ma[-4])
            else: kalman_turned_green = kalman_turned_red = False

            trend_length       = 0
            trend_start_kalman = kalman_ma[-1]
            for i in range(len(kalman_ma) - 1, 0, -1):
                if kalman_ma[i] > kalman_ma[i - 1]:
                    trend_length      += 1
                    trend_start_kalman = kalman_ma[i - 1]
                else: break

            atr_climb = (kalman_ma[-1] - trend_start_kalman) / atr_14 if atr_14 > 0 else 0.0

            if kalman_turned_green: secret_tajeom = "🟢 전환"
            elif is_kalman_uptrend:
                if trend_phase == "ACCELERATION":
                    if atr_climb >= 3.0 and trend_length >= 10: secret_tajeom = "🔴 3파 익절"
                    else: secret_tajeom = "🚀 가속"
                elif trend_phase == "STEADY":
                    if atr_climb >= 1.5 and trend_length >= 5: secret_tajeom = "🟡 2파 안정"
                    else: secret_tajeom = "🟢 1파 진행"
                elif trend_phase == "DECELERATION": secret_tajeom = "🟡 추세 감속"
                else: secret_tajeom = "🟢 추세 유지"
            elif kalman_turned_red: secret_tajeom = "📉 하락 전환"
            else: secret_tajeom = "📉 하락장 (관망)"
        except Exception as e:
            print(f"⚠️ [analyze_single_stock Kalman Engine Exception for {name}] {e}")
            atr_14 = current_price * 0.03
            is_kalman_uptrend = False
            kalman_turned_green = False
            kalman_turned_red = False
            trend_phase = "REVERSAL"
            secret_tajeom = ""
            slope_pct = accel_pct = 0.0
            trend_length = 0

        # ── [레이어 5] 수급 분석 및 거래량 분석 (안전 순서 상향 배치) ──
        is_volume_dead = (vol_ratio_yest <= 60) and (vol_ratio_10d <= 60)
        is_long_shadow = (upper_shadow_ratio >= 0.035) or (upper_shadow_ratio >= 0.02 and upper_shadow > real_body * 1.2) if is_warning_market else (upper_shadow_ratio >= 0.05) or (upper_shadow_ratio >= 0.025 and upper_shadow > real_body * 1.5)
        
        is_bottom_accumulation_shadow = False
        if is_long_shadow and is_today_yangbong and surge_rate_20d <= 0.15 and vol_ratio_yest >= 200:
            is_long_shadow = False
            is_bottom_accumulation_shadow = True

        if is_bottom_accumulation_shadow: shadow_text = "🌱 [캔들] 바닥권 매집봉"
        elif is_long_shadow: shadow_text = "⚠️ [캔들] 저항 출회"
        elif upper_shadow_ratio <= 0.015: shadow_text = "👑 [캔들] 몸통 마감"
        else: shadow_text = "🟡 [캔들] 일반형"

        acc_i_buy_won, acc_f_buy_won, dual_buy_days = 0, 0, 0
        i_buy_today, f_buy_today = 0, 0
        is_today_data_in_frgn = False
        today_str_dot = datetime.datetime.now(KST).strftime('%Y.%m.%d')

        # ── 수급 원자료 수집: 구 HTML 우선, 실패 시 JSON 폴백 ──
        #    두 소스의 값이 실측상 완전히 동일하므로(2026-08-26 검증) 어느 쪽을 타든 결과가 같다.
        #    아래 누적 로직은 한 벌만 두어, 소스가 바뀌어도 계산이 갈라지지 않게 한다.
        frgn_rows = []   # [(날짜 'YYYY.MM.DD', 종가, 기관수량, 외국인수량)]
        try:
            frgn_url  = f"https://finance.naver.com/item/frgn.naver?code={code}&_={int(time.time() * 1000)}"
            frgn_res  = GLOBAL_SESSION.get(frgn_url, headers=desktop_headers, verify=False, timeout=3)
            frgn_soup = BeautifulSoup(frgn_res.content, 'html.parser', from_encoding='euc-kr')
            for r_tag in frgn_soup.select("table.type2 tr"):
                cols = r_tag.select("td")
                if len(cols) >= 7 and cols[0].text.strip().replace('.', '').isdigit():
                    try: close_price_day = int(cols[1].text.strip().replace(',', ''))
                    except Exception: close_price_day = current_price
                    try: i_vol = int(cols[5].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except Exception: i_vol = 0
                    try: f_vol = int(cols[6].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except Exception: f_vol = 0
                    frgn_rows.append((cols[0].text.strip(), close_price_day, i_vol, f_vol))
                    if len(frgn_rows) >= 5: break
        except Exception as e:
            print(f"⚠️ [frgn Parsing Exception for {name}] {e}")

        if not frgn_rows:
            # 🆕 [개편 폴백] finance.naver.com 이 죽어도 수급점수(V2)가 0으로 무너지지 않게 한다.
            frgn_rows = fetch_investor_trend_json(code, 5)

        valid_days = 0
        for row_date_str, close_price_day, i_vol, f_vol in frgn_rows:
            if close_price_day <= 0: close_price_day = current_price
            i_buy_won = i_vol * close_price_day
            f_buy_won = f_vol * close_price_day

            if i_buy_won >= 50_000_000 and f_buy_won >= 50_000_000: dual_buy_days += 1
            if valid_days == 0:
                i_buy_today = i_buy_won
                f_buy_today = f_buy_won
                if row_date_str == today_str_dot: is_today_data_in_frgn = True

            acc_i_buy_won += i_buy_won
            acc_f_buy_won += f_buy_won
            valid_days += 1
            if valid_days >= 5: break
        
        pgtr_ntby_eok = 0.0  
        if KIS_TOKEN and KIS_APP_KEY and KIS_APP_SECRET:
            try:
                kis_h = {"authorization": f"Bearer {KIS_TOKEN}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P", "tr_id": "FHKST01010100"}
                kis_res = GLOBAL_SESSION.get(f"{KIS_URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", headers=kis_h, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, timeout=3).json()
                if kis_res.get("rt_cd") == "0":
                    out = kis_res["output"]
                    pgtr_qty = int(str(out.get("pgtr_ntby_qty", "0")).replace(",", "").replace("+", "") or "0")
                    pgtr_ntby_eok = (pgtr_qty * current_price) / 100_000_000
            except Exception as e:
                print(f"⚠️ [KIS pgtr Exception for {name}] {e}")

        if len(df_hist) >= 11: avg_tv_10d = df_hist['trading_value'].iloc[-11:-1].mean()
        elif len(df_hist) >= 2: avg_tv_10d = df_hist['trading_value'].iloc[:-1].mean()
        else: avg_tv_10d = trading_value

        smi_ratio = trading_value / avg_tv_10d if avg_tv_10d > 0 else 1.0
        market_cap_won = market_cap * 100_000_000
        turnover_rate = (trading_value / market_cap_won) * 100 if market_cap_won > 0 else 0.0

        pgtr_sign = "+" if pgtr_ntby_eok > 0 else ""
        pgtr_direction = pgtr_ntby_eok >= 0  

        if smi_ratio >= 5.0 and turnover_rate >= 3.0 and pgtr_direction: program_text = f"🔥 [수급강도 폭발] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio >= 2.5 and turnover_rate >= 2.0 and pgtr_direction: program_text = f"🔥 [수급강도 유입] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio >= 2.5 and not pgtr_direction: program_text = f"⚠️ [수급강도 혼조] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio <= 0.4: program_text = f"💤 [수급강도 절벽] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        else: program_text = f"⚪ [수급강도 평년] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
            
        acc_i_buy_eok = acc_i_buy_won / 100_000_000
        acc_f_buy_eok = acc_f_buy_won / 100_000_000
        today_dual_buy_ratio = ((i_buy_today + f_buy_today) / trading_value) * 100 if trading_value > 0 else 0.0
        is_foreigner_active_buy = (smi_ratio >= 3.0) and (turnover_rate >= 5.0) and (change_rate >= 0.04) and (acc_f_buy_eok >= 10) and (acc_f_buy_eok > acc_i_buy_eok)

        if dual_buy_days >= 3 and today_dual_buy_ratio >= 3.0 and i_buy_today >= 200_000_000 and f_buy_today >= 200_000_000 and acc_i_buy_eok >= 20:
            is_strong_dual_buy = True
            supply_text = " (🌟쌍끌이 모아가기)"
        elif i_buy_today >= 200_000_000 and f_buy_today >= 200_000_000: supply_text = " (🟢약한 양매수)"
        elif acc_i_buy_eok >= 20: supply_text = " (기관 누적매집)"

        is_leader_history = any((high_prices[i] - history[i-1]['close']) / history[i-1]['close'] >= 0.22 for i in range(1, len(history)) if history[i-1]['close'] > 0)
        leader_text = "🔥대장주(O)" if is_leader_history else "평범(X)"

        std20 = df_hist['close'].tail(20).std(ddof=0) if len(df_hist) >= 20 else 0
        disp_20 = (current_price / ma20) * 100 if ma20 > 0 else 100
        disp_text = f"{disp_20:.1f}%"
        upper_band = ma20 + (std20 * 2)
        lower_band = ma20 - (std20 * 2)
        band_width = (upper_band - lower_band) / ma20 if ma20 > 0 else 0

        if vol_ratio_10d <= 40: vol_status_text = "🟢 [V.에너지응축]"
        elif vol_ratio_10d <= 70: vol_status_text = "🟢 [V.거래감소]"
        elif vol_ratio_10d >= 200 and vol_ratio_yest >= 150: vol_status_text = "🔴 [V.쌍끌이폭발]"
        elif vol_ratio_10d >= 200: vol_status_text = "🔴 [V.거래과열]"
        else: vol_status_text = "🟡 [V.평년수준]"
        vol_ratio_text = f"전일비 {int(vol_ratio_yest):,}%"

        box_ratio = 999
        if len(df_hist) >= 20:
            max_20d_box, min_20d_box = int(df_hist['high'].tail(20).max()), int(df_hist['low'].tail(20).min())
            if min_20d_box > 0: box_ratio = (max_20d_box - min_20d_box) / min_20d_box

        is_converging = (band_width <= 0.20) or (ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035)
        is_platform_breakout = (box_ratio <= 0.15) and (vol_ratio_10d >= 300) and (current_price > ma20) and is_today_yangbong and (trading_value >= min_breakout_tv) and not is_shadow_disqualified

        if is_true_history_leader and is_deep_correction and not is_recent_overheated and is_volume_dead and not is_long_shadow and not is_financial_risk:
            if not is_upper_limit and ((abs(current_price - ma20) / ma20 < 0.03) or (abs(current_price - ma60) / ma60 < 0.03) or is_double_bottom):
                is_accumulation_cand = True

        is_jongbe_cand = (not is_upper_limit and not is_long_shadow and is_converging and vol_ratio_yest <= 80 and vol_ratio_10d <= 70 and current_price >= ma20 and is_near_high and not is_fatal_drop and trading_value >= min_nulim_tv)

        has_today_theme = False
        has_theme = False
        my_theme_name = "개별주/기타"

        if name in theme_rank_dict:
            my_theme_name = "🆕[당일] " + theme_rank_dict[name]['theme_name']
            is_theme_leader_raw = theme_rank_dict[name]['is_leader']
            has_theme = has_today_theme = True
        elif name in all_theme_map:
            my_theme_name = "🆕[당일] " + all_theme_map[name]['theme_name']
            is_theme_leader_raw = all_theme_map[name]['is_leader']
            has_theme = has_today_theme = True

        if not has_today_theme and name in past_theme_map:
            my_theme_name = "🕰️[과거] " + past_theme_map[name]

        is_danta_range = min_danta_rate <= change_rate < 0.295
        is_true_theme_leader = is_theme_leader_raw and (trading_value >= min_breakout_tv)
        is_theme_daejang = is_true_theme_leader and is_danta_range and not (is_junk or is_financial_risk)
        is_real_hubal = has_theme and not is_theme_leader_raw
        is_theme_hubal = is_real_hubal and is_danta_range and not (is_junk or is_financial_risk)

        if is_junk: signal = "🚨 매매제한 (관리/주의)"
        elif is_financial_risk: signal = "🚨 매매제한 (재무위험)"
        elif is_envelope_over_under: signal = "📉 하단매매 (역삼각형 스케일인)" + supply_text
        elif is_foreigner_active_buy: signal = "💎 외인 집중배팅 (Non-P)" + supply_text
        elif is_jongbe_cand: signal = "🎯 종가베팅 (M-1눌림)" + supply_text
        elif is_accumulation_cand: signal = "🌱 바닥 확인 (모아가기)" + supply_text
        elif is_platform_breakout: signal = "📦 플랫폼 탈출 (스윙)" + supply_text
        elif is_strong_dual_buy and is_converging: signal = "🌟 모아가기 (쌍끌이)"
        elif band_width <= 0.20 and current_price >= ma20: signal = ("🚀 N자파동 (밴드돌파)" if current_price >= upper_band * 0.98 else "👀 N자파동 (에너지응축)") + supply_text
        elif ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035: signal = ("📈 2차랠리 (이평수렴)" if current_price > ma20 else "⏳ 이평선 저항") + supply_text
        else: signal = ("🟢 낙폭과대 (과매도)" if current_price < lower_band else "⚡ 관망 (이격발생)") + supply_text

        track_type = "눌림" if (is_accumulation_cand or is_jongbe_cand or is_envelope_over_under) else ("돌파" if current_price >= ma20 else "눌림")
        is_core_buy_zone = (surge_rate_20d <= 0.25) and (change_rate < 0.07) and (current_price >= ma60 * 0.85)

        # 🆕 [수정] DB_중장기(산업리포트 픽) 이후 이미 크게 오른 종목은 "코어픽" 배지 제외 — 이미 시세가 다 나온 종목 대신
        #    아직 안 오른 종목만 배지가 붙도록. 이미 갖고 있는 250일치 history로 픽 날짜 시점 종가를 역추적해서 비교.
        # 🔧 [수정] 현재가만 비교하면 한 번 크게 올랐다가 눌린 종목은 배지가 다시 붙어버림 → 픽 이후 "최고가" 기준으로
        #    한 번이라도 임계값을 넘었으면 그 뒤로 계속 배제되도록 변경(되돌아와도 배지 부활 안 함).
        LONG_TERM_ALREADY_SURGED_THRESHOLD = 0.50  # 픽 이후 최고가가 +50% 이상이면 "이미 시세 나옴"으로 간주 (조정 가능)
        is_already_surged_since_pick = False
        pick_date_str = long_term_stocks.get(name) if isinstance(long_term_stocks, dict) else None
        if pick_date_str:
            try:
                pick_date_compact = pick_date_str.replace("-", "")  # history의 date는 "yyyymmdd" 원본 포맷
                pick_price = None
                max_high_since_pick = 0
                for h in history:
                    if h['date'] >= pick_date_compact:
                        if pick_price is None:
                            pick_price = h['close']  # 픽 날짜(또는 그 이후 첫 거래일) 종가를 기준가로
                        max_high_since_pick = max(max_high_since_pick, h['high'])
                if pick_price and pick_price > 0:
                    max_pct_since_pick = (max_high_since_pick - pick_price) / pick_price
                    if max_pct_since_pick >= LONG_TERM_ALREADY_SURGED_THRESHOLD:
                        is_already_surged_since_pick = True
            except Exception as e:
                print(f"⚠️ [코어픽 픽이후상승률 계산 에러 for {name}] {e}")

        is_long_term_pick = (name in long_term_stocks) and not is_recent_overheated and is_core_buy_zone and not is_already_surged_since_pick
        
        # 👑 [안전 고도화]: NameError 파쇄용 데이터 연산 및 Suffix 동적 초기화 구문 안착
        high_retention = current_price / today_high if today_high > 0 else 0
        is_relative_strong = (kospi_rate <= -1.0) and (change_rate >= 0.03)
        master_tajeom_suffix = ""
        
        if is_relative_strong: master_tajeom_suffix += " 💪(하락장 역행)"
        if high_retention >= 0.97 and change_rate >= 0.10 and trading_value >= 100_000_000_000:
            master_tajeom_suffix += " 👑(진성대장)"

        # 👑 [중복 블록 제거]: is_super_leader는 아래 V1/V2 점수 산출에 쓰이므로 먼저 정의만 한다.
        # (배지 부착은 master_tajeom이 최종 확정된 이후로 이동 — 덮어쓰기로 인한 배지 소실 방지)
        is_super_leader = (change_rate >= 0.15) and (trading_value >= 100_000_000_000) and (smi_ratio >= 3.0)

        # ② [반등 확인 게이트] 과매도/바닥권이 '떨어지는 칼'인지, 첫 반등이 나왔는지 판별
        # 칼만 상승전환 / 칼만 상승추세 / slope 양전 / ma5 회복 / 저점 절상 양봉 중 하나라도 충족해야 '반등 확인'
        prev_low_val = int(df_hist['low'].iloc[-2]) if len(df_hist) >= 2 else today_low
        is_rebound_confirmed = (
            kalman_turned_green or is_kalman_uptrend or (slope_pct > 0) or
            (current_price >= ma5) or
            (today_low > prev_low_val and is_today_yangbong)
        )

        tajeom_multiplier = 0.0
        master_tajeom_base = "⏸ 관망 · 조건미달"

        if is_fatal_drop:
            master_tajeom_base = "🚫 매매금지 · 위험"
            tajeom_multiplier = 0.0
        elif is_envelope_over_under:
            if is_rebound_confirmed:
                master_tajeom_base = "📉 과매도 · 역배팅"
                tajeom_multiplier = 1.45
            else:
                master_tajeom_base = "⏸ 관망 · 과매도 반등 미확인 (칼날 회피)"
                tajeom_multiplier = 0.0
        elif is_foreigner_active_buy:
            master_tajeom_base = "💎 외인 역발상 매집"
            tajeom_multiplier = 1.4
        elif is_upper_limit:
            master_tajeom_base = "🚀 대장 · 당일단타 (상한가 안착/추격금지)"
            tajeom_multiplier = 1.3
        elif is_jongbe_cand:
            master_tajeom_base = "🎯 종베 · 관성파동"
            tajeom_multiplier = 1.3
        elif is_accumulation_cand:
            if is_rebound_confirmed:
                master_tajeom_base = "🌱 바닥 · 분할매수"
                tajeom_multiplier = 1.4
            else:
                master_tajeom_base = "⏸ 관망 · 바닥권 반등 미확인 (칼날 회피)"
                tajeom_multiplier = 0.0
        elif is_theme_daejang:
            master_tajeom_base = "🚀 대장 · 당일단타"
            tajeom_multiplier = 1.3
        elif is_theme_hubal:
            master_tajeom_base = "🚀 테마 후발주"
            tajeom_multiplier = 1.15
        elif is_platform_breakout:
            master_tajeom_base = "📦 박스 돌파 · 스윙"
            tajeom_multiplier = 1.25
        elif "1차" in secret_tajeom or "🟢 전환" in secret_tajeom:
            master_tajeom_base = "🔍 칼만 전환 · 관심"
            tajeom_multiplier = 1.35
        elif ("🌟" in signal):
            master_tajeom_base = "🌟 기준봉 포착"
            tajeom_multiplier = 0.9
        else:
            master_tajeom_base = "⏸ 관망 · 조건미달"
            tajeom_multiplier = 0.6

        master_tajeom = master_tajeom_base + master_tajeom_suffix

        if is_warning_market and track_type == "돌파":
            tajeom_multiplier = 0.0
            master_tajeom = "⏸ 관망 · 하락장 돌파매매 금지 조항 적용"

        if not is_fatal_drop and tajeom_multiplier > 0.0:
            if not is_envelope_over_under and (is_long_shadow or is_huge_gap):
                master_tajeom += " ⚠️(윗꼬리/이격)"
                if is_warning_market and is_long_shadow and not (is_foreigner_active_buy or is_long_term_pick):
                    tajeom_multiplier = 0.0
                    master_tajeom = "⏸ 관망 · 윗꼬리 리스크 과다"
                else: tajeom_multiplier -= 0.3
            # ③ 과매도(역배팅)도 하락 추세 전환/3파 익절 veto 적용 — 떨어지는 칼 회피 (면제 조항 제거)
            if "3파 익절" in secret_tajeom or "하락 전환" in secret_tajeom:
                tajeom_multiplier = 0.0
                master_tajeom = "⏸ 관망 · 3차 파동/하락전환 고점 리스크"

        gijunbong_open = 0
        if len(df_hist) >= 1:
            recent_20 = df_hist.tail(20)
            max_tv_idx = recent_20['trading_value'].idxmax()
            gijunbong_open = int(recent_20.loc[max_tv_idx, 'open'])

        if is_envelope_over_under:
            target_price = int(ma20)
            stop_loss = int(current_price * 0.93)
        else:
            if is_accumulation_cand or is_long_term_pick:
                if gijunbong_open > 0 and gijunbong_open < current_price: stop_loss = gijunbong_open
                else: stop_loss = int(min(ma60, recent_60d_min * 1.02))
                target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.15)
            elif is_kalman_uptrend:
                target_price = int(current_price + (atr_14 * 2.0))
                stop_loss = int(current_price - (atr_14 * 1.0))
            else:
                target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.05)
                stop_loss = int(min(ma20, current_price * 0.95))

        if is_foreigner_active_buy and not is_envelope_over_under:
            stop_loss = int(current_price * 0.96) if not (is_accumulation_cand or is_long_term_pick) else stop_loss
            target_price = int(current_price * 1.15) if not (is_accumulation_cand or is_long_term_pick) else target_price

        # ==========================================================================
        # 🛡️ [목표가/손절가 방어망] 데이터 검증 + 손익비 기반 목표가 재선택 + 손절폭 상하한
        #    (외인 역발상 매집 재조정까지 다 반영된 "최종" target_price/stop_loss를 검증)
        # ==========================================================================

        # ① [데이터 검증] 60일 고가는 52주(250일) 고가를 수학적으로 넘을 수 없음 — 넘으면 이상치로 보고 눌러줌
        if display_high_60d > display_high_250d and display_high_250d > 0:
            display_high_60d = display_high_250d
            if is_accumulation_cand or is_long_term_pick or (not is_kalman_uptrend and not is_envelope_over_under):
                target_price = int(display_high_60d) if display_high_60d > current_price else target_price

        MIN_RR_RATIO = 1.5     # 최소 손익비(상승여력 ÷ 하락위험) — 이 밑이면 목표가를 투사 방식으로 재선택
        MIN_STOP_PCT = 0.035   # 손절폭 하한: 현재가 대비 -3.5%보다 타이트하면 정상 변동에도 털릴 위험
        MAX_STOP_PCT = 0.16    # 손절폭 상한: 현재가 대비 -16%를 넘으면 손실 자체가 과도

        is_stop_too_wide = False
        if not is_envelope_over_under and target_price > 0 and stop_loss > 0 and current_price > 0:
            risk = current_price - stop_loss
            reward = target_price - current_price

            # ② [손익비 기반 목표가 재선택] 과거 저항선(60일고가 등) 기준 목표가로는 손익비가 안 나오는 경우
            #    (예: 이미 전고점 코앞까지 올라온 종목) → 미래 투사(ATR 기반) 목표가로 자동 전환
            if risk > 0 and reward / risk < MIN_RR_RATIO:
                projected_target = int(current_price + (atr_14 * 2.5))
                if projected_target > target_price:
                    target_price = projected_target

            # ③ [손절폭 상하한] 계산 방식과 무관하게, 현재가 대비 손절폭이 항상 정상 범위 안에 있도록 강제
            min_stop_price = int(current_price * (1 - MAX_STOP_PCT))  # 이보다 낮으면(=폭이 더 넓으면) 안 됨
            max_stop_price = int(current_price * (1 - MIN_STOP_PCT))  # 이보다 높으면(=폭이 더 좁으면) 안 됨
            if stop_loss < min_stop_price:
                is_stop_too_wide = True  # 보정 "전" 상태를 먼저 기록해둬야 아래 ④에서 진짜 과도했는지 판단 가능
            stop_loss = max(min_stop_price, min(stop_loss, max_stop_price))

        # ④ [손절폭 과도 시 추천 자체 취소] 폭만 억지로 좁혀서 숫자를 그럴듯하게 만드는 대신,
        #    애초에 정상적인 손절 범위를 못 잡을 만큼 불안정한 종목이라는 뜻이므로 관망으로 강등
        if is_stop_too_wide and not is_envelope_over_under and "관망" not in master_tajeom and "매매금지" not in master_tajeom:
            master_tajeom = "⏸ 관망 · 손절폭 과도(정상범위 밖)"
            tajeom_multiplier = 0.0

        if secret_tajeom and "관망" not in master_tajeom and "매수금지" not in master_tajeom and not is_upper_limit:
            master_tajeom = f"{master_tajeom} | {secret_tajeom}"

        # 👑 [배지 부착 — 최종 위치]: master_tajeom이 완전히 확정된 이후에 부착해야 덮어쓰기로 소실되지 않는다.
        #    (🔧 "관망" 상태로 강등된 경우엔 모순되는 강세 배지가 덧붙지 않도록 가드 추가)
        # 🆕 [추세템플릿] 미너비니 스타일 8계명 중 7개(RS등급 제외 — 전종목 상대비교가 필요해 별도 작업 필요) 체크
        minervini_diag = None
        is_minervini_template = False
        if ma50 and ma150 and ma200 and ma200_1mo_ago and low_52w > 0:
            cond1 = current_price > ma150 and current_price > ma200
            cond2 = ma150 > ma200
            cond3 = ma200 > ma200_1mo_ago
            cond4 = ma50 > ma150 and ma50 > ma200 and current_price > ma50
            cond5 = current_price >= low_52w * 1.30
            cond6 = current_price >= display_high_250d * 0.75
            is_minervini_template = cond1 and cond2 and cond3 and cond4 and cond5 and cond6
            # 🆕 [진단용] 0개가 나왔을 때 "정상적으로 희귀해서"인지 "조건이 잘못돼서"인지 구분하기 위해
            #    조건별 통과 여부를 따로 기록 — 스캔 끝난 뒤 집계해서 로그로 남김
            minervini_diag = {"has_data": True, "c1": cond1, "c2": cond2, "c3": cond3, "c4": cond4, "c5": cond5, "c6": cond6}
        else:
            minervini_diag = {"has_data": False}

        if "관망" not in master_tajeom:
            if is_foreigner_active_buy: master_tajeom += " 💎(외인집중)"
            elif is_long_term_pick:     master_tajeom += " 🎖️(코어픽)"
            elif is_super_leader:       master_tajeom += " 🔥(절대대장)"
        # 🔧 [수정] 🏆(추세템플릿)만은 관망 게이트 밖으로 분리 — 미너비니 조건을 만족하는 종목은 이미 많이
        #    올라있어 "손절폭 과도"·"3차 파동 리스크" 등 다른 사유로도 관망 판정을 같이 받을 구조적 가능성이
        #    높음. 그 경우 조건은 만족해도 배지가 영원히 안 붙는 문제가 있어, 독립적으로 붙도록 분리함.
        if is_minervini_template:
            master_tajeom += " 🏆(추세템플릿)"

        # ==========================================================================
        # 📊 [트랙 1] V1 차트 기술점수 산출 엔진
        # ==========================================================================
        v1_base = 15  
        if is_near_52w_high: v1_base += 20
        elif is_near_high: v1_base += 15
        
        if is_accumulation_cand:
            v1_base += 25
            if is_double_bottom: v1_base += 15
        elif is_jongbe_cand: v1_base += 25
        elif is_platform_breakout: v1_base += 20
        
        if current_price >= ma20: v1_base += 10
        if vol_ratio_yest >= 250: v1_base += 15
        elif is_volume_dead and (is_jongbe_cand or is_accumulation_cand): v1_base += 15
        
        if upper_shadow_ratio <= 0.015: v1_base += 10
        elif is_bottom_accumulation_shadow: v1_base += 15
        
        if trend_phase == "ACCELERATION" or secret_tajeom == "🟢 전환": v1_base += 15
        elif trend_phase == "STEADY": v1_base += 10
        if is_long_term_pick: v1_base += 15
        
        # ✨ [V1 핵심 가산점 매핑]: 면책 대신 정량 점수 버프로 전환
        if is_foreigner_active_buy: v1_base += 15
        if is_super_leader: v1_base += 20

        quant_score = int(v1_base * tajeom_multiplier)
        quant_score = min(100, max(0, quant_score))

        # ==========================================================================
        # 📊 [트랙 2] V2 수급 거래점수 산출 엔진
        # ==========================================================================
        v2_base = 15  
        if is_strong_dual_buy: v2_base += 35
        elif is_foreigner_active_buy: v2_base += 35
        elif "기관 누적매집" in supply_text: v2_base += 25
        
        if acc_i_buy_eok >= 30: v2_base += 15
        if acc_f_buy_eok >= 30: v2_base += 15
        
        if "[수급강도 폭발]" in program_text: v2_base += 25
        elif "[수급강도 유입]" in program_text: v2_base += 15
        elif "[수급강도 혼조]" in program_text: v2_base += 5
        elif "[수급강도 절벽]" in program_text: v2_base -= 20
        
        if pgtr_ntby_eok >= 30: v2_base += 15
        elif pgtr_ntby_eok <= -30: v2_base -= 20
        
        if is_theme_daejang or is_true_theme_leader: v2_base += 25
        elif is_theme_hubal or has_today_theme: v2_base += 10
        if acc_i_buy_eok <= -100 or acc_f_buy_eok <= -100: v2_base -= 15
        
        # ✨ [V2 핵심 가산점 매핑]: 면책 대신 정량 점수 버프로 전환
        if is_long_term_pick: v2_base += 15
        if is_super_leader: v2_base += 20

        cutoff_score = 40 if is_warning_market else 25
        if "⏸ 관망" not in master_tajeom and "🚫" not in master_tajeom:
            # 👑 [필터 게이트 봉쇄]: 'not is_absolute_protected' 예외조항을 완벽히 도려냈습니다.
            # 가산점을 받았음에도 퀀트/수급 스코어가 커트라인(하락장 40점)에 미달하면 국물도 없이 즉시 조건미달 탈락 처리합니다.
            if quant_score < cutoff_score and v2_base < cutoff_score and not is_envelope_over_under:
                master_tajeom = f"⏸ 관망 · 조건미달 (V1:{quant_score}점 / V2:{v2_base}점)"

        score_display = f"{quant_score}점 ({track_type})"
        is_seed_tag = "SEED" if (is_accumulation_cand or is_long_term_pick or is_envelope_over_under) else "NORMAL"

        has_s_tier = (is_strong_dual_buy or is_foreigner_active_buy or "기관 누적매집" in supply_text)
        has_a_tier = ("👑(진성대장)" in master_tajeom or is_theme_daejang or is_super_leader)
        has_b_tier = (is_jongbe_cand or is_accumulation_cand or is_platform_breakout)

        try:
            high_250d_ratio = current_price / high_250d_calc if high_250d_calc > 0 else 0.0
            is_absolute_liquidity = (trading_value >= 15_000_000_000)
            is_volume_shuting = (vol_ratio_yest >= 150.0)
            # 위치요건(고가 근처 0.70~1.00) 제거: 수급(매집)은 바닥에서도 일어나 스캐너의 종베·바닥 픽과 상충 → 수급TOP2 영구 사망.
            #   유동성(150억)+거래량폭발(전일비 150%)의 '실거래 품질'만으로 게이트 → 수급 강한 종목이 차트 위치 불문 통과.
            is_v2_gate_passed = is_absolute_liquidity and is_volume_shuting
        except Exception: is_v2_gate_passed = False   # 게이트 판정 실패 시 fail-closed(미통과)

        if is_v2_gate_passed:
            if has_s_tier: v2_quant_score = 85 + (v2_base * 0.15)
            elif has_a_tier: v2_quant_score = 70 + (v2_base * 0.15)
            elif has_b_tier: v2_quant_score = 55 + (v2_base * 0.15)
            else: v2_quant_score = 40 + (v2_base * 0.15)
        else:
            v2_quant_score = (v2_base * 0.65) + (quant_score * 0.25)

        v2_quant_score = min(100, max(0, int(v2_quant_score)))
        v2_score_display = f"{v2_quant_score}점 ({track_type}_V2)"
        v2_gate_flag = "GATE_PASS" if is_v2_gate_passed else "GATE_FAIL"  # 4번: 수급채널 오염 차단용 플래그

        i_sign = "+" if acc_i_buy_eok > 0 else ""
        f_sign = "+" if acc_f_buy_eok > 0 else ""
        frgn_label = " 🌎💎(외인대량)" if acc_f_buy_eok >= 50 else (" 🌎(외인집중)" if acc_f_buy_eok >= 20 else (" 🌎🔵(외인이탈)" if acc_f_buy_eok <= -20 else ""))
        supply_status_col = f"🏦기(5일):{i_sign}{acc_i_buy_eok:.1f}억 / 🌎외(5일):{f_sign}{acc_f_buy_eok:.1f}억{frgn_label}"
        
        krx_str = f"'+{krx_rate:.2f}% ({krx_close:,}원)" if krx_close > 0 and krx_rate > 0 else (f"'{krx_rate:.2f}% ({krx_close:,}원)" if krx_close > 0 else "")
        nxt_str = f"'+{nxt_rate:.2f}% ({nxt_close:,}원)" if nxt_close > 0 and nxt_rate > 0 else (f"'{nxt_rate:.2f}% ({nxt_close:,}원)" if nxt_close > 0 else "")

        result_row = [
            name, f"'{code}", current_price, f"{change_rate * 100:.2f}%",
            int(ma5), int(ma20), vol_ratio_text, signal,
            master_tajeom, "AI 브리핑 대기중", today_high, today_low, int(display_high_60d),
            market_cap, shadow_text, dist_text, disp_text, leader_text, vol_status_text, my_theme_name,
            program_text, int(display_high_250d), supply_status_col,
            target_price, stop_loss, is_seed_tag,
            krx_str, nxt_str, market_type, 
            quant_score, score_display,
            v2_quant_score, v2_score_display,
            raw_rs_score if raw_rs_score is not None else "",  # 🆕 인덱스 33 — 스캔 완료 후 백분위(RS등급)로 덮어써짐
            v2_gate_flag
        ]
        return result_row, minervini_diag
    except Exception as e:
        print(f"❌ 분석 에러 [{name}]: {e}")
        return fail_fallback, None

# ==================================================================
# 📡 [구글 시트 연동 레이어]: 멀티프로세싱 가동 및 V1/V2 투트랙 실증 엔진
# ==================================================================
def update_technical_data(df_theme, all_theme_map):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)

        today_date = datetime.datetime.now(KST).date()

        cleanup_and_reorder(doc, "접속로그", 1)
        cleanup_and_reorder(doc, "DB_중장기", 0)

        long_term_stocks = {}  # 🔧 [수정] 이름만 담던 set에서, "종목명 → 최신 픽 날짜" dict로 변경 (픽 이후 상승률 계산용)
        try:
            db_trend_data = doc.worksheet("DB_중장기").get_all_values()
            for row in db_trend_data[1:]:
                if len(row) >= 5:
                    pick_date = str(row[0]).strip()
                    for col_idx in [3, 4]: 
                        if len(row) > col_idx and row[col_idx].strip():
                            stock_nm = row[col_idx].split('(')[0].strip() 
                            # 같은 종목이 여러 리포트에 반복 등장하면, 가장 최신 픽 날짜를 기준으로 삼음(재확인된 픽으로 취급)
                            if stock_nm not in long_term_stocks or pick_date > long_term_stocks[stock_nm]:
                                long_term_stocks[stock_nm] = pick_date
        except Exception as e: print(f"⚠️ [DB_중장기 Parsing Error] {e}")

        print("▶️ 기술적 지표 초고속 멀티프로세싱 판독 시작...")
        is_warning_market = check_warning_market()
        kospi_rate = get_kospi_fluctuation_rate()
        index_above_ma5 = is_index_above_ma5()

        try: name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in doc.worksheet("기업정보").get_all_values()[1:] if len(row) >= 3}
        except Exception as e:
            print(f"⚠️ [기업정보 Read Error] {e}")
            name_to_code = {}

        try: static_sheet = doc.worksheet("DB_정적데이터")
        except Exception:
            static_sheet = doc.add_worksheet(title="DB_정적데이터", rows="1000", cols="6")
            static_sheet.append_row(["종목코드", "종목명", "시가총액", "관리종목", "재무위험", "만성적자"])

        now_time = datetime.datetime.now(KST)
        # 👑 [긴급 보정 1]: 정적데이터 시트 로딩 검증과 리셋 타임을 수학적으로 완전 격리분리
        is_official_reset_time = (now_time.hour == 7) or (now_time.hour == 8 and now_time.minute < 50)
        is_preserve_time = now_time.hour < 8 or (now_time.hour == 8 and now_time.minute < 50)
        is_regular_market = (9 <= now_time.hour < 15) or (now_time.hour == 15 and now_time.minute <= 40)
        
        static_db = {}
        # 🛡️ [생명주기 이전] 7시 batch_clear 폐지 — DB_정적데이터는 hyeoks_static_collector.py가 단독 소유(clear→write).
        # omakase는 순수 reader로서 '항상 읽기만' 한다. (수집기 실패 시 전일 스냅샷이 보존되어 위험게이트가 꺼지지 않음)
        junk_loaded = 0
        try:
            for row in static_sheet.get_all_values()[1:]:
                if len(row) >= 6:
                    code_key = str(row[0]).replace("'", "").strip().zfill(6)
                    cap_clean = re.sub(r'[^0-9]', '', str(row[2]))
                    static_db[code_key] = {
                        'market_cap': int(cap_clean) if cap_clean else 0,
                        'is_junk': _sheet_bool(row[3]),
                        'is_fin_risk': _sheet_bool(row[4]),
                        'is_chronic_loss': _sheet_bool(row[5])
                    }
                    if static_db[code_key]['is_junk']:
                        junk_loaded += 1
        except Exception as e: print(f"⚠️ [Static Sheet Read Error] {e}")

        # 🚨 [조용한 실패 방어] 위험종목 게이트(관리종목·거래정지·투자경고)는 깨져도 예외가 나지 않는다.
        #    그냥 빈 명단이 되어 걸러져야 할 종목이 조용히 추천에 섞인다 — 가장 위험한 실패 방식이다.
        #    실제로 2026-08-26까지 이 게이트가 통째로 열려 있었다(시트는 'TRUE', 코드는 'True'와 비교해
        #    275종목 전부가 False로 읽혔고, 백테스트 픽 13건이 위험종목이었다).
        #    → 명단이 비정상적으로 작으면 정상으로 간주하지 말고 크게 알린다. 스캔 자체는 계속하되
        #      (네이버 일시 장애로 파이프라인 전체가 멈추는 게 더 나쁘다) 사람이 반드시 인지하도록 한다.
        MIN_EXPECTED_JUNK = 50   # hyeoks_static_collector.py 의 MIN_TOTAL 과 동일 기준
        if junk_loaded < MIN_EXPECTED_JUNK:
            alarm = (f"🚨 [위험종목 게이트 이상] DB_정적데이터에서 읽어낸 is_junk 종목이 "
                     f"{junk_loaded}개뿐입니다(기대 {MIN_EXPECTED_JUNK}개 이상). "
                     f"관리종목·거래정지·투자경고가 걸러지지 않고 추천에 섞일 수 있습니다. "
                     f"hyeoks_static_collector.py 수집 결과와 시트 값 형식을 즉시 확인하세요.")
            print(alarm)
            send_telegram_alert(alarm)
        else:
            print(f"🛡️ [위험종목 게이트] is_junk {junk_loaded}종목 로드 완료 (정상)")

        theme_rank_dict = {}
        try:
            realtime_data = doc.worksheet("수급_실시간").get_all_values()
            if len(realtime_data) > 1:
                header = realtime_data[0]
                date_idx = header.index('날짜') if '날짜' in header else 0
                rank_idx = header.index('순위') if '순위' in header else 2
                theme_idx = header.index('테마명') if '테마명' in header else 3
                name_idx = header.index('종목명') if '종목명' in header else 4
                latest_date_str = str(realtime_data[1][date_idx]).strip()
                theme_rank_tracker = {}
                for row in realtime_data[1:]:
                    if len(row) > max(date_idx, rank_idx, theme_idx, name_idx):
                        if str(row[date_idx]).strip() == latest_date_str:
                            try: t_rank = int(row[rank_idx])
                            except Exception: continue
                            t_name = str(row[theme_idx]).strip()
                            s_name = str(row[name_idx]).strip()
                            if t_rank not in theme_rank_tracker: theme_rank_tracker[t_rank] = []
                            theme_rank_tracker[t_rank].append(s_name)
                            theme_rank_dict[s_name] = {'theme_rank': t_rank, 'theme_name': t_name, 'is_leader': False}
                            all_theme_map[s_name] = {'theme_name': t_name, 'is_leader': False}
                for s_name, info in theme_rank_dict.items():
                    t_rank = info['theme_rank']
                    if t_rank in theme_rank_tracker and len(theme_rank_tracker[t_rank]) > 0:
                        is_leader = (theme_rank_tracker[t_rank][0] == s_name)
                        theme_rank_dict[s_name]['is_leader'] = is_leader
                        all_theme_map[s_name]['is_leader'] = is_leader
            else: today_date = datetime.datetime.now(KST).date()
        except Exception as e:
            print(f"⚠️ [realtime_data Step Error] {e}")
            today_date = datetime.datetime.now(KST).date()

        past_theme_map = {}
        try:
            three_months_ago = today_date - datetime.timedelta(days=90)
            for sheet_name in ["수급_Raw", "수급_실시간"]:
                try:
                    raw_data = doc.worksheet(sheet_name).get_all_values()
                    if len(raw_data) > 1:
                        header = raw_data[0]
                        date_idx = header.index('날짜') if '날짜' in header else 0
                        theme_idx = header.index('테마명') if '테마명' in header else (2 if sheet_name == "수급_Raw" else 3)
                        name_idx = header.index('종목명') if '종목명' in header else (3 if sheet_name == "수급_Raw" else 4)
                        for row in raw_data[1:]:
                            if len(row) > max(date_idx, theme_idx, name_idx):
                                r_date_str = str(row[date_idx]).strip()
                                s_name = str(row[name_idx]).strip()
                                t_name = str(row[theme_idx]).strip()
                                if s_name and t_name and t_name != "개별주/기타":
                                    try:
                                        row_date = datetime.datetime.strptime(r_date_str, '%Y-%m-%d').date()
                                        if row_date != today_date and row_date >= three_months_ago: past_theme_map[s_name] = t_name
                                    except Exception: continue
                except Exception as e: print(f"⚠️ [past_theme_map Loop Exception for {sheet_name}] {e}")
        except Exception as e: print(f"⚠️ [past_theme_map overall block Exception] {e}")

        target_names = set()
        try:
            raw_data = doc.worksheet("수급_Raw").get_all_values()
            for row in raw_data[1:]:
                if len(row) >= 7:
                    stock_name = str(row[-4]).strip()
                    if stock_name and stock_name not in ["#REF!", "로딩중...", "데이터대기", "FALSE"]: target_names.add(stock_name)
        except Exception as e: print(f"⚠️ [target_names Raw Extraction Error] {e}")

        if not df_theme.empty:
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            for t in top_10_themes: target_names.add(str(t).strip())

        for t_name in all_theme_map.keys(): target_names.add(str(t_name).strip())

        theme_historical_max = defaultdict(int)
        target_dict = {}
        for name in list(target_names):
            code = name_to_code.get(name) or search_code_from_naver(name)
            if code and code not in target_dict.values(): target_dict[name] = code

        results = []
        minervini_diags = []
        worker_count = bounded_workers(len(target_dict))
        SCAN_DEADLINE_SEC = 480  # 🆕 [수정] 8분 예산 — 10분 간격 트리거보다 여유 있게 짧게 잡아서, 느린 회차가
        #    다음 트리거와 겹쳐 뒤로 줄줄이 밀리는 걸 방지 (예전엔 시간 제한이 아예 없어서 15분씩 걸리기도 했음)
        scan_start = time.time()
        print(f"⚡ {len(target_dict)}개 고유 종목을 {worker_count}개의 스레드로 동시 타격합니다... (최대 {SCAN_DEADLINE_SEC}초 예산)")

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=worker_count)
        future_to_name = {executor.submit(analyze_single_stock, name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate, past_theme_map, static_db, theme_historical_max, long_term_stocks, index_above_ma5): name for name, code in target_dict.items()}

        remaining = set(future_to_name.keys())
        deadline_hit = False
        while remaining:
            left = SCAN_DEADLINE_SEC - (time.time() - scan_start)
            if left <= 0:
                deadline_hit = True
                break
            done, remaining = concurrent.futures.wait(remaining, timeout=left, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                stock_name = future_to_name[future]
                try:
                    res, diag = future.result()
                    if res:
                        results.append(res)
                        if diag: minervini_diags.append(diag)
                except Exception as e:
                    print(f"⚠️ [Thread Result Error for {stock_name}] {e}")
                    continue

        if deadline_hit:
            print(f"⏱️ [시간 예산 초과] {SCAN_DEADLINE_SEC}초 경과 — 남은 {len(remaining)}개 종목은 이번 회차에서 건너뜁니다(다음 회차에 자연스럽게 다시 스캔됨).")
            executor.shutdown(wait=False, cancel_futures=True)  # 아직 시작 안 한 작업은 취소, 이미 실행 중인 건 기다리지 않고 바로 다음으로 진행
        else:
            executor.shutdown(wait=True)

        print(f"⏱️ 스캔 소요시간: {time.time() - scan_start:.1f}초 ({len(results)}/{len(target_dict)}개 종목 처리 완료)")

        # 🆕 [RS등급] 전종목을 놓고 백분위 순위를 매겨서, 각 결과의 index 33(원점수)을 최종 RS등급(1~99)으로 덮어씀
        rs_candidates = [(i, r[33]) for i, r in enumerate(results) if len(r) > 33 and isinstance(r[33], (int, float))]
        if len(rs_candidates) >= 10:  # 표본이 너무 적으면 백분위 자체가 의미 없어서 스킵
            ranked = sorted(rs_candidates, key=lambda x: x[1])
            n = len(ranked)
            for rank, (i, _) in enumerate(ranked):
                percentile = int(1 + (rank / (n - 1)) * 98) if n > 1 else 50
                results[i][33] = percentile
                if percentile >= 90 and "관망" not in str(results[i][8]) and "매매금지" not in str(results[i][8]):
                    results[i][8] = str(results[i][8]) + " ⭐(RS강세)"  # 🆕 상대강도 상위 10% 배지
            print(f"✅ [RS등급] {n}개 종목 백분위 계산 완료 (1~99)")
        else:
            for r in results:
                if len(r) > 33: r[33] = ""
            print(f"⏭ [RS등급] 표본 부족({len(rs_candidates)}개)으로 이번 회차는 건너뜀")

        # 🆕 [진단용] 미너비니 추세템플릿 0개가 "정상적으로 희귀해서"인지 "조건이 잘못돼서"인지 구분하기 위해
        #    조건별 통과 종목 수를 집계해서 로그로 남김. 특정 조건 하나만 유독 0에 가깝다면 그 조건을 의심할 것.
        with_data = [d for d in minervini_diags if d.get("has_data")]
        if with_data:
            n = len(with_data)
            c_counts = {k: sum(1 for d in with_data if d[k]) for k in ["c1", "c2", "c3", "c4", "c5", "c6"]}
            all_pass = sum(1 for d in with_data if all(d[k] for k in ["c1", "c2", "c3", "c4", "c5", "c6"]))
            print(f"🔎 [추세템플릿 진단] 150·200일선 데이터 보유 {n}개 종목 기준 — "
                  f"①현재가>150·200일선:{c_counts['c1']} ②150>200일선:{c_counts['c2']} "
                  f"③200일선우상향:{c_counts['c3']} ④50일선정배열:{c_counts['c4']} "
                  f"⑤신저가+30%:{c_counts['c5']} ⑥신고가-25%:{c_counts['c6']} → 전체충족:{all_pass}개")

        results.sort(key=lambda x: x[29] if len(x) > 29 else 0, reverse=True)

        # 🆕 [실적 악화 경고] DB_중장기(장기 보유 후보) 종목 중, 실적점수(V3)가 낮게 확인되는 종목에
        #    경고 배지를 붙임 — "사놓고 방치"가 아니라 실적이 꺾이면 눈에 띄게 하려는 목적.
        #    V3 데이터가 아직 없는 종목은 판단 근거가 없으니 경고하지 않음(fail-open).
        try:
            v3_warn_map = {}
            earn_rows = doc.worksheet("DB_실적").get_all_values()[1:]
            for row in earn_rows:
                if len(row) > 8 and row[0].strip():
                    try: v3_warn_map[str(row[0]).strip().zfill(6)] = int(row[8])
                    except Exception: pass
            EARNINGS_WARNING_THRESHOLD = 20  # hyeoks_analyst.py의 중기 픽 필터 기준과 통일
            for r in results:
                if len(r) > 1 and r[0] in long_term_stocks:
                    r_code = str(r[1]).replace("'", "").strip().zfill(6)
                    v3 = v3_warn_map.get(r_code)
                    if v3 is not None and v3 < EARNINGS_WARNING_THRESHOLD:
                        r[8] = str(r[8]) + " 🔻(실적 악화 주의)"
        except Exception as e:
            print(f"⚠️ [실적 악화 경고 배지 처리 스킵] {e}")

        existing_data = {}
        try:
            db_scanner_sheet = doc.worksheet("DB_스캐너")
            old_data = db_scanner_sheet.get_all_values()
            for row in old_data[1:]:
                if len(row) > 15:
                    saved_code = str(row[2]).replace("'", "").strip().zfill(6)
                    # 🆕 [수정] 22번째 칸(인덱스 21)에 "간단 브리핑 종목이 연속 몇 번 밀려났는지" 유예 카운터 저장
                    grace_raw = str(row[21]).strip() if len(row) > 21 else ""
                    grace_count = int(grace_raw) if grace_raw.isdigit() else 0
                    existing_data[saved_code] = {"briefing": str(row[9]).strip(), "target": row[14], "stop": row[15], "raw_row": row, "grace_count": grace_count}
        except Exception as e: print(f"⚠️ [existing_data cache lookup Error] {e}")

        for r in results:
            c_code = str(r[1]).replace("'", "").strip().zfill(6)
            if is_regular_market:
                r[26] = r[27] = ""
                r[28] = "정규장 진행중"
            if c_code in existing_data:
                # 👑 [긴급 보정 2]: 빈 정적시트 강제 트랩을 피하기 위해 시간 검증 전용 플래그(is_official_reset_time)로 가드 교체
                if not is_official_reset_time:
                    r[9] = existing_data[c_code]["briefing"]
                    r[23] = existing_data[c_code]["target"]
                    r[24] = existing_data[c_code]["stop"]
                if is_preserve_time and not is_regular_market:
                    if not r[26] and not r[27]:
                        r[26] = str(existing_data[c_code]["raw_row"][16]).strip() if len(existing_data[c_code]["raw_row"]) > 16 else ""
                        r[27] = str(existing_data[c_code]["raw_row"][17]).strip() if len(existing_data[c_code]["raw_row"]) > 17 else ""
                        r[28] = str(existing_data[c_code]["raw_row"][18]).strip() if len(existing_data[c_code]["raw_row"]) > 18 else "정규장"

        try: helper_sheet = doc.worksheet("주가데이터_보조")
        except Exception: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="33")

        extended_headers = [
            "종목명", "종목코드", "현재가", "등락률", "5일평균", "20일평균", "거래량비율", "AI신호",
            "마스터타점", "브리핑상태", "당일고가", "당일저가", "60일고가", "시가총액", "캔들상태",
            "전고거리", "20일이격", "대장구분", "거래과열", "테마명", "프로그램", "52주고가",
            "기관/외인 누적(5일)", "목표가(AI)", "손절가(AI)", "종목쿼터", "시간외단일가(18시)", "NXT야간종가(20시)", "장구분",
            "V1 차트점수", "V1 표시", "V2 수급점수", "V2 표시", "RS등급"
        ]
        def _row_for_helper_sheet(r):
            # 🆕 [수정] "관망 · 조건미달"류(실제 매매 신호 없음)는 목표가/손절가도 "관망"으로 비워서,
            #    기계적으로 계산된 숫자가 마치 근거 있는 추천처럼 보이지 않도록 함. results 자체는 안 건드림(DB_스캐너 선정에 영향 없게).
            row = list(r[:34]) + [""] * max(0, 34 - len(r[:34]))  # 🔧 33→34로 확장해서 RS등급(인덱스33) 포함, v2_gate_flag(34)는 여전히 제외
            if len(row) > 8 and ("관망" in str(row[8]) or "매매금지" in str(row[8])):
                if len(row) > 23: row[23] = "관망"
                if len(row) > 24: row[24] = "관망"
            return row

        helper_sheet_data = [extended_headers] + [_row_for_helper_sheet(r) for r in results]
        try:
            helper_sheet.update(range_name="A1", values=helper_sheet_data, value_input_option="USER_ENTERED")
            helper_sheet.batch_clear([f"A{len(helper_sheet_data) + 1}:AG"])
            apply_change_rate_formatting(doc, helper_sheet, len(helper_sheet_data), col_index=3,
                                          extra_numeric_rules=[(33, 90, {"red": 0.1, "green": 0.6, "blue": 0.2})])  # 🆕 등락률 색상 + RS등급 90↑ 초록 강조
        except Exception as e: print(f"⚠️ [helper_sheet update Error] {e}")

        portfolio_protected_names = set()
        try:
            portfolio_rows = doc.worksheet("DB_중장기").get_all_values()
            for row in portfolio_rows[1:]: 
                if len(row) > 4:
                    if row[3] and str(row[3]).strip(): portfolio_protected_names.add(str(row[3]).strip()) 
                    if row[4] and str(row[4]).strip(): portfolio_protected_names.add(str(row[4]).strip()) 
            print(f"📦 [포트폴리오 동기화 완료] 중기 전략 핵심주 보호막 가동: {list(portfolio_protected_names)}")
        except Exception as e:
            print(f"⚠️ [DB_중장기 시트 연동 실패] {e}")

        scanner_keywords = ["🎯", "💎", "🌱", "🚀", "📦", "🔍"]
        all_candidates = []
        processed_codes = set()

        cutoff_score = 40 if is_warning_market else 25

        for r in results:
            if len(r) < 29: continue
            종목명 = str(r[0]).strip()
            종목코드 = str(r[1]).replace("'", "").zfill(6)
            processed_codes.add(종목코드) 
            
            tajeom = str(r[8]).replace("🎖️(코어픽/면책)", "").replace("(코어픽/면책)", "").replace("🎖️", "").strip()
            
            v1_num = parse_score_num(r[29]) if len(r) > 29 else 0
            v2_num = parse_score_num(r[31]) if len(r) > 31 else 0
            max_current_score = max(v1_num, v2_num)

            if "관망" in tajeom or "조건미달" in tajeom or max_current_score < cutoff_score:
                continue

            if any(kw in tajeom for kw in scanner_keywords) or (종목명 in portfolio_protected_names):
                combined_score_display = f"V1:{v1_num}점 / V2:{v2_num}점"
                하이브리드_링크 = f'=HYPERLINK("https://m.stock.naver.com/domestic/stock/{종목코드}/total", "{종목명}")'
                row_data = [
                    하이브리드_링크, r[28] if len(r) > 28 and r[28] else "정규장", f"'{종목코드}", r[2], r[3], r[19], r[7], r[6],
                    tajeom, r[9], combined_score_display, r[20], r[21], r[22], r[23], r[24], r[26], r[27], r[28],
                    v1_num, v2_num, "0",  # 🆕 자연 선정에 든 종목은 유예 카운터를 0으로 리셋
                    r[33] if len(r) > 33 and r[33] != "" else ""  # 🆕 RS등급(1~99) — 그레이스 카운터 뒤에 추가해 기존 위치 안 건드림
                ]
                all_candidates.append(row_data)

        seed_cands, normal_cands = [], []
        for cand in all_candidates:
            tajeom_str = str(cand[8])
            if "🌱" in tajeom_str or "코어 포트폴리오" in tajeom_str or "📉 과매도" in tajeom_str or "[중장기/모아가기]" in tajeom_str or "[하단]" in tajeom_str:
                seed_cands.append(cand)
            else: normal_cands.append(cand)

        def get_v1_score(x):
            try: return int(x[19])
            except Exception: return 0

        def get_v2_score(x):
            try: return int(x[20])
            except Exception: return 0

        def union_top_n(cand_list, n_each):
            by_v1 = sorted(cand_list, key=get_v1_score, reverse=True)[:n_each]
            by_v2 = sorted(cand_list, key=get_v2_score, reverse=True)[:n_each]
            seen, picked = set(), []
            for c in by_v1 + by_v2:
                if c[2] not in seen:           
                    seen.add(c[2])
                    picked.append(c)
            picked.sort(key=lambda x: max(get_v1_score(x), get_v2_score(x)), reverse=True)
            return picked

        # ④ [국면 정합성]: 뉴스 키워드 1위 테마가 2위를 압도하는 '압축 장세'면 SEED(바닥) 쿼터 5→2 축소
        seed_quota = 5
        kw_counts = []  # 🔧 [수정] try 블록이 조기 실패해도 아래 백테스트 집중도 기록에서 참조 가능하도록 안전 기본값
        try:
            kw_rows = doc.worksheet("뉴스_키워드").get_all_values()[1:6]
            kw_counts = [int(re.sub(r'[^0-9]', '', str(kr[3]))) for kr in kw_rows if len(kr) >= 4 and re.sub(r'[^0-9]', '', str(kr[3]))]
            if len(kw_counts) >= 2 and kw_counts[0] >= 10 and kw_counts[0] >= kw_counts[1] * 1.5:
                seed_quota = 2
                print(f"🧲 압축 장세 감지 (1위 {kw_counts[0]} vs 2위 {kw_counts[1]}) → SEED 쿼터 5→2 축소")
        except Exception as e:
            print(f"⚠️ 뉴스 키워드 집중도 판정 스킵: {e}")

        seed_pool = union_top_n(seed_cands, 5)
        normal_pool = union_top_n(normal_cands, 15)

        seed_final = seed_pool[:seed_quota]
        normal_final = normal_pool[:(20 - len(seed_final))]
        top_20_results = seed_final + normal_final
        top_20_codes = {str(x[2]).replace("'", "").strip().zfill(6) for x in top_20_results if len(x) > 2}

        # 구출 시 stale 대신 '이번 스캔의 신선한 시장데이터'를 쓰기 위한 코드 인덱스 (주가데이터_보조에 이미 찍힌 그 값)
        results_by_code = {str(r[1]).replace("'", "").strip().zfill(6): r for r in results if len(r) >= 29}

        GRACE_LIMIT = 3  # 🆕 간단 브리핑 종목이 기준선 근처에서 들락날락해도 이 횟수(사이클)까지는 버텨줌

        if not is_official_reset_time:
            for c_code, data in existing_data.items():
                if c_code not in top_20_codes:
                    briefing_text = str(data["briefing"]).strip()
                    is_full_report = any(key in briefing_text for key in ["리포트 발송 완료", "리포트 작성 완료"])
                    is_simple_brief = ("간단 브리핑" in briefing_text) and not is_full_report
                    prev_grace = data.get("grace_count", 0)

                    # 👑 [수정] 리포트는 기존대로 무제한 절대 보존. 간단 브리핑은 "연속 밀려난 횟수"가 GRACE_LIMIT 미만일 때만
                    #    구제하고, 그 한도를 넘으면 더 이상 안 살려줌(기준선 근처 진동 종목이 무한정 안 쌓이게).
                    if is_full_report:
                        should_rescue, new_grace = True, 0
                    elif is_simple_brief and prev_grace < GRACE_LIMIT:
                        should_rescue, new_grace = True, prev_grace + 1
                    else:
                        should_rescue, new_grace = False, 0

                    if not should_rescue:
                        continue

                    fresh = results_by_code.get(c_code)
                    if fresh is not None and parse_score_num(fresh[2]) > 0:
                        # ✅ 이번 스캔에서 분석됨 → 신선한 현재가/시간외/NXT/장구분으로 스캐너행 재구성
                        #    (브리핑/타겟은 아래 1689 오버레이가 보존하므로 여기선 시장데이터만 fresh로)
                        fr = fresh
                        f_code = str(fr[1]).replace("'", "").zfill(6)
                        f_tajeom = str(fr[8]).replace("🎖️(코어픽/면책)", "").replace("(코어픽/면책)", "").replace("🎖️", "").strip()
                        f_v1 = parse_score_num(fr[29]) if len(fr) > 29 else 0
                        f_v2 = parse_score_num(fr[31]) if len(fr) > 31 else 0
                        f_link = f'=HYPERLINK("https://m.stock.naver.com/domestic/stock/{f_code}/total", "{str(fr[0]).strip()}")'
                        clean_row = [
                            f_link, fr[28] if (len(fr) > 28 and fr[28]) else "정규장", f"'{f_code}", fr[2], fr[3], fr[19], fr[7], fr[6],
                            f_tajeom, fr[9], f"V1:{f_v1}점 / V2:{f_v2}점", fr[20], fr[21], fr[22], fr[23], fr[24], fr[26], fr[27], fr[28],
                            f_v1, f_v2, "0",
                            fr[33] if len(fr) > 33 and fr[33] != "" else ""  # 🆕 RS등급도 fresh 데이터에서 가져옴
                        ]
                    else:
                        # ⚠️ 이번 스캔에 없던 종목 → 부득이 stale raw_row, 장구분만 정직 교정(정규장 아니면 장마감)
                        clean_row = list(data["raw_row"])
                        if len(clean_row) > 8:
                            clean_row[8] = str(clean_row[8]).replace("🎖️(코어픽/면책)", "").replace("(코어픽/면책)", "").replace("🎖️", "").strip()
                        now_mk = datetime.datetime.now(KST)
                        is_reg_now = (9 <= now_mk.hour < 15) or (now_mk.hour == 15 and now_mk.minute <= 40)
                        if not is_reg_now:
                            while len(clean_row) <= 18: clean_row.append("")
                            for idx in (1, 18):
                                if str(clean_row[idx]).strip() == "정규장 진행중":
                                    clean_row[idx] = "장마감"

                    while len(clean_row) <= 22: clean_row.append("")
                    clean_row[21] = str(new_grace)  # 🆕 이번에 구제됐다면 갱신된 유예 카운터를 기록
                    top_20_results.append(clean_row)
                    top_20_codes.add(c_code)

        top_20_results.sort(key=lambda x: max(get_v1_score(x), get_v2_score(x)), reverse=True)

        # 🔧 [수정] 예전엔 무조건 20개로 잘라서, 구제된 종목(특히 간단 브리핑)이 다시 잘려나가는 문제가 있었음.
        #    자연 선정은 이미 20개 이하로 구성되고, 구제분은 리포트=무제한/간단브리핑=유예한도로 이미 스스로 제한되므로
        #    별도 절삭은 필요 없음. 다만 예기치 못한 폭증 방지용 안전 상한만 넉넉하게 둠.
        SAFETY_CEILING = 40
        if len(top_20_results) > SAFETY_CEILING:
            top_20_results = top_20_results[:SAFETY_CEILING]

        if not is_official_reset_time:
            for row in top_20_results:
                if len(row) > 15:
                    code = str(row[2]).replace("'", "").strip().zfill(6)
                    if code in existing_data:
                        existing_briefing = str(existing_data[code]["briefing"]).strip()
                        if existing_briefing != "AI 브리핑 대기중" and existing_briefing != "":
                            row[9] = existing_briefing
                            row[14] = existing_data[code]["target"]
                            row[15] = existing_data[code]["stop"]

        if top_20_results:
            try:
                db_scanner_sheet.update(range_name="A2", values=top_20_results, value_input_option="USER_ENTERED")
                db_scanner_sheet.batch_clear([f"A{len(top_20_results) + 2}:AC"])
                apply_change_rate_formatting(doc, db_scanner_sheet, len(top_20_results) + 1, col_index=4,
                                              extra_numeric_rules=[(22, 90, {"red": 0.1, "green": 0.6, "blue": 0.2})])  # 🆕 등락률 색상 + RS등급 90↑ 초록 강조
                print(f"🎯 DB_스캐너 {len(top_20_results)}개 전송 완료 (하이재킹 버그 수정 및 데이터 영구 락 보존 완료)")
            except Exception as e: print(f"⚠️ [DB_스캐너 update Error] {e}")

        # ==========================================================================
        # 📊 [백테스트 V6 — Step 1] 진입 로그 (불변 append-only, 신 26열 스키마)
        #   · 추적(T+N 캡처)은 Step 2에서 셀 타겟 업데이트로 별도 구현 — 이번 단계는 '진입 적재'만.
        #   · 구 D1(가짜 T+N)·D6/D7(전체 rewrite) 로직 폐기.
        # ==========================================================================
        try:
            try:
                bt_sheet = doc.worksheet("백테스트_로그")
                bt_data = bt_sheet.get_all_values()
            except Exception:
                bt_sheet = doc.add_worksheet(title="백테스트_로그", rows="5000", cols="26")
                bt_data = []

            # 🆕 [트레일링 스탑 + 목표가 알림] Step1(진입 적재, 15시 한정)과 무관하게 매 스캔 사이클(10분)마다 실행 —
            #    장중 내내 목표가 도달을 놓치지 않고 바로 텔레그램으로 알려야 하므로 EOD 시간대에 가두지 않음.
            try:
                check_target_alerts_and_trailing_stop(doc, bt_sheet)
            except Exception as e:
                print(f"⚠️ [트레일링 스탑 호출 실패] {e}")

            today_str = today_date.strftime('%Y-%m-%d')
            now_time_bt = datetime.datetime.now(KST)

            # ── 1회성 마이그레이션: 신 스키마(26열)가 아니면 기존 로그를 레거시로 통째 이관 후 새 헤더로 리셋 ──
            is_new_schema = bool(bt_data) and len(bt_data[0]) >= 26 and str(bt_data[0][0]).strip() == "trade_id"
            if not is_new_schema:
                if len(bt_data) > 1:
                    try: legacy_sheet = doc.worksheet("백테스트_로그_레거시")
                    except Exception: legacy_sheet = doc.add_worksheet(title="백테스트_로그_레거시", rows="5000", cols="20")
                    legacy_sheet.append_rows(bt_data, value_input_option="USER_ENTERED")
                    print(f"📦 [마이그레이션] 구 백테스트 로그 {len(bt_data) - 1}행 → 백테스트_로그_레거시 격리 (D1 오염 데이터 분리)")
                bt_sheet.clear()
                bt_sheet.update(range_name="A1", values=[BT_HEADER], value_input_option="USER_ENTERED")
                bt_data = [BT_HEADER]
                print("🆕 [백테스트 V6] 신 26열 스키마로 초기화 (컷오프 이후 데이터만 유효 표본)")
            elif len(bt_data[0]) < len(BT_HEADER):
                # 🔧 [수정] "26열 이상이면 최신"이라는 위 판단이 26→32열 확장을 인식 못 해서, 데이터는 32칸으로
                #    잘 쓰이는데 정작 제목줄(1행)이 안 바뀌어 새로 추가된 T+20/T+60/T+120 칸이 이름 없이
                #    비어 보이던 문제. 기존 데이터 행은 전혀 안 건드리고 제목줄 한 줄만 안전하게 갱신.
                bt_sheet.update(range_name="A1", values=[BT_HEADER], value_input_option="USER_ENTERED")
                bt_data[0] = BT_HEADER
                print(f"🔧 [백테스트 헤더 확장] {len(bt_data[0])}열 → {len(BT_HEADER)}열로 제목줄만 갱신 (기존 데이터는 그대로 보존)")

            existing_ids = set(str(row[0]).strip() for row in bt_data[1:] if row and str(row[0]).strip())

            # ── [Step 2] 추적: fchart 시리즈로 거래일·T+1시가·호라이즌종가 캡처·동결 (omakase 7시 단독, 셀 타겟 update) ──
            if is_official_reset_time and len(bt_data) > 1:
                print("▶ [백테스트 V6 Step2] 진입 종목 성과 추적 (거래일 기준 캡처·동결)...")
                index_bars = {"KOSPI": get_daily_bars("KOSPI", 150), "KOSDAQ": get_daily_bars("KOSDAQ", 150)}  # 🔧 T+120 추적 위해 80→150일로 확장
                index_close_map = {k: {b['date']: b['close'] for b in v} for k, v in index_bars.items()}
                horizon_map = {1: (17, 21), 3: (18, 22), 5: (19, 23), 10: (20, 24), 20: (26, 29), 60: (27, 30), 120: (28, 31)}  # 호라이즌 → (종목col, 지수col) 0-based
                changed_rows = set()
                TOTAL_BUDGET_SEC = 540  # 🆕 [수정] Step1+Step2 합쳐서 9분(10분 트리거보다 1분 여유) 안에는 끝내도록
                #    전체 예산을 공유함. T+20/60/120 추가 이후 조회할 종목이 늘어나 Step2까지 포함한 전체 실행이
                #    10분을 넘기면, 다음 트리거가 대기열에 밀렸다가 그다음 트리거에 취소되는 연쇄(깃허브의
                #    대기열 자동정리 동작)가 오전 시간대(Step2가 도는 7~8:50시)에 몰려서 나타났을 가능성이 있음.
                step2_deadline = scan_start + TOTAL_BUDGET_SEC

                for i in range(1, len(bt_data)):
                    if time.time() > step2_deadline:
                        print(f"⏱️ [Step2 시간 예산 초과] 전체 실행 {TOTAL_BUDGET_SEC}초 경과 — 남은 행은 다음 실행에서 이어서 처리합니다.")
                        break
                    row = bt_data[i]
                    while len(row) < 32: row.append("")
                    if str(row[28]).strip():   # 🔧 [수정] row[20](종목T+10)이 아니라 최장 호라이즌인 row[28](종목T+120)이
                        continue                # 채워졌을 때만 완결로 봄. 예전 체크는 T+10만 차도 "끝났다"고 오판해서
                                                 # 새로 늘린 T+20/60/120 칸을 영원히 못 채우는 버그였음.

                    # 🛡️ [수정] 이 아래 전체를 종목별로 감쌈 — 예전엔 안전장치가 없어서, 특정 종목 하나에서
                    #    예기치 못한 오류가 나면 반복문 전체가 죽어서 그 뒤 종목들이 통째로 스킵되고 있었음
                    #    (2026-07-16 배치에서 위더스제약만 처리되고 나머지 5개가 계속 비어있던 사고 원인).
                    try:
                        채널 = str(row[2]).strip()
                        진입일 = str(row[1]).strip()
                        벤치 = str(row[13]).strip() or "KOSPI"
                        code = str(row[4]).replace("'", "").strip().zfill(6)

                        # ⚡ 달력일 사전필터: 다음 미충족 호라이즌이 '달력상으로도' 도달 불가면 fetch 자체를 스킵.
                        #    (거래일수 ≤ 달력일 이므로 달력일 < next_h 면 캡처 불가 → 불필요 fetch 제거, 아침 10분 재실행 부하 완화)
                        next_h = next((h for h, (sidx, _ic) in horizon_map.items() if not str(row[sidx]).strip()), None)
                        if next_h is None:
                            continue
                        try:
                            _cal = (today_date - datetime.datetime.strptime(진입일, '%Y-%m-%d').date()).days
                        except Exception:
                            _cal = 999
                        if _cal < next_h:
                            continue

                        # 지수 base = 지수 T+1 시가 (§3 의도: 종목과 동일 보유창에서 알파 산출). P열(진입지수)은 참고용 신호일 종가.
                        idx_series = index_bars.get(벤치, index_bars["KOSPI"])
                        _ie = next((k for k, b in enumerate(idx_series) if b['date'] == 진입일), None)
                        idx_base = idx_series[_ie + 1]['open'] if (_ie is not None and _ie + 1 < len(idx_series)) else 0.0

                        bars = index_bars.get(벤치, index_bars["KOSPI"]) if 채널.startswith("지수벤치") else get_daily_bars(code, 150)  # 🔧 KOSPI/KOSDAQ 분리 후 startswith로 매칭  # 🔧 T+120 추적 위해 80→150일로 확장
                        if not bars: continue

                        entry_idx = next((k for k, b in enumerate(bars) if b['date'] == 진입일), None)
                        if entry_idx is None: continue
                        elapsed_td = (len(bars) - 1) - entry_idx
                        if elapsed_td < 1: continue

                        # 진입가 Q(=T+1 시가) 1회 캡처
                        if not str(row[16]).strip() and entry_idx + 1 < len(bars):
                            q_open = bars[entry_idx + 1]['open']
                            row[16] = int(q_open) if not 채널.startswith("지수벤치") else round(q_open, 2)
                            changed_rows.add(i)
                        try: base = float(str(row[16]).replace(',', '')) if str(row[16]).strip() else 0.0
                        except Exception: base = 0.0
                        if base <= 0: continue

                        # 🔧 [정밀도] 지수벤치 행은 종목=지수가 같은 시계열이므로 알파가 정확히 0이어야 한다.
                        #    그런데 종목 base는 시트에서 읽어온 값(표시 반올림으로 소수 유실 가능)이고 지수
                        #    base는 원값이라 미세하게 어긋나 알파가 ±0.01%로 찍혔다. 같은 base를 쓰게 해서
                        #    구조적으로 0이 되도록 고정한다.
                        if 채널.startswith("지수벤치"):
                            idx_base = base

                        idx_map = index_close_map.get(벤치, {})
                        captured = []
                        for h, (sidx, iidx) in horizon_map.items():
                            if elapsed_td >= h and not str(row[sidx]).strip() and entry_idx + h < len(bars):
                                s_close = bars[entry_idx + h]['close']
                                row[sidx] = f"{(s_close - base) / base * 100:.2f}%"
                                hdate = bars[entry_idx + h]['date']
                                i_close = idx_map.get(hdate, 0)
                                if i_close and idx_base > 0:
                                    row[iidx] = f"{(i_close - idx_base) / idx_base * 100:.2f}%"
                                captured.append(f"T+{h}:{elapsed_td}")   # 실제 캡처 거래일수(미스 캡처 감사)
                                changed_rows.add(i)
                        if captured:
                            prev_z = str(row[25]).strip()
                            row[25] = (prev_z + "," if prev_z else "") + ",".join(captured)

                        # 🆕 [목표가·손절가 터치 판정] 이미 받아온 bars의 고가·저가로, 보유 기간 동안
                        #    실제로 목표가·손절가를 건드린 적이 있는지 확인 — 추가 API 호출 없이 처리.
                        #    같은 날 둘 다 닿았을 수도 있는데(장중 순서는 알 수 없음), 보수적으로 손절을 먼저 체크.
                        if len(row) > 35 and 채널 != "" and not str(채널).startswith("지수벤치"):
                            try:
                                target_p = float(str(row[32]).replace(',', '')) if str(row[32]).strip() else None
                            except Exception:
                                target_p = None
                            try:
                                stop_p = float(str(row[33]).replace(',', '')) if str(row[33]).strip() else None
                            except Exception:
                                stop_p = None
                            stop_touched = str(row[35]).strip()
                            target_touched = str(row[34]).strip()
                            if (target_p or stop_p) and not (target_touched and stop_touched):
                                for d in range(1, elapsed_td + 1):
                                    if entry_idx + d >= len(bars): break
                                    bar_d = bars[entry_idx + d]
                                    if stop_p and not stop_touched and bar_d['low'] <= stop_p:
                                        row[35] = f"T+{d}"; stop_touched = f"T+{d}"; changed_rows.add(i)
                                    if target_p and not target_touched and bar_d['high'] >= target_p:
                                        row[34] = f"T+{d}"; target_touched = f"T+{d}"; changed_rows.add(i)
                                    if target_touched and stop_touched: break
                    except Exception as e:
                        print(f"⚠️ [백테스트 V6 Step2] {row[3] if len(row) > 3 else '?'}({row[4] if len(row) > 4 else '?'}) 추적 중 오류로 스킵(다음 종목은 계속 진행): {e}")
                        continue

                if changed_rows:
                    updates = [{'range': f'Q{i + 1}:AJ{i + 1}', 'values': [bt_data[i][16:36]]} for i in sorted(changed_rows)]  # 🔧 목표가·손절가·터치 열(32~35) 추가로 범위 확장(AF→AJ)
                    bt_sheet.batch_update(updates, value_input_option="USER_ENTERED")
                    print(f"✅ [백테스트 V6 Step2] {len(changed_rows)}행 추적 셀 업데이트 (호라이즌별 캡처·동결 · 전체 rewrite 없음)")
                else:
                    print("⏭ [백테스트 V6 Step2] 추적 변경 없음")

                compute_channel_kelly(doc)  # 🆕 [수정] Step1이 아니라 Step2 직후로 이동 — T+N 수익률이 실제로
                #    갱신되는 건 Step2(매일 7~8:50시)라서, 켈리 계산도 여기 붙어야 맞음. Step1(15시)은
                #    빈 칸인 새 행만 추가할 뿐이라 켈리 계산과 무관했음.
                compute_channel_comparison_dashboard(doc)  # 🆕 채널별 T+1~T+120 비교 대시보드도 같이 갱신

            # ── 진입 적재 (15:00~15:30 EOD 윈도, append-only) ──
            is_eod_log_window = (now_time_bt.hour == 15 and 0 <= now_time_bt.minute < 30)

            # 🛡️ [휴장일 가드] GAS는 '평일'이면 무조건 워크플로를 쏘기 때문에, 임시공휴일처럼 장이 안 열린
            #    날에도 스캔이 돌아 진입 행이 쌓인다. 그런데 그날은 어느 종목의 일봉에도 존재하지 않는
            #    날짜라, Step2·저녁 캐치업이 쓰는 '진입일 == 일봉 날짜' 정확일치 매칭이 영원히 실패한다
            #    (= 진입가부터 T+120까지 평생 빈칸인 유령 행). 2026-08-17 임시공휴일에 실제로 10행이
            #    그렇게 쌓였고, 그 값들도 직전 거래일(8/14) 종가를 재탕한 중복 표본이었다.
            #    → 지수 일봉에 오늘 날짜가 있는지로 개장 여부를 판정한다. 정상 거래일이면 15시 시점에
            #      이미 당일 봉이 존재하고(장중 잠정치), 휴장일이면 마지막 봉이 전 거래일에 머문다.
            #    조회 실패 시엔 적재를 건너뛰는 쪽(fail-closed)으로 닫힌다 — EOD 창에 실행 기회가
            #    3번 있으므로 일시적 네트워크 오류는 다음 회차에 자연히 복구된다.
            if is_eod_log_window:
                _open_days = {b['date'] for b in get_daily_bars("KOSPI", 5)}
                if today_str not in _open_days:
                    is_eod_log_window = False
                    print(f"🚫 [휴장일 감지] {today_str}는 거래일이 아님(지수 일봉 없음) — 백테스트 진입 적재를 건너뜁니다.")
            new_rows = []
            if is_eod_log_window:
                entry_stage = 3 if kospi_rate <= -3.0 else (2 if is_warning_market else 1)
                # 🔧 [수정] "집중도" 열이 모든 채널에서 계속 빈 값이던 문제 — 이미 계산해둔 뉴스키워드 1위:2위 비율을 그대로 기록
                concentration_str = f"'{kw_counts[0]}:{kw_counts[1]}" if len(kw_counts) >= 2 else ""  # 🔧 앞에 ' 붙여 텍스트 강제(시:분으로 오인되던 버그 수정)
                idx_close_cache = {"KOSPI": get_index_close("KOSPI"), "KOSDAQ": get_index_close("KOSDAQ")}

                positive_badges = ["🎯", "💎", "🌟", "👑", "📦", "🔍", "🚀", "🌱"]
                negative_markers = ["📉", "관망", "조건미달", "🚫", "매매금지"]
                candidate_pool = [r for r in results if len(r) >= 35  # 🔧 r[34](v2_gate_flag)까지 안전하게 접근하려면 최소 35개 필요
                                  and not any(n in str(r[8]) for n in negative_markers)
                                  and any(p in str(r[8]) for p in positive_badges)]

                chart_top2 = sorted(candidate_pool, key=lambda x: x[29], reverse=True)[:2]
                gate_passed = [r for r in candidate_pool if r[34] == "GATE_PASS"]

                # ── [준비 완료 / 기본 미적용] 수급TOP2 V2 상단 컷오프 ──────────────────────
                # 왜 만들어 뒀나 (2026-08-07 백테스트_로그 189행 실증):
                #   · V2 점수대별 T+5 알파가 역U자였다. 40~59 +4.82%(승률75%), 60~79 +7.93%(82%),
                #     그런데 80~100 은 -5.89%(승률 22%)로 최악.
                #   · 수급TOP2는 'V2 상위 2개'를 뽑으므로 하필 이 최악 구간을 집게 된다.
                #     실제로 수급TOP2 중 V2 80+ 5건 평균 -7.60% vs V2 40~79 12건 +3.83%.
                #   · 해석: 극단적 수급 과열은 지속이 아니라 소진 신호(고점 추격)에 가깝다.
                #     이것이 수급TOP2가 대조군(랜덤2)보다도 부진했던 직접적 원인으로 보인다.
                #
                # 왜 지금 켜지 않나:
                #   · 문제 구간 표본이 아직 N=9로 얇아 -5.89%를 확정으로 보기 이르다.
                #   · 로직을 바꾸면 그 시점부터 표본 성격이 달라져, 변경 전/후를 비교할 기준선이 사라진다.
                #     2~3주 더 축적한 뒤 이 항목 하나만 켜고 변경일을 기록해 전/후를 분리 비교할 것.
                #
                # 켜는 법: 워크플로/실행 환경에 SUPPLY_V2_BAND=on 을 주면 즉시 활성화(코드 수정 불필요).
                #   밴드를 조정하려면 SUPPLY_V2_BAND_RANGE=45-79 형식으로 함께 지정.
                _band_on = os.environ.get("SUPPLY_V2_BAND", "off").strip().lower() in ("on", "true", "1")
                try:
                    _lo, _hi = (int(x) for x in os.environ.get("SUPPLY_V2_BAND_RANGE", "45-79").split("-"))
                except Exception:
                    _lo, _hi = 45, 79

                if _band_on:
                    _banded = [r for r in gate_passed if _lo <= parse_score_num(r[31]) <= _hi]
                    supply_top2 = sorted(_banded, key=lambda x: x[31], reverse=True)[:2]
                    print(f"🎛️ [수급TOP2] V2 밴드 필터 ON ({_lo}~{_hi}) — 후보 {len(gate_passed)}→{len(_banded)}개, 선정 {len(supply_top2)}개")
                else:
                    # 현행(기본): V2 상위 2개. 위 실증 근거대로라면 과열 구간을 집을 수 있음.
                    supply_top2 = sorted(gate_passed, key=lambda x: x[31], reverse=True)[:2]

                # 대조군 랜덤2: '배지필터 이전' 전체 results에서 결정론적 추출 (seed=날짜 → 재현가능, builtin hash() 비사용)
                valid_results = [r for r in results if len(r) >= 35 and parse_price_num(r[2]) > 0]
                rng = random.Random(today_str)
                random2 = rng.sample(valid_results, 2) if len(valid_results) >= 2 else list(valid_results)

                # 채널별 '오늘 이미 적재된 수'(이전 실행분 포함, bt_data 재독으로 반영) → 멀티런에도 하루 N개 고정
                channel_today_count = {}
                for row in bt_data[1:]:
                    if len(row) > 2 and str(row[1]).strip() == today_str:
                        ch = str(row[2]).strip()
                        channel_today_count[ch] = channel_today_count.get(ch, 0) + 1

                def add_channel(channel, picks, cap=2):
                    for r in picks:
                        if channel_today_count.get(channel, 0) >= cap:
                            break   # 오늘 이 채널 cap개 이미 확보 → 추가 실행의 중복 적재 차단(랜덤 폭증 버그 픽스)
                        s_code = str(r[1]).replace("'", "").strip().zfill(6)
                        tid = f"{today_str}_{channel}_{s_code}"
                        if tid in existing_ids: continue
                        bench = get_market_name(s_code)
                        # 🆕 목표가·손절가(AI가 이미 계산해둔 값)를 진입 시점 그대로 캡처 — "관망"류는 빈 값
                        # 🔧 [버그픽스] 예전엔 콤마만 지우고 isdigit()으로 검사해서 '61,000원'처럼
                        #    '원'이 붙은 형식을 전부 놓쳤다. DB_스캐너에 있는 종목은 위쪽 existing_data
                        #    동기화(r[23] = existing_data[...]["target"])로 '61,000원' 형식이 되기 때문에,
                        #    결과적으로 고득점 종목(차트TOP2·수급TOP2)만 목표가가 비고 스캐너 밖 종목
                        #    (랜덤2)은 채워지는 정반대 현상이 발생했다. → 숫자만 추출하도록 교정.
                        target_val = _parse_price_cell(r[23] if len(r) > 23 else "")
                        stop_val = _parse_price_cell(r[24] if len(r) > 24 else "")
                        new_rows.append([
                            tid, today_str, channel, r[0], f"'{s_code}", r[19], r[8], entry_stage, concentration_str,
                            r[29], r[31], r[34], r[22], bench, r[2], idx_close_cache.get(bench, 0.0)
                        ] + [""] * 16 + [target_val, stop_val, "", ""])
                        existing_ids.add(tid)
                        channel_today_count[channel] = channel_today_count.get(channel, 0) + 1

                add_channel("차트TOP2", chart_top2)
                add_channel("수급TOP2", supply_top2)
                add_channel("랜덤2", random2)

                # 지수벤치(KOSPI·KOSDAQ 각 1행/일) — 순수 지수보유 베이스라인. 둘을 별개 채널로 분리해서
                # 코스피·코스닥 이격이 클 때 나란히 비교할 수 있게 함(기존엔 KOSPI 하나만 있었음).
                tid_idx = f"{today_str}_지수벤치_KOSPI"
                if tid_idx not in existing_ids:
                    ixc = idx_close_cache.get("KOSPI", 0.0)
                    new_rows.append([tid_idx, today_str, "지수벤치_KOSPI", "KOSPI", "'KOSPI", "", "지수보유", entry_stage, concentration_str,
                                     "", "", "", "", "KOSPI", ixc, ixc] + [""] * 16 + ["", "", "", ""])
                    existing_ids.add(tid_idx)

                tid_idx_kq = f"{today_str}_지수벤치_KOSDAQ"
                if tid_idx_kq not in existing_ids:
                    ixc_kq = idx_close_cache.get("KOSDAQ", 0.0)
                    new_rows.append([tid_idx_kq, today_str, "지수벤치_KOSDAQ", "KOSDAQ", "'KOSDAQ", "", "지수보유", entry_stage, concentration_str,
                                     "", "", "", "", "KOSDAQ", ixc_kq, ixc_kq] + [""] * 16 + ["", "", "", ""])
                    existing_ids.add(tid_idx_kq)

            if new_rows:
                bt_sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
                print(f"✅ [백테스트 V6 Step1] 진입 {len(new_rows)}행 append 완료 (차트/수급/랜덤/지수 · 추적은 Step2)")
                sort_and_format_backtest_log(doc, bt_sheet)  # 🆕 새 행이 추가된 직후에만 정렬+서식 재적용
            else:
                print("⏭ [백테스트 V6 Step1] 진입 추가 없음 (EOD 윈도 외 또는 전부 중복)")
        except Exception as e:
            print(f"⚠️ [백테스트 V6 Step1 에러] {e}")

    except Exception as e: print(f"❌ 전체 업데이트 에러: {e}")

if __name__ == "__main__":
    is_market_closed = False 
    df_theme, _, all_theme_map = get_real_money_themes()
    df_news, df_naver, df_main_news = get_news_keywords(), get_naver_search_ranking(), get_naver_main_news()
    
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(SHEET_URL)
    
    update_google_sheet(doc, df_theme, df_news, df_naver, df_main_news, is_market_closed)
    update_technical_data(df_theme, all_theme_map)
    manage_schedule_sheet(schedules=get_market_schedule())
    
    print(f"🎉 모든 주입 패치 작업 완료! (KST {datetime.datetime.now(KST).strftime('%H:%M:%S')})")

    now_kst = datetime.datetime.now(KST)
    if now_kst.hour == 15 and 0 <= now_kst.minute <= 50:
        try:
            posted_data = doc.worksheet("리포트_게시").get_all_values()
            today_str = now_kst.strftime('%Y-%m-%d')
            already_posted = any(today_str in str(row[0]) for row in posted_data[:5] if row)
            if not already_posted:
                GOOGLE_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
                response = GLOBAL_SESSION.post(GOOGLE_WEBHOOK_URL, timeout=30)
                if response.status_code == 200: print("✅ 구글 자동 릴레이 바통터치 성공")
        except Exception as e: print(f"❌ 릴레이 에러: {e}")
