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

# ==========================================
# 기본 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
TARGET_PERCENT = 3.0
KST = datetime.timezone(datetime.timedelta(hours=9))
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_URL_BASE = "https://openapi.koreainwestment.com:9443"

now_kst_check = datetime.datetime.now(KST)
if 4 <= now_kst_check.hour < 7:
    print(f"🌙 현재 시간({now_kst_check.strftime('%H:%M')}): 시스템을 휴식 모드로 전환합니다. (04시~07시)")
    sys.exit(0)

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
                except: continue
            return datetime.datetime(1900, 1, 1)
            
        rows.sort(key=lambda x: parse_date(x[sort_col_idx]), reverse=True)
        sheet.batch_clear(['A2:Z10000'])
        
        if rows:
            sheet.update(range_name="A2", values=rows, value_input_option="USER_ENTERED")
        print(f"✅ [{sheet_name}] 최신순 정렬 및 오염 데이터 청소 완료")
    except Exception as e:
        print(f"⚠️ [{sheet_name}] 정렬 실패: {e}")

def normalize_date_format(date_str, current_year="2026"):
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
        res = requests.post(f"{KIS_URL_BASE}/oauth2/tokenP", headers=headers, json=body, timeout=5)
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

STOPWORDS = ['코스피', '코스닥', '증시', '주식', '투자', '종목', '시장', '지수', '대형주', '중소형주', '외인', '기관', '개인', '외국인', '매수', '매도', '순매수', '순매도', '거래', '대금', '주가', '펀드', '사모', '상장', '상폐', '공모', '특징주', '테마', '테마주', '관련', '관련주', '수혜', '수혜주', '장세', '개장', '출발', '마감', '초반', '후반', '오전', '오후', '장중', '증권', '증권사', '운용', '자사', '괴리', '프리미어', '가치', '밸류', '공시', '병합', '분할', '상승', '하락', '급등', '급락', '강세', '약세', '폭락', '반등', '조정', '랠리', '위축', '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '돌파', '경신', '연속', '최고', '최저', '신고가', '신저가', '최고치', '최저치', '최고가', '최저가', '급증', '급감', '확산', '진정', '완화', '악화', '개선', '회복', '최대', '사상', '역대', '최초', '최신', '규모', '수준', '가격', '목표가', '상향', '하향', '박살', '킬러', '대규모', '변동', '오픈', '호재', '연계', '대비', '경제', '금융', '기업', '정부', '자산', '머니', '한국', '미국', '국내', '글로벌', '뉴욕', '회장', '대표', '임원', '주주', '총회', '이유', '때문', '달러', '금리', '인상', '인하', '동결', '연준', '파월', '물가', '지표', '고용', '기름값', '주유소', '석유', '신용', '수익', '매출', '적자', '흑자', '배당', '지분', '인수', '합병', '사업', '추진', '공급', '계약', '체결', '실적', '발표', '이익', '반사이익', '현금', '자회사', '계열사', '지주사', '관계사', '기내식', '서비스', '오늘', '내일', '이번', '주간', '월간', '분기', '시간', '하루', '하루만', '올해', '내년', '지난해', '전일', '전주', '전월', '동기', '내달', '연말', '연초', '이날', '당일', '최근', '현재', '이후', '이전', '상반기', '하반기', '당분간', '예상', '전망', '기대', '우려', '경고', '목표', '분석', '평가', '결정', '검토', '참여', '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', '타격', '피해', '직격탄', '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', '달성', '기준', '행사', '이사', '의결', '개정', '취지', '적극', '개최', '진행', '예정', '상황', '필요', '대응', '마련', '운영', '관리', '적용', '이용', '사용', '활용', '확보', '제공', '구축', '기반', '중심', '노력', '계획', '정밀', '경우', '이상', '이하', '가운데', '가장', '포함', '제외', '기대감', '우려감', '불확실성', '가능성', '움직임', '분위기', '흐름', '국면', '대목', '차원', '입장', '배경', '결과', '모습', '모멘텀', '현상', '차이', '비중', '비율', '단계', '목적', '대상', '조원', '억원', '만원', '천원', '전문', '현지', '사회', '생산자', '제도', '재고', '면제', '속보', '단독', '기자', '특파원', '앵커', '저작권', '무단', '전재', '재배포', '금지', '뉴스', '보도', '자료', '사진', '관계자', '주장', '설명', '강조', '위원회', '법안', '회의', '통과', '정책', '의원', '장관', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인', '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '공개', '이란', '국민연금', '종전', '전쟁', '트럼프', '제안', '찬성', '대통령', '사내', '협상', '출시', '계좌', '중동', '상품', '체제', '変更', '투자증권', '성장', '시그널', '신규', '정치', '외교', '합의', '수출', '수입', '도입', '본격', '소식', '임박', '부각', '주도']
AD_FILTER = ['펀드', '투어', '캠페인', '서비스', '최초', '강화', '고객', '연금', '마스터', '코리아', '정책', '개최', '박람회', '전시회', '프로모션', '할인', '기획전', '페스티벌', '출시', '협약', 'MOU', '체결', '선정', '어워드', '스마트픽', '팔자', '사자', '증가', '감소', '목표', '꺾인', '주석', '전망', '우려', '기대', '연내', '내달', '오늘', '내일', '돌파', '연속', '급락', '투자', '매수', '매도', '수익']
THEME_BLACKLIST = ['코로나19', '메르스', '지카바이러스', '우한폐렴', '원숭이두창', '엠폭스', '아프리카돼지열병', '구제역', '광우병', '야놀자(Yanolja)', '리비안(RIVIAN)']

def check_warning_market():
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'}
    warning_count = 0
    try:
        url = f"https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1&_={int(time.time() * 1000)}"
        res = local_session.get(url, headers=headers, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) == 20:
            ma20 = sum(prices) / 20
            ma5  = sum(prices[:5]) / 5
            if prices[0] < ma20: warning_count += 1
            if ma5 < ma20:       warning_count += 1
    except: pass

    try:
        kospi_rate = get_kospi_fluctuation_rate()
        if kospi_rate <= -1.0: warning_count += 1
    except: pass

    return warning_count >= 1

def is_index_above_ma5():
    """📈 코스닥 지수가 5일선 위에 안착해 반등하는지 시황 체커"""
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        url = f"https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=5&page=1&_={int(time.time() * 1000)}"
        res = local_session.get(url, headers=headers, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) >= 5:
            ma5 = sum(prices[:5]) / 5
            return prices[0] >= ma5
    except: pass
    return True

def get_kospi_fluctuation_rate():
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'}
    try:
        url = f"https://m.stock.naver.com/api/index/KOSPI/basic?_={int(time.time() * 1000)}"
        res = local_session.get(url, headers=headers, verify=False, timeout=3).json()
        rate_str = res.get("fluctuationsRatio", "0")
        return float(str(rate_str).replace(',', ''))
    except:
        return 0.0

def search_code_from_naver(stock_name):
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = local_session.get(url, headers=headers, timeout=3).json()
        if data.get('result') and data['result'].get('stocks'):
            return data['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_news_keywords():
    local_session = requests.Session()
    try:
        now_minute = datetime.datetime.now(KST).minute
        if not (30 <= now_minute < 40): return pd.DataFrame()
        full_text = ""
        theme_phrases = []
        for page in range(1, 10):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = local_session.get(url, verify=False, timeout=5)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
            for sub in soup.select('.articleSubject a'):
                title_text = sub.get_text(strip=True)
                full_text += title_text + " \n "
                for m in re.findall(r"['\"‘“](.*?)['\"’”]", title_text):
                    clean = re.sub(r'(수혜|관련주|테마주|대장주|강세|상한가|특징주|급등|주목|부각)', '', m).strip()
                    clean = re.sub(r'[^\w\s]', '', clean).strip()
                    if 1 < len(clean) <= 12 and clean.count(' ') <= 1 and not any(ad in clean for ad in AD_FILTER):
                        theme_phrases.append(clean)
                for m in re.findall(r'([가-힣a-zA-Z0-9]+)(?:\s+)?(?:관련주|테마주|수혜주|대장주|섹터|주도주)', title_text):
                    m = re.sub(r'[^\w\s]', '', m).strip()
                    if 1 < len(m) <= 10 and not any(ad in m for ad in AD_FILTER): theme_phrases.append(m)
        core_keywords = ['의료AI', '비만치료제', '전고체', '자율주행', '로봇', '반도체', '바이오시밀러', '원격진료', '탈플라스틱', '신재생', '원전', '우주항공', 'UAM', '메타버스', 'OLED', 'LFP', 'HBM', 'CXL', '온디바이스', 'AI', '초전도체', '양자암호', '저전력', '데이터센터', '웹툰', '비트코인', 'STO', '밸류업', '방산', '조선', '피지컬AI', '전력설비', '유리기판', '액침냉각', '엔터', '화장품', '미용기기', '제약', '바이오', '이차전지', '2차전지', '폐배터리', '수소', '태양광', '마이크로바이옴']
        for word in core_keywords: theme_phrases.extend([word] * full_text.count(word))
        final_keywords = [word for word in theme_phrases if word not in STOPWORDS and not any(junk in word for junk in ['특징주', '강세', '급등', '상승', '하락'])]
        top_10 = [(word, count) for word, count in Counter(final_keywords).most_common() if count > 1][:10]
        if not top_10: return pd.DataFrame()
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        return pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_10, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
    except Exception as e: return pd.DataFrame()

def get_market_cap(code):
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = local_session.get(url, headers=headers, verify=False, timeout=3)
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
    except:
        pass
    return 0

def get_real_money_themes():
    local_session = requests.Session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    try:
        now = datetime.datetime.now(KST)
        is_market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 30)
        time_str = now.strftime('%H:%M')
        
        res = local_session.get("https://finance.naver.com/sise/theme.naver", headers=headers, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        table = soup.find('table', {'class': 'theme_area'}) or soup.find('table', {'class': 'type_1'})
        if not table:
            return pd.DataFrame(), is_market_closed, {}

        raw_themes = [{'name': a.text.strip(), 'url': "https://finance.naver.com" + a['href']} for tds in [tr.find_all('td') for tr in table.find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a]
        themes = [t for t in raw_themes if not any(b in t['name'] for b in THEME_BLACKLIST)][:20]
        
        theme_data_list = []
        print("▶️ 실시간 주도 테마 수집 시작 (1등 독식 5배수 필터 적용)...")
        
        for theme in themes:
            try:
                soup = BeautifulSoup(local_session.get(theme['url'], headers=headers, verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                stocks = []
                type_5_table = soup.find('table', {'class': 'type_5'})
                if not type_5_table: continue
                
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
                        except: continue
                        
                stocks_val = sorted(stocks, key=lambda x: x['value'], reverse=True)[:5]
                if len(stocks_val) >= 2:
                    stocks_rate = sorted(stocks_val, key=lambda x: x['rate'], reverse=True)
                    theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks_rate})
            except: continue
            
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
    local_session = requests.Session()
    try:
        soup = BeautifulSoup(local_session.get("https://finance.naver.com/sise/lastsearch2.naver", verify=False).content, 'html.parser', from_encoding='euc-kr')
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
        return pd.DataFrame(data, columns=['순위', '종목명', '현재가', '등락률(%)', '종목코드'])
    except: return pd.DataFrame()

def get_naver_main_news():
    local_session = requests.Session()
    try:
        soup = BeautifulSoup(local_session.get("https://finance.naver.com/news/mainnews.naver", verify=False, timeout=5).content, 'html.parser', from_encoding='cp949')
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
        return pd.DataFrame(news_list, columns=['업데이트 시간', '언론사', '기사 제목', '요약 내용', '기사 링크'])
    except: return pd.DataFrame()

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
            is_real_closing = now_check.hour > 15 or (now_check.hour == 15 and now_check.minute >= 30) or now_check.hour < 9
            
            if is_market_closed or is_real_closing:
                try:
                    sheet_raw = doc.worksheet("수급_Raw")
                    today_str = df_theme.iloc[0]['날짜']
                    all_data = sheet_raw.get_all_values()
                    df_raw = df_theme.drop(columns=['시간'])
                    combined_data = df_raw.values.tolist() + [row for row in all_data[1:] if len(row) > 0 and row[0] != today_str]
                    combined_data.sort(key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
                    combined_data.sort(key=lambda x: x[0], reverse=True)
                    sheet_raw.batch_clear(['A2:Z'])
                    sheet_raw.update(range_name="A2", values=combined_data, value_input_option="USER_ENTERED")
                    print("✅ [수급_Raw] 누적 기록 완료")
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
    local_session = requests.Session()
    try:
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        res = local_session.get(url, verify=False, timeout=5)
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
                include_kws = ['실적', '발표', '만기', '배당', '금통위', 'FOMC', '고용', '학회', '임상', '상장', '개막', '출시']
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
            except: return datetime.date(2099, 12, 31)
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
            except:
                pass
        if hide_start != -1:
            requests_list.append({"updateDimensionProperties": {"range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": hide_start, "endIndex": hide_end}, "properties": {"hiddenByUser": True}, "fields": "hiddenByUser"}})
        if requests_list:
            doc.batch_update({"requests": requests_list})
            print(f"📅 HYEOKS 주요일정 관리 완료 (완벽 중복제거 + 서술형 뉴스 차단 + 포맷팅 + 과거숨김)")
    except Exception as e:
        print(f"❌ 주요일정 시트 관리 에러: {e}")

class ScannerState:
    def __init__(self):
        self.super_leader_count = 0
        self.platform_leader_count = 0

global_state = ScannerState()

def safe_int(v, default=0):
    try:
        if v in [None, "", "null"]: return default
        return int(str(v).replace(",", "").strip())
    except: return default

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
    req = session_obj if session_obj else requests.Session()
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
        res = req.get(f"{KIS_URL_BASE}/uapi/uapi/domestic-stock/v1/quotations/inquire-daily-overtimeprice", headers=headers, params=params, verify=False, timeout=5)
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
    except Exception: pass

    try:
        r = req.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3, verify=False)
        if r.status_code == 200:
            j = r.json()
            night_info = j.get("nightMarketPriceInfo") or j.get("overMarketPriceInfo") or {}
            nxt_price = safe_int(night_info.get("closePrice") or night_info.get("price") or night_info.get("overPrice"))
            if nxt_price > 0: nxt_close = nxt_price
    except Exception: pass

    if krx_close == 0:
        try:
            r = req.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3, verify=False)
            if r.status_code == 200:
                j = r.json()
                ot_info = j.get("overTimePriceInfo") or j.get("overMarketPriceInfo") or {}
                ot_price = safe_int(ot_info.get("closePrice") or ot_info.get("price") or ot_info.get("overPrice"))
                if ot_price > 0: krx_close = ot_price
        except Exception: pass

    return krx_close, nxt_close

def analyze_single_stock(name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate, past_theme_map, static_db, theme_historical_max, long_term_stocks, index_above_ma5):
    local_session = requests.Session()
    time.sleep(random.uniform(0.1, 0.4))
    
    fail_fallback = [
        name, f"'{code}", 0, "0.00%", 0, 0, "전일비 100%", "⚡ 관망 (데이터 수집 오류)",
        "👀 [관망] 타점 미도달", "AI 브리핑 대기중", 0, 0, 0, 0, "🟡 일반형", "📉 이격 과다",
        "100.0%", "평범(X)", "🟡 [V.평년수준]", "개별주/기타", "⚪ [M.평년수준] 1.0배 (0.0%)", 0,
        "🏦기:0.0억 / 🌎외:0.0억", 0, 0, "NORMAL", "", "", "정규장", 0, "0점 (오류)"
    ]

    try:
        desktop_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

        # --------------------------------------------------
        # STEP 1: fchart 일봉 데이터 수집 및 거래대금 축적
        # --------------------------------------------------
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        try:
            res = local_session.get(url, verify=False, timeout=3)
            root = ET.fromstring(res.text)
        except Exception:
            return fail_fallback, None

        history = []
        high_prices = []
        max_hist_tv_krw = 0

        for item in root.findall(".//item"):
            data = item.get("data").split("|")
            open_p, high_p, low_p, close_p, vol = int(data[1]), int(data[2]), int(data[3]), int(data[4]), int(data[5])
            if vol == 0: continue
            day_tv_krw = close_p * vol
            if day_tv_krw > max_hist_tv_krw:
                max_hist_tv_krw = day_tv_krw
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

        today_str_ymd = datetime.datetime.now(KST).strftime('%Y%m%d')
        if last_day['date'] == today_str_ymd:
            yest_close = int(df_hist['close'].iloc[-2]) if len(df_hist) >= 2 else current_price
        else:
            yest_close = current_price

        change_rate = (current_price - yest_close) / yest_close if yest_close > 0 else 0.0

        # --------------------------------------------------
        # STEP 2: 네이버 실시간 API 동기화
        # --------------------------------------------------
        live_success = False
        try:
            rt_url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            rt_json = local_session.get(rt_url, headers=desktop_headers, verify=False, timeout=2).json()
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
        except: pass

        if not live_success:
            try:
                mobile_headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X)"}
                rt_json2 = local_session.get(f"https://m.stock.naver.com/api/stock/{code}/basic", headers=mobile_headers, verify=False, timeout=2).json()
                if rt_json2 and rt_json2.get('closePrice'):
                    live_p = int(str(rt_json2['closePrice']).replace(',', '').strip())
                    if live_p > 0:
                        current_price = live_p
                        if rt_json2.get('accumulatedTradingVolume'): today_vol = int(str(rt_json2['accumulatedTradingVolume']).replace(',', '').strip())
                        if rt_json2.get('highPrice'): today_high = int(str(rt_json2['highPrice']).replace(',', '').strip())
                        if rt_json2.get('lowPrice'): today_low = int(str(rt_json2['lowPrice']).replace(',', '').strip())
                        if rt_json2.get('openPrice'): open_price = int(str(rt_json2['openPrice']).replace(',', '').strip())
                        if rt_json2.get('fluctuationsRatio'): change_rate = float(str(rt_json2['fluctuationsRatio']).replace('%', '').replace('+', '').strip()) / 100.0
                        live_success = True
            except: pass

        static_info = static_db.get(code)
        static_info_to_save = None
        if static_info:
            market_cap, is_junk, is_financial_risk, is_chronic_loss = static_info['market_cap'], static_info['is_junk'], static_info['is_fin_risk'], static_info['is_chronic_loss']
        else:
            market_cap, is_junk, is_financial_risk, is_chronic_loss = get_market_cap(code), False, False, False
            static_info_to_save = [f"'{code}", name, market_cap, str(is_junk), str(is_financial_risk), str(is_chronic_loss)]
            
        is_fatal_drop = is_junk or is_financial_risk

        krx_close = 0
        nxt_close = 0
        now_kst_api = datetime.datetime.now(KST)
        is_regular_market = (9 <= now_kst_api.hour < 15) or (now_kst_api.hour == 15 and now_kst_api.minute <= 40)
        market_type = "정규장 진행중" if is_regular_market else "정규장"

        if not is_regular_market:
            try:
                krx_close, nxt_close = fetch_extra_closing_prices_from_kis(code, session_obj=local_session)
            except Exception: pass
            if nxt_close > 0:
                krx_close = 0
                market_type = "NXT"
            elif krx_close > 0:
                market_type = "KRX"

        krx_rate = ((krx_close - current_price) / current_price * 100) if krx_close > 0 and current_price > 0 else 0.0
        nxt_rate = ((nxt_close - current_price) / current_price * 100) if nxt_close > 0 and current_price > 0 else 0.0

        # --------------------------------------------------
        # STEP 3: 파생 변수 계산 및 종산 필터 탑재
        # --------------------------------------------------
        is_upper_limit = change_rate >= 0.295
        yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) >= 2 else today_vol
        trading_value = current_price * today_vol

        ma5 = int(df_hist['close'].tail(5).mean()) if len(df_hist) >= 5 else current_price
        ma20 = int(df_hist['close'].tail(20).mean()) if len(df_hist) >= 20 else current_price
        ma60 = int(df_hist['close'].tail(60).mean()) if len(df_hist) >= 60 else current_price

        # 1. 일목 120선 대용 (6개월 최고/최저 중앙값) 분석 엔진 이식
        high_prices_120 = high_prices[-120:] if len(high_prices) >= 120 else high_prices
        low_prices_120 = [h['low'] for h in history[-120:]] if len(history) >= 120 else [h['low'] for h in history]
        highest_120d = max(high_prices_120[:-1]) if len(high_prices_120) > 1 else today_high
        lowest_120d = min(low_prices_120[:-1]) if len(low_prices_120) > 1 else today_low
        ilmok_120_mid = (highest_120d + lowest_120d) / 2
        is_ilmok_sangsang = current_price > ilmok_120_mid

        # 2. 엔벨로프 하단선 (-20% 과매도선) 추출
        envelope_lower_20 = ma20 * 0.80
        min_nulim_tv = 10_000_000_000 if is_warning_market else 5_000_000_000
        min_breakout_tv = 10_000_000_000  # 돌파 최소 거래대금 100억
        min_danta_rate = 0.03            # 단타 최소 등락률 3%
        is_envelope_over_under = (
            current_price <= envelope_lower_20
            and trading_value >= min_nulim_tv
            and not is_upper_limit          # 상한가 종목 제외
            and change_rate <= 0.10         # 10% 이상 급등 종목 제외
        )

        high_prices_60 = high_prices[-60:] if len(high_prices) >= 60 else high_prices
        high_60d_calc = max(high_prices_60[:-1]) if len(high_prices_60) > 1 else today_high
        high_250d_calc = max(high_prices[:-1]) if len(high_prices) > 1 else today_high
        display_high_60d = max(high_prices_60) if high_prices_60 else today_high
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

        # 3. 3시 이후 장막판 가짜 돌파 분쇄용 윗꼬리 감쇠 필터 (타임락 대체 보완책)
        is_afternoon_check = (now_kst_api.hour == 15)
        is_shadow_disqualified = False
        if is_afternoon_check:
            daily_range = today_high - today_low
            if daily_range > 0 and (upper_shadow / daily_range) > 0.30:
                is_shadow_disqualified = True
            if upper_shadow_ratio >= 0.03:
                is_shadow_disqualified = True

        gap_ratio = (open_price - yest_close) / yest_close if yest_close > 0 else 0
        is_huge_gap = gap_ratio >= 0.04

        avg_vol_10 = df_hist['volume'].tail(11).head(10).mean() if len(df_hist) >= 2 else today_vol
        vol_ratio_10d = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
        vol_ratio_yest = (today_vol / yest_vol) * 100 if yest_vol > 0 else 0
        surge_rate_20d = (current_price - recent_20d_min) / recent_20d_min if recent_20d_min > 0 else 0

        if is_upper_limit and today_vol > 0:
            vol_ratio_10d = max(vol_ratio_10d, 500)
            vol_ratio_yest = max(vol_ratio_yest, 500)

        # --------------------------------------------------
        # STEP 4: Adaptive 칼만 필터 및 가속도 추세 엔진
        # --------------------------------------------------
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
                else:
                    K = K_init

                x_hat = x_hat + K * (float(z) - x_hat)
                p     = (1 - K) * p_hat
                kalman_ma.append(x_hat)

            if len(kalman_ma) >= 11:
                slope_1  = kalman_ma[-1] - kalman_ma[-2]
                slope_3  = (kalman_ma[-1] - kalman_ma[-4]) / 3
                accel    = slope_1 - slope_3
            else:
                slope_1 = slope_3 = accel = 0.0

            slope_pct = slope_1 / current_price * 100 if current_price > 0 else 0
            accel_pct = accel   / current_price * 100 if current_price > 0 else 0

            if   slope_pct > 0.3  and accel_pct >  0.05:  trend_phase = "ACCELERATION"
            elif slope_pct > 0.1  and accel_pct >= -0.05: trend_phase = "STEADY"
            elif slope_pct > 0    and accel_pct <  -0.05: trend_phase = "DECELERATION"
            else:                                          trend_phase = "REVERSAL"

            is_kalman_uptrend   = slope_pct > 0.05
            is_kalman_downtrend = slope_pct < -0.05

            min_slope_th = current_price * 0.0008
            if len(kalman_ma) >= 4:
                z_now  = kalman_ma[-1] - kalman_ma[-2]
                z_prev = kalman_ma[-2] - kalman_ma[-3]
                kalman_turned_green = (z_now > min_slope_th and z_prev > 0 and kalman_ma[-2] >= kalman_ma[-3] and kalman_ma[-3] <= kalman_ma[-4])
                kalman_turned_red   = (z_now < -min_slope_th and z_prev < 0 and kalman_ma[-2] <= kalman_ma[-3] and kalman_ma[-3] >= kalman_ma[-4])
            else:
                kalman_turned_green = kalman_turned_red = False

            trend_length       = 0
            trend_start_kalman = kalman_ma[-1]
            for i in range(len(kalman_ma) - 1, 0, -1):
                if kalman_ma[i] > kalman_ma[i - 1]:
                    trend_length      += 1
                    trend_start_kalman = kalman_ma[i - 1]
                else:
                    break

            atr_climb = (kalman_ma[-1] - trend_start_kalman) / atr_14 if atr_14 > 0 else 0.0

            if kalman_turned_green:
                secret_tajeom = "🟢 [시크릿] 추세 전환 (1차 매수 타점)"
            elif is_kalman_uptrend:
                if trend_phase == "ACCELERATION":
                    if atr_climb >= 3.0 and trend_length >= 10: secret_tajeom = "🔴 [시크릿] 3차 파동 가속 (전량 익절)"
                    else: secret_tajeom = "🚀 [시크릿] 상승 가속 (불타기 or 익절 준비)"
                elif trend_phase == "STEADY":
                    if atr_climb >= 1.5 and trend_length >= 5: secret_tajeom = "🟡 [시크릿] 2차 파동 안정 (본절 스탑 상향)"
                    else: secret_tajeom = "🟢 [시크릿] 1차 파동 진행 (추세 홀딩)"
                elif trend_phase == "DECELERATION": secret_tajeom = "🟡 [시크릿] 추세 감속 (절반 익절 검토)"
                else: secret_tajeom = "🟢 [시크릿] 상승 추세 유지"
            elif kalman_turned_red: secret_tajeom = "📉 [시크릿] 하락 추세 전환 (전량 매도)"
            else: secret_tajeom = "📉 [시크릿] 노이즈 및 하락장 (관망)"

            trend_start_price = current_price
        except Exception as e:
            atr_14 = current_price * 0.03
            is_kalman_uptrend = False
            kalman_turned_red = False
            trend_phase = "REVERSAL"
            trend_start_price = current_price
            secret_tajeom = ""
            slope_pct = accel_pct = 0.0
            trend_length = 0

        # --------------------------------------------------
        # STEP 5 ~ STEP 9: 캔들 및 수급 파싱 (진성 5일 누적분만 보존)
        # --------------------------------------------------
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

        is_strong_dual_buy = False
        supply_text        = ""
        acc_i_buy_won      = 0
        acc_f_buy_won      = 0
        dual_buy_days      = 0
        i_buy_today        = 0
        f_buy_today        = 0
        is_today_data_in_frgn = False
        today_str_dot = datetime.datetime.now(KST).strftime('%Y.%m.%d')

        try:
            frgn_url  = f"https://finance.naver.com/item/frgn.naver?code={code}&_={int(time.time() * 1000)}"
            frgn_res  = local_session.get(frgn_url, headers=desktop_headers, verify=False, timeout=3)
            frgn_soup = BeautifulSoup(frgn_res.content, 'html.parser', from_encoding='euc-kr')
            rows      = frgn_soup.select("table.type2 tr")
            valid_days = 0

            for r_tag in rows:
                cols = r_tag.select("td")
                if len(cols) >= 7 and cols[0].text.strip().replace('.', '').isdigit():
                    row_date_str = cols[0].text.strip()
                    try: close_price_day = int(cols[1].text.strip().replace(',', ''))
                    except: close_price_day = current_price
                    try: i_vol = int(cols[5].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except: i_vol = 0
                    try: f_vol = int(cols[6].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except: f_vol = 0

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
        except Exception: pass
        # STEP 8 루프 끝난 후, 아래 블록 추가
        # ── KIS 당일 기관 순매수 보강 ──────────────────
        inst_ntby_eok = 0.0
        if KIS_TOKEN and KIS_APP_KEY and KIS_APP_SECRET:
            try:
                kis_h = {
                    "authorization": f"Bearer {KIS_TOKEN}",
                    "appkey": KIS_APP_KEY,
                    "appsecret": KIS_APP_SECRET,
                    "custtype": "P",
                    "tr_id": "FHKST01010100"
                }
                kis_res = local_session.get(
                    "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=kis_h,
                    params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code},
                    verify=False,
                    timeout=3
                ).json()
                if kis_res.get("rt_cd") == "0":
                    out = kis_res["output"]
                    pgtr_qty = int(
                        str(out.get("pgtr_ntby_qty", "0"))
                        .replace(",", "").replace("+", "") or "0"
                    )
                    pgtr_ntby_eok = (pgtr_qty * current_price) / 100_000_000
            except:
                pass

        # ============================================================
        # 🎯 [프로그램 완전 대체] 100% 무결성 스마트 머니 강도(SMI) 지수 연산 레이어
        # ============================================================
        # 기존 SMI 블록 (smi_ratio 계산 후) 전체를 아래로 교체

        # ── SMI 수급 강도 지수 ──
        if len(df_hist) >= 11:
            avg_tv_10d = df_hist['trading_value'].iloc[-11:-1].mean()
        elif len(df_hist) >= 2:
            avg_tv_10d = df_hist['trading_value'].iloc[:-1].mean()
        else:
            avg_tv_10d = trading_value

        smi_ratio = trading_value / avg_tv_10d if avg_tv_10d > 0 else 1.0
        market_cap_won = market_cap * 100_000_000
        turnover_rate = (trading_value / market_cap_won) * 100 if market_cap_won > 0 else 0.0
        pg_amount_eok = trading_value / 100_000_000  # 스케일링 호환용

        # ── 기관 순매수 부호 처리 ──
        pgtr_sign = "+" if pgtr_ntby_eok > 0 else ""
        pgtr_direction = pgtr_ntby_eok >= 0  # True = 매수 방향

        if smi_ratio >= 5.0 and turnover_rate >= 3.0 and pgtr_direction:
            program_text = f"🔥 [수급강도 폭발] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio >= 2.5 and turnover_rate >= 2.0 and pgtr_direction:
            program_text = f"🔥 [수급강도 유입] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio >= 2.5 and not pgtr_direction:
            # 거래량은 폭발했지만 프로그램은 매도 → 개인 단타 or 세력 분산 가능성
            program_text = f"⚠️ [수급강도 혼조] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        elif smi_ratio <= 0.4:
            program_text = f"💤 [수급강도 절벽] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        else:
            program_text = f"⚪ [수급강도 평년] {smi_ratio:.1f}배 / 프로그램:{pgtr_sign}{pgtr_ntby_eok:.1f}억"
        acc_i_buy_eok = acc_i_buy_won / 100_000_000
        acc_f_buy_eok = acc_f_buy_won / 100_000_000
        today_dual_buy_ratio = ((i_buy_today + f_buy_today) / trading_value) * 100 if trading_value > 0 else 0.0
        is_foreigner_active_buy = (
            # 조건 1: SMI 폭발 + 외인 5일 누적 양수 확인
            (smi_ratio >= 3.0)
            and (turnover_rate >= 5.0)
            and (change_rate >= 0.04)
            and (acc_f_buy_eok >= 10)             # 외인 5일 누적 최소 +10억
            and (acc_f_buy_eok > acc_i_buy_eok)   # 외인이 기관보다 많이 산 경우
        ) or (
            # 조건 2: 프로그램 대량 유입 + 외인 누적 확인
            pgtr_ntby_eok >= 500
            and change_rate >= 0.03
            and acc_f_buy_eok >= 50
        )

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

        is_accumulation_cand = False
        if is_true_history_leader and is_deep_correction and not is_recent_overheated and is_volume_dead and not is_long_shadow and not is_financial_risk:
            if not is_upper_limit and ((abs(current_price - ma20) / ma20 < 0.03) or (abs(current_price - ma60) / ma60 < 0.03) or is_double_bottom):
                is_accumulation_cand = True

        # --------------------------------------------------
        # 테마 매핑 및 대장 레이블 추출
        # --------------------------------------------------
        is_danta_range = min_danta_rate <= change_rate < 0.295
        has_today_theme = False
        has_theme = False
        is_theme_leader_raw = False
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
            has_theme = False

        is_true_theme_leader = is_theme_leader_raw and (trading_value >= min_breakout_tv)
        is_theme_daejang = is_true_theme_leader and is_danta_range and not (is_junk or is_financial_risk)
        is_real_hubal = has_theme and not is_theme_leader_raw
        is_theme_hubal = is_real_hubal and is_danta_range and not (is_junk or is_financial_risk)

        # --------------------------------------------------
        # 🎯 STEP 10: [신접갈거조재 무결성 검증 통과]
        # --------------------------------------------------
        is_neat_tail = True
        if (today_high - today_low) > 0:
            is_neat_tail = ((today_high - current_price) / (today_high - today_low)) <= 0.15

        is_tight_gap = (current_price >= high_60d_calc * 0.96) and (current_price <= high_60d_calc * 1.04)
        is_theme_valid = (my_theme_name != "개별주/기타") or (leader_text == "🔥대장주(O)")

        is_jongbe_cand = (
            index_above_ma5                      
            and not is_upper_limit               
            and is_today_yangbong                
            and is_neat_tail                     
            and is_tight_gap                     
            and is_converging                    
            and is_theme_valid                   
            and not is_fatal_drop                
            and (trading_value >= 30_000_000_000) 
        )

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

        is_near_high = current_price >= (high_60d_calc * 0.90) or yest_close >= (high_60d_calc * 0.90)
        is_near_52w_high = current_price >= (high_250d_calc * 0.90) or yest_close >= (high_250d_calc * 0.90)

        if is_near_52w_high: dist_text = "🎯 52주신고가 턱밑"
        elif is_near_high: dist_text = "🎯 60일전고 턱밑"
        elif current_price >= high_60d_calc * 0.80: dist_text = "🟢 매물대 소화중"
        elif is_deep_correction: dist_text = "📉 고점 대비 큰 폭 조정"
        else: dist_text = "📉 이격 과다"

        # --------------------------------------------------
        # STEP 13: 가변 장세 스코어링 및 점수 바인딩 정상화
        # --------------------------------------------------
        base_score = 0
        master_tajeom_suffix = ""

        track_type = "눌림" if (is_accumulation_cand or is_jongbe_cand or is_envelope_over_under) else ("돌파" if current_price >= ma20 else "눌림")
        if is_warning_market and track_type == "돌파":
            base_score -= 25
            master_tajeom_suffix += " ⚠️[하락장 돌파제한]"

        is_core_buy_zone = (surge_rate_20d <= 0.25) and (change_rate < 0.07) and (current_price >= ma60 * 0.85)
        is_long_term_pick = (name in long_term_stocks) and not is_recent_overheated and is_core_buy_zone
        
        if is_long_term_pick:
            base_score += 20  
            master_tajeom_suffix += " 🎖️(코어 포트폴리오)"

        # 스마트머니 가산점 결합 처리
        if "[M.기관폭발]" in program_text or "[M.수급유입]" in program_text: base_score += 25
        if "[M.기관폭발]" in program_text: base_score += 15

        is_relative_strong = (kospi_rate <= -1.0) and (change_rate >= 0.03)
        if is_relative_strong:
            master_tajeom_suffix += " 💪(하락장 역행)"
            base_score += 10

        if is_jongbe_cand:
            base_score += 50                     
            if is_near_52w_high: base_score += 15
            if is_strong_dual_buy: base_score += 15
        elif is_accumulation_cand:
            base_score += 40
            if is_double_bottom: base_score += 15
            if acc_i_buy_eok >= 10: base_score += 10
        else:
            if is_near_52w_high: base_score += 20
            elif current_price >= (high_60d_calc * 0.90): base_score += 15
            if vol_ratio_yest >= 300 and vol_ratio_10d >= 200: base_score += 15
            elif vol_ratio_yest >= 150: base_score += 10
            if is_strong_dual_buy: base_score += 15
            if acc_i_buy_eok >= 50: base_score += 15

        high_retention = current_price / today_high if today_high > 0 else 0
        if high_retention >= 0.97 and change_rate >= 0.10 and trading_value >= 100_000_000_000:
            base_score += 30
            master_tajeom_suffix += " 👑(진성대장)"

        tajeom_multiplier = 0.0
        master_tajeom_base = "⏸️ [대기] 분석 중"

        if is_fatal_drop:
            master_tajeom_base = "🚫 [제외] 상폐/재무위험"
            tajeom_multiplier = 0.0
        elif is_envelope_over_under:
            master_tajeom_base = "📉 [하단] 역삼각형 스케일인 과이격 타점"
            tajeom_multiplier = 1.45
            target_price = int(ma20)
            stop_loss = int(current_price * 0.93)
        elif is_foreigner_active_buy:
            master_tajeom_base = "💎 [관심/수급] 외인 집중배팅 (Non-프로그램)"
            tajeom_multiplier = 1.4
        elif is_upper_limit:
            master_tajeom_base = "🚀 [당일/단타] 대장주 불기둥 (상한가 안착/추격금지)"
            tajeom_multiplier = 1.3
        elif is_jongbe_cand:
            master_tajeom_base = "🎯 [종베] 주도주 관성 파동"
            tajeom_multiplier = 1.5              
        elif is_accumulation_cand:
            master_tajeom_base = "🌱 [중장기/모아가기] 지지선 방어 및 거래량 바닥"
            tajeom_multiplier = 1.4
        elif is_theme_daejang:
            master_tajeom_base = "🚀 [당일/단타] 대장주 불기둥"
            tajeom_multiplier = 1.3
        elif is_theme_hubal:
            master_tajeom_base = "🚀 [당일/단타] 테마 후발주"
            tajeom_multiplier = 1.15
        elif is_platform_breakout:
            master_tajeom_base = "📦 [스윙/추세] 박스권 탈출"
            tajeom_multiplier = 1.25
        elif "1차" in secret_tajeom or "🟢 [시크릿] 추세 전환" in secret_tajeom:
            master_tajeom_base = "🕵️ [관심/수급] 세력선 포착 (시크릿)"
            tajeom_multiplier = 1.35
        elif ("🌟" in signal):
            master_tajeom_base = "🌟 [관심/수급] 기준봉 포착"
            tajeom_multiplier = 0.9
        else:
            master_tajeom_base = "👀 [관망] 타점 미도달"
            tajeom_multiplier = 0.6

        master_tajeom = master_tajeom_base + master_tajeom_suffix

        if not is_fatal_drop and not is_envelope_over_under:
            if is_long_shadow or is_huge_gap:
                master_tajeom += " ⚠️(윗꼬리/이격)"
                if is_warning_market and is_long_shadow and not (is_foreigner_active_buy or is_long_term_pick):
                    tajeom_multiplier = 0.0
                    master_tajeom = "👀 [관망] 하락장 윗꼬리 리스크 과다"
                else: tajeom_multiplier -= 0.3
            if "3차 파동" in secret_tajeom or "하락 추세 전환" in secret_tajeom:
                tajeom_multiplier = 0.0
                master_tajeom = "👀 [관망] 3차 파동 고점 리스크"

        if not is_envelope_over_under:
            if is_accumulation_cand or is_long_term_pick:
                stop_loss = int(min(ma60 * 0.95, recent_60d_min * 0.98))
                target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.15)
            elif is_kalman_uptrend:
                target_price = int(current_price + (atr_14 * 2.0))
                stop_loss = int(current_price - (atr_14 * 1.0))
            else:
                target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.05)
                stop_loss = int(min(ma20, current_price * 0.95))

        if secret_tajeom and "관망" not in master_tajeom and "매수금지" not in master_tajeom and not is_upper_limit:
            master_tajeom = f"{master_tajeom} | {secret_tajeom}"

        is_super_leader = (change_rate >= 0.15) and (trading_value >= 100_000_000_000) and (smi_ratio >= 3.0)
        is_absolute_protected = is_super_leader or is_foreigner_active_buy or is_long_term_pick

        if is_absolute_protected and not is_envelope_over_under:
            stop_loss = int(current_price * 0.96) if not (is_accumulation_cand or is_long_term_pick) else stop_loss
            target_price = int(current_price * 1.15) if not (is_accumulation_cand or is_long_term_pick) else target_price
            tajeom_multiplier = max(1.2, tajeom_multiplier)
            if is_foreigner_active_buy: master_tajeom += " 💎(외인집중/면책)"
            elif is_long_term_pick: master_tajeom += " 🎖️(코어픽/면책)"
            else: master_tajeom += " 🔥(절대대장/면책)"

        quant_score = int(max(0, (base_score + 10) * tajeom_multiplier))
        cutoff_score = 40 if is_warning_market else 25
        
        if quant_score < cutoff_score and not is_absolute_protected and not is_envelope_over_under:
            master_tajeom = f"👀 [관망] 스코어 미달 (기준:{cutoff_score}점)"

        score_display = f"{quant_score}점 ({track_type})"
        is_seed_tag = "SEED" if (is_accumulation_cand or is_long_term_pick or is_envelope_over_under) else "NORMAL"

        i_sign = "+" if acc_i_buy_eok > 0 else ""
        f_sign = "+" if acc_f_buy_eok > 0 else ""
        frgn_label = ""
        if acc_f_buy_eok >= 50:
            frgn_label = " 🌎💎(외인대량)"
        elif acc_f_buy_eok >= 20:
            frgn_label = " 🌎(외인집중)"
        elif acc_f_buy_eok <= -20:
            frgn_label = " 🌎🔵(외인이탈)"

        supply_status_col = (
            f"🏦기(5일):{i_sign}{acc_i_buy_eok:.1f}억 / "
            f"🌎외(5일):{f_sign}{acc_f_buy_eok:.1f}억"
            f"{frgn_label}"
        )
        krx_str = f"'{'+' if krx_rate > 0 else ''}{krx_rate:.2f}% ({krx_close:,}원)" if krx_close > 0 else ""
        nxt_str = f"'{'+' if nxt_rate > 0 else ''}{nxt_rate:.2f}% ({nxt_close:,}원)" if nxt_close > 0 else ""

        result_row = [
            name, f"'{code}", current_price, f"{change_rate * 100:.2f}%",
            int(ma5), int(ma20), vol_ratio_text, signal,
            master_tajeom, "AI 브리핑 대기중", today_high, today_low, int(display_high_60d),
            market_cap, shadow_text, dist_text, disp_text, leader_text, vol_status_text, my_theme_name,
            program_text, int(display_high_250d), supply_status_col,
            target_price, stop_loss, is_seed_tag,
            krx_str, nxt_str, market_type, 
            quant_score, score_display
        ]

        return result_row, static_info_to_save
    except Exception as e:
        print(f"❌ 분석 에러 [{name}]: {e}")
        return fail_fallback, None

def update_technical_data(df_theme, all_theme_map):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)

        cleanup_and_reorder(doc, "접속로그", 1)
        cleanup_and_reorder(doc, "DB_중장기", 0)

        long_term_stocks = set()
        try:
            db_trend_data = doc.worksheet("DB_중장기").get_all_values()
            for row in db_trend_data[1:]:
                if len(row) >= 5:
                    for col_idx in [3, 4]: 
                        if len(row) > col_idx and row[col_idx].strip():
                            stock_nm = row[col_idx].split('(')[0].strip() 
                            long_term_stocks.add(stock_nm)
        except Exception as e: pass

        print("▶️ 기술적 지표 초고속 멀티프로세싱 판독 시작...")
        is_warning_market = check_warning_market()
        kospi_rate = get_kospi_fluctuation_rate()
        index_above_ma5 = is_index_above_ma5()

        name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in doc.worksheet("기업정보").get_all_values()[1:] if len(row) >= 3}

        try: static_sheet = doc.worksheet("DB_정적데이터")
        except:
            static_sheet = doc.add_worksheet(title="DB_정적데이터", rows="1000", cols="6")
            static_sheet.append_row(["종목코드", "종목명", "시가총액", "관리종목", "재무위험", "만성적자"])

        now_time = datetime.datetime.now(KST)
        is_reset_time = (now_time.hour == 7) or (now_time.hour == 8 and now_time.minute < 50) or len(static_sheet.get_all_values()) <= 5
        is_preserve_time = now_time.hour < 8 or (now_time.hour == 8 and now_time.minute < 50)
        is_regular_market = (9 <= now_time.hour < 15) or (now_time.hour == 15 and now_time.minute <= 40)
        
        static_db = {}
        if is_reset_time: static_sheet.batch_clear(['A2:F'])
        else:
            try:
                for row in static_sheet.get_all_values()[1:]:
                    if len(row) >= 6:
                        code_key = str(row[0]).replace("'", "").strip().zfill(6)
                        cap_clean = re.sub(r'[^0-9]', '', str(row[2]))
                        static_db[code_key] = {
                            'market_cap': int(cap_clean) if cap_clean else 0,
                            'is_junk': row[3] == 'True',
                            'is_fin_risk': row[4] == 'True',
                            'is_chronic_loss': row[5] == 'True'
                        }
            except: pass

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
                today_date = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d').date()
                theme_rank_tracker = {}
                for row in realtime_data[1:]:
                    if len(row) > max(date_idx, rank_idx, theme_idx, name_idx):
                        if str(row[date_idx]).strip() == latest_date_str:
                            try: t_rank = int(row[rank_idx])
                            except: continue
                            t_name = str(row[theme_idx]).strip()
                            s_name = str(row[name_idx]).strip()
                            if t_rank not in theme_rank_tracker: theme_rank_tracker[t_rank] = []
                            theme_rank_tracker[t_rank].append(s_name)
                            theme_rank_dict[s_name] = {'theme_rank': t_rank, 'theme_name': t_name, 'is_leader': False}
                            all_theme_map[s_name] = {'theme_name': t_name, 'is_leader': False}
                for s_name, info in theme_rank_dict.items():
                    t_rank = info['theme_rank']
                    is_leader = (theme_rank_tracker[t_rank][0] == s_name)
                    theme_rank_dict[s_name]['is_leader'] = is_leader
                    all_theme_map[s_name]['is_leader'] = is_leader
            else: today_date = datetime.datetime.now(KST).date()
        except: today_date = datetime.datetime.now(KST).date()

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
                                    row_date = datetime.datetime.strptime(r_date_str, '%Y-%m-%d').date()
                                    if row_date != today_date and row_date >= three_months_ago:
                                        if s_name not in past_theme_map: past_theme_map[s_name] = t_name
                except: pass
            try:
                scanner_data = doc.worksheet("DB_스캐너").get_all_values()
                for row in scanner_data[1:]:
                    if len(row) > 5 and row[5]:
                        if 'HYPERLINK' in str(row[0]):
                            m = re.search(r', "([^"]+)"\)', str(row[0]))
                            s_name = m.group(1) if m else str(row[0])
                        else: s_name = str(row[0]).strip()
                        t_name = str(row[5]).replace("🆕[당일]", "").replace("🕰️[과거]", "").strip()
                        if s_name and t_name and t_name != "개별주/기타" and s_name not in past_theme_map:
                            if s_name not in theme_rank_dict: past_theme_map[s_name] = t_name
            except: pass
        except: pass

        target_names = set()
        try:
            raw_data = doc.worksheet("수급_Raw").get_all_values()
            for row in raw_data[1:]:
                if len(row) >= 7:
                    stock_name = str(row[-4]).strip()
                    if stock_name and stock_name not in ["#REF!", "로딩중...", "데이터대기", "FALSE"]: target_names.add(stock_name)
        except: pass

        if not df_theme.empty:
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            for t in top_10_themes: target_names.add(str(t).strip())

        for t_name in all_theme_map.keys(): target_names.add(str(t_name).strip())

        theme_historical_max = defaultdict(int)
        if not df_theme.empty:
            curr_theme_sums = df_theme.groupby('테마명')['거래대금(억원)'].sum().to_dict()
            for t_name, t_sum in curr_theme_sums.items():
                clean_t = t_name.split(' (대장:')[0].strip()
                if t_sum > theme_historical_max[clean_t]: theme_historical_max[clean_t] = t_sum

        try:
            raw_data_values = doc.worksheet("수급_Raw").get_all_values()
            if len(raw_data_values) > 1:
                header = raw_data_values[0]
                date_idx = header.index('날짜') if '날짜' in header else 0
                theme_idx = header.index('테마명') if '테마명' in header else 2
                val_idx = header.index('거래대금(억원)') if '거래대금(억원)' in header else 6
                daily_sums = defaultdict(int)
                for row in raw_data_values[1:]:
                    if len(row) > max(date_idx, theme_idx, val_idx):
                        dt = row[date_idx]
                        th = row[theme_idx].split(' (대장:')[0].strip()
                        try:
                            v = int(str(row[val_idx]).replace(',', '').strip())
                            daily_sums[(dt, th)] += v
                        except: pass
                for (dt, th), t_sum in daily_sums.items():
                    if t_sum > theme_historical_max[th]: theme_historical_max[th] = t_sum
        except: pass

        target_dict = {}
        for name in list(target_names):
            code = name_to_code.get(name) or search_code_from_naver(name)
            if code and code not in target_dict.values(): target_dict[name] = code

        results = []
        new_static_data = []
        print(f"⚡ {len(target_dict)}개 고유 종목을 30개의 스레드로 동시 타격합니다...")

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_name = {executor.submit(analyze_single_stock, name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate, past_theme_map, static_db, theme_historical_max, long_term_stocks, index_above_ma5): name for name, code in target_dict.items()}
            for future in concurrent.futures.as_completed(future_to_name):
                res, static_res = future.result()
                if res: results.append(res)
                if static_res: new_static_data.append(static_res)

        if new_static_data:
            static_sheet.append_rows(new_static_data, value_input_option="USER_ENTERED")

        results.sort(key=lambda x: x[29], reverse=True)

        existing_data = {}
        try:
            db_scanner_sheet = doc.worksheet("DB_스캐너")
            old_data = db_scanner_sheet.get_all_values()
            for row in old_data[1:]:
                if len(row) > 15:
                    saved_code = str(row[2]).replace("'", "").strip().zfill(6)
                    existing_data[saved_code] = {"briefing": str(row[9]).strip(), "target": row[14], "stop": row[15], "raw_row": row}
        except: pass

        for r in results:
            c_code = str(r[1]).replace("'", "").strip().zfill(6)
            if is_regular_market:
                r[26] = r[27] = ""
                r[28] = "정규장 진행중"
            
            if c_code in existing_data:
                if not is_reset_time:
                    r[9] = existing_data[c_code]["briefing"]
                    r[23] = existing_data[c_code]["target"]
                    r[24] = existing_data[c_code]["stop"]
                if is_preserve_time and not is_regular_market:
                    if not r[26] and not r[27]:
                        r[26] = str(existing_data[c_code]["raw_row"][16]).strip() if len(existing_data[c_code]["raw_row"]) > 16 else ""
                        r[27] = str(existing_data[c_code]["raw_row"][17]).strip() if len(existing_data[c_code]["raw_row"]) > 17 else ""
                        r[28] = str(existing_data[c_code]["raw_row"][18]).strip() if len(existing_data[c_code]["raw_row"]) > 18 else "정규장"

        try: helper_sheet = doc.worksheet("주가데이터_보조")
        except: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="29")

        extended_headers = [
            "종목명", "종목코드", "현재가", "등락률", "5일평균", "20일평균", "거래량비율", "AI신호",
            "마스터타점", "브리핑상태", "당일고가", "당일저가", "60일고가", "시가총액", "캔들상태",
            "전고거리", "20일이격", "대장구분", "거래과열", "테마명", "프로그램", "52주고가",
            "기관/외인 누적(5일)", "목표가(AI)", "손절가(AI)", "종목쿼터", "시간외단일가(18시)", "NXT야간종가(20시)", "장구분"
        ]
        helper_sheet.batch_clear(['A1:AC'])
        helper_sheet_data = [extended_headers] + [r[:29] for r in results]
        helper_sheet.update(range_name="A1", values=helper_sheet_data, value_input_option="USER_ENTERED")

        scanner_keywords = ["[종베]", "[스윙/눌림]", "[스윙/추세]", "[당일/단타]", "[관심/수급]", "[중장기/모아가기]", "[하단]"]
        all_candidates = []
        processed_codes = set()

        for r in results:
            종목명 = r[0]
            종목코드 = str(r[1]).replace("'", "").zfill(6)
            processed_codes.add(종목코드) 
            tajeom = r[8]
            
            if any(kw in tajeom for kw in scanner_keywords):
                하이퍼링크 = f'=HYPERLINK("https://m.stock.naver.com/domestic/stock/{종목코드}/total", "{종목명}")'
                row_data = [
                    하이퍼링크, r[28] if r[28] else "정규장", f"'{종목코드}", r[2], r[3], r[19], r[7], r[6],
                    tajeom, r[9], r[30], r[20], r[21], r[22], r[23], r[24], r[26], r[27], r[28]
                ]
                all_candidates.append(row_data)

        seed_cands = []
        normal_cands = []
        for cand in all_candidates:
            tajeom_str = str(cand[8])
            if "🌱" in tajeom_str or "[중장기/모아가기]" in tajeom_str or "코어 포트폴리오" in tajeom_str or "[하단]" in tajeom_str:
                seed_cands.append(cand)
            else: normal_cands.append(cand)

        def get_score_num(x):
            try: return int(str(x[10]).split('점')[0]) if '점' in str(x[10]) else 0
            except: return 0

        seed_cands.sort(key=get_score_num, reverse=True)
        normal_cands.sort(key=get_score_num, reverse=True)

        MAX_SEED_COUNT = 5
        MAX_NORMAL_COUNT = 15

        vip_retention_cands = []
        pure_normal_cands = []
        for cand in normal_cands:
            if cand[9] != "AI 브리핑 대기중" and str(cand[9]).strip() != "":
                vip_retention_cands.append(cand)
            else: pure_normal_cands.append(cand)

        final_seed = seed_cands[:MAX_SEED_COUNT]
        final_normal = vip_retention_cands + pure_normal_cands[:max(0, MAX_NORMAL_COUNT - len(vip_retention_cands))]

        top_20_results = final_seed + final_normal
        top_20_results.sort(key=get_score_num, reverse=True)

        if not is_reset_time:
            top_20_codes = {str(x[2]).replace("'", "").strip().zfill(6) for x in top_20_results if len(x) > 2}
            for c_code, data in existing_data.items():
                if c_code not in top_20_codes:
                    if any(key in data["briefing"] for key in ["리포트 발송 완료"]):
                        top_20_results.append(data["raw_row"])
                        top_20_codes.add(c_code)

        db_scanner_sheet = doc.worksheet("DB_스캐너")
        db_scanner_sheet.batch_clear(['A2:AC'])
        if top_20_results:
            db_scanner_sheet.update(range_name="A2", values=top_20_results, value_input_option="USER_ENTERED")
        print(f"🎯 DB_스캐너 {len(top_20_results)}개 전송 완료 (VIP 강제 소환 완료)")

        if is_reset_time:
            try:
                bt_sheet = doc.worksheet("백테스트_로그")
                bt_data = bt_sheet.get_all_values()
                header_row = ["진입일", "종목명", "종목코드", "테마명", "진입가", "타점유형", "퀀트점수", "T+1수익률", "T+3수익률", "T+5수익률", "T+10수익률"]
                if len(bt_data) == 0:
                    bt_sheet.append_row(header_row)
                    bt_data = bt_sheet.get_all_values()
                elif "진입" not in str(bt_data[0][0]).replace(" ", ""):
                    bt_sheet.insert_row(header_row, index=1)
                    bt_data = bt_sheet.get_all_values()

                today_date_bt = datetime.datetime.now(KST).date()
                updated = False
                for i in range(1, len(bt_data)):
                    row = bt_data[i]
                    while len(row) < 11: row.append("")
                    try:
                        entry_date = datetime.datetime.strptime(row[0], '%Y-%m-%d').date()
                        days_elapsed = (today_date_bt - entry_date).days
                        tajeom_type = str(row[5])
                        is_seed = "SEED" in tajeom_type or "중장기" in tajeom_type or "모아가기" in tajeom_type
                        
                        needs_t1  = (days_elapsed >= 1 and row[7] == "" and not is_seed)
                        needs_t3  = (days_elapsed >= 3 and row[8] == "")
                        needs_t5  = (days_elapsed >= 5 and row[9] == "")
                        needs_t10 = (days_elapsed >= 10 and row[10] == "" and is_seed)
                        
                        if is_seed and days_elapsed >= 1 and row[7] == "":
                            row[7] = "-"
                            updated = True

                        if needs_t1 or needs_t3 or needs_t5 or needs_t10:
                            t_code = str(row[2]).replace("'", "").zfill(6)
                            entry_p = int(str(row[4]).replace(',', '').replace('원', ''))
                            rt_res = requests.get(f"https://m.stock.naver.com/api/stock/{t_code}/basic", verify=False, timeout=3).json()
                            curr_p = int(str(rt_res.get('closePrice', '0')).replace(',', ''))
                            if curr_p > 0:
                                rtn = ((curr_p - entry_p) / entry_p) * 100
                                if needs_t1: row[7] = f"{rtn:.2f}%"
                                if needs_t3: row[8] = f"{rtn:.2f}%"
                                if needs_t5: row[9] = f"{rtn:.2f}%"
                                if needs_t10: row[10] = f"{rtn:.2f}%"
                                updated = True
                    except: pass
                if updated:
                    bt_sheet.update(range_name="A1", values=bt_data, value_input_option="USER_ENTERED")
                    print("✅ 백테스트 수익률 자동 갱신 완료")
            except: pass

    except Exception as e:
        print(f"❌ 전체 업데이트 에러: {e}")

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
                response = requests.post(GOOGLE_WEBHOOK_URL, timeout=30)
                if response.status_code == 200: print("✅ 구글 자동 릴레이 바통터치 성공")
        except Exception as e: print(f"❌ 릴레이 에러: {e}")
