import os, re, time, datetime, requests, gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import sys
import xml.etree.ElementTree as ET
from collections import Counter
import concurrent.futures
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 기본 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
TARGET_PERCENT = 3.0
KST = datetime.timezone(datetime.timedelta(hours=9))

now_kst_check = datetime.datetime.now(KST)
# 💡 [테스트용] 새벽 2시~7시만 휴식
if 2 <= now_kst_check.hour < 7:
    print(f"🌙 현재 시간({now_kst_check.strftime('%H:%M')}): 시스템을 휴식 모드로 전환합니다. (02시~07시)")
    sys.exit(0)

# ==========================================
# 💡 [V11.2] 시트 자동 정렬 및 청소 함수
# ==========================================
def cleanup_and_reorder(doc, sheet_name, sort_col_idx):
    try:
        sheet = doc.worksheet(sheet_name)
        data = sheet.get_all_values()
        if len(data) <= 2: return
        
        header = data[0]
        rows = [r for r in data[1:] if len(r) > sort_col_idx and str(r[sort_col_idx]).strip()]
        
        def parse_date(val):
            val = str(val).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y. %m. %d"):
                try: return datetime.datetime.strptime(val, fmt)
                except: continue
            return datetime.datetime(1900, 1, 1) 
            
        rows.sort(key=lambda x: parse_date(x[sort_col_idx]), reverse=True)
        
        sheet.batch_clear(['A2:Z'])
        sheet.update(range_name="A2", values=[header] + rows, value_input_option="USER_ENTERED")
        print(f"✅ [{sheet_name}] 최신순 정렬 및 청소 완료")
    except Exception as e:
        print(f"⚠️ [{sheet_name}] 정렬 실패: {e}")

# ==========================================
# 💡 한국투자증권 API 인증 엔진
# ==========================================
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
KIS_URL_BASE = "https://openapi.koreainvestment.com:9443"

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

STOPWORDS = ['코스피', '코스닥', '증시', '주식', '투자', '종목', '시장', '지수', '대형주', '중소형주', '외인', '기관', '개인', '외국인', '매수', '매도', '순매수', '순매도', '거래', '대금', '주가', '펀드', '사모', '상장', '상폐', '공모', '특징주', '테마', '테마주', '관련', '관련주', '수혜', '수혜주', '장세', '개장', '출발', '마감', '초반', '후반', '오전', '오후', '장중', '증권', '증권사', '운용', '자사', '괴리', '프리미어', '가치', '밸류', '공시', '병합', '분할', '상승', '하락', '급등', '급락', '강세', '약세', '폭락', '반등', '조정', '랠리', '위축', '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '돌파', '경신', '연속', '최고', '최저', '신고가', '신저가', '최고치', '최저치', '최고가', '최저가', '급증', '급감', '확산', '진정', '완화', '악화', '개선', '회복', '최대', '사상', '역대', '최초', '최신', '규모', '수준', '가격', '목표가', '상향', '하향', '박살', '킬러', '대규모', '변동', '오픈', '호재', '연계', '대비', '경제', '금융', '기업', '정부', '자산', '머니', '한국', '미국', '국내', '글로벌', '뉴욕', '회장', '대표', '임원', '주주', '총회', '이유', '때문', '달러', '금리', '인상', '인하', '동결', '연준', '파월', '물가', '지표', '고용', '기름값', '주유소', '석유', '신용', '수익', '매출', '적자', '흑자', '배당', '지분', '인수', '합병', '사업', '추진', '공급', '계약', '체결', '실적', '발표', '이익', '반사이익', '현금', '자회사', '계열사', '지주사', '관계사', '기내식', '서비스', '오늘', '내일', '이번', '주간', '월간', '분기', '시간', '하루', '하루만', '올해', '내년', '지난해', '전일', '전주', '전월', '동기', '내달', '연말', '연초', '이날', '당일', '최근', '현재', '이후', '이전', '상반기', '하반기', '당분간', '예상', '전망', '기대', '우려', '경고', '목표', '분석', '평가', '결정', '검토', '참여', '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', '타격', '피해', '직격탄', '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', '달성', '기준', '행사', '이사', '의결', '개정', '취지', '적극', '개최', '진행', '예정', '상황', '필요', '대응', '마련', '운영', '관리', '적용', '이용', '사용', '활용', '확보', '제공', '구축', '기반', '중심', '노력', '계획', '정도', '경우', '이상', '이하', '가운데', '가장', '포함', '제외', '기대감', '우려감', '불확실성', '가능성', '움직임', '분위기', '흐름', '국면', '대목', '차원', '입장', '배경', '결과', '모습', '모멘텀', '현상', '차이', '비중', '비율', '단계', '목적', '대상', '조원', '억원', '만원', '천원', '전문', '현지', '사회', '생산자', '제도', '재고', '면제', '속보', '단독', '기자', '특파원', '앵커', '저작권', '무단', '전재', '재배포', '금지', '뉴스', '보도', '자료', '사진', '관계자', '주장', '설명', '강조', '위원회', '법안', '회의', '통과', '정책', '의원', '장관', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인', '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '공개', '이란', '국민연금', '종전', '전쟁', '트럼프', '제안', '찬성', '대통령', '사내', '협상', '출시', '계좌', '중동', '상품', '체제', '변경', '투자증권', '성장', '시그널', '신규', '정치', '외교', '합의', '수출', '수입', '도입', '본격', '소식', '임박', '부각', '주도']
AD_FILTER = ['펀드', '투어', '캠페인', '서비스', '최초', '강화', '고객', '연금', '마스터', '코리아', '정책', '개최', '박람회', '전시회', '프로모션', '할인', '기획전', '페스티벌', '출시', '협약', 'MOU', '체결', '선정', '어워드', '스마트픽', '팔자', '사자', '증가', '감소', '목표', '꺾인', '주석', '전망', '우려', '기대', '연내', '내달', '오늘', '내일', '돌파', '연속', '급락', '투자', '매수', '매도', '수익']
THEME_BLACKLIST = ['코로나19', '메르스', '지카바이러스', '우한폐렴', '원숭이두창', '엠폭스', '아프리카돼지열병', '구제역', '광우병', '야놀자(Yanolja)', '리비안(RIVIAN)']

def check_warning_market():
    local_session = requests.Session()
    try:
        url = "https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1"
        res = local_session.get(url, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) == 20: return prices[0] < (sum(prices) / 20)
    except: pass
    return False

def get_kospi_fluctuation_rate():
    local_session = requests.Session()
    try:
        res = local_session.get("https://m.stock.naver.com/api/index/KOSPI/basic", verify=False, timeout=3).json()
        rate_str = res.get("fluctuationsRatio", "0")
        return float(str(rate_str).replace(',', ''))
    except:
        return 0.0

def search_code_from_naver(stock_name):
    local_session = requests.Session()
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = local_session.get(url).json()
        if data.get('result') and data['result'].get('stocks'): return data['result']['stocks'][0]['itemCode']
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
    try:
        res = local_session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        market_sum_tag = soup.find('em', id='_market_sum')
        if not market_sum_tag: return 0  # 999999에서 0으로 수정
        market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
        if '조' in market_sum_str:
            parts = market_sum_str.split('조')
            return int(parts[0].strip()) * 10000 + (int(parts[1].strip()) if len(parts)>1 and parts[1].strip() else 0)
        else: return int(market_sum_str)
    except: return 0  # 999999에서 0으로 수정 

def get_real_money_themes():
    local_session = requests.Session()
    try:
        now = datetime.datetime.now(KST)
        is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
        time_str = now.strftime('%H:%M')
        
        res = local_session.get("https://finance.naver.com/sise/theme.naver", verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        table = soup.find('table', {'class': 'type_1'})
        if not table:
            return pd.DataFrame(), is_market_closed, {}

        raw_themes = [{'name': a.text.strip(), 'url': "https://finance.naver.com" + a['href']} for tds in [tr.find_all('td') for tr in table.find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a]
        themes = [t for t in raw_themes if not any(b in t['name'] for b in THEME_BLACKLIST)][:20] 
                        
        theme_data_list = []
        print("▶️ 실시간 주도 테마 수집 시작 (군집성 필터 적용)...")
        for theme in themes:
            try:
                soup = BeautifulSoup(local_session.get(theme['url'], verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                stocks = []
                type_5_table = soup.find('table', {'class': 'type_5'})
                if not type_5_table: continue
                
                for tr in type_5_table.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) >= 9:
                        try:
                            s_name = tds[0].find('a').text.strip()
                            s_code = f"'{tds[0].find('a')['href'].split('code=')[-1]}"
                            rate_str, val_str = tds[4].text.strip(), tds[8].text.strip()
                            if '%' not in rate_str or '-' in rate_str or '0.00' in rate_str: continue
                            rate_num = float(rate_str.replace('%', '').replace('+', '').replace(',', '').strip())
                            val_num = int(val_str.replace(',', '').strip())
                            if rate_num >= TARGET_PERCENT and val_num > 0 and get_market_cap(s_code.replace("'", "")) >= 1000:
                                stocks.append({'name': s_name, 'code': s_code, 'rate': rate_num, 'value': val_num})
                        except: continue
                
                stocks_val = sorted(stocks, key=lambda x: x['value'], reverse=True)[:5]
                if len(stocks_val) >= 2:
                    stocks_rate = sorted(stocks_val, key=lambda x: x['rate'], reverse=True)
                    theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks_rate})
            except: continue
            
        if not theme_data_list: return pd.DataFrame(), is_market_closed, {}
        
        grouped_themes = {}
        for t_data in theme_data_list: grouped_themes.setdefault(t_data['stocks'][0]['code'], []).append(t_data)
            
        merged_themes = []
        for top_code, t_list in grouped_themes.items():
            theme_names = list(dict.fromkeys(t['theme_name'] for t in t_list))
            merged_name = " / ".join(theme_names) + f" (대장: {t_list[0]['stocks'][0]['name']})" if len(theme_names) > 1 else theme_names[0]
            unique_stocks = {s['code']: s for t in t_list for s in t['stocks']}
            merged_stocks_val = sorted(unique_stocks.values(), key=lambda x: x['value'], reverse=True)[:5]
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
                
        final_rows = [{'날짜': now.strftime('%Y-%m-%d'), **({'시간': time_str} if not is_market_closed else {}), '순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)} for rank, t_data in enumerate(final_themes, 1) for s in t_data['stocks']]
        return pd.DataFrame(final_rows), is_market_closed, all_theme_map
    except Exception as e:
        return pd.DataFrame(), False, {}

def get_naver_search_ranking():
    local_session = requests.Session()
    try:
        soup = BeautifulSoup(local_session.get("https://finance.naver.com/sise/lastsearch2.naver", verify=False).content, 'html.parser', from_encoding='euc-kr')
        data = []
        search_blacklist = ['삼성전자', 'SK하이닉스', '현대차', '기아', 'LG에너지솔루션', 'POSCO홀딩스', '셀트리온', 'NAVER', '카카오']
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

def update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        doc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)).open_by_url(SHEET_URL)
        
        if not df_theme.empty:
            sheet = doc.worksheet("수급_Raw" if is_market_closed else "수급_실시간")
            if is_market_closed:
                today_str = df_theme.iloc[0]['날짜'] 
                all_data = sheet.get_all_values()
                combined_data = df_theme.values.tolist() + [row for row in all_data[1:] if len(row) > 0 and row[0] != today_str]
                combined_data.sort(key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
                combined_data.sort(key=lambda x: x[0], reverse=True)
                sheet.batch_clear(['A2:Z'])
                sheet.update(range_name="A2", values=combined_data, value_input_option="USER_ENTERED")
            else:
                sheet.batch_clear(['A2:Z']) 
                sheet.update(range_name="A2", values=df_theme.values.tolist(), value_input_option="USER_ENTERED")
                
        for df, sheet_name in [(df_news, "뉴스_키워드"), (df_naver, "네이버_검색상위"), (df_main_news, "네이버_주요뉴스")]:
            if not df.empty:
                sheet = doc.worksheet(sheet_name)
                sheet.batch_clear(['A2:Z'])
                sheet.update(range_name="A2", values=df.values.tolist(), value_input_option="USER_ENTERED")
    except Exception as e: 
        print(f"❌ 데이터 업데이트 에러: {e}")

def get_market_schedule():
    local_session = requests.Session()
    """네이버 금융 오늘의 증시 일정 수집 (순수 일정만 추출)"""
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
                
                include_kws = ['실적', '발표', '만기', '배당', '금통위', 'FOMC', '고용', '학회', '임상', '상장', '개막', '출시']
                exclude_kws = [
                    '주주총회', '주총', '공모', '청약',
                    '전망', '주목', '대기', '반환점', '서프라이즈', '쇼크', 
                    '기대감', '우려', '물귀신', '박스권', '코스피', '코스닥', 
                    '증시', '마감', '시황', '특징주', '주간'
                ]
                
                if any(kw in title for kw in include_kws) and not any(ex_kw in title for ex_kw in exclude_kws):
                    if "증시 전망" not in title and "외환전망" not in title:
                        if clean_title not in seen_titles:
                            schedules.append([today_str, title, "📅 자동수집(당일)"])
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
        requests_list.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": 1, "endIndex": len(valid_rows) + 1},
                "properties": {"hiddenByUser": False},
                "fields": "hiddenByUser"
            }
        })

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
            requests_list.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": hide_start, "endIndex": hide_end},
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser"
                }
            })

        if requests_list:
            doc.batch_update({"requests": requests_list})
            print(f"📅 HYEOKS 주요일정 관리 완료 (완벽 중복제거 + 서술형 뉴스 차단 + 포맷팅 + 과거숨김)")

    except Exception as e:
        print(f"❌ 주요일정 시트 관리 에러: {e}")

def analyze_single_stock(name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate):
    local_session = requests.Session()
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        res = local_session.get(url, verify=False, timeout=3)
        root = ET.fromstring(res.text)
        
        history = []
        high_prices = []
        items = root.findall(".//item")
        
        for item in items:
            data = item.get("data").split("|")
            date_str = data[0]
            open_p, high_p, low_p, close_p, vol = int(data[1]), int(data[2]), int(data[3]), int(data[4]), int(data[5])
            
            if vol == 0: 
                continue
            
            history.append({
                "date": date_str,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p, 
                "volume": vol
            })
            high_prices.append(high_p)
            
        if len(history) < 2: return None

        last_day = history[-1]
        open_price, today_high, today_low, current_price, today_vol = last_day['open'], last_day['high'], last_day['low'], last_day['close'], last_day['volume']
        
        df_hist = pd.DataFrame(history)
        
        prev_price = int(df_hist['close'].iloc[-2]) if len(df_hist) >= 2 else current_price
        yest_close = prev_price 
        
        change_rate = (current_price - prev_price) / prev_price if prev_price > 0 else 0.0
        
        yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) >= 2 else today_vol
        yest_tv = prev_price * yest_vol 
        
        try:
            high_low = df_hist['high'] - df_hist['low']
            high_close = (df_hist['high'] - df_hist['close'].shift()).abs()
            low_close = (df_hist['low'] - df_hist['close'].shift()).abs()
            tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
            atr_14 = tr.rolling(14).mean().iloc[-1]
            if pd.isna(atr_14) or atr_14 == 0: atr_14 = current_price * 0.03 
            
            prices = df_hist['close'].values
            kalman_ma = []
            x_hat = prices[0]
            p = 1.0
            Q, R = 1e-4, 1e-2  
            for z in prices:
                p_hat = p + Q
                K = p_hat / (p_hat + R)
                x_hat = x_hat + K * (z - x_hat)
                p = (1 - K) * p_hat
                kalman_ma.append(x_hat)
            kalman_ma2 = []
            x2, p2 = kalman_ma[0], 1.0
            for z in kalman_ma:
                p2_hat = p2 + Q
                K2 = p2_hat / (p2_hat + R * 3)  # R을 키워 더 느리게(안정적으로)
                x2 = x2 + K2 * (z - x2)
                p2 = (1 - K2) * p2_hat
                kalman_ma2.append(x2)
            curr_kalman = kalman_ma[-1]
            prev_kalman = kalman_ma[-2] if len(kalman_ma) > 1 else curr_kalman
            pprev_kalman = kalman_ma[-3] if len(kalman_ma) > 2 else prev_kalman
            
            is_kalman_uptrend = curr_kalman > prev_kalman
            kalman_turned_green = (curr_kalman > prev_kalman) and (prev_kalman <= pprev_kalman)
            kalman_turned_red = (curr_kalman < prev_kalman) and (prev_kalman >= pprev_kalman)
            
            trend_start_price = current_price
            for i in range(len(kalman_ma)-1, 1, -1):
                if kalman_ma[i] <= kalman_ma[i-1]:
                    trend_start_price = prices[i] 
                    break
            
            price_climb = current_price - trend_start_price
            
            secret_tajeom = ""
            if kalman_turned_green:
                secret_tajeom = "🟢 [시크릿] 추세 전환 (1차 매수 타점)"
            elif is_kalman_uptrend:
                if price_climb >= atr_14 * 3.0:
                    secret_tajeom = "🔴 [시크릿] 3차 파동 도달 (전량 익절)"
                elif price_climb >= atr_14 * 2.0:
                    secret_tajeom = "🟡 [시크릿] 2차 파동 진행 (본절 스탑 상향)"
                elif price_climb >= atr_14 * 1.0:
                    secret_tajeom = "🟢 [시크릿] 1차 파동 진행 (추세 홀딩)"
                else:
                    secret_tajeom = "🟢 [시크릿] 상승 추세 유지"
            elif kalman_turned_red:
                secret_tajeom = "📉 [시크릿] 하락 추세 전환 (전량 매도)"
            else:
                secret_tajeom = "📉 [시크릿] 노이즈 및 하락장 (관망)"
                
        except Exception as e:
            print(f"Kalman Engine Error: {e}")
            atr_14 = current_price * 0.03
            is_kalman_uptrend = False
            secret_tajeom = ""

        trading_value = current_price * today_vol
        high_prices_60 = high_prices[-60:] if len(high_prices) >= 60 else high_prices
        
        high_60d_calc = max(high_prices_60[:-1]) if len(high_prices_60) > 1 else today_high
        high_250d_calc = max(high_prices[:-1]) if len(high_prices) > 1 else today_high
        
        display_high_60d = max(high_prices_60) if len(high_prices_60) > 0 else today_high
        display_high_250d = max(high_prices) if len(high_prices) > 0 else today_high
        
        min_20d = int(df_hist['close'].tail(20).min()) if len(df_hist) >= 20 else int(df_hist['close'].min())
        surge_rate_20d = (current_price - min_20d) / min_20d if min_20d > 0 else 0
        is_high_altitude = surge_rate_20d >= 0.50 
        
        min_250d = int(df_hist['close'].min()) 
        surge_rate_250d = (current_price - min_250d) / min_250d if min_250d > 0 else 0
        is_mega_trend_exhausted = surge_rate_250d >= 2.0 
        
        body_top = max(current_price, open_price)
        body_bottom = min(current_price, open_price)
        upper_shadow = today_high - body_top
        real_body = body_top - body_bottom
        
        upper_shadow_ratio = upper_shadow / current_price if current_price > 0 else 0
        is_long_shadow = (upper_shadow_ratio >= 0.05) or (upper_shadow_ratio >= 0.025 and upper_shadow > real_body * 1.5)
        shadow_text = "⚠️ [캔들] 저항 출회" if is_long_shadow else ("👑 [캔들] 몸통 마감" if upper_shadow_ratio <= 0.015 else "🟡 [캔들] 일반형")
        
        today_body_ratio = real_body / open_price if open_price > 0 else 0
        is_today_yangbong = current_price >= open_price
        gap_ratio = (open_price - prev_price) / prev_price if prev_price > 0 else 0
        is_huge_gap = gap_ratio >= 0.04
        
        risk_soup = BeautifulSoup(local_session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
        
        market_sum_tag = risk_soup.find('em', id='_market_sum')
        market_cap = 0
        if market_sum_tag:
            market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
            if '조' in market_sum_str:
                parts = market_sum_str.split('조')
                market_cap = int(parts[0].strip()) * 10000 + (int(parts[1].strip()) if len(parts)>1 and parts[1].strip() else 0)
            else: market_cap = int(market_sum_str)

        # 정규식에 '코넥스'를 추가하여 즉각 쳐냄
        is_junk = bool(risk_soup.find('img', alt=re.compile('관리종목|환기종목|거래정지|투자위험|코넥스')))
        is_financial_risk, is_chronic_loss = False, False
        fin_table = risk_soup.find('table', {'class': 'tb_type1 tb_num tb_type1_ifrs'})
        if fin_table:
            op_profits = []
            total_equity, capital_stock = None, None
            for tr in fin_table.find('tbody').find_all('tr'):
                th = tr.find('th')
                if not th: continue
                title = th.text.strip()
                if title == '영업이익':
                    for td in tr.find_all('td')[:3]:
                        try: op_profits.append(float(td.text.replace(',', '').strip()))
                        except: pass
                elif title == '자본총계':
                    for td in reversed(tr.find_all('td')):
                        try: total_equity = float(td.text.replace(',', '').strip()); break
                        except: pass
                elif title == '자본금':
                    for td in reversed(tr.find_all('td')):
                        try: capital_stock = float(td.text.replace(',', '').strip()); break
                        except: pass
            if capital_stock and total_equity and total_equity < capital_stock: is_financial_risk = True
            if len(op_profits) == 3 and all(p < 0 for p in op_profits): is_chronic_loss = True

        is_strong_dual_buy = False 
        is_weak_dual_buy = False   
        supply_text = ""
        
        acc_i_buy_won = 0
        dual_buy_days = 0            
        i_buy_today = 0
        f_buy_today = 0
        today_dual_buy_ratio = 0.0

        program_text = "확인불가"
        pg_amount_eok = 0.0 

        try:
            # 1. User-Agent 헤더 추가 (봇 차단 우회)
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            frgn_url = f"https://finance.naver.com/item/frgn.naver?code={code}"
            # 2. headers 속성 포함하여 요청
            frgn_res = local_session.get(frgn_url, headers=headers, verify=False, timeout=3)
            frgn_soup = BeautifulSoup(frgn_res.content, 'html.parser', from_encoding='euc-kr')
            
            # 3. '>' 기호를 빼고 띄어쓰기로 변경하여 유연하게 tr 태그 수집
            rows = frgn_soup.select("table.type2 tr")
            
            valid_days = 0
            for r_tag in rows:
                cols = r_tag.select("td")
                if len(cols) >= 7 and cols[0].text.strip().replace('.', '').isdigit():
                    close_price_day = int(cols[1].text.strip().replace(',', ''))
                    
                    try: i_vol = int(cols[5].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except: i_vol = 0
                    
                    try: f_vol = int(cols[6].text.strip().replace(',', '').replace('+', '').replace(' ', ''))
                    except: f_vol = 0
                    
                    i_buy_won = i_vol * close_price_day
                    f_buy_won = f_vol * close_price_day

                    if i_buy_won >= 50_000_000 and f_buy_won >= 50_000_000:
                        dual_buy_days += 1
                    
                    if valid_days == 0:
                        i_buy_today = i_buy_won
                        f_buy_today = f_buy_won
                        today_dual_buy_ratio = ((i_buy_today + f_buy_today) / trading_value) * 100 if trading_value > 0 else 0
                        
                        if f_vol != 0:
                            pg_amount_won = f_vol * current_price 
                            pg_amount_eok = pg_amount_won / 100_000_000 
                            pg_ratio = (abs(pg_amount_won) / trading_value) * 100 if trading_value > 0 else 0.0
                                
                            if pg_amount_eok >= 30 and pg_ratio >= 10.0: program_text = f"🔴 [P.대량유입] +{int(pg_amount_eok):,}억 ({pg_ratio:.1f}%)"
                            elif pg_amount_eok >= 10 and pg_ratio >= 5.0: program_text = f"🔴 [P.매수우위] +{int(pg_amount_eok):,}억 ({pg_ratio:.1f}%)"
                            elif pg_amount_eok <= -30 and pg_ratio >= 10.0: program_text = f"🔵 [P.대량출회] {int(pg_amount_eok):,}억 ({pg_ratio:.1f}%)"
                            elif pg_amount_eok <= -10 and pg_ratio >= 5.0: program_text = f"🔵 [P.매도우위] {int(pg_amount_eok):,}억 ({pg_ratio:.1f}%)"
                            else:
                                sign = "+" if pg_amount_eok > 0 else ""
                                program_text = f"⚪ [P.관망중] {sign}{int(pg_amount_eok):,}억 ({pg_ratio:.1f}%)"
                        else:
                            program_text = "⚪ [P.관망중] 0원 (0.0%)"
                            
                    acc_i_buy_won += i_buy_won
                    valid_days += 1
                    
                    if valid_days >= 5:
                        break
        except Exception: pass

        acc_i_buy_eok = acc_i_buy_won / 100_000_000 

        if dual_buy_days >= 3 and today_dual_buy_ratio >= 3.0 and i_buy_today >= 200_000_000 and f_buy_today >= 200_000_000 and acc_i_buy_eok >= 20:
            is_strong_dual_buy = True
            supply_text = " (🌟쌍끌이 모아가기)"
        elif i_buy_today >= 200_000_000 and f_buy_today >= 200_000_000:
            is_weak_dual_buy = True
            supply_text = " (🟢약한 양매수)"
        elif acc_i_buy_eok >= 20:
            supply_text = " (기관 누적매집)"

        ma5 = int(df_hist['close'].tail(5).mean()) if len(df_hist) >= 5 else current_price
        ma20 = int(df_hist['close'].tail(20).mean()) if len(df_hist) >= 20 else current_price
        std20 = df_hist['close'].tail(20).std(ddof=0) if len(df_hist) >= 20 else 0
        disp_20 = (current_price / ma20) * 100 if ma20 > 0 else 100
        disp_text = f"{disp_20:.1f}%"
        
        is_leader_history = False
        for i in range(1, len(history)):
            if history[i-1]['close'] > 0 and (high_prices[i] - history[i-1]['close']) / history[i-1]['close'] >= 0.22:
                is_leader_history = True; break
        leader_text = "🔥대장주(O)" if is_leader_history else "평범(X)"

        upper_band = ma20 + (std20 * 2) 
        lower_band = ma20 - (std20 * 2) 
        band_width = (upper_band - lower_band) / ma20 if ma20 > 0 else 0 
        
        avg_vol_10 = df_hist['volume'].tail(11).head(10).mean() if len(df_hist) >= 2 else today_vol
        vol_ratio_10d = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
        
        vol_ratio_yest = (today_vol / yest_vol) * 100 if yest_vol > 0 else 0

        is_upper_limit = change_rate >= 0.295
        if is_upper_limit and today_vol > 0:
            vol_ratio_10d = max(vol_ratio_10d, 500)
            vol_ratio_yest = max(vol_ratio_yest, 500)

        is_converging = (band_width <= 0.20) or (ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035)
        
        if vol_ratio_10d <= 40: vol_status_text = "🟢 [V.에너지응축]"
        elif vol_ratio_10d <= 70: vol_status_text = "🟢 [V.거래감소]"
        elif vol_ratio_10d >= 200 and vol_ratio_yest >= 150: vol_status_text = "🔴 [V.쌍끌이폭발]" 
        elif vol_ratio_10d >= 200: vol_status_text = "🔴 [V.거래과열]"
        else: vol_status_text = "🟡 [V.평년수준]"
        
        vol_ratio_text = f"전일비 {int(vol_ratio_yest):,}%"
        
        box_ratio = 999
        if len(df_hist) >= 20:
            max_20d_box = int(df_hist['high'].tail(20).max())
            min_20d_box = int(df_hist['low'].tail(20).min())
            if min_20d_box > 0:
                box_ratio = (max_20d_box - min_20d_box) / min_20d_box
        
        # ★ [개선] 시장 상황에 따른 동적 허들 변수 설정
        min_breakout_tv = 50_000_000_000 if is_warning_market else 30_000_000_000     # 돌파 대장주 거래대금 (500억 / 300억)
        min_nulim_tv    = 10_000_000_000 if is_warning_market else 5_000_000_000      # 눌림목 거래대금 (100억 / 50억)
        min_danta_rate  = 0.10           if is_warning_market else 0.06               # 단타 인식 상승률 (10% / 6%)

        is_platform_breakout = (box_ratio <= 0.15) and (vol_ratio_10d >= 300) and (current_price > ma20) and is_today_yangbong and (trading_value >= min_breakout_tv)

        if is_junk: signal = "🚨 매매제한 (관리/주의)"
        elif is_financial_risk: signal = "🚨 매매제한 (재무위험)"
        elif is_platform_breakout: signal = "📦 플랫폼 탈출 (스윙)" + supply_text
        elif is_strong_dual_buy and is_converging: signal = "🌟 모아가기 (쌍끌이)"
        elif band_width <= 0.20 and current_price >= ma20: signal = "🚀 N자파동 (밴드돌파)" + supply_text if current_price >= upper_band * 0.98 else "👀 N자파동 (에너지응축)" + supply_text
        elif ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035: signal = "📈 2차랠리 (이평수렴)" + supply_text if current_price > ma20 else "⏳ 이평선 저항" + supply_text
        else: signal = "🟢 낙폭과대 (과매도)" + supply_text if current_price < lower_band else "⚡ 관망 (이격발생)" + supply_text
        
        is_near_high = current_price >= (high_60d_calc * 0.90) or yest_close >= (high_60d_calc * 0.90)
        is_near_52w_high = current_price >= (high_250d_calc * 0.90) or yest_close >= (high_250d_calc * 0.90)
        
        if is_near_52w_high: dist_text = "🎯 52주신고가 턱밑"
        elif is_near_high: dist_text = "🎯 60일전고 턱밑"
        elif current_price >= high_60d_calc * 0.80: dist_text = "🟢 매물대 소화중"
        else: dist_text = "📉 이격 과다"

        is_danta_range = min_danta_rate <= change_rate < 0.295
        
        if name in theme_rank_dict:
            my_theme_name = theme_rank_dict[name]['theme_name']
            is_theme_leader_raw = theme_rank_dict[name]['is_leader']
            has_theme = True
        elif name in all_theme_map:
            my_theme_name = all_theme_map[name]['theme_name']
            is_theme_leader_raw = all_theme_map[name]['is_leader']
            has_theme = True
        else:
            my_theme_name, is_theme_leader_raw, has_theme = "개별주/기타", False, False
        
        # 동적 허들을 적용한 대장주 판별
        is_true_theme_leader = is_theme_leader_raw and (trading_value >= min_breakout_tv)
        is_theme_daejang_sang = is_true_theme_leader and is_upper_limit and not (is_junk or is_financial_risk)
        is_theme_daejang = is_true_theme_leader and is_danta_range and not (is_junk or is_financial_risk)
        is_real_hubal = has_theme and not is_theme_leader_raw
        is_theme_hubal_sang = is_real_hubal and is_upper_limit and not (is_junk or is_financial_risk)
        is_theme_hubal = is_real_hubal and is_danta_range and not (is_junk or is_financial_risk)
        is_individual = (not has_theme) or (is_theme_leader_raw and trading_value < min_breakout_tv)
        is_individual_sang = is_individual and is_upper_limit and not (is_junk or is_financial_risk)
        is_individual_surge = is_individual and is_danta_range and not (is_junk or is_financial_risk)

        is_breakout_track = current_price >= ma20
        track_type = "돌파" if is_breakout_track else "눌림"
        
        flag_days = 0
        for d in range(1, 4):
            anchor_idx = -(d + 1)
            if len(df_hist) >= abs(anchor_idx) + 1:
                anchor_close = int(df_hist['close'].iloc[anchor_idx])
                anchor_open = int(df_hist['open'].iloc[anchor_idx])
                anchor_vol = int(df_hist['volume'].iloc[anchor_idx])
                anchor_prev_close = int(df_hist['close'].iloc[anchor_idx - 1]) if len(df_hist) > abs(anchor_idx) + 1 else anchor_open
                
                anchor_tv = anchor_close * anchor_vol
                anchor_change = (anchor_close - anchor_prev_close) / anchor_prev_close if anchor_prev_close > 0 else 0
                hist_before_anchor = high_prices[:anchor_idx] if anchor_idx < -1 else high_prices[:-1]
                high_60d_anchor = max(hist_before_anchor) if len(hist_before_anchor) > 0 else anchor_close
                
                if anchor_tv >= min_breakout_tv and anchor_change >= 0.10 and anchor_close > anchor_open and (anchor_close >= high_60d_anchor * 0.90):
                    is_holding = True
                    for j in range(anchor_idx + 1, 0): 
                        if not (anchor_close * 0.97 <= int(df_hist['close'].iloc[j]) <= anchor_close * 1.12): is_holding = False; break
                        if ((int(df_hist['close'].iloc[j]) - int(df_hist['close'].iloc[j-1])) / int(df_hist['close'].iloc[j-1])) < -0.035: is_holding = False; break
                        if int(df_hist['volume'].iloc[j]) > anchor_vol * 0.45: is_holding = False; break
                    if is_holding: flag_days = d; break

        is_recent_breakout = False
        breakout_days_ago = 0
        for d in range(1, 6):
            check_idx = -d
            if len(df_hist) >= abs(check_idx) + 1:
                check_close = int(df_hist['close'].iloc[check_idx])
                hist_before_check = high_prices[:check_idx] if check_idx < -1 else high_prices[:-1]
                high_60d_check = max(hist_before_check) if len(hist_before_check) > 0 else check_close
                
                if check_close > high_60d_check:
                    is_recent_breakout = True
                    breakout_days_ago = d
                    break

        # 동적 허들을 적용한 눌림목 판단
        is_extreme_nulim = (
            is_recent_breakout and                               
            (current_price >= high_60d_calc * 0.90) and          
            (vol_ratio_yest <= 110) and                          
            (vol_ratio_10d <= 150) and                           
            (not is_long_shadow) and
            (trading_value >= min_nulim_tv)                      
        )
        is_ss_breakout = (trading_value >= min_breakout_tv) and (change_rate >= 0.05) and not is_long_shadow and is_near_high
        
        now_kst_tajeom = datetime.datetime.now(KST)
        is_overnight_time = (now_kst_tajeom.hour >= 14) 
        
        is_overnight_breakout = (
            (trading_value >= min_breakout_tv) and             
            (pg_amount_eok >= 5 or is_strong_dual_buy) and 
            (acc_i_buy_eok >= 3) and                        
            (0.04 <= change_rate <= 0.28) and               
            (current_price >= today_high * 0.95) and        
            (is_near_high or is_near_52w_high) and          
            is_breakout_track and not is_long_shadow
        )
        
        is_overnight_pullback = (
            is_extreme_nulim and                            
            (current_price >= ma5) and                     
            (pg_amount_eok >= 3 or i_buy_today >= 30_000_000) 
        )

        is_overnight_candidate = is_overnight_time and (is_overnight_breakout or is_overnight_pullback)
        is_fatal_drop = is_junk or is_financial_risk
        
        base_score = 0
        if is_near_52w_high: base_score += 20
        elif current_price >= (high_60d_calc * 0.90): base_score += 15
        elif current_price >= (high_60d_calc * 0.85): base_score += 5
        
        if vol_ratio_yest >= 300 and vol_ratio_10d >= 200: base_score += 15
        elif vol_ratio_yest >= 150: base_score += 10
        elif vol_ratio_10d <= 35: base_score += 15 
        elif vol_ratio_10d <= 50: base_score += 5
        
        if "대량유입" in program_text: base_score += 25
        elif "매수우위" in program_text: base_score += 15
        elif "대량출회" in program_text: base_score -= 20
        elif "매도우위" in program_text: base_score -= 10
        
        if is_strong_dual_buy: base_score += 15
        if acc_i_buy_eok >= 50: base_score += 15 
        elif acc_i_buy_eok >= 10: base_score += 5

        high_retention = current_price / today_high if today_high > 0 else 0
        if high_retention >= 0.97 and change_rate >= 0.10 and trading_value >= 100_000_000_000: 
            base_score += 30 
            master_tajeom = " 👑(진성대장)"
        elif high_retention >= 0.95 and change_rate >= 0.07 and trading_value >= 50_000_000_000: 
            base_score += 15  
            master_tajeom = " 🥈(준대장)"
        else:
            master_tajeom = ""
            
        if is_near_52w_high and "대량유입" in program_text:
            base_score += 20 
            
        tajeom_multiplier = 0.0
        master_tajeom_base = "⏸️ [대기] 분석 중"
        
        if is_fatal_drop:
            master_tajeom_base = "🚫 [제외] 상폐/재무위험"
            tajeom_multiplier = 0.0
            
        elif is_overnight_candidate:
            if is_breakout_track and not is_overnight_pullback:
                master_tajeom_base = "🌙 [종베] 신고가 돌파 대기"
            else:
                master_tajeom_base = "🌙 [종베] 거래급감 눌림"
            tajeom_multiplier = 1.5  
            
        elif is_theme_daejang:
            master_tajeom_base = "🚀 [당일/단타] 대장주 불기둥"
            tajeom_multiplier = 1.3  
        elif is_theme_hubal:
            master_tajeom_base = "🚀 [당일/단타] 테마 후발주"
            tajeom_multiplier = 1.15
            
        elif is_platform_breakout or is_ss_breakout:
            master_tajeom_base = "📦 [스윙/추세] 박스권 탈출"
            tajeom_multiplier = 1.25  
            
        elif (is_extreme_nulim or flag_days > 0) and (change_rate < 0.06):
            if current_price >= high_60d_calc * 0.95:
                master_tajeom_base = "🎯 [스윙/눌림] 전고점 지지"
            else:
                master_tajeom_base = "🎯 [스윙/눌림] 20일선 방어전"
            tajeom_multiplier = 1.3  
            
        elif "1차" in secret_tajeom or "🟢 [시크릿] 추세 전환" in secret_tajeom:
            master_tajeom_base = "🕵️ [관심/수급] 세력선 포착 (시크릿)"
            tajeom_multiplier = 1.35
            
        elif ("🌟" in signal) or ((change_rate >= 0.05) and (trading_value >= 20_000_000_000) and (pg_amount_eok >= 10 or is_strong_dual_buy)):
            master_tajeom_base = "🌟 [관심/수급] 기준봉 포착"
            tajeom_multiplier = 0.9  
            
        else:
            master_tajeom_base = "👀 [관망] 타점 미도달"
            tajeom_multiplier = 0.6  
            
        master_tajeom = master_tajeom_base + master_tajeom

        # ★ [개선] 하락장 윗꼬리 페널티 극대화 (강제 관망)
        if not is_fatal_drop:
            if is_long_shadow or is_huge_gap:
                master_tajeom += " ⚠️(윗꼬리/이격)" 
                if is_warning_market:
                    tajeom_multiplier = 0.0  # 하락장에서는 윗꼬리 종목 점수를 0점으로 만들어버림
                    master_tajeom = "👀 [관망] 하락장 윗꼬리 리스크"
                else:
                    tajeom_multiplier -= 0.3 
            
            if is_chronic_loss: tajeom_multiplier -= 0.3
            if is_high_altitude: tajeom_multiplier -= 0.2
            
            if is_mega_trend_exhausted and market_cap < 100000:
                tajeom_multiplier -= 0.3
                master_tajeom += " ⚠️(대시세 고점)"
                
            if "3차 파동" in secret_tajeom or "하락 추세 전환" in secret_tajeom:
                tajeom_multiplier = 0.0  # 점수를 0점으로 만들어버림
                master_tajeom = "👀 [관망] 3차 파동 고점 리스크" # 매혹적인 매수 배지 강제 삭제

        now_kst_tajeom = datetime.datetime.now(KST)
        is_after_1030 = (now_kst_tajeom.hour * 100 + now_kst_tajeom.minute >= 1030)
        
        if "돌파" in master_tajeom and is_after_1030 and not is_overnight_candidate:
            tajeom_multiplier -= 0.3 

        if is_kalman_uptrend:
            target_price = int(current_price + (atr_14 * 2.0)) 
            stop_loss = int(current_price - (atr_14 * 1.0))
            
            if "2차" in secret_tajeom or "3차" in secret_tajeom:
                stop_loss = int(trend_start_price) 
        else:
            target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.05)
            stop_loss = int(min(ma20, current_price * 0.95))
            if stop_loss >= current_price: stop_loss = int(current_price * 0.96)

        if secret_tajeom and "관망" not in master_tajeom and "매수금지" not in master_tajeom:
            master_tajeom = f"{master_tajeom} | {secret_tajeom}"

        is_super_leader = (change_rate >= 0.15) and (trading_value >= 100_000_000_000) and ("대량유입" in program_text or "매수우위" in program_text)
        
        if is_super_leader:
            stop_loss = int(current_price * 0.96)
            target_price = int(current_price * 1.15)
            tajeom_multiplier = max(1.2, tajeom_multiplier) 
            master_tajeom += " 🔥(절대대장/면책)" 
            
        quant_score = int(max(0, (base_score + 10) * tajeom_multiplier))
        
        # ★ [개선] 하락장 스코어 컷오프 상향
        cutoff_score = 40 if is_warning_market else 25
        if quant_score < cutoff_score and not is_super_leader:
            master_tajeom = f"👀 [관망] 스코어 미달 (기준:{cutoff_score}점)"
            
        score_display = f"{quant_score}점 ({track_type})"
        
        is_hedge_theme = any(kw in my_theme_name for kw in ['방산', '방위산업', '해운', '조선', '석유', '가스', '전쟁', '사료', '원자재', '품절주', '식품'])
        if is_hedge_theme and kospi_rate <= -0.5:
            hedge_premium = 5 + int((abs(kospi_rate) - 0.5) / 0.1) * 1 
            quant_score += hedge_premium
            master_tajeom += f" 🛡️(+{hedge_premium}점)"

        return [
            name, f"'{code}", current_price, f"{change_rate * 100:.2f}%", 
            int(ma5), int(ma20), vol_ratio_text, signal, 
            score_display, master_tajeom, today_high, today_low, int(display_high_60d), 
            market_cap, shadow_text, dist_text, disp_text, leader_text, vol_status_text, my_theme_name,
            program_text,
            int(display_high_250d), f"{int(acc_i_buy_eok)}억",
            "AI 데이터 계산중", "AI 데이터 계산중"
        ]
    except Exception as e:
        return None

def update_technical_data(df_theme, all_theme_map):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)
        
        cleanup_and_reorder(doc, "접속로그", 1) 
        cleanup_and_reorder(doc, "DB_중장기", 0) 

        print("▶️ 기술적 지표 초고속 멀티프로세싱 판독 시작...")
        is_warning_market = check_warning_market()
        if is_warning_market: print("⚠️ 코스닥 20일선 이탈(하락장) 감지! 스캐너 허들을 대폭 상향합니다.")
        
        kospi_rate = get_kospi_fluctuation_rate()
        if kospi_rate <= -0.5:
            print(f"📊 코스피 실시간 등락률: {kospi_rate:.2f}% (해지 프리미엄 발동 대기)")
        
        name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in doc.worksheet("기업정보").get_all_values()[1:] if len(row) >= 3}
        
        target_names = set()
        
        try:
            raw_data = doc.worksheet("수급_Raw").get_all_values()
            for row in raw_data[1:]:
                if len(row) >= 7:
                    stock_name = str(row[-4]).strip()
                    if stock_name and stock_name not in ["#REF!", "로딩중...", "데이터대기", "FALSE"]:
                        target_names.add(stock_name)
        except: pass

        try:
            for row in doc.worksheet("대시보드").get_all_values()[4:]:
                if len(row) > 2 and str(row[2]).strip() and str(row[2]).strip() != "#REF!": 
                    target_names.add(str(row[2]).strip())
        except: pass

        if not df_theme.empty:
            theme_rank_dict = {}
            theme_rank_tracker = {}
            for index, row in df_theme.iterrows():
                t_rank, s_name, t_name = int(row['순위']), str(row['종목명']).strip(), row['테마명']
                if t_rank not in theme_rank_tracker: theme_rank_tracker[t_rank] = []
                theme_rank_tracker[t_rank].append(s_name)
                is_leader_in_this_theme = (len(theme_rank_tracker[t_rank]) == 1)
                
                if s_name not in theme_rank_dict: theme_rank_dict[s_name] = {'theme_rank': t_rank, 'is_leader': is_leader_in_this_theme, 'theme_name': t_name}
                else:
                    if is_leader_in_this_theme:
                        theme_rank_dict[s_name]['is_leader'] = True
                        theme_rank_dict[s_name]['theme_name'] = t_name
                        
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            for t in top_10_themes: target_names.add(str(t).strip())
            
        for t_name in all_theme_map.keys(): target_names.add(str(t_name).strip())

        
        target_dict = {}
        for name in list(target_names):
            code = name_to_code.get(name) or search_code_from_naver(name)
            if code and code not in target_dict.values():
                target_dict[name] = code

        results = []
        print(f"⚡ {len(target_dict)}개 고유 종목을 30개의 스레드로 동시 타격합니다...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_name = {executor.submit(analyze_single_stock, name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate): name for name, code in target_dict.items()}
            for future in concurrent.futures.as_completed(future_to_name):
                res = future.result()
                if res: results.append(res)

        results.sort(key=lambda x: x[18], reverse=True) 
        
        if results:
            try: 
                db_scanner_sheet = doc.worksheet("DB_스캐너")
                existing_data = {}
                old_data = db_scanner_sheet.get_all_values()
                
                now_time = datetime.datetime.now(KST)
                is_reset_time = (now_time.hour == 7) or (now_time.hour == 8 and now_time.minute < 50)

                if not is_reset_time:
                    for row in old_data[1:]:
                        if len(row) > 15:
                            saved_code = str(row[2]).replace("'", "").strip().zfill(6)
                            briefing = str(row[9]).strip()
                            target = str(row[14]).strip()
                            stop = str(row[15]).strip()
                            
                            if "대기중" not in briefing and "계산중" not in target and "계산 대기" not in target:
                                existing_data[saved_code] = {
                                    "briefing": briefing,
                                    "target": target,
                                    "stop": stop,
                                    "raw_row": row
                                }
            except: 
                doc.add_worksheet(title="DB_스캐너", rows="50", cols="17")
                existing_data = {}
                now_time = datetime.datetime.now(KST)
                is_reset_time = (now_time.hour == 7) or (now_time.hour == 8 and now_time.minute < 50)
            
            for r in results:
                c_code = str(r[1]).replace("'", "").strip().zfill(6)
                if c_code in existing_data:
                    r[23] = existing_data[c_code]["target"]
                    r[24] = existing_data[c_code]["stop"]
            
            try: helper_sheet = doc.worksheet("주가데이터_보조")
            except: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="23")
            
            helper_sheet.batch_clear(['A2:Z'])
            helper_sheet.update(range_name="A2", values=results, value_input_option="USER_ENTERED")
            print(f"✅ 총 {len(results)}개 종목 판독 완료 (주가데이터_보조 업데이트 완료)")
            
            scanner_keywords = ["[종베]", "[스윙/눌림]", "[스윙/추세]", "[당일/단타]", "[관심/수급]"]
            
            scanner_results = []
            for r in results:
                tajeom = r[9]
                if any(kw in tajeom for kw in scanner_keywords):
                    종목명 = r[0]
                    종목코드 = r[1].replace("'", "").zfill(6)
                    하이퍼링크 = f'=HYPERLINK("https://m.stock.naver.com/domestic/stock/{종목코드}/total", "{종목명}")'
                    시장구분 = "확인불가" 
                    현재가 = r[2]
                    등락률 = r[3]
                    테마명 = r[19]
                    AI신호 = r[7]
                    거래량비율 = r[6]
                    스코어 = r[8]
                    프로그램 = r[20]
                    고가_52주 = r[21]
                    기관누적수급 = r[22]
                    
                    ai_briefing = "AI 브리핑 대기중"
                    ai_target = "AI 데이터 계산중"
                    ai_stop = "AI 데이터 계산중"
                    
                    if 종목코드 in existing_data:
                        ai_briefing = existing_data[종목코드]["briefing"]
                        ai_target = existing_data[종목코드]["target"]
                        ai_stop = existing_data[종목코드]["stop"]
                        
                    scanner_results.append([
                        하이퍼링크, 시장구분, f"'{종목코드}", 현재가, 등락률, 테마명, AI신호, 거래량비율, 
                        tajeom, ai_briefing, 스코어, 프로그램, 고가_52주, 기관누적수급, ai_target, ai_stop
                    ])
                    
            scanner_results.sort(key=lambda x: int(str(x[10]).split('점')[0]), reverse=True)
            
            MAX_DISPLAY_COUNT = 20
            top_20_results = scanner_results[:MAX_DISPLAY_COUNT]
            top_20_codes = {str(x[2]).replace("'", "").strip().zfill(6) for x in top_20_results}

            if not is_reset_time:
                for c_code, data in existing_data.items():
                    if "리포트 발송 완료" in data["briefing"] and c_code not in top_20_codes:
                        top_20_results.append(data["raw_row"])
                        top_20_codes.add(c_code)

            if is_reset_time:
                try:
                    bt_sheet = doc.worksheet("백테스트_로그")
                    bt_data = bt_sheet.get_all_values()
                    if len(bt_data) == 0 or bt_data[0][0] != "진입일":
                        bt_sheet.clear()
                        bt_sheet.append_row(["진입일", "종목명", "종목코드", "테마명", "진입가", "타점유형", "퀀트점수", "T+1수익률", "T+3수익률"])
                        bt_data = bt_sheet.get_all_values()
                    
                    today_date = datetime.datetime.now(KST).date()
                    updated = False
                    
                    for i in range(1, len(bt_data)):
                        row = bt_data[i]
                        while len(row) < 9: row.append("") 
                        
                        try:
                            entry_date = datetime.datetime.strptime(row[0], '%Y-%m-%d').date()
                            days_elapsed = (today_date - entry_date).days
                            
                            needs_t1 = (days_elapsed >= 1 and row[7] == "")
                            needs_t3 = (days_elapsed >= 3 and row[8] == "")
                            
                            if needs_t1 or needs_t3:
                                t_code = str(row[2]).replace("'", "").zfill(6)
                                entry_p = int(str(row[4]).replace(',', '').replace('원', ''))
                                rt_res = requests.get(f"https://m.stock.naver.com/api/stock/{t_code}/basic", verify=False, timeout=3).json()
                                curr_p = int(str(rt_res.get('closePrice', '0')).replace(',', ''))
                                
                                if curr_p > 0:
                                    rtn = ((curr_p - entry_p) / entry_p) * 100
                                    if needs_t1: row[7] = f"{rtn:.2f}%"
                                    if needs_t3: row[8] = f"{rtn:.2f}%"
                                    updated = True
                        except: pass
                        
                    if updated:
                        bt_sheet.update(range_name="A1", values=bt_data, value_input_option="USER_ENTERED")
                        print("✅ 아침 백테스트 로그 수익률 자동 갱신 완료!")
                except Exception as e:
                    print(f"⚠️ 백테스트 업데이트 에러: {e}")

            db_scanner_sheet = doc.worksheet("DB_스캐너")
            db_scanner_sheet.batch_clear(['A2:Z'])
            if top_20_results: 
                db_scanner_sheet.update(range_name="A2", values=top_20_results, value_input_option="USER_ENTERED")
            print(f"🎯 DB_스캐너 {len(top_20_results)}개 전송 (초기화시간:{is_reset_time})")

    except Exception as e:
        print(f"❌ 전체 업데이트 에러: {e}")

if __name__ == "__main__":
    df_theme, is_market_closed, all_theme_map = get_real_money_themes()
    df_news, df_naver, df_main_news = get_news_keywords(), get_naver_search_ranking(), get_naver_main_news()
    update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed)
    
    today_schedules = get_market_schedule()
    manage_schedule_sheet(today_schedules)
    
    update_technical_data(df_theme, all_theme_map)
    
    now_kst = datetime.datetime.now(KST)
    if now_kst.hour == 15 and 0 <= now_kst.minute <= 50:
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
            gc = gspread.authorize(creds)
            doc = gc.open_by_url(SHEET_URL)
            posted_data = doc.worksheet("리포트_게시").get_all_values()
            today_str = now_kst.strftime('%Y-%m-%d')
            already_posted = any(today_str in str(row[0]) for row in posted_data[:5] if row)
            
            if not already_posted:
                GOOGLE_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
                print("⏳ 아직 오늘 리포트가 없습니다! 구글에 바통을 넘깁니다...")
                response = requests.post(GOOGLE_WEBHOOK_URL, timeout=30)
                if response.status_code == 200: print("✅ 바통 터치 성공!")
                else: print(f"❌ 바통 터치 실패 (Status: {response.status_code})")
            else: print("✅ 오늘 리포트가 이미 발행되었으므로 릴레이를 생략합니다.")
        except Exception as e: print(f"❌ 바통 터치 통신 에러: {e}")
