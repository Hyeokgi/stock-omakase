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

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

STOPWORDS = ['코스피', '코스닥', '증시', '주식', '투자', '종목', '시장', '지수', '대형주', '중소형주', '외인', '기관', '개인', '외국인', '매수', '매도', '순매수', '순매도', '거래', '대금', '주가', '펀드', '사모', '상장', '상폐', '공모', '특징주', '테마', '테마주', '관련', '관련주', '수혜', '수혜주', '장세', '개장', '출발', '마감', '초반', '후반', '오전', '오후', '장중', '증권', '증권사', '운용', '자사', '괴리', '프리미어', '가치', '밸류', '공시', '병합', '분할', '상승', '하락', '급등', '급락', '강세', '약세', '폭락', '반등', '조정', '랠리', '위축', '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '돌파', '경신', '연속', '최고', '최저', '신고가', '신저가', '최고치', '최저치', '최고가', '최저가', '급증', '급감', '확산', '진정', '완화', '악화', '개선', '회복', '최대', '사상', '역대', '최초', '최신', '규모', '수준', '가격', '목표가', '상향', '하향', '박살', '킬러', '대규모', '변동', '오픈', '호재', '연계', '대비', '경제', '금융', '기업', '정부', '자산', '머니', '한국', '미국', '국내', '글로벌', '뉴욕', '회장', '대표', '임원', '주주', '총회', '이유', '때문', '달러', '금리', '인상', '인하', '동결', '연준', '파월', '물가', '지표', '고용', '기름값', '주유소', '석유', '신용', '수익', '매출', '적자', '흑자', '배당', '지분', '인수', '합병', '사업', '추진', '공급', '계약', '체결', '실적', '발표', '이익', '반사이익', '현금', '자회사', '계열사', '지주사', '관계사', '기내식', '서비스', '오늘', '내일', '이번', '주간', '월간', '분기', '시간', '하루', '하루만', '올해', '내년', '지난해', '전일', '전주', '전월', '동기', '내달', '연말', '연초', '이날', '당일', '최근', '현재', '이후', '이전', '상반기', '하반기', '당분간', '예상', '전망', '기대', '우려', '경고', '목표', '분석', '평가', '결정', '검토', '참여', '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', '타격', '피해', '직격탄', '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', '달성', '기준', '행사', '이사', '의결', '개정', '취지', '적극', '개최', '진행', '예정', '상황', '필요', '대응', '마련', '운영', '관리', '적용', '이용', '사용', '활용', '확보', '제공', '구축', '기반', '중심', '노력', '계획', '정도', '경우', '이상', '이하', '가운데', '가장', '포함', '제외', '기대감', '우려감', '불확실성', '가능성', '움직임', '분위기', '흐름', '국면', '대목', '차원', '입장', '배경', '결과', '모습', '모멘텀', '현상', '차이', '비중', '비율', '단계', '목적', '대상', '조원', '억원', '만원', '천원', '전문', '현지', '사회', '생산자', '제도', '재고', '면제', '속보', '단독', '기자', '특파원', '앵커', '저작권', '무단', '전재', '재배포', '금지', '뉴스', '보도', '자료', '사진', '관계자', '주장', '설명', '강조', '위원회', '법안', '회의', '통과', '정책', '의원', '장관', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인', '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '공개', '이란', '국민연금', '종전', '전쟁', '트럼프', '제안', '찬성', '대통령', '사내', '협상', '출시', '계좌', '중동', '상품', '체제', '변경', '투자증권', '성장', '시그널', '신규', '정치', '외교', '합의', '수출', '수입', '도입', '본격', '소식', '임박', '부각', '주도']
AD_FILTER = ['펀드', '투어', '캠페인', '서비스', '최초', '강화', '고객', '연금', '마스터', '코리아', '정책', '개최', '박람회', '전시회', '프로모션', '할인', '기획전', '페스티벌', '출시', '협약', 'MOU', '체결', '선정', '어워드', '스마트픽', '팔자', '사자', '증가', '감소', '목표', '꺾인', '주석', '전망', '우려', '기대', '연내', '내달', '오늘', '내일', '돌파', '연속', '급락', '투자', '매수', '매도', '수익']
THEME_BLACKLIST = ['코로나19', '메르스', '지카바이러스', '우한폐렴', '원숭이두창', '엠폭스', '아프리카돼지열병', '구제역', '광우병', '야놀자(Yanolja)', '리비안(RIVIAN)']

def check_warning_market():
    try:
        url = "https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1"
        res = session.get(url, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) == 20: return prices[0] < (sum(prices) / 20)
    except: pass
    return False

def get_kospi_fluctuation_rate():
    try:
        res = session.get("https://m.stock.naver.com/api/index/KOSPI/basic", verify=False, timeout=3).json()
        rate_str = res.get("fluctuationsRatio", "0")
        return float(str(rate_str).replace(',', ''))
    except:
        return 0.0

def search_code_from_naver(stock_name):
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = session.get(url).json()
        if data.get('result') and data['result'].get('stocks'): return data['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_news_keywords():
    try:
        now_minute = datetime.datetime.now(KST).minute
        if not (30 <= now_minute < 40): return pd.DataFrame() 
        full_text = ""
        theme_phrases = []
        for page in range(1, 10):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = session.get(url, verify=False, timeout=5)
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
    try:
        res = session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        market_sum_tag = soup.find('em', id='_market_sum')
        if not market_sum_tag: return 999999 
        market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
        if '조' in market_sum_str:
            parts = market_sum_str.split('조')
            return int(parts[0].strip()) * 10000 + (int(parts[1].strip()) if len(parts)>1 and parts[1].strip() else 0)
        else: return int(market_sum_str)
    except: return 999999 

def get_real_money_themes():
    try:
        now = datetime.datetime.now(KST)
        is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
        time_str = now.strftime('%H:%M')
        
        res = session.get("https://finance.naver.com/sise/theme.naver", verify=False, timeout=5)
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
                soup = BeautifulSoup(session.get(theme['url'], verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
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
    try:
        soup = BeautifulSoup(session.get("https://finance.naver.com/sise/lastsearch2.naver", verify=False).content, 'html.parser', from_encoding='euc-kr')
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
    try:
        soup = BeautifulSoup(session.get("https://finance.naver.com/news/mainnews.naver", verify=False, timeout=5).content, 'html.parser', from_encoding='cp949')
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

# 💡 [업데이트] 네이버 캘린더 파싱 및 필터링 함수 (서술형 뉴스/전망 기사 차단)
def get_market_schedule():
    """네이버 금융 오늘의 증시 일정 수집 (순수 일정만 추출)"""
    try:
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        res = session.get(url, verify=False, timeout=5)
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
                
                # 1. 긍정 키워드: 반드시 포함되어야 하는 '일정' 관련 단어
                include_kws = ['실적', '발표', '만기', '배당', '금통위', 'FOMC', '고용', '학회', '임상', '상장', '개막', '출시']
                
                # 2. 💡 [핵심] 부정 키워드: 기획 기사, 주간 전망, 서술형 뉴스를 걸러내는 단어
                exclude_kws = [
                    '주주총회', '주총', '공모', '청약', # 기존 제외
                    '전망', '주목', '대기', '반환점', '서프라이즈', '쇼크', 
                    '기대감', '우려', '물귀신', '박스권', '코스피', '코스닥', 
                    '증시', '마감', '시황', '특징주', '주간'
                ]
                
                # 3. 필터링 로직: 긍정 키워드가 하나라도 있고, 부정 키워드는 하나도 없어야 함
                if any(kw in title for kw in include_kws) and not any(ex_kw in title for ex_kw in exclude_kws):
                    # 제목에 '[', ']' 대괄호나 '…' 같은 말줄임표가 포함되어 있으면 보통 기획 기사이므로 한 번 더 거름
                    if "증시 전망" not in title and "외환전망" not in title:
                        if clean_title not in seen_titles:
                            schedules.append([today_str, title, "📅 자동수집(당일)"])
                            seen_titles.add(clean_title)
        
        return schedules
    except Exception as e:
        print(f"❌ 일정 수집 에러: {e}")
        return []

# 💡 [업데이트] 주요일정 시트 자동 유지보수 함수 (스마트 포맷팅 + 과거 숨김 + 3개월 삭제)
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

        # 1. 포맷 정규화 및 3개월 이전 데이터 자동 폐기
        valid_rows = []
        for row in rows:
            if not row or not row[0]: continue
            
            # 기존 "2026. 3. 3" 같은 포맷을 "2026-03-03"으로 변환하여 파싱 시도
            raw_date = str(row[0]).strip().replace('.', '-').replace(' ', '').strip('-')
            try:
                row_date = datetime.datetime.strptime(raw_date, '%Y-%m-%d').date()
                if row_date >= three_months_ago:
                    row[0] = row_date.strftime('%Y-%m-%d')
                    valid_rows.append(row)
            except ValueError:
                valid_rows.append(row)

        # 2. 💡 [핵심] 시트 병합 시 띄어쓰기 무시하고 중복 2차 철통 방어
        existing_titles_clean = [str(r[1]).replace(" ", "").strip() for r in valid_rows if len(r) > 1 and r[0] == today.strftime('%Y-%m-%d')]
        
        for sch in schedules:
            clean_sch_title = str(sch[1]).replace(" ", "").strip()
            if clean_sch_title not in existing_titles_clean:
                valid_rows.append(sch)
                existing_titles_clean.append(clean_sch_title) # 방금 넣은 것도 바로 메모

        # 3. 날짜 오름차순 정렬
        def sort_key(x):
            try: return datetime.datetime.strptime(x[0], '%Y-%m-%d').date()
            except: return datetime.date(2099, 12, 31)
        valid_rows.sort(key=sort_key)

        # 4. 시트 덮어쓰기 (표준화된 포맷으로)
        sheet.batch_clear(['A2:C'])
        if valid_rows:
            sheet.update(range_name="A2", values=valid_rows, value_input_option="USER_ENTERED")

        # 5. 과거 날짜 자동 숨김 처리 로직
        requests_list = []
        
        # 전체 행 숨김 해제 (초기화)
        requests_list.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet.id, "dimension": "ROWS", "startIndex": 1, "endIndex": len(valid_rows) + 1},
                "properties": {"hiddenByUser": False},
                "fields": "hiddenByUser"
            }
        })

        # 과거 날짜 숨김 인덱스 계산
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
    try:
        # 1. fchart 데이터 가져오기 (과거 차트 분석용)
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        root = ET.fromstring(session.get(url, verify=False, timeout=3).text)
        
        history = []
        high_prices = []
        items = root.findall(".//item")
        
        for item in items:
            data = item.get("data").split("|")
            date_str = data[0]
            open_p, high_p, low_p, close_p, vol = int(data[1]), int(data[2]), int(data[3]), int(data[4]), int(data[5])
            
            # 💡 멀쩡한 캔들을 삭제하던 악성 필터링 제거! (거래량 0인 완전 빈 캔들만 스킵)
            if vol == 0 and close_p == 0: continue
            
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
        
        # 2. 💡 네이버 실시간 API에서 '현재가'와 '등락률' 직접 뜯어오기!
        try:
            rt_url = f"https://m.stock.naver.com/api/stock/{code}/basic"
            rt_res = session.get(rt_url, verify=False, timeout=3).json()
            
            rt_close = int(str(rt_res.get('closePrice', '0')).replace(',', ''))
            rt_open = int(str(rt_res.get('openPrice', '0')).replace(',', ''))
            rt_high = int(str(rt_res.get('highPrice', '0')).replace(',', ''))
            rt_low = int(str(rt_res.get('lowPrice', '0')).replace(',', ''))
            rt_vol = int(str(rt_res.get('accumulatedTradingVolume', '0')).replace(',', ''))
            
            rt_rate_str = str(rt_res.get('fluctuationsRatio', '0')).replace(',', '')
            rt_rate = float(rt_rate_str) / 100.0 
            
            rt_date_raw = rt_res.get('localTradedAt', '') 
            
            if rt_close > 0 and len(rt_date_raw) >= 10:
                rt_date = rt_date_raw[:10].replace('-', '') 
                
                if history[-1]['date'] == rt_date:
                    history[-1]['close'] = rt_close
                    if rt_open > 0: history[-1]['open'] = rt_open
                    if rt_high > 0: history[-1]['high'] = rt_high
                    if rt_low > 0: history[-1]['low'] = rt_low
                    if rt_vol > 0: history[-1]['volume'] = rt_vol
                    high_prices[-1] = history[-1]['high']
                elif rt_date > history[-1]['date']:
                    history.append({
                        "date": rt_date,
                        "open": rt_open,
                        "high": rt_high,
                        "low": rt_low,
                        "close": rt_close,
                        "volume": rt_vol
                    })
                    high_prices.append(rt_high)
        except Exception:
            pass

        # 3. 확정된 지표 추출
        last_day = history[-1]
        open_price, today_high, today_low, current_price, today_vol = last_day['open'], last_day['high'], last_day['low'], last_day['close'], last_day['volume']
        
        df_hist = pd.DataFrame(history)
        
        # 💡 [치명적 오류 복구] 아래 코드에서 사용하는 prev_price 변수명 부활!!
        prev_price = int(df_hist['close'].iloc[-2]) if len(df_hist) >= 2 else current_price
        yest_close = prev_price  # 혹시 모를 충돌을 막기 위해 yest_close도 같이 선언
        
        # API의 실시간 등락률이 있다면 최우선 적용! 없으면 수동 계산
        change_rate = (current_price - prev_price) / prev_price if prev_price > 0 else 0.0
        try:
            if 'rt_rate' in locals() and history[-1]['date'] == rt_date:
                change_rate = rt_rate
        except: pass
        
        yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) >= 2 else today_vol
        yest_tv = prev_price * yest_vol 

        trading_value = current_price * today_vol
 
        high_prices_60 = high_prices[-60:] if len(high_prices) >= 60 else high_prices
        
        high_60d_calc = max(high_prices_60[:-1]) if len(high_prices_60) > 1 else today_high
        high_250d_calc = max(high_prices[:-1]) if len(high_prices) > 1 else today_high
        
        display_high_60d = max(high_prices_60) if len(high_prices_60) > 0 else today_high
        display_high_250d = max(high_prices) if len(high_prices) > 0 else today_high
        
        min_20d = int(df_hist['close'].tail(20).min()) if len(df_hist) >= 20 else int(df_hist['close'].min())
        surge_rate_20d = (current_price - min_20d) / min_20d if min_20d > 0 else 0
        is_high_altitude = surge_rate_20d >= 0.50 
        
        # 💡 250일(1년) 장기 대시세 피로도 체크 로직
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
        
        risk_soup = BeautifulSoup(session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
        
        market_sum_tag = risk_soup.find('em', id='_market_sum')
        market_cap = 999999
        if market_sum_tag:
            market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
            if '조' in market_sum_str:
                parts = market_sum_str.split('조')
                market_cap = int(parts[0].strip()) * 10000 + (int(parts[1].strip()) if len(parts)>1 and parts[1].strip() else 0)
            else: market_cap = int(market_sum_str)

        is_junk = bool(risk_soup.find('img', alt=re.compile('관리종목|환기종목|거래정지|투자위험')))
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
            frgn_url = f"https://finance.naver.com/item/frgn.naver?code={code}"
            frgn_res = session.get(frgn_url, verify=False, timeout=3)
            frgn_soup = BeautifulSoup(frgn_res.content, 'html.parser', from_encoding='euc-kr')
            rows = frgn_soup.select("table.type2 > tr")
            
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
        
        is_platform_breakout = (box_ratio <= 0.15) and (vol_ratio_10d >= 300) and (current_price > ma20) and is_today_yangbong

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

        is_danta_range = 0.17 <= change_rate < 0.295
        
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
        
        is_true_theme_leader = is_theme_leader_raw and (trading_value >= 100_000_000_000)
        is_theme_daejang_sang = is_true_theme_leader and is_upper_limit and not (is_junk or is_financial_risk)
        is_theme_daejang = is_true_theme_leader and is_danta_range and not (is_junk or is_financial_risk)
        is_real_hubal = has_theme and not is_theme_leader_raw
        is_theme_hubal_sang = is_real_hubal and is_upper_limit and not (is_junk or is_financial_risk)
        is_theme_hubal = is_real_hubal and is_danta_range and not (is_junk or is_financial_risk)
        is_individual = (not has_theme) or (is_theme_leader_raw and trading_value < 100_000_000_000)
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
                
                if anchor_tv >= 80_000_000_000 and anchor_change >= 0.12 and anchor_close > anchor_open and (anchor_close >= high_60d_anchor * 0.90):
                    is_holding = True
                    for j in range(anchor_idx + 1, 0): 
                        if not (anchor_close * 0.97 <= int(df_hist['close'].iloc[j]) <= anchor_close * 1.12): is_holding = False; break
                        if ((int(df_hist['close'].iloc[j]) - int(df_hist['close'].iloc[j-1])) / int(df_hist['close'].iloc[j-1])) < -0.035: is_holding = False; break
                        if int(df_hist['volume'].iloc[j]) > anchor_vol * 0.45: is_holding = False; break
                    if is_holding: flag_days = d; break

        # 💡 [HYEOKS 리빌딩 v3] '전고점 돌파 후 거래급감 눌림목' 로직 적용 (스텔스 대체)
        is_recent_breakout = False
        breakout_days_ago = 0
        for d in range(1, 6): # 최근 5일 이내에 돌파했는지 확인
            check_idx = -d
            if len(df_hist) >= abs(check_idx) + 1:
                check_close = int(df_hist['close'].iloc[check_idx])
                hist_before_check = high_prices[:check_idx] if check_idx < -1 else high_prices[:-1]
                high_60d_check = max(hist_before_check) if len(hist_before_check) > 0 else check_close
                
                if check_close > high_60d_check:
                    is_recent_breakout = True
                    breakout_days_ago = d
                    break

        # 2. 거래급감 눌림목 조건 (종베 후보)
        is_extreme_nulim = (
            is_recent_breakout and                               # 최근 5일 내 전고점 돌파 이력이 있고
            (current_price >= high_60d_calc * 0.95) and          # 현재가가 그 돌파했던 전고점을 훼손하지 않고 지지 중이며
            (vol_ratio_yest <= 60) and                           # ★완화★ 전일 대비 거래량이 60% 이하로 급감 (종베 포착량 증가)
            (vol_ratio_10d <= 100) and                           # ★완화★ 최근 10일 평균 거래량 이하 수준
            (not is_today_yangbong or today_body_ratio <= 0.02) and # 오늘 쉬어가는 흐름
            (not is_long_shadow)                                 # 윗꼬리를 달며 매물을 맞은 흔적이 없을 것
        )
       
        # 1. 종가베팅 및 신고가 돌파 조건 계산 (완화됨)
        is_ss_breakout = (trading_value >= 100_000_000_000) and (change_rate >= 0.04) and not is_long_shadow and is_near_high
        
        now_kst_tajeom = datetime.datetime.now(KST)
        is_overnight_time = (now_kst_tajeom.hour >= 14) # ★확대★ 오후 2시부터 종베 후보를 띄워 직장인 준비 시간 확보
        
        is_overnight_breakout = (
            (trading_value >= 50_000_000_000) and           # ★완화★ 1000억 -> 500억
            (pg_amount_eok >= 10 or is_strong_dual_buy) and # ★완화★ 프로그램 20억 -> 10억
            (acc_i_buy_eok >= 3) and                        # ★완화★ 기관누적매집 5억 -> 3억
            (0.04 <= change_rate <= 0.28) and               # ★완화★ 6% 상승 -> 4% 상승
            (current_price >= today_high * 0.95) and        # ★완화★ 최고가 대비 -4% 마감 -> -5% 마감
            (is_near_high or is_near_52w_high) and          
            is_breakout_track and not is_long_shadow
        )
        
        is_overnight_pullback = (
            is_extreme_nulim and                            
            (current_price >= ma5) and                      
            (pg_amount_eok >= 5 or i_buy_today >= 50_000_000) # ★완화★ 쉬는 날에도 프로그램 5억 이상이면 통과
        )

        is_overnight_candidate = is_overnight_time and (is_overnight_breakout or is_overnight_pullback)

        # 2. 🚨 100% 하드 드랍 (상장폐지 위험, 거래정지, 자본잠식 등 찐 리스크만)
        is_fatal_drop = is_junk or is_financial_risk
        
        # 3. 베이스 모멘텀 스코어 산출 (수급, 거래량, 테마 강도의 기초 체력)
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

        # 4. 타점 계수 (Multiplier) 및 듀얼 배지 부여 (사용자 친화적 개편)
        tajeom_multiplier = 0.0
        master_tajeom = "⏸️ [대기] 분석 중"
        
        if is_fatal_drop:
            master_tajeom = "🚫 [제외] 상폐/재무위험"
            tajeom_multiplier = 0.0
        elif is_overnight_candidate:
            if is_breakout_track and not is_overnight_pullback:
                master_tajeom = "🌙 [종베] 신고가 돌파 대기"
            else:
                master_tajeom = "🌙 [종베] 거래급감 눌림"
            tajeom_multiplier = 1.5  
        elif is_extreme_nulim or flag_days > 0:
            if current_price >= high_60d_calc * 0.95:
                master_tajeom = "🎯 [스윙/눌림] 전고점 지지"
            else:
                master_tajeom = "🎯 [스윙/눌림] 20일선 방어전"
            tajeom_multiplier = 1.3  
        elif is_platform_breakout or is_ss_breakout:
            master_tajeom = "📦 [스윙/추세] 박스권 탈출"
            tajeom_multiplier = 1.1  
        elif is_theme_daejang or is_theme_hubal:
            master_tajeom = "🚀 [당일/단타] 대장주 불기둥"
            tajeom_multiplier = 1.0  
        elif "🌟" in signal or (change_rate >= 0.12 and trading_value >= 50_000_000_000):
            master_tajeom = "🌟 [관심/수급] 기준봉 포착"
            tajeom_multiplier = 0.9  
        else:
            master_tajeom = "👀 [관망] 타점 미도달"
            tajeom_multiplier = 0.6  

        # 5. 차트 훼손 및 리스크에 따른 감점 
        if not is_fatal_drop:
            if is_long_shadow or is_huge_gap:
                master_tajeom = "👀 [관망] 윗꼬리 저항/이격 큼"
                tajeom_multiplier = 0.5
            
            if is_chronic_loss: tajeom_multiplier -= 0.3
            if is_high_altitude: tajeom_multiplier -= 0.2
            
            # 💡 [수정 1] 시가총액 10조 이상 우량주(삼성전자 등)는 대시세 고점 페널티 면제
            if is_mega_trend_exhausted and market_cap < 100000:
                tajeom_multiplier -= 0.3
                master_tajeom += " ⚠️(대시세 고점)"

        # 6. 오후장 휩쏘 방지 (오후 돌파는 계수 삭감)
        if "돌파" in master_tajeom and is_after_1030 and not is_overnight_candidate:
            tajeom_multiplier -= 0.3 

        # 💡 [수정 2] 고정 5%/-3% 폐기 -> 이평선 및 전고점 기반 다이내믹 가격 설정
        if is_breakout_track: # 돌파 및 종베 타점
            target_price = int(current_price * 1.10) # 위가 열려있으므로 +10% 기대
            stop_loss = int(max(ma5, today_low)) # 5일선 혹은 당일 저가 이탈 시 칼손절
        else: # 스윙 및 눌림목 타점
            target_price = int(display_high_60d) if display_high_60d > current_price else int(current_price * 1.10) # 이전 고점 탈환 목표
            stop_loss = int(min(ma20, current_price * 0.95)) # 20일선 생명선 이탈 시 손절
            
        if stop_loss >= current_price: stop_loss = int(current_price * 0.96)
        if target_price <= current_price: target_price = int(current_price * 1.05)

        # 💡 [수정 3] 손익비(Risk/Reward) 필터링 (SK이노베이션 오류 방지)
        upside = target_price - current_price
        downside = current_price - stop_loss
        
        if downside > 0 and (upside / downside) < 1.0: # 먹을 폭보다 잃을 폭이 크거나 저항이 코앞이면
            if "스윙" in master_tajeom or "눌림" in master_tajeom:
                tajeom_multiplier -= 0.4
                master_tajeom = "👀 [관망] 손익비 불량 (저항 근접)"

        # 7. 최종 스코어 산출 
        quant_score = int(max(0, base_score * tajeom_multiplier))
        
        # 8. 시장 해지 프리미엄 추가 (하락장 방어)
        is_hedge_theme = any(kw in my_theme_name for kw in ['방산', '방위산업', '해운', '조선', '석유', '가스', '전쟁', '사료', '원자재', '품절주', '식품'])
        if is_hedge_theme and kospi_rate <= -0.5:
            hedge_premium = 5 + int((abs(kospi_rate) - 0.5) / 0.1) * 1 
            quant_score += hedge_premium
            master_tajeom += f" 🛡️(+{hedge_premium}점)"

        # =====================================================================
        # (이 아래는 기존 return [ name, f"'{code}", ... ] 구문이 그대로 이어집니다)
        return [
            name, f"'{code}", current_price, f"{change_rate * 100:.2f}%", 
            int(ma5), int(ma20), vol_ratio_text, signal, 
            score_display, master_tajeom, today_high, today_low, int(display_high_60d), 
            market_cap, shadow_text, dist_text, disp_text, leader_text, vol_status_text, my_theme_name,
            program_text,
            int(display_high_250d), f"{int(acc_i_buy_eok)}억",
            target_price, stop_loss  # ⬅️ 이 두 개가 인덱스 23, 24번으로 새롭게 추가됨
        ]
    except Exception as e:
        return None

def update_technical_data(df_theme, all_theme_map):
    try:
        print("▶️ 기술적 지표 초고속 멀티스레딩 판독 시작...")
        is_warning_market = check_warning_market()
        if is_warning_market: print("⚠️ 코스닥 20일선 이탈(하락장) 감지!")
        
        kospi_rate = get_kospi_fluctuation_rate()
        if kospi_rate <= -0.5:
            print(f"📊 코스피 실시간 등락률: {kospi_rate:.2f}% (해지 프리미엄 발동 대기)")
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        doc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)).open_by_url(SHEET_URL)
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
                t_rank, s_name, t_name = int(row['순위']), row['종목명'], row['테마명']
                if t_rank not in theme_rank_tracker: theme_rank_tracker[t_rank] = []
                theme_rank_tracker[t_rank].append(s_name)
                is_leader_in_this_theme = (len(theme_rank_tracker[t_rank]) == 1)
                
                if s_name not in theme_rank_dict: theme_rank_dict[s_name] = {'theme_rank': t_rank, 'is_leader': is_leader_in_this_theme, 'theme_name': t_name}
                else:
                    if is_leader_in_this_theme:
                        theme_rank_dict[s_name]['is_leader'] = True
                        theme_rank_dict[s_name]['theme_name'] = t_name
                        
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            for t in top_10_themes: target_names.add(t)
            
        for t_name in all_theme_map.keys(): target_names.add(t_name)

        target_dict = {}
        for name in list(target_names):
            code = name_to_code.get(name) or search_code_from_naver(name)
            if code: target_dict[name] = code

        results = []
        print(f"⚡ {len(target_dict)}개 종목을 30개의 스레드로 동시 타격합니다...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_name = {executor.submit(analyze_single_stock, name, code, is_warning_market, theme_rank_dict, all_theme_map, kospi_rate): name for name, code in target_dict.items()}
            for future in concurrent.futures.as_completed(future_to_name):
                res = future.result()
                if res: results.append(res)

        results.sort(key=lambda x: x[18], reverse=True) 
        
        if results:
            try: helper_sheet = doc.worksheet("주가데이터_보조")
            except: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="23")
            
            helper_sheet.batch_clear(['A2:Z'])
            helper_sheet.update(range_name="A2", values=results, value_input_option="USER_ENTERED")
            print(f"✅ 총 {len(results)}개 종목 판독 완료 (주가데이터_보조 업데이트 완료)")
            
            # 💡 개편된 듀얼 배지 키워드 스캐너 통과
            scanner_keywords = ["[종베]", "[스윙/눌림]", "[스윙/추세]", "[당일/단타]", "[관심/수급]"]
            
            scanner_results = []
            for r in results:
                tajeom = r[9]
                if any(kw in tajeom for kw in scanner_keywords):
                    종목명 = r[0]
                    종목코드 = r[1].replace("'", "")
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
                    목표가 = r[23]
                    손절가 = r[24]
                    
                    scanner_results.append([하이퍼링크, 시장구분, f"'{종목코드}", 현재가, 등락률, 테마명, AI신호, 거래량비율, tajeom, "AI 브리핑 대기중", 스코어, 프로그램, 고가_52주, 기관누적수급, 목표가, 손절가])
                    
            if scanner_results:
                scanner_results.sort(key=lambda x: int(str(x[10]).split('점')[0]), reverse=True)
                
                MAX_DISPLAY_COUNT = 20
                scanner_results = scanner_results[:MAX_DISPLAY_COUNT]

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
                                if "대기중" not in str(row[9]):
                                    existing_data[saved_code] = {
                                        "briefing": str(row[9]).strip(),
                                        "target": str(row[14]).strip(),
                                        "stop": str(row[15]).strip(),
                                        "raw_row": row  # 💡 나중에 부활시키기 위해 기존 행 전체를 백업!
                                    }
                except: 
                    db_scanner_sheet = doc.add_worksheet(title="DB_스캐너", rows="50", cols="17")
                    existing_data = {}
                    now_time = datetime.datetime.now(KST)
                    is_reset_time = (now_time.hour == 7) or (now_time.hour == 8 and now_time.minute < 50)
                
                final_scanner_results = []
                included_codes = set() # 💡 현재 상위 20개에 포함된 종목들 추적
                
                for res in scanner_results:
                    check_code = str(res[2]).replace("'", "").strip().zfill(6)
                    while len(res) < 16: res.append("")
                    
                    if check_code in existing_data:
                        res[9] = existing_data[check_code]["briefing"]
                        # 💡 기존 시트에 AI 값이 있다면 유지하되, '계산 대기' 상태면 파이썬 계산값을 덮어씀
                        if "계산" not in str(existing_data[check_code]["target"]):
                            res[14] = existing_data[check_code]["target"]
                            res[15] = existing_data[check_code]["stop"]
                    else:
                        res[9] = "AI 브리핑 대기중"
                        # 💡 res[14]와 res[15]를 "계산 대기"로 덮어쓰지 않고 파이썬 원본 계산값을 그대로 보존!
                        
                    final_scanner_results.append(res)
                    included_codes.add(check_code)

                # 💡 [핵심 해결책] 리포트 발급 VIP 종목이 20위 밖으로 밀려났더라도 스캐너에 강제 소환!
                # 단, 아침 리셋 시간(is_reset_time)에는 살려내지 않고 깔끔하게 날려버림.
                if not is_reset_time:
                    for code, data in existing_data.items():
                        if "리포트 발송 완료" in data["briefing"] and code not in included_codes:
                            final_scanner_results.append(data["raw_row"])
                
                db_scanner_sheet.batch_clear(['A2:Z'])
                db_scanner_sheet.update(range_name="A2", values=final_scanner_results, value_input_option="USER_ENTERED")
                print(f"🎯 DB_스캐너 {len(final_scanner_results)}개 전송 (초기화시간:{is_reset_time})")

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
