import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib3
import datetime
import gspread
import re
import xml.etree.ElementTree as ET
from collections import Counter
from oauth2client.service_account import ServiceAccountCredentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
TARGET_PERCENT = 5.0
KST = datetime.timezone(datetime.timedelta(hours=9))

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'})
# ==========================================

STOPWORDS = [
    '코스피', '코스닥', '증시', '주식', '투자', '종목', '시장', '지수', '대형주', '중소형주', 
    '외인', '기관', '개인', '외국인', '매수', '매도', '순매수', '순매도', '거래', '대금', 
    '주가', '펀드', '사모', '상장', '상폐', '공모', '특징주', '테마', '테마주', '관련', '관련주', 
    '수혜', '수혜주', '장세', '개장', '출발', '마감', '초반', '후반', '오전', '오후', '장중',
    '증권', '증권사', '운용', '자사', '괴리', '프리미어', '가치', '밸류', '공시', '병합', '분할',
    '상승', '하락', '급등', '급락', '강세', '약세', '폭락', '반등', '조정', '랠리', '위축', 
    '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '돌파', '경신', '연속', '최고', '최저', 
    '신고가', '신저가', '최고치', '최저치', '최고가', '최저가', '급증', '급감', '확산', '진정', 
    '완화', '악화', '개선', '회복', '최대', '사상', '역대', '최초', '최신', '규모', '수준', '가격', 
    '목표가', '상향', '하향', '박살', '킬러', '대규모', '변동', '오픈', '호재', '연계', '대비',
    '경제', '금융', '기업', '정부', '자산', '머니', '한국', '미국', '국내', '글로벌', '뉴욕', 
    '회장', '대표', '임원', '주주', '총회', '이유', '때문', '달러', '금리', '인상', '인하', '동결', 
    '연준', '파월', '물가', '지표', '고용', '기름값', '주유소', '석유', '신용', '수익', '매출', 
    '적자', '흑자', '배당', '지분', '인수', '합병', '사업', '추진', '공급', '계약', '체결', 
    '실적', '발표', '이익', '반사이익', '현금', '자회사', '계열사', '지주사', '관계사', '기내식', '서비스',
    '오늘', '내일', '이번', '주간', '월간', '분기', '시간', '하루', '하루만', '올해', '내년', 
    '지난해', '전일', '전주', '전월', '동기', '내달', '연말', '연초', '이날', '당일', '최근', 
    '현재', '이후', '이전', '상반기', '하반기', '당분간',
    '예상', '전망', '기대', '우려', '경고', '목표', '분석', '평가', '결정', '검토', '참여', 
    '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', '타격', '피해', '직격탄', 
    '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', '달성', '기준',
    '행사', '이사', '의결', '개정', '취지', '적극', '개최', '진행', '예정', '상황', '필요', '대응',
    '마련', '운영', '관리', '적용', '이용', '사용', '활용', '확보', '제공', '구축', '기반', '중심',
    '노력', '계획', '정도', '경우', '이상', '이하', '가운데', '가장', '포함', '제외', '기대감',
    '우려감', '불확실성', '가능성', '움직임', '분위기', '흐름', '국면', '대목', '차원', '입장',
    '배경', '결과', '모습', '모멘텀', '현상', '차이', '비중', '비율', '단계', '목적', '대상',
    '조원', '억원', '만원', '천원', '전문', '현지', '사회', '생산자', '제도', '재고', '면제',
    '속보', '단독', '기자', '특파원', '앵커', '저작권', '무단', '전재', '재배포', '금지', '뉴스',
    '보도', '자료', '사진', '관계자', '주장', '설명', '강조', '위원회', '법안', '회의', '통과',
    '정책', '의원', '장관', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인',
    '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '공개',
    '이란', '국민연금', '종전', '전쟁', '트럼프', '제안', '찬성', '대통령', '사내', '협상',
    '출시', '계좌', '중동', '상품', '체제', '변경', '투자증권', '성장', '시그널', '신규',
    '정치', '외교', '합의', '수출', '수입', '도입', '본격', '소식', '임박', '부각', '주도'
]

AD_FILTER = [
    '펀드', '투어', '캠페인', '서비스', '최초', '강화', '고객', '연금', '마스터', 
    '코리아', '정책', '개최', '박람회', '전시회', '프로모션', '할인', '기획전', 
    '페스티벌', '출시', '협약', 'MOU', '체결', '선정', '어워드', '스마트픽',
    '팔자', '사자', '증가', '감소', '목표', '꺾인', '주석', '전망', '우려', '기대',
    '연내', '내달', '오늘', '내일', '돌파', '연속', '급락', '투자', '매수', '매도', '수익'
]

def check_warning_market():
    try:
        url = "https://m.stock.naver.com/api/index/KOSDAQ/price?pageSize=20&page=1"
        res = session.get(url, verify=False, timeout=3).json()
        prices = [float(item['closePrice'].replace(',', '')) for item in res]
        if len(prices) == 20:
            ma20 = sum(prices) / 20
            if prices[0] < ma20:
                return True
    except:
        pass
    return False

def search_code_from_naver(stock_name):
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = session.get(url).json()
        if data.get('result') and data['result'].get('stocks'):
            return data['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_news_keywords():
    try:
        full_text = ""
        theme_phrases = []
        
        for page in range(1, 10):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = session.get(url, verify=False, timeout=5)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
            
            subjects = soup.select('.articleSubject a')
            for sub in subjects:
                title_text = sub.get_text(strip=True)
                full_text += title_text + " \n "
                
                matches = re.findall(r"['\"‘“](.*?)['\"’”]", title_text)
                for m in matches:
                    clean = re.sub(r'(수혜|관련주|테마주|대장주|강세|상한가|특징주|급등|주목|부각)', '', m).strip()
                    clean = re.sub(r'[^\w\s]', '', clean).strip()
                    if 1 < len(clean) <= 12 and clean.count(' ') <= 1:
                        if not any(ad in clean for ad in AD_FILTER):
                            theme_phrases.append(clean)
                
                matches2 = re.findall(r'([가-힣a-zA-Z0-9]+)(?:\s+)?(?:관련주|테마주|수혜주|대장주|섹터|주도주)', title_text)
                for m in matches2:
                    m = re.sub(r'[^\w\s]', '', m).strip()
                    if 1 < len(m) <= 10 and not any(ad in m for ad in AD_FILTER):
                        theme_phrases.append(m)
            time.sleep(0.3)
            
        core_keywords = [
            '의료AI', '비만치료제', '전고체', '자율주행', '로봇', '반도체', '바이오시밀러', 
            '원격진료', '탈플라스틱', '신재생', '원전', '우주항공', 'UAM', '메타버스', 
            'OLED', 'LFP', 'HBM', 'CXL', '온디바이스', 'AI', '초전도체', '양자암호', 
            '저전력', '데이터센터', '웹툰', '비트코인', 'STO', '밸류업', '방산', '조선',
            '피지컬AI', '전력설비', '유리기판', '액침냉각', '엔터', '화장품', '미용기기',
            '제약', '바이오', '이차전지', '2차전지', '폐배터리', '수소', '태양광', '마이크로바이옴'
        ]
        for word in core_keywords:
            count = full_text.count(word)
            for _ in range(count):
                theme_phrases.append(word)

        final_keywords = []
        for word in theme_phrases:
            if word not in STOPWORDS and not any(junk in word for junk in ['특징주', '강세', '급등', '상승', '하락']):
                final_keywords.append(word)
                
        raw_top = Counter(final_keywords).most_common()
        top_10 = [(word, count) for word, count in raw_top if count > 1][:10]
        
        if not top_10: return pd.DataFrame()
            
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        return pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_10, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
    except Exception as e:
        return pd.DataFrame()

market_cap_cache = {} 
def get_market_cap(code):
    if code in market_cap_cache: return market_cap_cache[code]
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = session.get(url, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        market_sum_tag = soup.find('em', id='_market_sum')
        if not market_sum_tag: return 999999 
        market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
        if '조' in market_sum_str:
            parts = market_sum_str.split('조')
            final_cap = int(parts[0].strip()) * 10000 + (int(parts[1].strip()) if len(parts)>1 and parts[1].strip() else 0)
        else: final_cap = int(market_sum_str)
        market_cap_cache[code] = final_cap
        return final_cap
    except: return 999999 

def get_real_money_themes():
    now = datetime.datetime.now(KST)
    is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
    time_str = now.strftime('%H:%M')
    res = session.get("https://finance.naver.com/sise/theme.naver", verify=False)
    soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
    
    themes = [{'name': a.text.strip(), 'url': "https://finance.naver.com" + a['href']} for tds in [tr.find_all('td') for tr in soup.find('table', {'class': 'type_1'}).find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a][:20]
                    
    theme_data_list = []
    print("▶️ 실시간 주도 테마 수집 시작 (거래대금 필터링 + 등락률 랭킹)...")
    for theme in themes:
        try:
            soup = BeautifulSoup(session.get(theme['url'], verify=False).content, 'html.parser', from_encoding='cp949')
            stocks = []
            for tr in soup.find('table', {'class': 'type_5'}).find_all('tr'):
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
            
            # 🚀 [1차: 거래대금으로 찐 종목 5개 선별]
            stocks_val = sorted(stocks, key=lambda x: x['value'], reverse=True)[:5]
            
            if stocks_val and not (len(stocks_val) >= 2 and stocks_val[0]['value'] >= stocks_val[1]['value'] * 10):
                # 🚀 [2차: 선별된 5개 안에서 대장주 서열은 '등락률' 순으로 재정렬]
                stocks_rate = sorted(stocks_val, key=lambda x: x['rate'], reverse=True)
                theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks_rate})
        except: continue
        time.sleep(0.1)
        
    if not theme_data_list: return pd.DataFrame(), is_market_closed, {}
    
    grouped_themes = {}
    for t_data in theme_data_list: grouped_themes.setdefault(t_data['stocks'][0]['code'], []).append(t_data)
        
    merged_themes = []
    for top_code, t_list in grouped_themes.items():
        theme_names = list(dict.fromkeys(t['theme_name'] for t in t_list))
        merged_name = " / ".join(theme_names) + f" (대장: {t_list[0]['stocks'][0]['name']})" if len(theme_names) > 1 else theme_names[0]
        unique_stocks = {s['code']: s for t in t_list for s in t['stocks']}
        
        # 🚀 [병합 시에도 거래대금 TOP5 선별 후, 등락률 순으로 최종 정렬]
        merged_stocks_val = sorted(unique_stocks.values(), key=lambda x: x['value'], reverse=True)[:5]
        merged_stocks_rate = sorted(merged_stocks_val, key=lambda x: x['rate'], reverse=True)
        
        merged_themes.append({'theme_name': merged_name, 'theme_sum': sum(s['value'] for s in merged_stocks_val), 'stocks': merged_stocks_rate})
        
    merged_themes = sorted(merged_themes, key=lambda x: x['theme_sum'], reverse=True)
    
    # 💡 [핵심 백업] 대시보드에서 통폐합(삭제) 당하기 전의 '모든 원본 테마 정보'를 사전으로 백업
    all_theme_map = {}
    for m_data in merged_themes:
        for idx, s in enumerate(m_data['stocks']):
            if s['name'] not in all_theme_map:
                all_theme_map[s['name']] = {
                    'theme_name': m_data['theme_name'],
                    'is_leader': (idx == 0) # 1등 종목이면 대장주로 인식
                }

    final_themes = []
    for m_data in merged_themes:
        # 교집합 통폐합 로직 (대시보드 깔끔화용)
        if not any(len(set(s['code'] for s in m_data['stocks']).intersection(set(s['code'] for s in f_data['stocks']))) >= 2 for f_data in final_themes):
            final_themes.append(m_data)
        if len(final_themes) >= 10: break
            
    final_rows = [{'날짜': now.strftime('%Y-%m-%d'), **({'시간': time_str} if not is_market_closed else {}), '순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)} for rank, t_data in enumerate(final_themes, 1) for s in t_data['stocks']]
    
    return pd.DataFrame(final_rows), is_market_closed, all_theme_map

def get_naver_search_ranking():
    try:
        soup = BeautifulSoup(session.get("https://finance.naver.com/sise/lastsearch2.naver", verify=False).content, 'html.parser', from_encoding='euc-kr')
        data = []
        search_blacklist = ['삼성전자', 'SK하이닉스', '현대차', '기아', 'LG에너지솔루션', 'POSCO홀딩스', '셀트리온', 'NAVER', '카카오']
        for row in soup.find('table', {'class': 'type_5'}).find_all('tr'):
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
                sheet.clear()
                sheet.update(range_name="A1", values=[all_data[0] if all_data else df_theme.columns.values.tolist()] + combined_data, value_input_option="USER_ENTERED")
            else:
                sheet.clear() 
                sheet.update(range_name="A1", values=[df_theme.columns.values.tolist()] + df_theme.values.tolist(), value_input_option="USER_ENTERED")
                
        for df, sheet_name in [(df_news, "뉴스_키워드"), (df_naver, "네이버_검색상위"), (df_main_news, "네이버_주요뉴스")]:
            if not df.empty:
                sheet = doc.worksheet(sheet_name)
                sheet.clear()
                sheet.update(range_name="A1", values=[df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")
    except: pass

def update_technical_data(df_theme, all_theme_map):
    try:
        print("▶️ 기술적 지표, 투트랙 채점(돌파/눌림 분리) 및 퀀트 아차상 판독 시작...")
        
        is_warning_market = check_warning_market()
        if is_warning_market:
            print("⚠️ 코스닥 20일선 이탈(하락장) 감지! 돌파 타점에 경고 태그를 부착합니다.")
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        doc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)).open_by_url(SHEET_URL)
        
        name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in doc.worksheet("기업정보").get_all_values()[1:] if len(row) >= 3}
        target_names = set()
        
        theme_rank_dict = {}

        try:
            raw_data = doc.worksheet("수급_Raw").get_all_values()
            for row in raw_data[1:]:
                if len(row) >= 7:
                    stock_name = str(row[-4]).strip()
                    val_str = str(row[-1]).replace(',', '').replace('억원', '').replace('"', '').strip()
                    if val_str.isdigit() and int(val_str) >= 1000:
                        if stock_name and stock_name not in ["#REF!", "로딩중...", "데이터대기", "FALSE"]:
                            target_names.add(stock_name)
        except: pass

        try:
            for row in doc.worksheet("대시보드").get_all_values()[4:]:
                if len(row) > 2 and str(row[2]).strip() and str(row[2]).strip() != "#REF!": 
                    target_names.add(str(row[2]).strip())
        except: pass
        
        if not df_theme.empty:
            theme_rank_tracker = {}
            for index, row in df_theme.iterrows():
                t_rank = int(row['순위'])
                s_name = row['종목명']
                t_name = row['테마명']
                
                if t_rank not in theme_rank_tracker:
                    theme_rank_tracker[t_rank] = []
                    
                theme_rank_tracker[t_rank].append(s_name)
                is_leader_in_this_theme = (len(theme_rank_tracker[t_rank]) == 1)
                
                if s_name not in theme_rank_dict:
                    theme_rank_dict[s_name] = {'theme_rank': t_rank, 'is_leader': is_leader_in_this_theme, 'theme_name': t_name}
                else:
                    if is_leader_in_this_theme:
                        theme_rank_dict[s_name]['is_leader'] = True
                        theme_rank_dict[s_name]['theme_name'] = t_name
                        
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            for t in top_10_themes: target_names.add(t)
            
        # 💡 [추가] 대시보드에서 짤린 테마의 종목들도 모조리 스캐너 분석 대상에 강제 포함!
        for t_name in all_theme_map.keys():
            target_names.add(t_name)

        results = []
        for name in list(target_names):
            try:
                code = name_to_code.get(name) or search_code_from_naver(name)
                if not code: continue
                
                url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
                root = ET.fromstring(session.get(url, verify=False, timeout=3).text)
                
                history = []
                high_prices = []
                items = root.findall(".//item")
                for item in items:
                    data = item.get("data").split("|")
                    history.append({"close": int(data[4]), "volume": int(data[5]), "open": int(data[1])})
                    high_prices.append(int(data[2]))
                    
                if len(history) < 1: continue
                
                today_data = items[-1].get("data").split("|")
                open_price = int(today_data[1])
                today_high = int(today_data[2])
                today_low = int(today_data[3])
                current_price = int(today_data[4])
                today_vol = int(today_data[5])
                
                if today_vol == 0:
                    continue
                
                df_hist = pd.DataFrame(history)
                prev_price = int(df_hist['close'].iloc[-2]) if len(df_hist) > 1 else current_price
                change_rate = (current_price - prev_price) / prev_price if prev_price > 0 else 0.0
                
                yest_close = prev_price
                yest_open = int(df_hist['open'].iloc[-2]) if len(df_hist) > 1 else yest_close
                yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) > 1 else 0
                yest_prev_close = int(df_hist['close'].iloc[-3]) if len(df_hist) > 2 else yest_open
                
                trading_value = current_price * today_vol
                high_60d = max(high_prices[:-1]) if len(high_prices) > 1 else today_high
                
                min_20d = int(df_hist['close'].tail(20).min()) if len(df_hist) >= 20 else int(df_hist['close'].min())
                surge_rate_20d = (current_price - min_20d) / min_20d if min_20d > 0 else 0
                is_high_altitude = surge_rate_20d >= 0.50 
                
                body_top = max(current_price, open_price)
                body_bottom = min(current_price, open_price)
                upper_shadow = today_high - body_top
                real_body = body_top - body_bottom
                
                upper_shadow_ratio = upper_shadow / current_price if current_price > 0 else 0
                is_long_shadow = (upper_shadow_ratio >= 0.05) or (upper_shadow_ratio >= 0.025 and upper_shadow > real_body * 1.5)
                shadow_text = "⚠️ 윗꼬리 위험" if is_long_shadow else ("👑 깔끔한 단봉" if upper_shadow_ratio <= 0.015 else "🟡 보통 캔들")
                
                today_body_ratio = real_body / open_price if open_price > 0 else 0
                is_today_yangbong = current_price >= open_price
                
                gap_ratio = (open_price - prev_price) / prev_price if prev_price > 0 else 0
                is_huge_gap = gap_ratio >= 0.04
                
                market_cap = get_market_cap(code)
                
                risk_soup = BeautifulSoup(session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                is_junk = bool(risk_soup.find('img', alt=re.compile('관리종목|환기종목|거래정지|투자위험')))
                
                is_financial_risk = False
                is_chronic_loss = False
                try:
                    fin_table = risk_soup.find('table', {'class': 'tb_type1 tb_num tb_type1_ifrs'})
                    if fin_table:
                        op_profits = []
                        total_equity = None
                        capital_stock = None
                        for tr in fin_table.find('tbody').find_all('tr'):
                            th = tr.find('th')
                            if not th: continue
                            title = th.text.strip()
                            if title == '영업이익':
                                for td in tr.find_all('td')[:3]:
                                    val = td.text.replace(',', '').strip()
                                    try: op_profits.append(float(val))
                                    except: pass
                            elif title == '자본총계':
                                for td in reversed(tr.find_all('td')):
                                    val = td.text.replace(',', '').strip()
                                    try: total_equity = float(val); break
                                    except: pass
                            elif title == '자본금':
                                for td in reversed(tr.find_all('td')):
                                    val = td.text.replace(',', '').strip()
                                    try: capital_stock = float(val); break
                                    except: pass
                        
                        if capital_stock and total_equity and total_equity < capital_stock:
                            is_financial_risk = True
                        if len(op_profits) == 3 and all(p < 0 for p in op_profits):
                            is_chronic_loss = True
                except: pass

                is_dual_buy, f_buy, i_buy, supply_text = False, 0, 0, ""
                try:
                    today_trend = session.get(f"https://m.stock.naver.com/api/stock/{code}/investor/trend", verify=False, timeout=3).json().get('investorTrendList', [{}])[0]
                    f_buy, i_buy = int(str(today_trend.get('foreignerStraightPurchasePrice', '0')).replace(',', '')), int(str(today_trend.get('institutionStraightPurchasePrice', '0')).replace(',', ''))
                    if f_buy > 0 and i_buy > 0: is_dual_buy, supply_text = True, " (쌍끌이🔥)"
                    elif f_buy > 0: supply_text = " (외인매수)"
                    elif i_buy > 0: supply_text = " (기관매수)"
                except: pass

                ma5 = int(df_hist['close'].tail(5).mean()) if len(df_hist) >= 5 else current_price
                ma20 = int(df_hist['close'].tail(20).mean()) if len(df_hist) >= 20 else current_price
                std20 = df_hist['close'].tail(20).std(ddof=0) if len(df_hist) >= 20 else 0
                
                disp_20 = (current_price / ma20) * 100 if ma20 > 0 else 100
                disp_text = f"{disp_20:.1f}%"
                
                is_leader_history = False
                for i in range(1, len(history)):
                    past_prev_c = history[i-1]['close']
                    past_curr_h = high_prices[i]
                    if past_prev_c > 0 and (past_curr_h - past_prev_c) / past_prev_c >= 0.22:
                        is_leader_history = True
                        break
                leader_text = "🔥대장주(O)" if is_leader_history else "평범(X)"

                upper_band = ma20 + (std20 * 2) 
                lower_band = ma20 - (std20 * 2) 
                band_width = (upper_band - lower_band) / ma20 if ma20 > 0 else 0 
                
                avg_vol_10 = df_hist['volume'].tail(11).head(10).mean() if len(df_hist) >= 2 else today_vol
                vol_ratio = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
                is_converging = (band_width <= 0.20) or (ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035)
                
                vol_status_text = "🟢 극감 (씨마름)" if vol_ratio <= 35 else ("🟢 감소 (안정)" if vol_ratio <= 60 else ("🔴 폭증 (과열)" if vol_ratio >= 200 else "🟡 보통"))
                
                if is_junk: signal = "🚨 [위험] 매매금지 (잡주/경고)"
                elif is_financial_risk: signal = "🚨 [위험] 매매금지 (자본잠식위험)"
                elif is_dual_buy and is_converging: signal = "🌟 모아가기 (쌍끌이)"
                elif band_width <= 0.20 and current_price >= ma20: signal = "🚀 N자파동 (밴드돌파)" + supply_text if current_price >= upper_band * 0.98 else "👀 N자파동 (에너지응축)" + supply_text
                elif ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035: signal = "📈 2차랠리 (이평수렴)" + supply_text if current_price > ma20 else "⏳ 이평선 저항" + supply_text
                else: signal = "🟢 낙폭과대 (과매도)" + supply_text if current_price < lower_band else "⚡ 관망 (이격발생)" + supply_text
                
                is_near_high = current_price >= (high_60d * 0.90) or yest_close >= (high_60d * 0.90)
                dist_text = "🎯 전고점 턱밑" if is_near_high else ("🟢 매물대 소화중" if current_price >= high_60d * 0.80 else "📉 이격 과다")

                is_upper_limit = change_rate >= 0.295
                is_danta_range = 0.17 <= change_rate < 0.295
                
                # 💡 [핵심 패치] 대시보드에서 짤렸더라도, 백업된 테마 데이터가 있으면 테마주로 완전 복권!
                if name in theme_rank_dict:
                    my_theme_name = theme_rank_dict[name]['theme_name']
                    is_theme_leader_raw = theme_rank_dict[name]['is_leader']
                    has_theme = True
                elif name in all_theme_map:
                    my_theme_name = all_theme_map[name]['theme_name']
                    is_theme_leader_raw = all_theme_map[name]['is_leader']
                    has_theme = True
                else:
                    my_theme_name = "개별주/기타"
                    is_theme_leader_raw = False
                    has_theme = False
                
                is_true_theme_leader = is_theme_leader_raw and (trading_value >= 100_000_000_000)
                is_weak_theme_leader = is_theme_leader_raw and (trading_value < 100_000_000_000)
                
                is_theme_daejang_sang = is_true_theme_leader and is_upper_limit and not (is_junk or is_financial_risk)
                is_theme_daejang = is_true_theme_leader and is_danta_range and not (is_junk or is_financial_risk)
                
                is_real_hubal = has_theme and not is_theme_leader_raw
                is_theme_hubal_sang = is_real_hubal and is_upper_limit and not (is_junk or is_financial_risk)
                is_theme_hubal = is_real_hubal and is_danta_range and not (is_junk or is_financial_risk)
                
                is_individual = (not has_theme) or is_weak_theme_leader
                is_individual_sang = is_individual and is_upper_limit and not (is_junk or is_financial_risk)
                is_individual_surge = is_individual and is_danta_range and not (is_junk or is_financial_risk)

                is_breakout_track = current_price >= ma20
                track_type = "돌파" if is_breakout_track else "눌림"
                quant_score = 0
                
                if not (is_junk or is_financial_risk):
                    if is_breakout_track:
                        if current_price >= (high_60d * 0.90): quant_score += 20
                        elif current_price >= (high_60d * 0.85): quant_score += 10
                        if vol_ratio >= 300: quant_score += 20
                        elif vol_ratio >= 150: quant_score += 10
                        if is_dual_buy: quant_score += 25
                        elif f_buy > 0 or i_buy > 0: quant_score += 10
                        if band_width <= 0.20: quant_score += 10
                        if ma5 > ma20 and current_price >= ma5: quant_score += 15
                    else:
                        if vol_ratio <= 35: quant_score += 20
                        elif vol_ratio <= 50: quant_score += 10
                        if is_today_yangbong or today_body_ratio <= 0.015: quant_score += 15
                        if ma20 * 0.95 <= current_price <= ma20 * 1.02: quant_score += 15
                        if is_dual_buy: quant_score += 15
                        elif f_buy > 0 or i_buy > 0: quant_score += 10
                        if band_width <= 0.15: quant_score += 15

                score_display = f"{quant_score}점 ({track_type})"

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
                        is_near_high_anchor = anchor_close >= (high_60d_anchor * 0.90)
                        
                        if anchor_tv >= 80_000_000_000 and anchor_change >= 0.12 and anchor_close > anchor_open and is_near_high_anchor:
                            is_holding = True
                            for j in range(anchor_idx + 1, 0): 
                                curr_close = int(df_hist['close'].iloc[j])
                                curr_prev_close = int(df_hist['close'].iloc[j-1])
                                curr_vol = int(df_hist['volume'].iloc[j])
                                curr_change = (curr_close - curr_prev_close) / curr_prev_close if curr_prev_close > 0 else 0
                                
                                if not (anchor_close * 0.97 <= curr_close <= anchor_close * 1.12):
                                    is_holding = False; break
                                if curr_change < -0.035:
                                    is_holding = False; break
                                if curr_vol > anchor_vol * 0.45:
                                    is_holding = False; break

                            if is_holding:
                                flag_days = d
                                break
                
                if market_cap >= 50000:
                    is_ss_breakout = (trading_value >= 150_000_000_000) and (change_rate >= 0.025) and not is_long_shadow and is_near_high
                    is_runner_up_breakout = not is_ss_breakout and is_breakout_track and (quant_score >= 35) and (trading_value >= 80_000_000_000) and (change_rate >= 0.015) and not is_long_shadow
                    is_runner_up_pullback = not is_breakout_track and flag_days != 3 and (quant_score >= 35) and (vol_ratio <= 60) and (is_today_yangbong or today_body_ratio <= 0.02)
                
                elif market_cap >= 10000:
                    is_ss_breakout = (trading_value >= 100_000_000_000) and (change_rate >= 0.04) and not is_long_shadow and is_near_high
                    is_runner_up_breakout = not is_ss_breakout and is_breakout_track and (quant_score >= 35) and (trading_value >= 60_000_000_000) and (change_rate >= 0.025) and not is_long_shadow
                    is_runner_up_pullback = not is_breakout_track and flag_days != 3 and (quant_score >= 35) and (vol_ratio <= 50) and (is_today_yangbong or today_body_ratio <= 0.015)
                
                elif market_cap >= 5000:
                    is_ss_breakout = (trading_value >= 80_000_000_000) and (vol_ratio >= 150) and (change_rate >= 0.06) and not is_long_shadow and is_near_high
                    is_runner_up_breakout = not is_ss_breakout and is_breakout_track and (quant_score >= 40) and (trading_value >= 40_000_000_000) and (change_rate >= 0.04) and not is_long_shadow
                    is_runner_up_pullback = not is_breakout_track and flag_days != 3 and (quant_score >= 40) and (vol_ratio <= 40) and (is_today_yangbong or today_body_ratio <= 0.015)
                
                else:
                    is_ss_breakout = (trading_value >= 50_000_000_000) and (vol_ratio >= 200) and (change_rate >= 0.08) and not is_long_shadow and is_near_high
                    is_runner_up_breakout = not is_ss_breakout and is_breakout_track and (quant_score >= 45) and (trading_value >= 30_000_000_000) and (change_rate >= 0.05) and not is_long_shadow
                    is_runner_up_pullback = not is_breakout_track and flag_days != 3 and (quant_score >= 45) and (vol_ratio <= 35) and (is_today_yangbong or today_body_ratio <= 0.01)

                master_tajeom = "⏸️ 관망 및 대기"
                
                if len(history) < 20: master_tajeom = "⚠️ 신규상장 (데이터 부족)"
                elif is_junk: master_tajeom = "🚨 매매금지 (딱지)"
                elif is_financial_risk: master_tajeom = "🚨 매매금지 (자본잠식)"
                
                elif is_theme_daejang_sang:
                    master_tajeom = "👑 [테마대장] 상한가 안착" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 50; score_display = f"{quant_score}점 ({track_type})"
                elif is_theme_daejang:
                    master_tajeom = "🚀 [테마대장] 당일 주도주" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 45; score_display = f"{quant_score}점 ({track_type})"
                elif is_theme_hubal_sang:
                    master_tajeom = "🔒 [후발주] 상한가 안착" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 40; score_display = f"{quant_score}점 ({track_type})"
                elif is_theme_hubal:
                    master_tajeom = "🏃 [후발주] 테마 추종" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 35; score_display = f"{quant_score}점 ({track_type})"
                elif is_individual_sang:
                    master_tajeom = "🔒 [개별주] 상한가 안착" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 30; score_display = f"{quant_score}점 ({track_type})"
                elif is_individual_surge:
                    master_tajeom = "🐎 [개별주] 나홀로 상승중" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 25; score_display = f"{quant_score}점 ({track_type})"
                    
                elif is_long_shadow: master_tajeom = "⚠️ 윗꼬리 위험 (매수금지)"
                elif is_huge_gap: master_tajeom = "⚠️ 갭상승 과다 (추격금지)"
                
                elif is_ss_breakout: 
                    master_tajeom = "👑 [핵심] 신고가 돌파 ⚠️(주의장세)" if is_warning_market else "👑 [핵심] 신고가 돌파"
                    quant_score += 20; score_display = f"{quant_score}점 ({track_type})"
                elif flag_days == 3:
                    master_tajeom = "🎯 [타점] 눌림목 3일 차 완성 (비중 40%)" + (" ⚠️(주의장세)" if is_warning_market else "")
                    quant_score += 10; score_display = f"{quant_score}점 ({track_type})"

                elif is_runner_up_breakout:
                    master_tajeom = "👀 [관심] 돌파 턱밑 대기 (아차상)"
                    quant_score += 5; score_display = f"{quant_score}점 ({track_type})"
                elif is_runner_up_pullback:
                    master_tajeom = "👀 [관심] 눌림목 방어 테스트 (아차상)"
                    quant_score += 5; score_display = f"{quant_score}점 ({track_type})"

                elif flag_days == 2:
                    master_tajeom = "🚩 [분할매수] 눌림목 2일 차 (비중 30%)" + (" ⚠️(주의장세)" if is_warning_market else "")
                elif flag_days == 1:
                    master_tajeom = "🚩 [분할매수] 단기 눌림 진입 (비중 30%)" + (" ⚠️(주의장세)" if is_warning_market else "")
                elif "🌟" in signal: 
                    master_tajeom = "🌟 [우량] 기관/외인 수급 유입" 
                    quant_score += 15; score_display = f"{quant_score}점 ({track_type})"
                elif change_rate >= 0.12 and trading_value >= 50_000_000_000: 
                    master_tajeom = "👀 [관심] 신규 기준봉 출현 (수급 집중)" + (" ⚠️(주의장세)" if is_warning_market else "")

                if is_chronic_loss and "[" in master_tajeom:
                    quant_score -= 10
                    score_display = f"{quant_score}점 ({track_type})"
                    master_tajeom += " ⚠️(3년적자)"

                if is_high_altitude and "[" in master_tajeom:
                    quant_score -= 10
                    score_display = f"{quant_score}점 ({track_type})"
                    master_tajeom += " ⚠️고공권(단기대응)"

                results.append([
                    name, f"'{code}", current_price, f"{change_rate * 100:.2f}%", 
                    int(ma5), int(ma20), f"{int(vol_ratio):,}% 폭발🔥", signal, 
                    score_display, master_tajeom, today_high, today_low, high_60d, 
                    market_cap, shadow_text, dist_text, disp_text, leader_text, vol_status_text, quant_score, my_theme_name
                ])
            except Exception as e:
                continue

        results.sort(key=lambda x: x[19], reverse=True) 
        final_results = [r[:19] + [r[20]] for r in results]

        if final_results:
            try: helper_sheet = doc.worksheet("주가데이터_보조")
            except: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="20")
            helper_sheet.clear()
            headers = ["종목명", "종목코드", "현재가", "등락률", "5일선", "20일선", "거래량비율", "AI신호", "HYEOKS점수", "마스터타점", "오늘 고가", "오늘 저가", "60일 최고가", "시가총액(억)", "윗꼬리판독", "전고점위치", "20일이격도", "대장주이력", "거래량상태", "소속테마"]
            helper_sheet.update(range_name="A1", values=[headers] + final_results, value_input_option="USER_ENTERED")
            print(f"✅ 총 {len(final_results)}개 종목 판독 완료! (테마 복권 완료) 🚀")
            
    except Exception as e:
        print(f"❌ 전체 업데이트 에러: {e}")

if __name__ == "__main__":
    df_theme, is_market_closed, all_theme_map = get_real_money_themes()
    df_news, df_naver, df_main_news = get_news_keywords(), get_naver_search_ranking(), get_naver_main_news()
    update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed)
    update_technical_data(df_theme, all_theme_map)
