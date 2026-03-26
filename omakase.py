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

# 🚀 [속도 최적화] 통신 고속도로 유지
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
    '문의', '사항', '고객', '센터', '안내', '감사', '반대', '선임', '공개', '자본', '공개'
]

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
        for page in range(1, 4):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = session.get(url, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
            subjects = soup.find_all(['dt', 'dd'], {'class': 'articleSubject'})
            for sub in subjects: full_text += sub.get_text(strip=True) + " "
            summaries = soup.find_all('dd', {'class': 'articleSummary'})
            for summary in summaries:
                for span in summary.find_all('span'): span.decompose()
                full_text += summary.get_text(strip=True) + " "
            time.sleep(0.3)
            
        if len(full_text) < 100: return pd.DataFrame()
            
        from kiwipiepy import Kiwi
        kiwi = Kiwi()
        nouns = [token.form for token in kiwi.tokenize(full_text) if token.tag in ['NNG', 'NNP'] and len(token.form) > 1 and token.form not in STOPWORDS]
        top_15 = Counter(nouns).most_common(15)
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        return pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_15, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
    except: return pd.DataFrame()

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
    print("▶️ 실시간 주도 테마 수집 시작...")
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
            stocks = sorted(stocks, key=lambda x: x['value'], reverse=True)[:3]
            if stocks and not (len(stocks) >= 2 and stocks[0]['value'] >= stocks[1]['value'] * 10):
                theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks})
        except: continue
        time.sleep(0.1)
        
    if not theme_data_list: return pd.DataFrame(), is_market_closed
    
    grouped_themes = {}
    for t_data in theme_data_list: grouped_themes.setdefault(t_data['stocks'][0]['code'], []).append(t_data)
        
    merged_themes = []
    for top_code, t_list in grouped_themes.items():
        theme_names = list(dict.fromkeys(t['theme_name'] for t in t_list))
        merged_name = " / ".join(theme_names) + f" (대장: {t_list[0]['stocks'][0]['name']})" if len(theme_names) > 1 else theme_names[0]
        unique_stocks = {s['code']: s for t in t_list for s in t['stocks']}
        merged_stocks = sorted(unique_stocks.values(), key=lambda x: x['value'], reverse=True)[:3]
        merged_themes.append({'theme_name': merged_name, 'theme_sum': sum(s['value'] for s in merged_stocks), 'stocks': merged_stocks})
        
    merged_themes = sorted(merged_themes, key=lambda x: x['theme_sum'], reverse=True)
    final_themes = []
    for m_data in merged_themes:
        if not any(len(set(s['code'] for s in m_data['stocks']).intersection(set(s['code'] for s in f_data['stocks']))) >= 2 for f_data in final_themes):
            final_themes.append(m_data)
        if len(final_themes) >= 10: break
            
    final_rows = [{'날짜': now.strftime('%Y-%m-%d'), **({'시간': time_str} if not is_market_closed else {}), '순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)} for rank, t_data in enumerate(final_themes, 1) for s in t_data['stocks']]
    return pd.DataFrame(final_rows), is_market_closed

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
        print("▶️ 네이버 주요 뉴스 수집 시작...")
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

def update_technical_data(df_theme):
    try:
        print("▶️ 기술적 지표, 수급, 💯스코어링, 🎯타점 판독 엔진 가동...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        doc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)).open_by_url(SHEET_URL)
        
        name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in doc.worksheet("기업정보").get_all_values()[1:] if len(row) >= 3}
        
        target_names = set()
        
        # 💡 [치명적 버그 수정] 대시보드 종목을 놓치지 않고 "무조건" 확보하는 철통 방어 로직!
        try:
            print("▶️ 대시보드 종목을 최우선으로 수집합니다...")
            dash_data = doc.worksheet("대시보드").get_all_values()
            for row in dash_data[4:]: # 5번째 줄(인덱스 4)부터 스캔
                if len(row) > 2:
                    name = str(row[2]).strip() # 대시보드 C열 (과거 대장주 명)
                    if name and name not in ["#REF!", "로딩중...", "데이터대기"]: 
                        target_names.add(name)
            print(f"✅ 대시보드에서 {len(target_names)}개 종목 확보 완료!")
        except Exception as e: 
            print(f"❌ 대시보드 탭 수집 에러: {e}")

        try:
            scan_names = doc.worksheet("스캐너_마스터").col_values(1)[1:]
            for name in scan_names:
                name_clean = str(name).strip()
                if name_clean and name_clean not in ["#REF!", "로딩중...", "데이터대기"]: target_names.add(name_clean)
        except Exception as e: pass
        
        # 대시보드가 다 비어있는 최악의 상황일 때만 실시간 테마를 수혈합니다.
        if not df_theme.empty:
            top_3_themes = df_theme[df_theme['순위'] <= 3]['종목명'].tolist()
            top_10_themes = df_theme[df_theme['순위'] <= 10]['종목명'].tolist()
            if len(target_names) < 5: 
                print("⚠️ 대시보드 종목이 부족하여 오늘 주도테마 종목을 추가합니다.")
                for t in top_10_themes: target_names.add(t)
        else: top_3_themes, top_10_themes = [], []

        print(f"▶️ 최종 분석 대상 종목 수: {len(target_names)}개")

        results = []
        for name in list(target_names):
            try:
                code = name_to_code.get(name) or search_code_from_naver(name)
                if not code: continue
                
                url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
                root = ET.fromstring(session.get(url, verify=False, timeout=3).text)
                
                history = []
                high_prices = []
                for item in root.findall(".//item"):
                    data = item.get("data").split("|")
                    history.append({"close": int(data[4]), "volume": int(data[5])})
                    high_prices.append(int(data[2]))
                    
                if len(history) < 20: continue
                
                # 📊 지표 14개 추출 로직
                today_data = root.findall(".//item")[-1].get("data").split("|")
                today_high = int(today_data[2])
                today_low = int(today_data[3])
                high_60d = max(high_prices)
                market_cap = get_market_cap(code)
                
                risk_soup = BeautifulSoup(session.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                is_junk = bool(risk_soup.find('img', alt=re.compile('관리종목|환기종목|거래정지|투자위험')))
                
                is_dual_buy, f_buy, i_buy, supply_text = False, 0, 0, ""
                try:
                    today_trend = session.get(f"https://m.stock.naver.com/api/stock/{code}/investor/trend", verify=False, timeout=3).json().get('investorTrendList', [{}])[0]
                    f_buy, i_buy = int(str(today_trend.get('foreignerStraightPurchasePrice', '0')).replace(',', '')), int(str(today_trend.get('institutionStraightPurchasePrice', '0')).replace(',', ''))
                    if f_buy > 0 and i_buy > 0: is_dual_buy, supply_text = True, " (쌍끌이🔥)"
                    elif f_buy > 0: supply_text = " (외인매수)"
                    elif i_buy > 0: supply_text = " (기관매수)"
                except: pass

                df_hist = pd.DataFrame(history)
                current_price = int(df_hist['close'].iloc[-1])
                prev_price = int(df_hist['close'].iloc[-2]) if len(df_hist) > 1 else current_price
                change_rate = (current_price - prev_price) / prev_price if prev_price > 0 else 0.0
                
                today_vol = df_hist['volume'].iloc[-1]
                ma5, ma20 = df_hist['close'].tail(5).mean(), df_hist['close'].tail(20).mean()
                std20 = df_hist['close'].tail(20).std(ddof=0) 
                upper_band, lower_band = ma20 + (std20 * 2), ma20 - (std20 * 2) 
                band_width = (upper_band - lower_band) / ma20 if ma20 > 0 else 0 
                
                avg_vol_10 = df_hist['volume'].tail(11).head(10).mean()
                vol_ratio = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
                is_converging = (band_width <= 0.20) or (ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035)
                
                if is_junk: signal = "🚨 [위험] 매매금지 (잡주/경고)"
                elif is_dual_buy and is_converging: signal = "🌟 A급 스윙 (쌍끌이 모아가기)"
                elif band_width <= 0.20 and current_price >= ma20: signal = "🚀 N자파동 (밴드돌파)" + supply_text if current_price >= upper_band * 0.98 else "👀 N자파동 (에너지응축)" + supply_text
                elif ma20 > 0 and abs(ma5 - ma20) / ma20 <= 0.035: signal = "📈 2차랠리 (이평수렴)" + supply_text if current_price > ma20 else "⏳ 이평선 저항" + supply_text
                else: signal = "🟢 낙폭과대 (과매도)" + supply_text if current_price < lower_band else "⚡ 관망 (이격발생)" + supply_text
                    
                score = 0
                if not is_junk:
                    if band_width <= 0.10: score += 20
                    elif band_width <= 0.15: score += 15
                    elif band_width <= 0.20: score += 10
                    if is_dual_buy: score += 25
                    elif f_buy > 0 or i_buy > 0: score += 10
                    if vol_ratio >= 500: score += 20
                    elif vol_ratio >= 300: score += 15
                    elif vol_ratio >= 200: score += 10
                    elif vol_ratio >= 100: score += 5
                    if name in top_3_themes: score += 20
                    elif name in top_10_themes: score += 10
                    if ma5 > ma20 and current_price >= ma5: score += 15
                    elif ma5 > ma20: score += 10
                    elif current_price >= ma20: score += 5
                    
                yest_vol = int(df_hist['volume'].iloc[-2]) if len(df_hist) > 1 else 0
                v_yest_ratio = (yest_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
                
                master_tajeom = "⏸️ 관망 및 대기"
                if is_junk: master_tajeom = "🚨 매매금지"
                elif "🌟" in signal: master_tajeom = "🌟 [VIP] 쌍끌이 모아가기" 
                elif ("👀" in signal or "🚀" in signal) and vol_ratio <= 50 and v_yest_ratio >= 150: master_tajeom = "👑 [SS급] N자 단봉눌림 (종가베팅)"
                elif "🚀" in signal and vol_ratio >= 150: master_tajeom = "🎯 [S급] 2차랠리 돌파 (1차 진입)"
                elif "👀" in signal and vol_ratio <= 60: master_tajeom = "⏳ [A급] 바닥 매집 (종가베팅)"
                elif "🟢" in signal and vol_ratio <= 40: master_tajeom = "📉 [B급] 투매 소화 (종가베팅)"

                results.append([name, f"'{code}", current_price, f"{change_rate * 100:.2f}%", int(ma5), int(ma20), f"{int(vol_ratio):,}% 폭발🔥", signal, score, master_tajeom, today_high, today_low, high_60d, market_cap])
            except Exception as e:
                continue

        results.sort(key=lambda x: x[8], reverse=True) 

        if results:
            try: helper_sheet = doc.worksheet("주가데이터_보조")
            except: helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="150", cols="15")
            helper_sheet.clear()
            headers = ["종목명", "종목코드", "현재가", "등락률", "5일선", "20일선", "거래량비율", "AI신호", "오마카세점수", "마스터타점", "오늘 고가", "오늘 저가", "60일 최고가", "시가총액(억)"]
            helper_sheet.update(range_name="A1", values=[headers] + results, value_input_option="USER_ENTERED")
            print(f"✅ 총 {len(results)}개 종목 타점판독 업데이트 완료!")
    except Exception as e: print(f"❌ 기술적 지표 전체 업데이트 에러: {e}")

if __name__ == "__main__":
    print("🤖 1. 데이터 수집 시작...")
    df_theme, is_market_closed = get_real_money_themes()
    df_news, df_naver, df_main_news = get_news_keywords(), get_naver_search_ranking(), get_naver_main_news()
    
    print("🤖 2. 구글 시트로 전송 시작...")
    update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed)
    
    print("🤖 3. 주가 보조데이터 및 스코어링 계산 시작...")
    update_technical_data(df_theme) 
    print("✅ 모든 작업이 완료되었습니다!")
