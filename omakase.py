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
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit?gid=588079479#gid=588079479"
TARGET_PERCENT = 5.0
KST = datetime.timezone(datetime.timedelta(hours=9))
# ==========================================

# 🗑️ 회원님의 훌륭한 추가 단어들이 포함된 쓰레기통
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

def get_news_keywords():
    try:
        print("▶️ 뉴스 키워드 수집 시작 (스텔스 모드)...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        full_text = ""
        for page in range(1, 4):
            url = f"https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258&page={page}"
            res = requests.get(url, headers=headers, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
            
            subjects = soup.find_all(['dt', 'dd'], {'class': 'articleSubject'})
            for sub in subjects:
                full_text += sub.get_text(strip=True) + " "
                
            summaries = soup.find_all('dd', {'class': 'articleSummary'})
            for summary in summaries:
                for span in summary.find_all('span'):
                    span.decompose()
                full_text += summary.get_text(strip=True) + " "
            time.sleep(0.5)
            
        if len(full_text) < 100:
            return pd.DataFrame()
            
        from kiwipiepy import Kiwi
        kiwi = Kiwi()
        nouns = []
        for token in kiwi.tokenize(full_text):
            if token.tag in ['NNG', 'NNP'] and len(token.form) > 1 and token.form not in STOPWORDS:
                nouns.append(token.form)
                
        top_15 = Counter(nouns).most_common(15)
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        df = pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_15, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
        return df
    except Exception as e:
        print(f"❌ 뉴스 키워드 추출 에러: {e}")
        return pd.DataFrame()

# 🛡️ 1,000억 필터링 방패 + 캐싱(기억력) 마법 장착!
market_cap_cache = {} 

def get_market_cap(code):
    if code in market_cap_cache:
        return market_cap_cache[code]
        
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        res = requests.get(url, headers=headers, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        market_sum_tag = soup.find('em', id='_market_sum')
        if not market_sum_tag: 
            market_cap_cache[code] = 999999
            return 999999 
            
        market_sum_str = market_sum_tag.text.replace(',', '').replace('\t', '').replace('\n', '').strip()
        if '조' in market_sum_str:
            parts = market_sum_str.split('조')
            jo = int(parts[0].strip())
            eok = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 0
            final_cap = jo * 10000 + eok
        else:
            final_cap = int(market_sum_str.strip())
            
        market_cap_cache[code] = final_cap
        return final_cap
        
    except Exception as e:
        return 999999 

def get_real_money_themes():
    now = datetime.datetime.now(KST)
    is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
    is_weekend = now.weekday() >= 5
    
    if is_weekend or now.hour >= 20 or now.hour < 7:
        return pd.DataFrame(), True
        
    time_str = now.strftime('%H:%M')
    headers = {'User-Agent': 'Mozilla/5.0'}
    base_url = "https://finance.naver.com"
    res = requests.get(base_url + "/sise/theme.naver", headers=headers, verify=False)
    soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
    
    themes = [{'name': a.text.strip(), 'url': base_url + a['href']} for tds in [tr.find_all('td') for tr in soup.find('table', {'class': 'type_1'}).find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a][:20]
                    
    theme_data_list = []
    print("▶️ 실시간 주도 테마 및 튼튼한 대장주 수집 시작 (터보 모드 + 대장주 그룹핑)...")
    
    for theme in themes:
        try:
            res = requests.get(theme['url'], headers=headers, verify=False)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
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
                        
                        if rate_num >= TARGET_PERCENT and val_num > 0:
                            actual_code = s_code.replace("'", "")
                            market_cap = get_market_cap(actual_code)
                            if market_cap >= 1000:
                                stocks.append({'name': s_name, 'code': s_code, 'rate': rate_num, 'value': val_num})
                    except: 
                        continue
            
            stocks = sorted(stocks, key=lambda x: x['value'], reverse=True)[:3]
            if stocks:
                if len(stocks) >= 2 and stocks[0]['value'] >= (stocks[1]['value'] * 10):
                    continue 
                theme_data_list.append({'theme_name': theme['name'], 'stocks': stocks})
        except: 
            continue
        time.sleep(0.3)
        
    if not theme_data_list: return pd.DataFrame(), is_market_closed
    
    grouped_themes = {}
    for t_data in theme_data_list:
        top_code = t_data['stocks'][0]['code'] 
        if top_code not in grouped_themes:
            grouped_themes[top_code] = []
        grouped_themes[top_code].append(t_data)
        
    merged_themes = []
    for top_code, t_list in grouped_themes.items():
        theme_names = []
        for t in t_list:
            if t['theme_name'] not in theme_names:
                theme_names.append(t['theme_name'])
                
        top_stock_name = t_list[0]['stocks'][0]['name']
        
        if len(theme_names) > 1:
            merged_name = " / ".join(theme_names) + f" (대장: {top_stock_name})"
        else:
            merged_name = theme_names[0]
            
        unique_stocks = {}
        for t in t_list:
            for s in t['stocks']:
                unique_stocks[s['code']] = s
                
        merged_stocks = sorted(unique_stocks.values(), key=lambda x: x['value'], reverse=True)[:3]
        merged_sum = sum(s['value'] for s in merged_stocks)
        
        merged_themes.append({
            'theme_name': merged_name,
            'theme_sum': merged_sum,
            'stocks': merged_stocks
        })
        
    merged_themes = sorted(merged_themes, key=lambda x: x['theme_sum'], reverse=True)
    final_themes = []
    for m_data in merged_themes:
        current_codes = set([s['code'] for s in m_data['stocks']])
        is_duplicate = False
        for f_data in final_themes:
            f_codes = set([s['code'] for s in f_data['stocks']])
            if len(current_codes.intersection(f_codes)) >= 2:
                is_duplicate = True
                break
        if not is_duplicate:
            final_themes.append(m_data)
        if len(final_themes) >= 10:
            break
            
    final_rows = []
    for rank, t_data in enumerate(final_themes, start=1):
        for s in t_data['stocks']:
            row_data = {'날짜': now.strftime('%Y-%m-%d')}
            if not is_market_closed: row_data['시간'] = time_str
            row_data.update({'순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)})
            final_rows.append(row_data)
            
    return pd.DataFrame(final_rows), is_market_closed

def get_naver_search_ranking():
    try:
        url = "https://finance.naver.com/sise/lastsearch2.naver"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        
        table = soup.find('table', {'class': 'type_5'})
        rows = table.find_all('tr')
        data = []
        
        search_blacklist = ['삼성전자', 'SK하이닉스', '현대차', '기아', 'LG에너지솔루션', 'POSCO홀딩스', '셀트리온', 'NAVER', '카카오']
        
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 6:
                rank_text = tds[0].text.strip()
                if rank_text.isdigit(): 
                    a_tag = tds[1].find('a')
                    name = a_tag.text.strip()
                    
                    if name in search_blacklist:
                        continue
                        
                    s_code = a_tag['href'].split('code=')[-1] 
                    price = tds[3].text.strip()
                    rate = tds[5].text.strip()
                    market_cap = get_market_cap(s_code)
                    
                    if market_cap >= 1000:
                        # 💡 종목코드를 6자리로 규격화해서 데이터에 포함시킵니다!
                        formatted_code = f"{s_code:0>6}" 
                        data.append([len(data) + 1, name, price, rate, formatted_code]) 
                    else:
                        pass
                        
                    if len(data) >= 10: 
                        break
                        
        # 💡 컬럼에 '종목코드'를 추가했습니다!
        df = pd.DataFrame(data, columns=['순위', '종목명', '현재가', '등락률(%)', '종목코드'])
        return df
    except Exception as e:
        print(f"❌ 네이버 실시간 검색어 수집 실패: {e}")
        return pd.DataFrame()
def get_naver_main_news():
    try:
        print("▶️ 네이버 주요 뉴스 수집 시작...")
        url = "https://finance.naver.com/news/mainnews.naver"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        res = requests.get(url, headers=headers, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        news_list = []
        
        # 기사 제목이 있는 dt 또는 dd 태그 찾기
        subjects = soup.find_all(['dt', 'dd'], {'class': 'articleSubject'})
        
        for sub in subjects:
            a_tag = sub.find('a')
            if not a_tag: continue
            
            title = a_tag.text.strip()
            link = "https://finance.naver.com" + a_tag['href']
            
            # 요약 내용은 형제 노드에 있음
            summary_tag = sub.find_next_sibling('dd', {'class': 'articleSummary'})
            
            press = "언론사"
            summary = ""
            if summary_tag:
                press_tag = summary_tag.find('span', {'class': 'press'})
                if press_tag: press = press_tag.text.strip()
                
                # span 태그들(언론사, 날짜 등)을 제거하고 순수 텍스트만 남김
                for span in summary_tag.find_all('span'):
                    span.decompose()
                summary = summary_tag.text.strip()
                
            now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
            news_list.append([now_str, press, title, summary, link])
            
            if len(news_list) >= 20: # 주요뉴스 20개 추출
                break
                
        df = pd.DataFrame(news_list, columns=['업데이트 시간', '언론사', '기사 제목', '요약 내용', '기사 링크'])
        return df
    except Exception as e:
        print(f"❌ 네이버 주요 뉴스 수집 에러: {e}")
        return pd.DataFrame()

def update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        if not df_theme.empty:
            if not is_market_closed:
                sheet = doc.worksheet("수급_실시간")
                sheet.clear() 
                sheet.update("A1", [df_theme.columns.values.tolist()] + df_theme.values.tolist(), value_input_option="USER_ENTERED")
            else:
                sheet = doc.worksheet("수급_Raw")
                today_str = df_theme.iloc[0]['날짜'] 
                all_data = sheet.get_all_values()
                
                headers = all_data[0] if len(all_data) > 0 else df_theme.columns.values.tolist()
                past_data = [row for row in all_data[1:] if len(row) > 0 and row[0] != today_str]
                new_data = df_theme.values.tolist()
                
                combined_data = new_data + past_data
                combined_data.sort(key=lambda x: int(x[1]) if str(x[1]).isdigit() else 999)
                combined_data.sort(key=lambda x: x[0], reverse=True)
                
                sheet.clear()
                sheet.update("A1", [headers] + combined_data, value_input_option="USER_ENTERED")
                
        if not df_news.empty:
            sheet_news = doc.worksheet("뉴스_키워드")
            sheet_news.clear()
            sheet_news.update("A1", [df_news.columns.values.tolist()] + df_news.values.tolist(), value_input_option="USER_ENTERED")

        if not df_naver.empty:
            sheet_naver = doc.worksheet("네이버_검색상위")
            sheet_naver.clear()
            sheet_naver.update("A1", [df_naver.columns.values.tolist()] + df_naver.values.tolist(), value_input_option="USER_ENTERED")  

        if not df_main_news.empty:
            sheet_main_news = doc.worksheet("네이버_주요뉴스")
            sheet_main_news.clear()
            sheet_main_news.update("A1", [df_main_news.columns.values.tolist()] + df_main_news.values.tolist(), value_input_option="USER_ENTERED")
          
    except Exception as e:
        print(f"❌ Error: {e}")

# 💡 새롭게 추가된 기술적 지표 초고속 수집 엔진
def update_technical_data():
    try:
        print("▶️ 기술적 지표 (5일선/20일선/거래량) 파이썬 엔진 가동...")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        # 1. 기업정보 탭에서 종목코드 매핑
        info_sheet = doc.worksheet("기업정보")
        info_data = info_sheet.get_all_values()
        name_to_code = {row[0]: str(row[2]).zfill(6) for row in info_data[1:] if len(row) >= 3 and row[0] and row[2]}
        
        # 2. 스캐너와 대시보드에 있는 종목 이름만 쏙쏙 골라내기
        scanner_names = [row[0] for row in doc.worksheet("스캐너_마스터").col_values(1)[1:] if row[0]]
        dash_names = [row[2] for row in doc.worksheet("대시보드").get_all_values()[49:] if len(row) > 2 and row[2]]
        
        target_names = list(set(scanner_names + dash_names))
        results = []
        
        for name in target_names:
            code = name_to_code.get(name)
            if not code: continue
            
            # 네이버 차트 API에서 20일치 과거 데이터 수집
            url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=25&requestType=0"
            res = requests.get(url, verify=False, timeout=3)
            root = ET.fromstring(res.text)
            
            history = []
            for item in root.findall(".//item"):
                data = item.get("data").split("|") # 날짜, 시가, 고가, 저가, 종가, 거래량
                history.append({"close": int(data[4]), "volume": int(data[5])})
                
            if len(history) < 20: continue
                
            df_hist = pd.DataFrame(history)
            current_price = df_hist['close'].iloc[-1]
            today_vol = df_hist['volume'].iloc[-1]
            
           # 이동평균선 계산
        ma5 = df_hist['close'].tail(5).mean()
        ma20 = df_hist['close'].tail(20).mean()
        
        # ✨ [핵심 무기 장착] 볼린저 밴드 (20일 표준편차) 계산
        std20 = df_hist['close'].tail(20).std(ddof=0) 
        upper_band = ma20 + (std20 * 2) # 상한선
        lower_band = ma20 - (std20 * 2) # 하한선
        band_width = (upper_band - lower_band) / ma20 # 밴드폭 (에너지 응축 정도)
        
        # 전일 기준 과거 10일 평균 거래량
        avg_vol_10 = df_hist['volume'].tail(11).head(10).mean()
        vol_ratio = (today_vol / avg_vol_10) * 100 if avg_vol_10 > 0 else 0
        
        # 🎯 한층 더 정교해진 AI 턴어라운드 신호 판독 로직
        # 1. 에너지가 꽉 응축된 상태(밴드폭 20% 이내)에서 20일선 위로 고개를 드는 진짜 N자 파동
        if band_width <= 0.20 and current_price >= ma20:
            if current_price >= upper_band * 0.98: # 볼린저 밴드 상단 돌파 직전이거나 뚫었을 때
                signal = "🚀 N자파동 (밴드돌파)"
            else:
                signal = "👀 N자파동 (에너지응축)"
        
        # 2. 밴드폭이 넓은 일반적인 이평선 기준 판독
        elif abs(ma5 - ma20) / ma20 <= 0.035:
            signal = "📈 2차랠리 (이평수렴)" if current_price > ma20 else "⏳ 이평선 저항"
            
        # 3. 과매도 및 이격 과다 (단타용)
        else:
            signal = "🟢 낙폭과대 (과매도)" if current_price < lower_band else "⚡ 관망 (이격발생)"
            
        results.append([name, code, int(ma5), int(ma20), f"{int(vol_ratio):,}% 폭발🔥", signal])

        # 4. 주가데이터_보조 탭에 결과 덮어쓰기
        if results:
            try:
                helper_sheet = doc.worksheet("주가데이터_보조")
            except:
                helper_sheet = doc.add_worksheet(title="주가데이터_보조", rows="100", cols="20")
                
            helper_sheet.clear()
            headers = ["종목명", "종목코드", "5일선", "20일선", "거래량비율", "AI신호"]
            helper_sheet.update("A1", [headers] + results, value_input_option="USER_ENTERED")
            print(f"✅ 총 {len(results)}개 종목 기술적 지표 업데이트 완료! (로딩 딜레이 0%)")
            
    except Exception as e:
        print(f"❌ 기술적 지표 업데이트 에러: {e}")

# ==========================================
# 🚀 심장(Main) 엔진
# ==========================================
if __name__ == "__main__":
    print("🤖 1. 데이터 수집 시작...")
    df_theme, is_market_closed = get_real_money_themes()
    df_news = get_news_keywords()
    df_naver = get_naver_search_ranking()
    
    # 💡 방금 만든 주요 뉴스 함수를 여기서 실행합니다!
    df_main_news = get_naver_main_news()
    
    print("🤖 2. 구글 시트로 전송 시작...")
    # 💡 df_main_news 도 구글 시트로 쏴주도록 포함시켰습니다.
    update_google_sheet(df_theme, df_news, df_naver, df_main_news, is_market_closed)
    
    print("🤖 3. 주가 보조데이터(이평선) 계산 시작...")
    update_technical_data()
    
    print("✅ 모든 작업이 완료되었습니다!")
