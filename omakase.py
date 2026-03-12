import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib3
import datetime
import gspread
import re
from collections import Counter
from oauth2client.service_account import ServiceAccountCredentials

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit?gid=588079479#gid=588079479"
TARGET_PERCENT = 5.0
KST = datetime.timezone(datetime.timedelta(hours=9))
# ==========================================

# 🗑️ 회원님의 훌륭한 추가 단어들이 포함된 쓰레기통
STOPWORDS = ['코스피', '코스닥', '증시', '상승', '하락', '마감', '특징주', '강세', '약세', '급등', '급락',
             '주식', '투자', '종목', '외인', '기관', '개인', '매수', '매도', '순매수', '순매도', '전망',
             '수혜', '주가', '대비', '돌파', '우려', '기대', '연속', '최고', '최저', '대형주', '중소형주',
             '시장', '지수', '오늘', '내일', '이번', '주간', '월간', '분기', '실적', '발표', '목표가', '상향',
             '경고', '정부', '자산', '머니', '폭락', '변수', '게임', '한국', '미국', '국내', '외국인', '글로벌',
             '경제', '금융', '기업', '회장', '대표', '임원', '주주', '총회', '속보', '단독', '이유', '때문',
             '대금', '거래', '신고가', '신저가', '시간', '하루', '하루만', '올해', '내년', '만원', '천원',
             '조원', '억원', '달러', '금리', '인상', '인하', '동결', '연준', '파월', '물가', '지표', '고용',
             '북새통', '최대', '안전', '사모', '상장', '신용', '펀드', '기름값', '주유소', '뉴욕', '사상', 
             '역대', '최초', '최신', '규모', '기준', '확대', '축소', '대규모', '체결', '변경', '취소', 
             '결정', '검토', '참여', '진출', '포기', '중단', '재개', '완료', '시작', '종료', '영향', 
             '타격', '피해', '직격탄', '최고치', '최저치', '급증', '급감', '확산', '진정', '완화', '악화', 
             '개선', '회복', '부양', '지원', '규제', '단속', '강화', '철폐', '폐지', '유지', '보류', 
             '반등', '조정', '랠리', '위축', '냉각', '훈풍', '안도', '불안', '쇼크', '서프라이즈', '달성', 
             '수익', '매출', '적자', '흑자', '배당', '지분', '인수', '합병', '분할', '상폐', '공모', 
             '특징', '전일', '전주', '전월', '동기', '경신', '증권', '증권사', '개장', '출발', '사태', 
             '수준', '예상', '반사이익', '사업', '추진', '공급', '관련', '관련주', '테마', '장세', '박살', 
             '주의', '변동', '목표', '분석', '이익', '지난해', '전문', '킬러', '초반', '운용', '자사', '오전', 
             '성장', '이날', '밸류', '공시', '병합', '현금', '계약', '센터', '괴리', '프리미어', '가격', 
             '기내식', '서비스', '테마주']

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
            
        print(f"▶️ 수집된 전체 텍스트 길이: {len(full_text)}자")
        if len(full_text) < 100:
            print("❌ 텍스트 수집 실패!")
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
        print(f"▶️ 키워드 추출 성공! (1위: {top_15[0][0]}, 언급횟수: {top_15[0][1]}회)")
        return df
    except Exception as e:
        print(f"❌ 뉴스 키워드 추출 에러: {e}")
        return pd.DataFrame()

# 🛡️ 1,000억 필터링 방패 + 캐싱(기억력) 마법 장착!
market_cap_cache = {} # 한 번 검색한 종목 시총을 기억하는 메모장!

# 🛡️ 1,000억 필터링 방패 + 캐싱(기억력) 마법 장착!
market_cap_cache = {} # 한 번 검색한 종목 시총을 기억하는 메모장!

def get_market_cap(code):
    # 메모장에 이미 있는 종목이면 0.001초 만에 바로 대답!
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
            
        # 새로 알게 된 시총을 메모장에 기록!
        market_cap_cache[code] = final_cap
        return final_cap
        
    except Exception as e:
        return 999999 

def get_real_money_themes():
    now = datetime.datetime.now(KST)
    is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
    is_weekend = now.weekday() >= 5
    
    if is_weekend or now.hour >= 16 or now.hour < 9:
        return pd.DataFrame(), True
        
    time_str = now.strftime('%H:%M')
    headers = {'User-Agent': 'Mozilla/5.0'}
    base_url = "https://finance.naver.com"
    res = requests.get(base_url + "/sise/theme.naver", headers=headers, verify=False)
    soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
    
    # ⚡ [터보 엔진] 테마 수집 대상을 30개에서 20개로 압축! (시간 1/3 단축)
    themes = [{'name': a.text.strip(), 'url': base_url + a['href']} for tds in [tr.find_all('td') for tr in soup.find('table', {'class': 'type_1'}).find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a][:20]
                    
    theme_data_list = []
    print("▶️ 실시간 주도 테마 및 튼튼한 대장주 수집 시작 (터보 모드)...")
    
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
                            else:
                                pass # 터보 모드에서는 잡주 로그 생략으로 속도 향상
                    except: 
                        continue
            
            stocks = sorted(stocks, key=lambda x: x['value'], reverse=True)[:3]
            if stocks:
                if len(stocks) >= 2 and stocks[0]['value'] >= (stocks[1]['value'] * 10):
                    continue 
                theme_data_list.append({'theme_name': theme['name'], 'theme_sum': sum([s['value'] for s in stocks]), 'stocks': stocks})
        except: 
            continue
        # 쉬는 시간도 0.5초에서 0.3초로 약간 단축
        time.sleep(0.3)
        
    if not theme_data_list: return pd.DataFrame(), is_market_closed
    
    theme_data_list = sorted(theme_data_list, key=lambda x: x['theme_sum'], reverse=True)
    filtered_themes = []
    for t_data in theme_data_list:
        current_codes = set([s['code'] for s in t_data['stocks']])
        is_duplicate = False
        for f_data in filtered_themes:
            f_codes = set([s['code'] for s in f_data['stocks']])
            if len(current_codes.intersection(f_codes)) >= 2:
                is_duplicate = True
                break
        if not is_duplicate:
            filtered_themes.append(t_data)
        if len(filtered_themes) >= 10:
            break
            
    final_rows = []
    for rank, t_data in enumerate(filtered_themes, start=1):
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
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 6:
                rank_text = tds[0].text.strip()
                if rank_text.isdigit(): 
                    a_tag = tds[1].find('a')
                    name = a_tag.text.strip()
                    s_code = a_tag['href'].split('code=')[-1] 
                    price = tds[3].text.strip()
                    rate = tds[5].text.strip()
                    
                    market_cap = get_market_cap(s_code)
                    if market_cap >= 1000:
                        data.append([int(rank_text), name, price, rate])
                    else:
                        print(f"   🗑️ 검색어 잡주 차단: {name} (시총 {market_cap}억)")
                        
                    if len(data) >= 10: 
                        break
                        
        df = pd.DataFrame(data, columns=['순위', '종목명', '현재가', '등락률(%)'])
        return df
    except Exception as e:
        print(f"❌ 네이버 실시간 검색어 수집 실패: {e}")
        return pd.DataFrame()

def update_google_sheet(df_theme, df_news, df_naver, is_market_closed):
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
          
    except Exception as e:
        print(f"❌ Error: {e}")

# ==========================================
# 🚀 심장(Main) 엔진: 로봇이 깨어나서 해야 할 일들
# ==========================================
if __name__ == "__main__":
    print("🤖 1. 데이터 수집 시작...")
    df_theme, is_market_closed = get_real_money_themes()
    df_news = get_news_keywords()
    df_naver = get_naver_search_ranking()
    
    print("🤖 2. 구글 시트로 전송 시작...")
    update_google_sheet(df_theme, df_news, df_naver, is_market_closed)
    
    print("✅ 모든 작업이 완료되었습니다!")
