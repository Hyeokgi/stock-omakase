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

STOPWORDS = ['코스피', '코스닥', '증시', '상승', '하락', '마감', '특징주', '강세', '약세', '급등', '급락',
             '주식', '투자', '종목', '외인', '기관', '개인', '매수', '매도', '순매수', '순매도', '전망',
             '수혜', '주가', '대비', '돌파', '우려', '기대', '연속', '최고', '최저', '대형주', '중소형주',
             '시장', '지수', '오늘', '내일', '이번', '주간', '월간', '분기', '실적', '발표', '목표가', '상향']

def get_news_keywords():
    url = "https://finance.naver.com/news/mainnews.naver"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        titles = soup.find_all(['dt', 'dd'], {'class': 'articleSubject'})
        words = re.findall(r'[가-힣]+', " ".join([t.text.strip() for t in titles]))
        cleaned_words = [w for w in words if len(w) > 1 and w not in STOPWORDS]
        top_10 = Counter(cleaned_words).most_common(10)
        now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
        return pd.DataFrame([[now_str, rank, word, count] for rank, (word, count) in enumerate(top_10, 1)], columns=['업데이트시간', '순위', '키워드', '언급횟수'])
    except Exception as e:
        return pd.DataFrame()

def update_google_sheet(df_theme, df_news, is_market_closed):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        if not df_theme.empty:
            if not is_market_closed:
                sheet = doc.worksheet("수급_실시간")
                sheet.clear() 
                sheet.update("A1", [df_theme.columns.values.tolist()] + df_theme.values.tolist())
            else:
                sheet = doc.worksheet("수급_Raw")
                today_str = df_theme.iloc[0]['날짜'] 
                all_data = sheet.get_all_values()
                filtered_data = [row for row in all_data if len(row) > 0 and row[0] != today_str]
                filtered_data.extend(df_theme.values.tolist())
                sheet.clear()
                sheet.update("A1", filtered_data)
                
        if not df_news.empty:
            sheet_news = doc.worksheet("뉴스_키워드")
            sheet_news.clear()
            sheet_news.update("A1", [df_news.columns.values.tolist()] + df_news.values.tolist())
            
    except Exception as e:
        print(f"❌ Error: {e}")

def get_real_money_themes():
    now = datetime.datetime.now(KST)
    is_market_closed = now.hour > 15 or (now.hour == 15 and now.minute >= 40)
    time_str = now.strftime('%H:%M')
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    base_url = "https://finance.naver.com"
    res = requests.get(base_url + "/sise/theme.naver", headers=headers, verify=False)
    soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
    
    themes = [{'name': a.text.strip(), 'url': base_url + a['href']} for tds in [tr.find_all('td') for tr in soup.find('table', {'class': 'type_1'}).find_all('tr')] if len(tds) > 1 for a in [tds[0].find('a')] if a][:30]
                    
    theme_data_list = []
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
                            stocks.append({'name': s_name, 'code': s_code, 'rate': rate_num, 'value': val_num})
                    except: continue
            stocks = sorted(stocks, key=lambda x: x['value'], reverse=True)[:3]
            if stocks:
                theme_data_list.append({'theme_name': theme['name'], 'theme_sum': sum([s['value'] for s in stocks]), 'stocks': stocks})
        except: continue
        time.sleep(0.5) 
        
    if not theme_data_list: return pd.DataFrame(), is_market_closed
    
    # 🌟 [핵심 업데이트] 테마 중복 필터링 로직 🌟
    theme_data_list = sorted(theme_data_list, key=lambda x: x['theme_sum'], reverse=True)
    filtered_themes = []
    
    for t_data in theme_data_list:
        current_codes = set([s['code'] for s in t_data['stocks']])
        is_duplicate = False
        
        # 이미 랭킹에 올라간 테마들과 대장주 구성 비교
        for f_data in filtered_themes:
            f_codes = set([s['code'] for s in f_data['stocks']])
            # 겹치는 종목이 2개 이상이면 사실상 같은 테마로 간주하고 과감히 버림!
            if len(current_codes.intersection(f_codes)) >= 2:
                is_duplicate = True
                break
                
        if not is_duplicate:
            filtered_themes.append(t_data)
            
        if len(filtered_themes) >= 10: # 다양하게 10개가 채워지면 멈춤
            break
            
    final_rows = []
    for rank, t_data in enumerate(filtered_themes, start=1):
        for s in t_data['stocks']:
            row_data = {'날짜': now.strftime('%Y-%m-%d')}
            if not is_market_closed: row_data['시간'] = time_str
            row_data.update({'순위': rank, '테마명': t_data['theme_name'], '종목명': s['name'], '종목코드': s['code'], '등락률(%)': s['rate'], '거래대금(억원)': int(s['value']/100)})
            final_rows.append(row_data)
            
    return pd.DataFrame(final_rows), is_market_closed

df_themes, is_closed = get_real_money_themes()
df_news = get_news_keywords()
if not df_themes.empty or not df_news.empty:
    update_google_sheet(df_themes, df_news, is_closed)
