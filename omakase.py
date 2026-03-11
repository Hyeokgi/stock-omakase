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

# 🗑️ [업그레이드 1] 강력해진 쓰레기통 (매크로, 일반 경제 단어 싹 다 차단!)
# 🗑️ [업그레이드 완결판] 초강력 쓰레기통 (기자들의 습관성 단어 완벽 차단!)
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
             '주의', '변동', '킬러', '페이지', '주소', '입력', '방문', '삭제', '요청', '정확', '확인', '문의',
             '사항', '고객', '센터', '안내', '감사', '테마주']

# 🎯 [업그레이드 4] 네이버 로봇 방어막 완벽 우회 + 에러 페이지 원천 차단
def get_news_keywords():
    try:
        print("▶️ 뉴스 키워드 수집 시작...")
        list_url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        
        # 🛡️ [핵심 마법] 일반 사람의 최신 크롬 브라우저인 것처럼 완벽 위장!
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://finance.naver.com/news/'
        }
        
        res = requests.get(list_url, headers=headers, verify=False)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        
        article_links = []
        subjects = soup.find_all(['dt', 'dd'], {'class': 'articleSubject'})
        for sub in subjects:
            a_tag = sub.find('a', href=True)
            if a_tag:
                href = a_tag['href']
                if href.startswith('/'):
                    full_link = "https://finance.naver.com" + href
                elif href.startswith('http'):
                    full_link = href
                else:
                    full_link = "https://finance.naver.com/news/" + href
                
                if full_link not in article_links:
                    article_links.append(full_link)
                    
        article_links = article_links[:15]
        print(f"▶️ 찾은 뉴스 기사 개수: {len(article_links)}개")
        
        if not article_links:
            print("❌ 뉴스 링크를 찾지 못했습니다!")
            return pd.DataFrame()

        full_text = ""
        for link in article_links:
            try:
                # 개별 기사에 들어갈 때도 위장 신분증 제시!
                a_res = requests.get(link, headers=headers, verify=False)
                
                if 'finance.naver.com' in a_res.url:
                    a_soup = BeautifulSoup(a_res.content, 'html.parser', from_encoding='cp949')
                else:
                    a_soup = BeautifulSoup(a_res.content, 'html.parser', from_encoding='utf-8')
                
                # 🚨 [에러 페이지 방어 로직] 네이버가 차단 페이지를 주면 가차 없이 패스!
                if "요청하신 페이지를 찾을 수 없습니다" in a_soup.text or "주소가 잘못 입력되었거나" in a_soup.text:
                    continue

                article_body = a_soup.select_one('#dic_area, #newsct_article, #content, .articleCont, #articleBodyContents')
                
                if article_body:
                    full_text += article_body.get_text(separator=' ', strip=True) + " "
                else:
                    for p in a_soup.find_all('p'):
                        full_text += p.get_text(strip=True) + " "
                        
            except Exception as e:
                continue
            time.sleep(0.5) # 사람이 읽는 것처럼 조금 더 여유롭게 0.5초 휴식
            
        print(f"▶️ 전체 뉴스 텍스트 길이: {len(full_text)}자")
        
        if len(full_text) < 100:
            print("❌ 기사 본문을 충분히 가져오지 못했습니다. 네이버 차단 의심.")
            return pd.DataFrame()
            
        # 🧠 형태소 분석기 가동
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

def get_naver_search_ranking():
    try:
        url = "https://finance.naver.com/sise/lastsearch2.naver"
        headers = {'User-Agent': 'Mozilla/5.0'}
        # 튼튼한 BeautifulSoup 엔진으로 직접 추출!
        res = requests.get(url, headers=headers, verify=False)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        
        table = soup.find('table', {'class': 'type_5'})
        rows = table.find_all('tr')
        
        data = []
        for row in rows:
            tds = row.find_all('td')
            # 정상적인 데이터가 있는 줄인지 확인 (빈 줄 패스)
            if len(tds) >= 6:
                rank_text = tds[0].text.strip()
                if rank_text.isdigit(): # 순위가 숫자인 진짜 데이터만 추출
                    name = tds[1].text.strip()
                    price = tds[3].text.strip()
                    rate = tds[5].text.strip()
                    
                    data.append([int(rank_text), name, price, rate])
                    
                    if len(data) >= 10: # 딱 10위까지만 수집하고 멈춤
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

def get_real_money_themes():
    now = datetime.datetime.now(KST)
    is_market_closed = now.hour < 9 or now.hour > 15 or (now.hour == 15 and now.minute >= 40)
    is_weekend = now.weekday() >= 5
    
    # 🛑 [24시간 분리 엔진] 주말이거나, 오후 4시 ~ 다음날 아침 9시 이전이면 테마는 수집 종료!
    if is_weekend or now.hour >= 16 or now.hour < 9:
        return pd.DataFrame(), True
        
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
                # 🚨 [착시 테마 제거 엔진] 1위가 2위보다 거래대금이 10배 이상 크면 가짜 테마로 간주!
                if len(stocks) >= 2 and stocks[0]['value'] >= (stocks[1]['value'] * 10):
                    print(f"🚫 착시 테마 제외: {theme['name']} (1위 {stocks[0]['name']} 독주로 인한 왜곡)")
                    continue  # 바구니에 담지 않고 바로 다음 테마로 가차 없이 패스!
                    
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
