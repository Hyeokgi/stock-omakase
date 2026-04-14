import os, requests, datetime, time, json
from bs4 import BeautifulSoup
from google import genai
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))

def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get("https://finance.naver.com/news/mainnews.naver", headers=headers, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_items = []
        for dl in soup.find_all('dl'):
            subject = dl.find(['dt', 'dd'], {'class': 'articleSubject'})
            if subject and subject.find('a'):
                news_items.append(f"- {subject.find('a').text.strip()}")
            if len(news_items) >= 15: break
            
        if not news_items: return "뉴스 수집 실패 (데이터 빔)", ""
        return "글로벌 및 국내 주요 금융 뉴스 헤드라인", "\n".join(news_items)
    except Exception as e:
        return f"뉴스 수집 지연: {e}", ""

def get_after_hours_rate(code):
    try:
        clean_code = str(code).replace("'", "").strip().zfill(6)
        res = requests.get(f"https://finance.naver.com/item/sise_time_allday.naver?code={clean_code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        for row in soup.select('table.type2 tr'):
            if '18:00' in row.text or '17:50' in row.text or '17:40' in row.text:
                return row.select('span.tah')[1].text.strip()
        return "시간외 변동없음"
    except:
        return "조회불가"

def get_yesterday_korean_context():
    try:
        # 💡 [핵심 패치] KEY_1이 비어있으면 자동으로 KEY_2를 찾아 여는 쌍끌이 스위치!
        creds_str = os.environ.get("KEY_1")
        if not creds_str or len(creds_str.strip()) < 10:
            creds_str = os.environ.get("KEY_2")
            
        if not creds_str or len(creds_str.strip()) < 10:
            return "구글 시트 연동 실패: 깃허브(Settings -> Secrets)에 키가 완전히 증발했습니다! 키를 새로 등록해주세요."

        creds_dict = json.loads(creds_str)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:6]
        
        if not scanner_data or len(scanner_data[0]) < 13:
            return "구글 시트 데이터 로드 실패 (빈 시트이거나 열 개수 부족)"

        picks_info = []
        for r in scanner_data:
            if len(r) > 12 and r[0]:
                name, theme, tajeom = r[0], r[4], r[12]
                code_res = requests.get(f"https://m.stock.naver.com/api/search/all?keyword={name}", timeout=3).json()
                code = code_res['result']['stocks'][0]['itemCode'] if code_res.get('result') and code_res['result'].get('stocks') else ""
                nxt_rate = get_after_hours_rate(code) if code else "확인불가"
                picks_info.append(f"▪️ [{name}] 테마: {theme} | 시간외(NXT) 등락률: {nxt_rate}")
                
        return "\n".join(picks_info)
    except Exception as e:
        return f"구글 시트 연동 실패: {e}"

def generate_morning_briefing(market_data, news_data, kor_context):
    client = genai.Client(api_key=GEMINI_API_KEY)
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 데이터를 융합하여 '모닝 브리핑'을 작성해라.

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 한국장 포착 종목 및 시간외(NXT) 결과]
{kor_context}

[작성 지침]
1. 간결하고 묵직한 어조 유지. 불필요한 서론 금지.
2. 3단 구성:
   - 🌎 [글로벌 매크로 요약]: 제공된 뉴스 헤드라인들을 바탕으로 간밤의 시장 분위기를 3줄로 요약.
   - 🇰🇷 [어제 포착 종목 NXT 브리핑]: 어제 포착된 종목들({kor_context})의 시간외 단일가 등락률에 대한 평가 코멘트.
   - 🎯 [오늘의 액션 플랜]: 주요 뉴스와 한국의 시간외 데이터를 종합한 오늘 오전 9시~10시 구체적 행동 지침 제시.
"""
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-1.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            if "503" in str(e) or "429" in str(e): time.sleep(30 * (i + 1))
            else: raise e
    raise Exception("서버 응답 불가")

if __name__ == "__main__":
    market_data, news_data = get_us_market_summary()
    kor_context = get_yesterday_korean_context()
    
    if "실패" in market_data or "실패" in kor_context or "지연" in market_data:
        final_msg = f"🚨 [HYEOKS 시스템 경고] 모닝 데이터 수집 에러\n\n[에러 내용]\n- 뉴스 수집 상태: {market_data}\n- 한국장 상태: {kor_context}\n\n※ 문제를 수정해주세요."
    else:
        briefing_text = generate_morning_briefing(market_data, news_data, kor_context)
        today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
        final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': final_msg, 'parse_mode': 'Markdown'}
    )
    print("✅ 프로세스 완료!")
