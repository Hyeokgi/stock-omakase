import os, requests, datetime, time, re
from bs4 import BeautifulSoup
from google import genai
import gspread
from oauth2client.service_account import ServiceAccountCredentials

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))

def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get("https://finance.naver.com/world/", headers=headers, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        indices = soup.select('.world_index .idx_tit, .world_index .idx_val, .world_index .idx_fluc')
        market_data = " ".join([i.text.strip() for i in indices[:15]])
        news_items = soup.select('.news_area .tit')
        news_data = "\n".join([f"- {n.text.strip()}" for n in news_items[:5]])
        return market_data, news_data
    except Exception as e:
        return "미 증시 수집 지연", f"뉴스 수집 지연: {e}"

# 💡 [신규 무기] 어제 스캐너가 포착한 종목의 '시간외 단일가(NXT)' 등락률을 긁어오는 스나이퍼 엔진
def get_after_hours_rate(code):
    try:
        clean_code = str(code).replace("'", "").strip().zfill(6)
        url = f"https://finance.naver.com/item/sise_time_allday.naver?code={clean_code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        # 시간외 단일가 테이블에서 가장 최신(18:00) 체결가의 등락률 추출
        rows = soup.select('table.type2 tr')
        for row in rows:
            if '18:00' in row.text or '17:50' in row.text or '17:40' in row.text:
                rate_span = row.select('span.tah')[1]
                return rate_span.text.strip()
        return "보합(0.0%)"
    except:
        return "조회불가"

# 💡 [신규 무기] 구글 시트에서 '어제의 전리품(포착 종목)'을 가져와 시간외 종가와 결합
def get_yesterday_korean_context():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:6] # 상위 5개 종목만 추출
        
        picks_info = []
        for r in scanner_data:
            if len(r) > 12 and r[0]:
                name = r[0]
                theme = r[4]
                tajeom = r[12]
                # 종목 코드는 보조 시트나 이름 검색을 통해 매핑 (여기서는 이름 기반 텍스트 생성)
                # 시간외 종가 크롤링을 위해 네이버 검색 API로 코드 우회 획득
                code_res = requests.get(f"https://m.stock.naver.com/api/search/all?keyword={name}").json()
                code = code_res['result']['stocks'][0]['itemCode'] if code_res.get('result') and code_res['result'].get('stocks') else ""
                
                nxt_rate = get_after_hours_rate(code) if code else "확인불가"
                picks_info.append(f"▪️ [{name}] 테마: {theme} | 어제타점: {tajeom} | 시간외(NXT) 등락률: {nxt_rate}")
                
        return "\n".join(picks_info)
    except Exception as e:
        return f"한국장 수급 데이터 로드 실패: {e}"

def generate_morning_briefing():
    client = genai.Client(api_key=GEMINI_API_KEY)
    market_data, news_data = get_us_market_summary()
    kor_context = get_yesterday_korean_context()
    
    # 💡 [프롬프트 진화] 미국 매크로와 한국의 '어제 종가/시간외' 데이터를 교차 분석하도록 강제!
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 [밤사이 미국 데이터]와 [어제 한국장 포착 종목 및 시간외(NXT) 결과]를 융합하여, 오늘 아침 9시 장이 열리기 전 트레이더가 반드시 알아야 할 '모닝 브리핑'을 작성해라.

[밤사이 미국 데이터]
미 증시 요약: {market_data}
글로벌 뉴스: {news_data}

[어제 한국장 포착 종목 및 시간외(NXT) 결과]
{kor_context}

[작성 지침]
1. 간결하고 묵직한 어조: 전장의 장수에게 보고하듯 핵심만 찔러라. 뻔한 소리는 배제해라.
2. 3단 구성:
   - 🌎 [글로벌 매크로 요약]: 간밤에 미국장이 왜 움직였는지 3줄로 요약.
   - 🇰🇷 [어제 포착 종목 NXT 브리핑]: 어제 우리가 포착한 종목들({kor_context})의 시간외 단일가 등락률을 분석해라. 시간외에서 급등했다면 "오늘 시초가 갭상승 시 차익 실현", 하락했다면 "눌림목 지지 여부 확인" 등의 구체적 코멘트를 달아라.
   - 🎯 [오늘의 액션 플랜]: 미국의 매크로와 한국의 시간외 데이터를 종합했을 때, 오늘 아침 9시~10시에 어떤 테마를 버리고 어떤 테마를 주워야 하는지 날카로운 행동 지침을 제시해라.
"""
    
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            err_str = str(e).lower()
            if "503" in err_str or "429" in err_str or "quota" in err_str:
                time.sleep(30 * (i + 1))
            else: raise e
    raise Exception("서버 응답 불가")

if __name__ == "__main__":
    print("🌅 [HYEOKS 모닝 브리핑] 데이터 수집 및 AI 딥리딩 중...")
    briefing_text = generate_morning_briefing()
    today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': final_msg, 'parse_mode': 'Markdown'}
    )
    print("✅ 모닝 브리핑 텔레그램 발송 완료!")
