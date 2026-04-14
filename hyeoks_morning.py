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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        res = requests.get("https://finance.naver.com/news/mainnews.naver", headers=headers, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_items = []
        for dl in soup.find_all('dl'):
            subject = dl.find(['dt', 'dd'], {'class': 'articleSubject'})
            if subject and subject.find('a'):
                news_items.append(f"- {subject.find('a').text.strip()}")
            if len(news_items) >= 15: break
        if not news_items: return "뉴스 수집 실패", ""
        return "글로벌 및 국내 주요 금융 뉴스 헤드라인", "\n".join(news_items)
    except Exception as e:
        return f"뉴스 수집 에러: {e}", ""

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
        gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
        if not gcp_creds_str or len(gcp_creds_str.strip()) < 10:
            return "🚨 [진짜 구글시트 에러] 깃허브 환경변수(GCP_CREDENTIALS)가 비어있습니다."
        
        creds_dict = json.loads(gcp_creds_str)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:6]
    except Exception as e:
        return f"🚨 [진짜 구글시트 에러] 권한 또는 JSON 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 13:
        return "구글 시트 데이터가 비어있습니다."

    picks_info = []
    naver_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*'
    }

    for r in scanner_data:
        if len(r) > 12 and r[0]:
            name, theme = r[0], r[4]
            try:
                code_res = requests.get(f"https://m.stock.naver.com/api/search/all?keyword={name}", headers=naver_headers, timeout=5).json()
                code = code_res['result']['stocks'][0]['itemCode'] if code_res.get('result') and code_res['result'].get('stocks') else ""
                nxt_rate = get_after_hours_rate(code) if code else "코드검색실패"
                picks_info.append(f"▪️ [{name}] 테마: {theme} | 시간외(NXT) 등락률: {nxt_rate}")
            except Exception:
                picks_info.append(f"▪️ [{name}] 테마: {theme} | 시간외(NXT) 데이터 수집불가")
                
    return "\n".join(picks_info)

def generate_morning_briefing(market_data, news_data, kor_context):
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # 💡 [핵심 패치] AI의 거짓말(0% 보합) 방지 및 퀀트 우량주 폄하 금지 로직 추가
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 9시 장 개장 전 트레이더가 읽을 '모닝 브리핑'을 작성해라.

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 한국장 포착 종목 및 시간외(NXT) 결과]
{kor_context}

[작성 절대 지침 - 어기면 시스템 다운됨]
1. 군더더기 철저히 배제: "202X년 X월", 인사말, 서론, "보고자:" 등의 쓸데없는 양식을 절대 생성하지 마라. 오직 본문 3단 구성만 출력해라.
2. 3단 구성 체재:
   - 🌎 [글로벌 매크로 요약]: 제공된 뉴스 헤드라인들을 바탕으로 간밤의 시장 분위기(미 증시, 환율, 주요 이슈)를 3줄로 요약.
   
   - 🇰🇷 [어제 포착 종목 NXT 브리핑]: 어제 포착된 종목들을 뭉뚱그려 설명하지 말고, 전달받은 종목명({kor_context})을 '반드시 하나씩 개별적으로 나열'하며 시간외 등락률을 적어라.
     🚨 [데이터 팩트체크 주의]: 만약 데이터에 "확인불가", "데이터 수집불가" 등이 적혀있다면 절대 임의로 "0% 보합"이라고 짐작해서 거짓말하지 마라. "시간외 수집 누락(확인불가)"이라고 있는 그대로 팩트만 기재해라.
     🚨 [비(非)주도주 존중 주의]: 여기에 나열된 어제 포착 종목들은 모두 HYEOKS 퀀트 시스템에서 80점 이상의 최고점을 받은 특A급 우량 종목들이다. 오늘 글로벌 메인 뉴스 테마와 엮이지 않았다는 이유만으로 "소외 가능성, 무조건 현금화" 운운하며 함부로 잡주 취급하거나 폄하하지 마라. 각 종목이 가진 개별적인 차트 모멘텀이나 원래의 테마가 유효함을 인정하고, 객관적인 홀딩/대응 관점을 짧게 코멘트해라.
     
   - 🎯 [오늘의 액션 플랜]: 주요 뉴스와 한국 시간외 데이터를 종합하여, 오늘 오전 9시~10시에 어떤 테마를 집중 타격하고 어떤 테마를 보수적으로 볼지 실전 트레이딩 시나리오(Case 1, Case 2 등)를 구체적으로 제시해라.
"""
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            if "503" in str(e) or "429" in str(e): time.sleep(30 * (i + 1))
            else: raise e
    raise Exception("서버 응답 불가")

if __name__ == "__main__":
    market_data, news_data = get_us_market_summary()
    kor_context = get_yesterday_korean_context()
    
    if "실패" in market_data or "에러" in kor_context or "지연" in market_data:
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
