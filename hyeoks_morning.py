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

# 💡 [신규 탑재] 수석님의 FRED API 키
FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

# =====================================================================
# 1. 🌐 [신규 엔진] 미국 연준(FRED) 매크로 유동성 5대장 실시간 수집기
# =====================================================================
def get_global_liquidity_data():
    """
    FRED API를 통해 글로벌 매크로 유동성의 핵심 지표 5가지를 수집합니다.
    """
    print("🌐 글로벌 유동성(FRED) 데이터 수집 중...")
    
    # 추적할 5가지 핵심 지표 코드
    indicators = {
        "WTREGEN": "TGA (미 재무부 일반계정 / 단위: 십억 달러)", 
        "RRPONTSYD": "Reverse Repo (역레포 잔고 / 단위: 십억 달러)", 
        "BAMLH0A0HYM2": "High-Yield Spread (하이일드 스프레드 / 단위: %)", 
        "WALCL": "Fed Total Assets (연준 총자산 / 단위: 백만 달러)", 
        "M2SL": "M2 (미국 총통화량 / 단위: 십억 달러)" 
    }
    
    liquidity_report = []
    
    for series_id, name in indicators.items():
        try:
            # 가장 최근 데이터 2개를 가져와서 증감을 비교합니다.
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=2"
            res = requests.get(url, timeout=5).json()
            
            if 'observations' in res and len(res['observations']) >= 2:
                latest = res['observations'][0]
                prev = res['observations'][1]
                
                # 수치가 '.' 등으로 누락된 경우 예외 처리
                if latest['value'] == '.' or prev['value'] == '.':
                    continue
                    
                latest_val = float(latest['value'])
                prev_val = float(prev['value'])
                date = latest['date']
                
                # 증감폭 및 화살표 방향 설정
                diff = latest_val - prev_val
                if diff > 0:
                    trend = f"🔺 증가 (+{diff:,.2f})"
                elif diff < 0:
                    trend = f"🔻 감소 ({diff:,.2f})"
                else:
                    trend = "➖ 변동없음"
                    
                # 하이일드 스프레드는 %이므로 포맷팅을 다르게 함
                if series_id == "BAMLH0A0HYM2":
                    formatted_val = f"{latest_val:,.2f}%"
                else:
                    formatted_val = f"{latest_val:,.1f}"
                    
                liquidity_report.append(f"- {name}: {formatted_val} ({trend}) [기준일: {date}]")
            else:
                liquidity_report.append(f"- {name}: 데이터 수집 지연")
        except Exception as e:
            liquidity_report.append(f"- {name}: API 호출 에러")
            
    if not liquidity_report:
        return "유동성 데이터 수집 실패"
        
    return "\n".join(liquidity_report)

# =====================================================================
# 2. 📰 기존 네이버 뉴스 및 한국장 데이터 수집
# =====================================================================
def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        print("📰 네이버 주요 뉴스 수집 중...")
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
    """
    네이버 모바일 API를 사용하여 시간외 단일가(NXT 마감 기준) 등락률을 100% 정확하게 수집합니다.
    """
    try:
        clean_code = str(code).replace("'", "").strip().zfill(6)
        url = f"https://m.stock.naver.com/api/stock/{clean_code}/basic"
        
        # 브라우저인 것처럼 위장하여 API 호출
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        res = requests.get(url, headers=headers, timeout=3).json()
        
        # 시간외 등락률 데이터 추출
        if 'overTimeFluctuationsRatio' in res:
            nxt_rate = float(res['overTimeFluctuationsRatio'])
            if nxt_rate > 0:
                return f"🔴 +{nxt_rate}% 상승"
            elif nxt_rate < 0:
                return f"🔵 {nxt_rate}% 하락"
            else:
                return "➖ 보합(0%)"
        else:
            return "데이터 없음"
    except Exception as e:
        return "수집 에러"

def get_yesterday_korean_context():
    print("🇰🇷 어제 한국장 퀀트 타겟 종목 수집 중...")
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

# =====================================================================
# 3. 🧠 [최종 진화] 글로벌 유동성을 융합한 AI 프롬프트
# =====================================================================
def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 리포트 작성 중...")
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # 💡 [프롬프트 진화] 글로벌 유동성 지표(FRED)를 읽고 비중을 조절하는 로직 추가
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 9시 장 개장 전 트레이더가 읽을 '모닝 브리핑'을 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 한국장 포착 종목 및 시간외(NXT) 결과]
{kor_context}

[작성 절대 지침 - 어기면 시스템 다운됨]
1. 군더더기 철저히 배제: "202X년 X월", 인사말, 서론, "보고자:" 등의 쓸데없는 양식을 절대 생성하지 마라. 오직 본문 4단 구성만 출력해라.
2. 4단 구성 체재:
   - 🌎 [글로벌 유동성 및 매크로 요약]: 제공된 FRED 유동성 지표(TGA, 역레포, 스프레드 등)의 증감을 분석하여 현재 시장에 '실제 스마트머니(자금)'가 들어오고 있는지 나가는지 냉철하게 진단해라. 뉴스의 호재/악재 찌라시보다 이 유동성 지표의 방향성을 최우선으로 하여 오늘 증시의 뼈대를 3줄로 요약해라. (예: "뉴스는 공포를 조장하나 TGA가 방출되며 유동성은 공급 중이므로 하락은 매수 기회다" 등)
   
   - 🇰🇷 [어제 포착 종목 NXT 브리핑]: 어제 포착된 종목들을 뭉뚱그려 설명하지 말고, 전달받은 종목명({kor_context})을 '반드시 하나씩 개별적으로 나열'하며 시간외 등락률을 적어라.
     🚨 [데이터 팩트체크 주의]: 만약 데이터에 "확인불가", "데이터 수집불가" 등이 적혀있다면 절대 임의로 "0% 보합"이라고 짐작해서 거짓말하지 마라. "시간외 수집 누락(확인불가)"이라고 있는 그대로 팩트만 기재해라.
     🚨 [비(非)주도주 존중 주의]: 나열된 어제 포착 종목들은 모두 HYEOKS 퀀트 시스템에서 80점 이상의 최고점을 받은 우량 종목들이다. 뉴스 테마와 엮이지 않았다고 함부로 잡주 취급하지 말고, 각 종목이 가진 개별적인 차트 모멘텀이 유효함을 인정하며 객관적인 홀딩 관점을 짧게 코멘트해라.
     
   - 🎯 [오늘의 액션 플랜]: 유동성 지표와 한국 시간외 데이터를 종합하여, 오늘 오전 9시~10시에 어떤 테마를 집중 타격할지 실전 트레이딩 시나리오를 구체적으로 제시해라.
     🚨 [유동성 연동 비중 조절 (가장 중요)]: 만약 유동성 지표(M2 감소, 스프레드 급등 등)가 악화되는 하락장/경색 국면이라면, 퀀트 고득점 종목이라도 '비중을 평소의 절반 이하로 줄이고 당일 청산(짧은 방망이) 할 것'을 강력히 경고해라. 반대로 유동성이 풍부하다면 '강력한 눌림목 스윙 매수'를 지시해라.
"""
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            if "503" in str(e) or "429" in str(e): 
                print(f"⚠️ 구글 서버 혼잡. {30 * (i + 1)}초 대기 후 재시도...")
                time.sleep(30 * (i + 1))
            else: raise e
    raise Exception("서버 응답 불가")

if __name__ == "__main__":
    print("🚀 HYEOKS 모닝 브리핑 시스템 가동 시작...")
    
    # 1. 유동성 데이터 수집 (FRED)
    liquidity_data = get_global_liquidity_data()
    
    # 2. 뉴스 및 한국장 데이터 수집
    market_data, news_data = get_us_market_summary()
    kor_context = get_yesterday_korean_context()
    
    if "실패" in market_data or "에러" in kor_context or "에러" in liquidity_data:
        final_msg = f"🚨 [HYEOKS 시스템 경고] 모닝 데이터 수집 에러\n\n[에러 내용]\n- 유동성(FRED): {liquidity_data}\n- 뉴스 수집: {market_data}\n- 한국장: {kor_context}\n\n※ 문제를 수정해주세요."
    else:
        # 3. AI 리포트 생성
        briefing_text = generate_morning_briefing(market_data, news_data, kor_context, liquidity_data)
        today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
        final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    # 4. 텔레그램 발송
    print("📲 텔레그램 발송 중...")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={'chat_id': TELEGRAM_CHAT_ID, 'text': final_msg, 'parse_mode': 'Markdown'}
    )
    print("✅ 모든 프로세스 완료!")
