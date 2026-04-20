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

FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

# =====================================================================
# 1. 🌐 글로벌 유동성(FRED) 수집기
# =====================================================================
def get_global_liquidity_data():
    print("🌐 글로벌 유동성(FRED) 데이터 수집 중...")
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
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=2"
            res = requests.get(url, timeout=5).json()
            if 'observations' in res and len(res['observations']) >= 2:
                latest, prev = res['observations'][0], res['observations'][1]
                if latest['value'] == '.' or prev['value'] == '.': continue
                latest_val, prev_val = float(latest['value']), float(prev['value'])
                date = latest['date']
                diff = latest_val - prev_val
                trend = f"🔺 증가 (+{diff:,.2f})" if diff > 0 else (f"🔻 감소 ({diff:,.2f})" if diff < 0 else "➖ 변동없음")
                formatted_val = f"{latest_val:,.2f}%" if series_id == "BAMLH0A0HYM2" else f"{latest_val:,.1f}"
                liquidity_report.append(f"- {name}: {formatted_val} ({trend}) [기준일: {date}]")
            else:
                liquidity_report.append(f"- {name}: 데이터 수집 지연")
        except Exception as e:
            liquidity_report.append(f"- {name}: API 호출 에러")
    return "\n".join(liquidity_report) if liquidity_report else "유동성 데이터 수집 실패"

# =====================================================================
# 2. 📰 뉴스 및 한국장 데이터 수집 (NXT 크로스체크 매칭 로직)
# =====================================================================
def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0'}
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
        return "글로벌 및 국내 주요 금융 뉴스 헤드라인", "\n".join(news_items)
    except Exception as e:
        return f"뉴스 수집 에러: {e}", ""

def get_yesterday_korean_context():
    print("🇰🇷 어제 한국장 퀀트 타겟 종목 수집 중...")
    try:
        gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
        if not gcp_creds_str or len(gcp_creds_str.strip()) < 10:
            return "🚨 깃허브 환경변수(GCP_CREDENTIALS) 에러"
        
        creds_dict = json.loads(gcp_creds_str)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        # 1. 스캐너_마스터에서 어제 시장을 지배한 상위 5개 대장주 이름 추출
        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:6]

        # 💡 [퍼즐 완성] 주가데이터_보조 시트에서 시간외(NXT) 데이터를 추출하여 매핑 딕셔너리 생성
        helper_sheet = doc.worksheet("주가데이터_보조")
        helper_data = helper_sheet.get_all_values()[1:]
        nxt_map = {}
        for row in helper_data:
            if len(row) >= 21:  # 21번째 열 (인덱스 20)이 시간외(NXT)
                nxt_map[row[0].strip()] = row[20].strip()

    except Exception as e:
        return f"🚨 권한 또는 JSON 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 13:
        return "구글 시트 데이터가 비어있습니다."

    picks_info = []
    for r in scanner_data:
        if len(r) > 4 and r[0]:
            # 💡 [핵심 버그 패치] r[1]에 있는 '현재가'도 같이 뽑아서 AI에게 전달합니다!
            name, current_price, theme = r[0].strip(), r[1].strip(), r[4]
            nxt_rate = nxt_map.get(name, "시간외 수집 누락(확인불가)")
            picks_info.append(f"▪️ [{name}] 종가: {current_price}원 | 테마: {theme} | 시간외(NXT) 등락률: {nxt_rate}")
                
    return "\n".join(picks_info)

# =====================================================================
# 3. 🧠 투 트랙(Quant + News) 전략 AI 프롬프트
# =====================================================================
def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 리포트 작성 중...")
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 9시 장 개장 전 트레이더가 읽을 '모닝 브리핑'을 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 한국장 포착 종목 및 시간외(NXT) 결과]
{kor_context}

[작성 절대 지침 - 어기면 시스템 다운됨]
1. 군더더기 철저히 배제: 인사말, 서론, "보고자:" 등의 쓸데없는 양식을 절대 생성하지 마라. 오직 본문 4단 구성만 출력해라.
2. 4단 구성 체재:
   - 🌎 [글로벌 유동성 및 매크로 요약]: 제공된 FRED 유동성 지표의 증감을 분석하여 현재 시장에 '실제 스마트머니'가 들어오는지 진단해라.
   - 🇰🇷 [어제 포착 종목 NXT 브리핑]: 전달받은 어제 포착 종목명을 하나씩 나열해라. 제공된 종가와 시간외 등락률 수치를 팩트 그대로 적어라.
   - 🎯 [오늘의 액션 플랜]: 유동성 지표와 뉴스를 종합하여 실전 트레이딩 시나리오를 구체적으로 제시해라.
     🚨 [절대 규칙 1 - 투 트랙(Two-Track) 전략 필수]: 
        * 1순위 타겟: 반드시 [어제 한국장 포착 종목]에 나열된 '퀀트 검증 대장주' 안에서 선정하여 눌림/돌파 타점을 제시해라.
        * 2순위 타겟: [밤사이 주요 뉴스]를 분석하여 오늘 아침 시장의 투기적 수급이 쏠릴 만한 '신규/이슈 테마(예: 코로나, 지정학적 리스크 등)'를 적극적으로 발굴하고 단기 트레이딩 관점을 제안해라.
     🚨 [절대 규칙 2 - 가격 환각 절대 금지]: 제공된 데이터에 종목의 정확한 '종가(원)'가 없다면 절대 임의의 숫자를 지어내서 매수가/목표가를 제시하지 마라. 대신 '시초가 대비 -3% 눌림목', '전일 종가 지지 확인 후' 와 같이 기준점을 사용하여 타점을 설명해라.
   - 🚨 [유동성 연동 비중 조절]: 유동성이 악화되면 비중 축소 및 단기 청산을, 유동성이 좋다면 스윙 매수를 지시해라.
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
    liquidity_data = get_global_liquidity_data()
    market_data, news_data = get_us_market_summary()
    kor_context = get_yesterday_korean_context()
    
    if "실패" in market_data or "에러" in kor_context or "에러" in liquidity_data:
        final_msg = f"🚨 [HYEOKS 시스템 경고] 모닝 데이터 수집 에러\n\n[에러 내용]\n- 유동성(FRED): {liquidity_data}\n- 뉴스 수집: {market_data}\n- 한국장: {kor_context}\n\n※ 문제를 수정해주세요."
    else:
        briefing_text = generate_morning_briefing(market_data, news_data, kor_context, liquidity_data)
        today_str = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
        final_msg = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    # print("📲 텔레그램 발송 중...")
    # requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
    #               data={'chat_id': TELEGRAM_CHAT_ID, 'text': final_briefing, 'parse_mode': 'Markdown'})
    # print("✅ 모든 프로세스 완료!")

    # === 💡 [수정할 코드] 강력한 발송 엔진 및 에러 추적기 ===
    print("📲 텔레그램 발송 중...")
    
    # 1. 텔레그램 마크다운 V2 파싱 에러를 방지하기 위한 특수문자 무력화 (이스케이프 처리)
    # AI가 자주 쓰는 특수문자 중 짝이 안 맞으면 에러를 내는 것들을 안전하게 처리합니다.
    safe_briefing = final_briefing.replace('!', '\!').replace('.', '\.').replace('-', '\-').replace('(', '\(').replace(')', '\)').replace('+', '\+').replace('=', '\=').replace('>', '\>').replace('<', '\<')

    # 2. 메시지 전송 및 결과 확인
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID, 
        'text': safe_briefing, 
        'parse_mode': 'MarkdownV2' # V2로 업그레이드
    }
    
    response = requests.post(url, data=payload)
    
    if response.status_code == 200:
        print("✅ 텔레그램 발송 성공! 모든 프로세스 완료!")
    else:
        print(f"❌ 텔레그램 발송 실패! (상태 코드: {response.status_code})")
        print(f"🚨 텔레그램 서버 에러 메시지: {response.text}")
        
        # 마크다운 에러일 경우, 서식을 다 빼고 일반 텍스트(Plain Text)로 재전송 시도!
        print("🔄 일반 텍스트 모드로 재전송을 시도합니다...")
        fallback_payload = {
            'chat_id': TELEGRAM_CHAT_ID, 
            'text': final_briefing # 원본 텍스트 그대로
        }
        fallback_res = requests.post(url, data=fallback_payload)
        if fallback_res.status_code == 200:
             print("✅ 일반 텍스트 재전송 성공!")
        else:
             print("❌ 재전송도 실패했습니다. 토큰이나 텍스트 길이를 확인하세요.")
