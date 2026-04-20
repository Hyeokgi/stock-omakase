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
# 2. 📰 뉴스 및 한국장 데이터 수집
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
        
        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:6]

    except Exception as e:
        return f"🚨 권한 또는 JSON 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 13:
        return "구글 시트 데이터가 비어있습니다."

    picks_info = []
    for r in scanner_data:
        if len(r) > 4 and r[0]:
            name, current_price, theme = r[0].strip(), r[1].strip(), r[4]
            # 💡 [시간외 삭제 패치] 시간외 데이터를 제외하고 종가와 테마만 팩트로 꽂아줍니다!
            picks_info.append(f"▪️ [{name}] 종가: {current_price}원 | 테마: {theme}")
                
    return "\n".join(picks_info)

# =====================================================================
# 3. 🧠 투 트랙(Quant + News) 전략 AI 프롬프트 (유연성 & 통찰력 강화)
# =====================================================================
def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 리포트 작성 중...")
    
    # 💡 구글 검색 툴을 활성화하여 AI가 간밤의 시간외/NXT 이슈를 자체 검색할 수 있도록 옵션을 추가할 수 있지만,
    # 우선 제공된 데이터를 기반으로 가장 날카로운 추론을 하도록 프롬프트를 고도화합니다.
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로/퀀트 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 장 개장 전 트레이더가 읽을 '유연하고 통찰력 있는 모닝 브리핑 리포트'를 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 한국장 포착 주요 종목]
{kor_context}

[HYEOKS 리서치 작성 지침 - 가독성 절대 규칙]
1. 🚨 볼드체(**) 전면 금지: 텍스트에 ** 기호를 절대 사용하지 마라. 모바일 화면에서 눈이 매우 피로해진다. 
2. 대체 강조법: 종목명이나 핵심 키워드를 강조하고 싶을 때는 [종목명], <핵심어> 형태로 기호를 묶어서 표현해라.
3. 시각적 여백 활용: 글머리 기호(▪️, ▫️, 📌, 💡, 🚨)와 인용구 기호(>)를 적극 활용하여 줄바꿈을 자주 하고, 글이 뭉쳐 보이지 않게 깔끔하게 정돈해라.
4. 유연한 리포트 구조:
   🌎 [HYEOKS 매크로 & 유동성 뷰]
   - FRED 유동성 지표와 밤사이 뉴스를 종합하여 자금의 성격(위험선호 vs 방어적 선별장세) 진단.
   
   🎯 [오늘의 주도 예상 섹터]
   - 수급이 폭발할 강력한 테마/섹터 2~3개를 선정하고 짧고 명확하게 근거 제시.
   
   📈 [HYEOKS 톱픽 & 액션 플랜]
   - 총 5~6개의 추천 종목 선정 (어제 포착 종목 위주되 뉴스 기반 신규 주도주 추가 가능).
   - 각 종목별 작성 포맷 예시:
     > [종목명]
     - 흐름 추론: (전날 시간외 흐름 및 뉴스 임팩트 추론)
     - 액션 플랜: (시가 갭 대응 전략. 단, 제공되지 않은 정확한 가격(원)을 임의로 지어내지 말고 '전일 종가 부근', '시초가 대비 -3%' 등 기술적 기준점 활용)
5. 군더더기 배제: 인사말, 서론 등은 생략.
"""
    for i in range(10):
        try:
            # 기본 모델 생성 (필요 시 Search Grounding 옵션 추가 가능)
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
        final_briefing = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    # === 💡 [초강력 가독성 패치] 파이썬 자체 클렌징 엔진 ===
    print("📲 텔레그램 발송 중...")
    
    import re
    # 1. 시각적 찌꺼기(마크다운 볼드체 및 헤딩) 제거
    clean_briefing = final_briefing.replace('**', '')       
    clean_briefing = clean_briefing.replace('### ', '📍 ')  
    clean_briefing = clean_briefing.replace('## ', '📍 ')   
    
    # 🚨 문제의 주범이었던 꺾쇠(<, >) 강제 변환 코드는 삭제했습니다!
    # 대신 AI가 강조용으로 쓴 <핵심어> 형태를 [핵심어]로 안전하게만 바꿔줍니다.
    clean_briefing = re.sub(r'<([^>]+)>', r'[\1]', clean_briefing)
    
    # 2. 리스트의 별표(*)는 하위 네모(▫️)로 깔끔하게 변환
    clean_briefing = re.sub(r'^\s*\*\s+', '▫️ ', clean_briefing, flags=re.MULTILINE)
    
    # 3. 💡 [핵심 패치] AI가 출력한 인용구(>) 기호를 세련된 핀(📌) 이모지로 변환!
    clean_briefing = re.sub(r'^\s*>\s*', '📌 ', clean_briefing, flags=re.MULTILINE)
    
    # 4. 에러를 유발하는 parse_mode를 빼고 순수 텍스트+이모지로 안전하게 전송
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID, 
        'text': clean_briefing
    }
    
    response = requests.post(url, data=payload)
    
    if response.status_code == 200:
        print("✅ 텔레그램 발송 성공! 모든 프로세스 완료!")
    else:
        print(f"❌ 텔레그램 발송 실패! (상태 코드: {response.status_code})")
        print(f"🚨 텔레그램 서버 에러 메시지: {response.text}")
        print("🔄 일반 텍스트 모드로 재전송을 시도합니다...")
        fallback_payload = {
            'chat_id': TELEGRAM_CHAT_ID, 
            'text': final_briefing
        }
        fallback_res = requests.post(url, data=fallback_payload)
        if fallback_res.status_code == 200:
             print("✅ 일반 텍스트 재전송 성공!")
        else:
             print("❌ 재전송도 실패했습니다. 토큰이나 텍스트 길이를 확인하세요.")
