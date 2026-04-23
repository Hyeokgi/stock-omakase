import os, requests, datetime, time, json, re
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

# ==========================================
# 💡 KIS API 환경 변수
# ==========================================
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
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
# 2. 💎 VIP 데이터 추출 (KIS API 100% 직결) 및 종목코드 매칭
# =====================================================================
def search_code_from_naver(stock_name):
    try:
        url = "https://m.stock.naver.com/api/search/all"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        res = requests.get(url, headers=headers, params={'keyword': stock_name}, verify=False, timeout=3).json()
        if res.get('result') and res['result'].get('stocks'):
            return res['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_vip_deep_dive_data(code, kis_token):
    # 💡 [수정] 체결강도 및 낡은 수급 변수 영구 삭제
    vip = {"펀더멘털": "N/A"}
    
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET):
        return "⚠️ KIS API 토큰 없음"

    req = requests.Session()
    headers = {
        "authorization": f"Bearer {kis_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P"
    }

    # 1. KIS API 호출: 펀더멘털 (PER, PBR)
    try:
        headers["tr_id"] = "FHKST01010100"
        res_price = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, timeout=3).json()
        if res_price.get("rt_cd") == "0":
            output = res_price.get("output", {})
            per = output.get("per", "N/A")
            pbr = output.get("pbr", "N/A")
            vip["펀더멘털"] = f"PER: {per} / PBR: {pbr}"
    except: pass

    # 💡 [수정] 결과 포맷 단순화 (펀더멘털만 리턴, 나머지는 시트 데이터 활용)
    return f"📊 {vip['펀더멘털']}"

def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
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
    print("🇰🇷 어제 한국장 퀀트 타겟 종목 및 VIP 심층 데이터 수집 중...")
    try:
        gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
        if not gcp_creds_str or len(gcp_creds_str.strip()) < 10:
            return "🚨 깃허브 환경변수(GCP_CREDENTIALS) 에러"
        
        creds_dict = json.loads(gcp_creds_str)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        # KIS 토큰 확보
        kis_token = ""
        try:
            setting_sheet = doc.worksheet("⚙️설정")
            for row in setting_sheet.get_all_values():
                if len(row) >= 2 and row[0] == "KIS_TOKEN":
                    kis_token = row[1]
                    break
        except: pass

        name_to_code = {}
        try:
            company_sheet = doc.worksheet("기업정보").get_all_values()
            name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in company_sheet[1:] if len(row) >= 3}
        except: pass

        scanner_sheet = doc.worksheet("주가데이터_보조") # 💡 원본 시트에서 직행
        scanner_data = scanner_sheet.get_all_values()[1:5] # 상위 4개 종목 추출

    except Exception as e:
        return f"🚨 권한 또는 JSON 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 22:
        return "구글 시트 데이터가 비어있습니다."

    picks_info = []
    for r in scanner_data:
        if len(r) > 20 and r[0]:
            # 💡 [수정] 주가데이터_보조 시트 인덱스에 맞게 매핑
            name, current_price, theme = str(r[0]).strip(), str(r[2]).strip(), str(r[19]).strip()
            program_text = str(r[21]).strip() if r[21] else "⚪ [P.관망중]"
            nxt_text = str(r[20]).strip() if r[20] else "➖ 0.00%"
            vol_status = str(r[18]).strip() if r[18] else "🟡 [V.평년수준]"
            
            code = name_to_code.get(name) or search_code_from_naver(name)
            
            vip_data = "VIP 데이터 확인불가"
            if code:
                print(f"🔍 [{name} ({code})] VIP 데이터 수집 중...")
                vip_data = get_vip_deep_dive_data(code, kis_token)
            else:
                print(f"❌ [{name}] 종목 코드를 찾을 수 없습니다.")

            # 💡 [수정] 프롬프트에 제공할 데이터 규격화
            picks_info.append(f"▪️ [{name}] 종가: {current_price}원 | 테마: {theme}\n  [프로그램] {program_text}\n  [야간/시외] {nxt_text}\n  [거래량] {vol_status}\n  [펀더멘털] {vip_data}")
                
    return "\n".join(picks_info)

# =====================================================================
# 3. 🧠 투 트랙(Quant + News) 전략 AI 프롬프트
# =====================================================================
def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 리포트 작성 중...")
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    # 💡 [수정] 체결강도 삭제 및 거래량 배지 추가 지시
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로/퀀트 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 장 개장 전 트레이더가 읽을 '유연하고 통찰력 있는 모닝 브리핑 리포트'를 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 포착된 핵심 종목 및 심층 데이터]
{kor_context}

[HYEOKS 리서치 작성 지침 - 가독성 및 보고서 통일성 절대 규칙]
1. 🚨 볼드체 전면 금지: 텍스트에 별표 기호(**)를 절대 쓰지 마라. 모바일 가독성을 해친다.
2. 보고서 계층 구조(Hierarchy) 및 전용 아이콘 엄수 (임의 변형 금지):
   
   [파트 1: 종합 시황 및 매크로 (녹색 뱃지)]
   🟩 [HYEOKS 매크로 & 뉴스 종합 시황]
   🟢 유동성 환경 분석
   ▫️ (FRED 지표가 증시 자금에 미치는 영향 짧게 해석)
   🟢 핵심 뉴스 & 시장 내러티브 진단
   ▫️ (제공된 뉴스들을 깊이 있게 분석하여 오늘 시장의 쏠림 방향성, 주도 테마 탄생 배경을 2~3문단으로 썰을 풀어라.)
   
   [파트 2: 종목별 심층 분석 (파란색 뱃지)]
   🟦 [종목명]
   🔹 핵심 모멘텀 & VIP 수급
   ▫️ [🤖프로그램: 제공된 프로그램 데이터 그대로 사용] [🌙야간: 제공된 시외/NXT 데이터 그대로 사용] [📈거래량: 제공된 거래량 배지 그대로 사용] [📊펀더멘털: 제공된 PER/PBR 데이터 사용]
   ▫️ (데이터와 뉴스를 융합한 세력의 매집 의도 및 상승 논리 분석)
   🔹 실전 액션 플랜
   ▫️ 진입: (시가 갭 대응 전략 및 1차 진입 타점)
   ▫️ 대응: (주요 지지선 및 돌파 목표가)

3. 🚨 데이터 뱃지(Badge) 작성 절대 규칙:
   - 내가 제공한 [프로그램], [야간/시외], [거래량] 텍스트를 한 글자도 바꾸지 말고 괄호 안에 그대로 넣어라. 
   - 없는 데이터(체결강도 등)를 억지로 지어내지 마라.

4. 압축 브리핑: 핵심 추천 종목 총 3~4개만 엄선하여 브리핑해라.
5. 군더더기 배제: 인사말, 서론 등은 생략.
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
        final_briefing = f"🌅 [HYEOKS 모닝 브리핑] - {today_str}\n\n{briefing_text}"
    
    print("📲 텔레그램 발송 중...")
    clean_briefing = final_briefing.replace('**', '')       
    clean_briefing = clean_briefing.replace('### ', '▶️ ')  
    clean_briefing = clean_briefing.replace('## ', '▶️ ')   
    clean_briefing = re.sub(r'<([^>]+)>', r'[\1]', clean_briefing)
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': clean_briefing}
    
    response = requests.post(url, data=payload)
    if response.status_code == 200: print("✅ 텔레그램 발송 성공! 모든 프로세스 완료!")
    else:
        print(f"❌ 텔레그램 발송 실패! (상태 코드: {response.status_code})")
        requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': final_briefing})
