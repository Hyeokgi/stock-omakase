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
# 2. 💎 VIP 데이터 추출 및 종목 정보 수집기 (네이버 우회 및 인코딩 패치)
# =====================================================================
def search_code_from_naver(stock_name):
    try:
        url = "https://m.stock.naver.com/api/search/all"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
        # 💡 [오류 수정] 한글 깨짐 방지를 위해 params로 안전하게 전달!
        res = requests.get(url, headers=headers, params={'keyword': stock_name}, verify=False, timeout=3).json()
        if res.get('result') and res['result'].get('stocks'):
            return res['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_vip_deep_dive_data(code, kis_token):
    vip = {"체결강도": "야간초기화(0%)", "신용잔고율": "확인불가", "수급트렌드": "뚜렷한 연속 매수 없음", "펀더멘털": "N/A"}
    req = requests.Session()
    req.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'})
    
    # 1. KIS API 호출: 체결강도, PER, PBR
    if kis_token and KIS_APP_KEY and KIS_APP_SECRET:
        try:
            headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "tr_id": "FHKST01010100", "custtype": "P"}
            res_price = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
            if res_price.get("rt_cd") == "0":
                output = res_price.get("output", {})
                
                vlsr = output.get("vlsr", "0")
                vip["체결강도"] = f"{vlsr}%" if vlsr != "0" and vlsr != "" else "야간초기화(0%)"
                
                per = output.get("per", "N/A")
                pbr = output.get("pbr", "N/A")
                vip["펀더멘털"] = f"PER: {per} / PBR: {pbr}"
        except: pass

        # 2. KIS API 호출: 수급트렌드 (외인/기관 연속 매수)
        try:
            headers["tr_id"] = "FHKST01010900"
            res_inv = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-investor", headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
            if res_inv.get("rt_cd") == "0":
                inv_list = res_inv.get("output", [])
                f_con, i_con = 0, 0
                for day in inv_list:
                    if int(day.get("frgn_ntby_qty", "0")) > 0: f_con += 1
                    else: break
                for day in inv_list:
                    if int(day.get("orgn_ntby_qty", "0")) > 0: i_con += 1
                    else: break
                trends = []
                if f_con > 1: trends.append(f"외국인 {f_con}일 연속 매수")
                if i_con > 1: trends.append(f"기관 {i_con}일 연속 매수")
                if trends: vip["수급트렌드"] = " / ".join(trends)
        except: pass

    # 3. 💡 Naver 핀셋 크롤링: 신용잔고율 (절대 파싱 패치)
    try:
        import re
        main_soup = BeautifulSoup(req.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
        
        # '신용비율'이라는 단어가 들어간 th 태그를 찾고, 그 바로 옆의 td 값을 강제로 뜯어옵니다.
        credit_th = main_soup.find('th', string=re.compile('신용비율'))
        if credit_th:
            credit_td = credit_th.find_next_sibling('td')
            if credit_td:
                credit_val = credit_td.text.strip()
                vip["신용잔고율"] = credit_val if "%" in credit_val else f"{credit_val}%"
    except: pass
    
    return f"⚡체결강도:{vip['체결강도']} | ⚠️신용비율:{vip['신용잔고율']} | 📈수급:{vip['수급트렌드']} | 📊{vip['펀더멘털']}"
    
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

        # 💡 [핵심 패치] 네이버에 의존하지 않고, 우리 구글 시트 '기업정보'에서 종목코드를 100% 매칭합니다!
        name_to_code = {}
        try:
            company_sheet = doc.worksheet("기업정보").get_all_values()
            name_to_code = {str(row[0]).strip(): str(row[2]).strip().zfill(6) for row in company_sheet[1:] if len(row) >= 3}
        except: pass

        scanner_sheet = doc.worksheet("스캐너_마스터")
        scanner_data = scanner_sheet.get_all_values()[1:5]

    except Exception as e:
        return f"🚨 권한 또는 JSON 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 13:
        return "구글 시트 데이터가 비어있습니다."

    picks_info = []
    for r in scanner_data:
        if len(r) > 4 and r[0]:
            name, current_price, theme = r[0].strip(), r[1].strip(), r[4]
            program = r[16] if len(r) > 16 else "확인불가"
            
            # 💡 구글 시트에서 먼저 찾고, 없으면 네이버 안전 검색으로 폴백(Fallback)
            code = name_to_code.get(name) or search_code_from_naver(name)
            
            vip_data = "VIP 데이터 확인불가"
            if code:
                print(f"🔍 [{name} ({code})] VIP 데이터 수집 중...")
                vip_data = get_vip_deep_dive_data(code, kis_token)
            else:
                print(f"❌ [{name}] 종목 코드를 찾을 수 없습니다.")

            picks_info.append(f"▪️ [{name}] 종가: {current_price}원 | 테마: {theme}\n  [당일 프로그램] {program}\n  [VIP 딥리딩] {vip_data}")
                
    return "\n".join(picks_info)

# =====================================================================
# 3. 🧠 투 트랙(Quant + News) 전략 AI 프롬프트
# =====================================================================
def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 리포트 작성 중...")
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 수석 매크로/퀀트 애널리스트야.
아래의 데이터를 융합하여 오늘 아침 장 개장 전 트레이더가 읽을 '유연하고 통찰력 있는 모닝 브리핑 리포트'를 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 포착된 핵심 종목 및 VIP 심층 데이터]
{kor_context}

[HYEOKS 리서치 작성 지침 - 가독성 및 보고서 통일성 절대 규칙]
1. 🚨 볼드체 전면 금지: 텍스트에 별표 기호(**)를 절대 쓰지 마라. 모바일 가독성을 해친다.
2. 보고서 계층 구조(Hierarchy) 및 전용 아이콘 엄수 (임의 변형 금지):
   
   [파트 1: 종합 시황 및 매크로 (녹색 뱃지)]
   🟩 [HYEOKS 매크로 & 뉴스 종합 시황]
   🟢 유동성 환경 분석
   ▫️ (FRED 지표가 증시 자금에 미치는 영향 짧게 해석)
   🟢 핵심 뉴스 & 시장 내러티브 진단 (🔥분량 대폭 강화)
   ▫️ (단순 지표 나열이 아님. 제공된 [밤사이 글로벌/국내 주요 뉴스]를 깊이 있게 분석하여, 오늘 시장의 자금 쏠림 방향성, 주도 테마의 탄생 배경, 투자자들의 탐욕/공포 심리 상태를 2~3문단으로 상세하고 날카롭게 썰을 풀어라.)
   
   [파트 2: 종목별 심층 분석 (파란색 뱃지)]
   🟦 [종목명]
   🔹 핵심 모멘텀 & VIP 수급
   ▫️ [🤖프로그램: 데이터] [⚡체결강도: 데이터] [⚠️신용: 데이터] [📈수급: 데이터] [📊기본: 데이터]
   ▫️ (VIP 데이터와 뉴스를 융합한 세력의 매집 의도 및 상승 논리 분석)
   🔹 실전 액션 플랜
   ▫️ 진입: (시가 갭 대응 전략 및 1차 진입 타점)
   ▫️ 대응: (주요 지지선 및 돌파 목표가)

3. 데이터 뱃지(Badge) 통일화: 
   [핵심 모멘텀 & VIP 수급] 바로 밑에 반드시 `▫️ [🤖프로그램: ~] [⚡체결강도: ~] [⚠️신용: ~] [📈수급: ~] [📊기본: ~]` 형태의 한 줄 요약 뱃지를 달아라. 없는 데이터는 생략해도 좋으나 뱃지 형태는 지켜라.
4. 압축 브리핑: 시장을 주도할 핵심 테마 내에서 총 3~4개의 핵심 추천 종목만 엄선하여 브리핑해라.
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
    
    # === 💡 [초강력 가독성 패치] 파이썬 자체 클렌징 엔진 ===
    print("📲 텔레그램 발송 중...")
    
    import re
    clean_briefing = final_briefing.replace('**', '')       
    clean_briefing = clean_briefing.replace('### ', '▶️ ')  
    clean_briefing = clean_briefing.replace('## ', '▶️ ')   
    
    clean_briefing = re.sub(r'<([^>]+)>', r'[\1]', clean_briefing)
    
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
        print("🔄 일반 텍스트 모드로 재전송을 시도합니다...")
        fallback_res = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': final_briefing})
        if fallback_res.status_code == 200: print("✅ 일반 텍스트 재전송 성공!")
        else: print("❌ 재전송도 실패했습니다.")
