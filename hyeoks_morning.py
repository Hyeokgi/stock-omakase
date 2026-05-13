import os, requests, datetime, time, json, re
from bs4 import BeautifulSoup
from google import genai
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3
# 👇 [추가할 부분] 소켓 모듈을 불러와서 전역 타임아웃을 강제로 설정합니다.
import socket
socket.setdefaulttimeout(30) # 구글 시트 등 모든 통신이 30초 이상 지연되면 강제로 끊고 에러를 뱉음

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))

# ==========================================
# 💡 KIS API 환경 변수
# ==========================================
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

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

def search_code_from_naver(stock_name):
    try:
        url = "https://m.stock.naver.com/api/search/all"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, params={'keyword': stock_name}, verify=False, timeout=3).json()
        if res.get('result') and res['result'].get('stocks'):
            return res['result']['stocks'][0]['itemCode']
    except: pass
    return None

def get_vip_deep_dive_data(code, kis_token):
    vip = {"펀더멘털": "N/A"}
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET): return "⚠️ KIS API 토큰 없음"
    req = requests.Session()
    headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"}
    try:
        headers["tr_id"] = "FHKST01010100"
        res_price = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, timeout=3).json()
        if res_price.get("rt_cd") == "0":
            output = res_price.get("output", {})
            per, pbr = output.get("per", "N/A"), output.get("pbr", "N/A")
            vip["펀더멘털"] = f"PER: {per} / PBR: {pbr}"
    except: pass
    return f"📊 {vip['펀더멘털']}"

def get_us_market_summary():
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        print("📰 네이버 주요 뉴스 수집 중...")
        res = requests.get("https://finance.naver.com/news/mainnews.naver", headers=headers, verify=False, timeout=5)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_items = []
        for dl in soup.find_all('dl'):
            subject = dl.find(['dt', 'dd'], {'class': 'articleSubject'})
            if subject and subject.find('a'): news_items.append(f"- {subject.find('a').text.strip()}")
            if len(news_items) >= 15: break
        return "글로벌 및 국내 주요 금융 뉴스 헤드라인", "\n".join(news_items)
    except Exception as e: return f"뉴스 수집 에러: {e}", ""

def get_yesterday_korean_context():
    print("🇰🇷 어제 한국장 퀀트 타겟 종목 및 심층 데이터 수집 중...")
    try:
        gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        if gcp_creds_str and len(gcp_creds_str.strip()) > 10:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(gcp_creds_str), scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
            
        client = gspread.authorize(creds)
        doc = client.open_by_url(SHEET_URL)
        
        kis_token = ""
        try:
            for row in doc.worksheet("⚙️설정").get_all_values():
                if len(row) >= 2 and row[0] == "KIS_TOKEN":
                    kis_token = row[1]; break
        except: pass

        name_to_code = {}
        try:
            for row in doc.worksheet("기업정보").get_all_values()[1:]:
                if len(row) >= 3: name_to_code[str(row[0]).strip()] = str(row[2]).strip().zfill(6)
        except: pass

        scanner_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]

    except Exception as e: return f"🚨 파싱 오류: {e}"

    if not scanner_data or len(scanner_data[0]) < 21: return "구글 시트 데이터가 비어있습니다."

    valid_candidates = []
    for r in scanner_data:
        if len(r) > 20 and r[0]:
            name, code_str = str(r[0]).strip(), str(r[1]).replace("'", "").strip()
            current_price, score_str, tajeom = str(r[2]).strip(), str(r[8]).strip(), str(r[9]).strip()
            theme, vol_status, program_text = str(r[19]).strip(), str(r[18]).strip(), str(r[20]).strip()
            
            if name == "시장관망" or "000000" in code_str: continue
            if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|적자', tajeom): continue
            if "저항 출회" in str(r[14]) or "윗꼬리" in tajeom: continue
            if "관망" in tajeom and "관심" not in tajeom: continue

            try: num_score = int(re.search(r'(-?\d+)점', score_str).group(1)) if re.search(r'(-?\d+)점', score_str) else 0
            except: num_score = 0
            
            if num_score < 35: continue

            valid_candidates.append({
                'name': name,
                'code': code_str,
                'price': current_price,
                'theme': theme,
                'tajeom': tajeom,
                'score_str': score_str,
                'num_score': num_score,
                'vol_status': vol_status,
                'program_text': program_text
            })

    if not valid_candidates:
        return "🚨 [전일 기준 부합 종목 부재]\n시스템의 엄격한 'HYEOKS 퀀트 총합 스코어(최소 35점 이상)' 및 '적자 기업 제외' 필터를 통과한 주도주가 없습니다. 무리한 매매를 지양하고 시장 관망을 유지하십시오."

    valid_candidates.sort(key=lambda x: x['num_score'], reverse=True)

    picks_info = []
    for cand in valid_candidates[:10]:
        code = name_to_code.get(cand['name']) or search_code_from_naver(cand['name'])
        vip_data = "VIP 데이터 확인불가"
        if code:
            print(f"🔍 [{cand['name']} ({code})] VIP 데이터 수집 중...")
            vip_data = get_vip_deep_dive_data(code, kis_token)

        picks_info.append(f"▪️ [{cand['name']}] 종가: {cand['price']}원 | 테마: {cand['theme']}\n  [마스터타점] {cand['tajeom']} ({cand['score_str']})\n  [프로그램] {cand['program_text']}\n  [거래량] {cand['vol_status']}\n  [펀더멘털] {vip_data}")

    return "\n\n".join(picks_info)

def generate_morning_briefing(market_data, news_data, kor_context, liquidity_data):
    print("🤖 AI 매크로 분석 및 능동형 리포트 작성 중...")
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    is_empty_market = "기준 부합 종목 부재" in kor_context
    
    if is_empty_market:
        stock_prompt_instruction = """
   [파트 2: 종목별 심층 분석 (파란색 뱃지)]
   🚨 [관망 권고 및 리스크 관리]
   ▫️ (전일 시장에서 강력한 주도주나 수급 유입 종목이 부재했음을 알리고, 현금 보존의 중요성과 다음 타점을 기다려야 하는 이유를 트레이더 관점에서 정중하게 서술하십시오.)
   ▫️ (억지로 특정 종목을 추천하거나 지어내지 마십시오.)
"""
    else:
        stock_prompt_instruction = """
   [파트 2: 당일 주도주 능동 선정 및 심층 분석 (파란색 뱃지)]
   🚨 (핵심 지시: 귀하에게 제공된 '[어제 포착된 퀀트 필터 통과 후보 풀]'을 면밀히 분석하십시오. 단순 나열이 절대 아닙니다! 
   오늘 분석한 매크로 및 뉴스 내러티브와 완벽히 일치하며, 당장 오늘 아침 투자하기에 '가장 완벽한 승률'을 보여줄 것으로 판단되는 종목을 단기/스윙 상관없이 1개~3개만 귀하가 직접 '엄선'하십시오. 이미 시세가 과열된 고가(단기) 종목보다는, 횡보를 끝내고 박스권을 갓 탈출하려는 '급등 초입' 단계의 종목을 최우선으로 엄선하십시오.
   선택받지 못한 종목은 과감히 버리십시오.)
   
   🟦 [귀하가 엄선한 종목명]
   🔹 핵심 모멘텀 & 타점 딥리딩
   ▫️ [🤖프로그램: 제공된 프로그램 데이터] [📈거래량: 제공된 거래량 배지] [🎯타점: 제공된 마스터타점 및 점수]
   ▫️ (왜 이 종목을 오늘 아침 최우선 픽으로 '선정'했는지, 제공된 데이터와 뉴스를 융합하여 세력의 의도와 주도주 논리를 날카롭게 분석하십시오.)
   🔹 실전 액션 플랜
   ▫️ 진입: (마스터타점의 전략(돌파/눌림/종가베팅 등)에 맞는 시가 갭 및 장중 대응 전략)
   ▫️ 대응: (리스크 관리 및 비중 조절 팁)
"""

    prompt = f"""너는 대한민국 최상위 1% 실전 트레이더를 위한 HYEOKS 리서치 센터의 헤드 퀀트 매니저(Head Quant Manager)야.
아래의 데이터를 융합하여 오늘 아침 장 개장 전 트레이더가 읽을 '유연하고 통찰력 있는 모닝 브리핑 리포트'를 작성해라.

[글로벌 매크로 유동성 지표 (FRED)]
{liquidity_data}

[밤사이 글로벌/국내 주요 뉴스]
{news_data}

[어제 포착된 퀀트 필터 통과 후보 풀 (최대 10종목)]
{kor_context}

[HYEOKS 리서치 작성 지침 - 가독성 및 보고서 통일성 절대 규칙]
1. 🚨 볼드체 전면 금지: 텍스트에 별표 기호(**)를 절대 쓰지 마라. 모바일 가독성을 해친다.
2. 보고서 계층 구조(Hierarchy) 및 전용 아이콘 엄수 (임의 변형 금지):
   
   [파트 1: 종합 시황 및 매크로 (녹색 뱃지)]
   🟩 [HYEOKS 매크로 & 뉴스 종합 시황]
   🟢 유동성 환경 분석
   ▫️ (FRED 지표가 증시 자금에 미치는 영향 짧게 해석)
   🟢 핵심 뉴스 & 시장 내러티브 진단
   ▫️ (제공된 뉴스들을 깊이 있게 분석하여 오늘 시장의 쏠림 방향성을 분석해라.)
   {stock_prompt_instruction}

3. 🚨 데이터 뱃지(Badge) 작성 절대 규칙:
   - 내가 제공한 [프로그램], [거래량], [마스터타점] 텍스트를 한 글자도 바꾸지 말고 괄호 안에 그대로 넣어라. 
   - 없는 데이터를 억지로 지어내지 마라.

4. 군더더기 인사말은 생략.
"""
    for i in range(10):
        try:
            response = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            return response.text
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower(): 
                print(f"⚠️ 구글 서버 혼잡. {30 * (i + 1)}초 대기 후 재시도...")
                time.sleep(30 * (i + 1))
            else: raise e
    raise Exception("서버 응답 불가")

# ==========================================
# 💡 [신규] 새벽 6시 일괄 브리핑 굽기 (Batch Process)
# ==========================================
def parse_ai_json(text):
    try:
        clean_text = text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_text)
    except:
        try:
            t_match = re.search(r'"target_price"\s*:\s*(\d+)', text)
            s_match = re.search(r'"stop_loss"\s*:\s*(\d+)', text)
            b_match = re.search(r'"briefing"\s*:\s*"([^"]+)"', text)
            return {
                "briefing": b_match.group(1) if b_match else "분석 결과 텍스트 오류",
                "target_price": int(t_match.group(1)) if t_match else 0,
                "stop_loss": int(s_match.group(1)) if s_match else 0
            }
        except:
            return {"briefing": "응답 오류", "target_price": 0, "stop_loss": 0}

def batch_generate_briefings():
    print("🌅 [새벽 6시 배치] 전 종목 AI 브리핑 일괄 굽기 시작 (gemini-2.5-flash)...")
    try:
        gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        if gcp_creds_str and len(gcp_creds_str.strip()) > 10:
            creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(gcp_creds_str), scope)
        else:
            creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
            
        client_g = gspread.authorize(creds)
        doc = client_g.open_by_url(SHEET_URL)
        helper_sheet = doc.worksheet("주가데이터_보조")
        
        try: brief_db_sheet = doc.worksheet("브리핑_기록")
        except: brief_db_sheet = doc.add_worksheet(title="브리핑_기록", rows="200", cols="5")
        
        tech_data = helper_sheet.get_all_values()[1:]
        cands_list = []
        
        for r in tech_data:
            if len(r) < 21: continue
            name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
            curr_p, tajeom_raw = str(r[2]).strip(), str(r[9]).strip()
            seed_tag = str(r[25]).strip() if len(r) > 25 else "NORMAL"
            
            # 🚨 필터 대폭 개방: '관망'이 포함되어 있어도 150위 안이면 무조건 굽도록 조건 완화
            if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자', tajeom_raw): continue 
            tajeom_clean = tajeom_raw.split('⚠️')[0].strip().split('🎯')[0].strip()
            
            cands_list.append({
                'name': name, 'code': code, 'curr_p': curr_p, 'tajeom': tajeom_clean, 'type': seed_tag
            })

        client_gemini = genai.Client(api_key=GEMINI_API_KEY)
        results_to_save = [["종목코드", "브리핑", "목표가", "손절가"]]
        
        print(f"총 {len(cands_list)}개 타점 유효 종목 브리핑 생성 시작...")
        
        for cand in cands_list:
            print(f" - [{cand['name']}] 브리핑 굽는 중...")
            
            if cand['type'] == "SEED":
                guide_text = "중장기 모아가기 전략. 차트상 60일선, 기준봉 시가, 쌍바닥 등 의미있는 넉넉한 하단 지지선을 '손절가'로 명확한 가격(원)으로 제시하고, 하락시 2차 분할매수 시나리오를 제시하라."
            else:
                guide_text = "단기 스윙 전략. 손익비 1.5배 이상의 타이트한 '목표가'와 '손절가'를 명확한 가격(원)으로 제시하라."
                
            prompt = f"""
            당신은 수석 퀀트 애널리스트입니다. 기업 소개는 생략하고 기술적 위치와 차트 대응 전략만 60자 내외로 매우 짧게 요약하세요.
            ■ 종목명: {cand['name']} 
            ■ 현재가: {cand['curr_p']}원 
            ■ 타점배지: {cand['tajeom']}
            💡 가이드: {guide_text}
            
            반드시 아래 JSON 형식으로만 대답하십시오. (target_price와 stop_loss는 숫자만 입력)
            {{
                "briefing": "전략 요약",
                "target_price": 150000,
                "stop_loss": 135000
            }}
            """
            
            # 🔥 [무료버전 429 돌파 로직] 최대 10번 재시도, 실패 시 30초~90초 대기
            success = False
            for retry in range(10):
                try:
                    res = client_gemini.models.generate_content(model='gemini-2.5-flash', contents=prompt)
                    data = parse_ai_json(res.text)
                    
                    b_text = data.get('briefing', '분석 완료')
                    if not b_text.startswith("✅"): b_text = f"✅ [간단 브리핑] {b_text}"
                    
                    t_price = f"{int(data.get('target_price', 0)):,}원" if data.get('target_price') else "관망"
                    s_price = f"{int(data.get('stop_loss', 0)):,}원" if data.get('stop_loss') else "관망"
                    
                    results_to_save.append([f"'{cand['code']}", b_text, t_price, s_price])
                    success = True
                    time.sleep(2) # 기본 딜레이 (분당 15회 요청 제한 방어)
                    break
                except Exception as e:
                    if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                        wait_time = 30 * (retry + 1)
                        print(f"   ⚠️ 구글 서버 지연 감지. {wait_time}초 대기 후 재시도... ({retry+1}/10)")
                        time.sleep(wait_time)
                    else:
                        print(f"   에러: {e}")
                        break
            
            if not success:
                results_to_save.append([f"'{cand['code']}", "⚠️ 분석 일시 오류", "0", "0"])

        # 시트 덮어쓰기
        brief_db_sheet.clear()
        brief_db_sheet.update(range_name="A1", values=results_to_save, value_input_option="USER_ENTERED")
        print("🌅 새벽 6시 배치 브리핑 굽기 완료! (브리핑_기록 시트 저장 완료)")
    
    except Exception as e:
        print(f"❌ 배치 작업 에러: {e}")

# ==========================================
# 메인 실행 트리거
# ==========================================
if __name__ == "__main__":
    now_obj = datetime.datetime.now(KST)
    
    # 💡 [분기] 새벽 6시면 일괄 브리핑 생성(Batch), 그 외 시간이면 텔레그램 시황 발송
    if now_obj.hour == 6: # now_obj.hour == 6 대신 True로 변경
        batch_generate_briefings()
    else:
        print("🚀 HYEOKS 능동형 모닝 브리핑 시스템 가동 시작...")
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
