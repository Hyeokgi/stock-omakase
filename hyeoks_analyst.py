import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# ==========================================
# 1. 환경 설정 및 인증
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
KST = datetime.timezone(datetime.timedelta(hours=9))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

print("🤖 [HYEOKS 리서치 센터] 애널리스트 V7.9 (리미트 해제 & 무결점 필터) 가동...")

try: 
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e: 
    print(f"❌ API 초기화 실패: {e}"); exit(1)

def clean_emojis(text):
    emojis = ['🚨','💡','💎','🔥','📊','📈','📉','🎯','🛡️','⏰','⏸️','🐎','🌟','🔒','🔴','🔵','⚪','🟢','🟡','👑','⚡','🚀','👀','⏳','🔻','🔺','➖', '🛢️', '💵', '🇺🇸']
    for e in emojis: text = text.replace(e, '')
    return text.replace('  ', ' ').strip()

# ==========================================
# 2. 보조 데이터 수집 함수
# ==========================================
def get_target_stock_news(code):
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        headers = {'User-Agent': 'Mozilla/5.0'}
        res = requests.get(url, headers=headers, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_list = []
        for a_tag in soup.select('.title a')[:3]: news_list.append(f"- {a_tag.text.strip()}")
        return clean_emojis("\n".join(news_list)) if news_list else "당일 해당 종목의 개별 특징주 뉴스는 없습니다."
    except Exception: return "개별 뉴스 데이터를 수집하지 못했습니다."

def get_vip_deep_dive_data(code, kis_token):
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET): return "PER: N/A / PBR: N/A"
    req = requests.Session()
    headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"}
    try:
        headers["tr_id"] = "FHKST01010100"
        res = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                      headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
        if res.get("rt_cd") == "0":
            out = res.get("output", {})
            return f"PER: {out.get('per', 'N/A')} / PBR: {out.get('pbr', 'N/A')}"
    except: pass
    return "PER: N/A / PBR: N/A"

def safe_generate_content(contents):
    # 💡 깔끔하게 5번만 재시도하고, 안 되면 쿨하게 포기하는 원상 복구 로직
    for i in range(5): 
        try: 
            return client.models.generate_content(model='gemini-2.5-pro', contents=contents)
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                wait_time = 30 * (i + 1)
                print(f"⚠️ 구글 API 지연/할당량초과. {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
            else: raise e 
    raise Exception("❌ 구글 서버 할당량 초과 또는 무응답으로 최종 실패")

def get_global_liquidity_data():
    indicators = {"WTREGEN": "TGA 잔고", "RRPONTSYD": "역레포 잔고", "BAMLH0A0HYM2": "하이일드 스프레드", "WALCL": "연준 총자산", "M2SL": "M2 통화량"}
    report = []
    for sid, name in indicators.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=2"
            res = requests.get(url, timeout=5).json()
            if 'observations' in res and len(res['observations']) >= 2:
                latest, prev = res['observations'][0], res['observations'][1]
                l_val, p_val = float(latest['value']), float(prev['value'])
                diff = l_val - p_val
                trend = f"증가 (+{diff:,.2f})" if diff > 0 else (f"감소 ({diff:,.2f})" if diff < 0 else "변동없음")
                report.append(f"- {name}: {l_val:,.2f} ({trend})")
        except: pass
    return "\n".join(report) if report else "유동성 데이터 수집 불가합니다."

# ==========================================
# 3. 데이터 로드 및 종목 필터링
# ==========================================
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)

    KIS_TOKEN = ""
    try:
        for row in doc.worksheet("⚙️설정").get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN": KIS_TOKEN = row[1]; break
    except: pass

    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange, oil = clean_emojis(macro_data[1][4]), clean_emojis(macro_data[1][6]), clean_emojis(macro_data[1][7])
    news_keywords = clean_emojis("\n".join([f"{r[2]}({r[3]}회)" for r in doc.worksheet("뉴스_키워드").get_all_values()[1:6]]))
    
    # 💡 [V7.9 수정] 40행 제한 해제 (전체 종목 스캔)
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]
    liquidity = get_global_liquidity_data()
    
    valid_short, valid_mid = [], []
    is_down = False 

    SHORT_KEYWORDS = ["[테마대장]", "[핵심]", "신고가 돌파", "종가베팅", "[개별주] 상한가", "[후발주] 상한가", "당일 주도주", "독자 모멘텀", "테마 추종"]
    MID_KEYWORDS = ["플랫폼", "눌림", "수급 유입", "분할매수", "이평수렴", "[관심]", "방어", "[우량]"]

    for r in tech_data:
        # 인덱스 20번(프로그램)까지 있으므로 최소 21개의 컬럼 데이터가 있어야 함
        if len(r) < 21: continue
        
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        curr_p, chg, score_str, tajeom = str(r[2]).strip(), str(r[3]).strip(), str(r[8]).strip(), str(r[9]).strip()
        shadow, vol, prog = str(r[14]).strip(), str(r[18]).strip(), str(r[20]).strip()
        
        if "주의장세" in tajeom: is_down = True
        
        try:
            match = re.search(r'(-?\d+)점', score_str)
            num_score = int(match.group(1)) if match else 0
        except: num_score = 0

        # 💡 [V7.9 수정] 오탐지 방지를 위해 '3년적자'로 명확히 필터링
        if re.search(r'상한가|하한가|29\.|30\.', chg): continue
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자', tajeom): continue 
        if "저항 출회" in shadow or "윗꼬리" in tajeom: continue 
        if "관망" in tajeom and "관심" not in tajeom: continue

        info = clean_emojis(f"종목:{name}({code}), 현재가:{curr_p}원 ({chg}), 타점:{tajeom}, 퀀트점수:{score_str}, 테마:{r[19].strip()}, 프로그램:{prog}, 거래량:{vol}")
        cand = {'name': name, 'code': code, 'tajeom': tajeom, 'info': info, 'curr_p': int(curr_p.replace(',','')), 'score': num_score}

        is_short = any(kw in tajeom for kw in SHORT_KEYWORDS)
        is_mid = any(kw in tajeom for kw in MID_KEYWORDS)

        if is_short and num_score >= 40: 
            valid_short.append(cand)
        elif is_mid and num_score >= 35: 
            valid_mid.append(cand)

    status_txt = "코스피/코스닥 20일선 이탈 (보수적 접근)" if is_down else "코스피/코스닥 지지 (공격적 운영 가능)"

    # ==========================================
    # 4. 리포트 본문 생성 엔진
    # ==========================================
    today_korean = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    
    missing_status = ""
    if not valid_short and not valid_mid:
        missing_status = "금일은 단기 돌파 및 중기 스윙 매수 기준(최소 퀀트 점수)에 부합하는 종목이 전멸한 상태입니다. 철저한 관망과 현금 보존을 강력히 권고하십시오."
    elif not valid_short:
        missing_status = "금일 단기 돌파 기준에 부합하는 종목이 부재합니다. 단기 매매는 쉬어가고 시황에 집중하도록 안내하십시오."
    elif not valid_mid:
        missing_status = "금일 중기 스윙 기준에 부합하는 종목이 부재합니다. 스윙 매매는 쉬어가고 시황에 집중하도록 안내하십시오."
    else:
        missing_status = "금일 단기 및 중기 모두 조건에 부합하는 주도주가 포착되었습니다. 시장의 전반적인 흐름을 먼저 요약하십시오."

    macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
아래 데이터를 바탕으로 '오늘의 시황 및 매크로 브리핑'을 1페이지 분량으로 상세히 작성하십시오. 모든 문장은 정중한 존댓말(하십시오체)로 통일하십시오.

[기본 정보]
작성일: {today_korean} (리포트 서두에 반드시 이 날짜를 기재하십시오.)

[시장 상태 알림]
{missing_status}

[입력 데이터]
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange}, 유가 {oil}, 국내증시 {status_txt}
글로벌 유동성 현황:
{liquidity}
뉴스 키워드:
{news_keywords}

[출력 형식]
(이곳에는 특정 종목을 추천하지 마십시오. 오직 위 데이터를 분석하여 시장의 방향성과 트레이더가 취해야 할 스탠스를 논리적으로 깊이 있게 서술하십시오.)
"""
    market_summary = safe_generate_content(macro_prompt).text

    def generate_hyeoks_report(st_type, cands):
        if not cands: return "", "000000", None 
        
        # 💡 [V8.4 패치] 직관적이고 보편적인 3대 핵심 기법 네이밍 적용
pick_prompt = f"""귀하는 HYEOKS 리서치 센터의 종목 선정 위원회입니다.
현재 귀하에게 하달된 임무는 '{st_type.upper()}' 전략에 맞는 대장주를 찾는 것입니다.
아래 명시된 [HYEOKS 3대 핵심 기법]을 후보 리스트의 데이터(info)와 대조하여, 현재 전략에 가장 완벽하게 부합하는 최상위 1종목의 '6자리 종목코드'만 출력하십시오.

[HYEOKS 3대 핵심 기법]
1. [S-1] 진공 매물대 주도주 돌파 전략 (단기 극대화): 
   - 조건: 상단 매물대가 비어있는 '진공 구간' 돌파, 당일 테마 1위 대장주.
   - 수치: 폭발적인 거래량과 프로그램 대량 유입 필수. (윗꼬리가 긴 종목은 배제)
2. [M-1] 이평선 수렴 및 에너지 응축 눌림목 전략 (초단기 스윙): 
   - 조건: 강한 상승(기준봉) 이후 깃발 패턴이나 이평선(5일/20일) 부근에서 지지받는 종목.
   - 수치: 거래량이 평소 대비 극도로 마른 상태(V.에너지응축)여야 하며, 적자 기업은 무조건 배제.
3. [M-2] 플랫폼 탈출 및 주도 테마 순환매 전략 (중기 스윙):
   - 조건: 주도 테마 내에서 긴 기간 박스권(플랫폼)을 형성하다가 에너지를 응축하고 탈출하려는 우량주. 혹은 대장주 상승 후 후발주 타점.

[선정 가이드라인]
- 만약 '{st_type}'가 'short'라면, [S-1] 돌파 기법과 [M-1] 단기 눌림 기법에 가장 잘 맞는 종목을 최우선으로 찾으십시오.
- 만약 '{st_type}'가 'mid'라면, [M-2] 순환매 기법과 [M-1] 깊은 눌림 기법에 부합하는 종목을 찾으십시오.

[후보 리스트]
{chr(10).join([c['info'] for c in cands])}
"""
        
        # 💡 [V7.9 수정] AI 정규식 예외 처리 방어 로직 (기존 코드 유지)
        result_text = safe_generate_content(pick_prompt).text
        match = re.search(r'\d{6}', result_text)
        target_code = match.group() if match else cands[0]['code']
        
        best = next((c for c in cands if c['code'] == target_code), cands[0])
        
        vip = get_vip_deep_dive_data(best['code'], KIS_TOKEN)
        news = get_target_stock_news(best['code'])
        sub_title_prefix = "매물대 진공 구간 돌파 및 단기 슈팅 공략" if st_type == "short" else "에너지 응축 후 플랫폼 탈출 스윙 전략"

        detail_prompt = f"""귀하는 대한민국 최상위 1% 실전 트레이더들을 위한 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성하십시오. 한 리포트 내에서 말투가 바뀌지 않도록 모든 문장을 정중하고 격조 있는 존댓말(하십시오체)로 완전히 통일하십시오.

[입력 데이터]
종목 및 스캐너 판독: {best['info']}
펀더멘털: {vip}
최신 뉴스: {news}
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange}, 유가 {oil}, 국내증시 {status_txt}

[HYEOKS 딥리딩 절대 지침 - 명심하십시오]
1. 분량 및 깊이 (매우 중요): 귀하의 전문적인 통찰력을 발휘하여 충분히 길고 논리적으로 1.5~2페이지 분량이 나오도록 상세히 서술하십시오. 
2. 시각적 차트 딥리딩 허용: 첨부된 차트 이미지를 면밀히 판독하여, 의미 있는 지지선/저항선, 매물대, 라운드 피겨 등을 구체적인 '가격(원)'으로 과감하게 제시하십시오.
3. 실전 액션 플랜 강화: 두루뭉술한 조언을 배제하고, 차트 판독을 기반으로 한 구체적인 '진입 타점'과 명확한 '손절가'를 반드시 명시하십시오.
4. 가상계좌 규칙: 리포트 마지막 줄에만 [DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'} 형식으로 출력하십시오.
5. 프로그램 수급 연계: 제공된 [프로그램] 현황을 분석하여 주가 상승을 어떻게 뒷받침하는지 서술하십시오. 

[출력 양식 (마크다운 유지)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{best['name']} ({best['code']})</p>
<p class="subtitle">{sub_title_prefix}: (소제목 작성)</p>
</div>

<div class="summary-box">
<strong>Company Brief & 펀더멘털 요약 | HYEOKS 퀀트 데스크</strong><br><br>
(기업 개요 및 펀더멘털, 뉴스 요약)
</div>

## 1. 매크로 유동성 및 내러티브 고찰

## 2. 시각적 차트 판독 및 스마트머니 딥리딩
(첨부된 차트 이미지를 기반으로 지지/저항 라인을 구체적인 가격으로 명시하고, 거래량과 프로그램 수급을 엮어 세력의 매집 의도를 깊이 있게 분석하십시오.)

## 3. 실전 타점 시나리오 및 리스크 관리 전략
(명확한 진입 팁과, 구체적인 가격대(원)를 기반으로 한 손절 기준점을 제시하십시오.)

[DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'}
"""
        # 💡 이미지와 프롬프트를 깔끔하게 묶어서 한 번에 전송 (롤백)
        if best['code'] != "000000":
            img_path = f"temp_{best['code']}.png"
            try:
                res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best['code']}.png", headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
                with open(img_path, 'wb') as f: f.write(res.content)
                report_txt = safe_generate_content([detail_prompt, PIL.Image.open(img_path)]).text
                os.remove(img_path)
            except:
                report_txt = safe_generate_content(detail_prompt).text
        else:
            report_txt = safe_generate_content(detail_prompt).text
        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', report_txt)
        if match:
            pick_data = {'name': best['name'], 'code': best['code'], 'target': int(match.group(1).replace(',', '')), 'stop': int(match.group(2).replace(',', '')), 'split': match.group(3) == 'O', 'curr': best['curr_p']}
            report_txt = re.sub(r'\[DATA\].*', '', report_txt, flags=re.DOTALL).strip()
            
        return report_txt, best['code'], pick_data

    # 실행
    report_short, code_short, pick_short = generate_hyeoks_report("short", valid_short)
    if valid_short: time.sleep(15)
    report_mid, code_mid, pick_mid = generate_hyeoks_report("mid", valid_mid)

    # ==========================================
    # 5. 가상계좌 업데이트
    # ==========================================
    def update_portfolio(picks):
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")
        today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        
        rows = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
        if len(rows) <= 1 or rows[0][0] != "종목명":
            hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers]); rows = [headers]

        new_rows, closed_rows = [], []
        for r in rows[1:]:
            if len(r) < 10 or not r[0]: continue
            name, code = r[0], r[1].replace("'", "").strip().zfill(6)
            buy_p, amt, t_p, s_p = int(float(r[2].replace(',',''))), int(float(r[3].replace(',',''))), int(float(r[7].replace(',',''))), int(float(r[8].replace(',','')))
            try: curr_p = int(requests.get(f"https://m.stock.naver.com/api/stock/{code}/basic", verify=False, timeout=3).json()['closePrice'].replace(',',''))
            except: curr_p = buy_p
            
            rtn = (curr_p - buy_p) / buy_p
            reason = ""
            if curr_p >= t_p: reason = "목표가 도달"
            elif curr_p <= s_p: reason = "손절가 이탈"
            elif str(r[9]).strip() == "매도": reason = "수동매도"
            
            if reason: closed_rows.append([name, buy_p, curr_p, f"{rtn*100:.2f}%", today, f"{'승리' if rtn>0 else '패배'} ({reason})"])
            else: new_rows.append([name, f"'{code}", buy_p, amt, curr_p, f"{rtn*100:.2f}%", r[6], t_p, s_p, ""])

        for p in picks:
            if not p or p['code'] == "000000": continue
            idx = next((i for i, v in enumerate(new_rows) if v[0] == p['name']), -1)
            if idx != -1:
                if p['split']:
                    total_amt = new_rows[idx][3] + 1000000
                    avg_p = int(total_amt / ((new_rows[idx][3]/new_rows[idx][2]) + (1000000/p['curr'])))
                    new_rows[idx][2], new_rows[idx][3], new_rows[idx][4] = avg_p, total_amt, p['curr']
            else:
                new_rows.append([p['name'], f"'{p['code']}", p['curr'], 1000000, p['curr'], "0.00%", today, p['target'], p['stop'], ""])

        hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers] + new_rows, value_input_option="USER_ENTERED")
        if closed_rows:
            if not closed_sheet.get_all_values(): closed_sheet.update(range_name="A1", values=[["종목명", "매입단가", "매도단가", "수익률", "매도일자", "결과"]])
            for cr in closed_rows: closed_sheet.append_row(cr)

    update_portfolio([pick_short, pick_mid])

    # ==========================================
    # 6. HTML 조립 및 PDF 생성
    # ==========================================
    css = "<style>body{font-family:'NanumGothic',sans-serif;line-height:1.8;padding:30px;color:#222;font-size:110%;}.broker-name{color:#1a365d;font-weight:bold;font-size:22px;margin-bottom:15px;border-bottom:3px solid #1a365d;padding-bottom:10px;}.stock-title{font-size:32px;font-weight:900;margin:0;}.subtitle{font-size:18px;color:#2b6cb0;font-weight:bold;}.summary-box{background:#f8fafc;padding:20px;border-left:5px solid #1a365d;margin:20px 0;border-radius:5px;}h2{color:#1a365d;border-bottom:2px solid #edf2f7;margin-top:30px;padding-bottom:8px;}p{margin-bottom:15px;word-break:keep-all;}img{max-width:90%;border:1px solid #cbd5e0;border-radius:8px;}.chart-container{text-align:center;margin-top:40px;page-break-inside:avoid;}.page-break{page-break-before:always;}.alert-box{background:#fff5f5;padding:15px;border-left:5px solid #e53e3e;margin-bottom:20px;color:#c53030;font-weight:bold;}</style>"
    
    html = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>"
    
    html += "<div class='broker-name'>HYEOKS SECURITIES | DAILY MARKET REPORT</div>"
    
    if not valid_short and not valid_mid:
        html += "<div class='alert-box'>[전략 부재] 금일 시스템 필터를 통과한 추천 종목이 없습니다. 관망을 강력히 권장합니다.</div>"
    elif not valid_short:
        html += "<div class='alert-box'>[단기 전략 부재] 금일 단기 돌파 기준 부합 종목이 없어 중기 리포트만 발행됩니다.</div>"
    elif not valid_mid:
        html += "<div class='alert-box'>[중기 전략 부재] 금일 중기 스윙 기준 부합 종목이 없어 단기 리포트만 발행됩니다.</div>"
        
    html += f"<h2>글로벌 매크로 및 시황 요약</h2>{markdown.markdown(market_summary)}"

    if valid_short:
        html += f"<div class='page-break'></div>{markdown.markdown(report_short)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_short}.png'></div>"
        
    if valid_mid:
        html += f"<div class='page-break'></div>{markdown.markdown(report_mid)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_mid}.png'></div>"

    html += "</body></html>"

    pdf_file = f"HYEOKS_Daily_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    if GAS_WEB_APP_URL:
        with open(pdf_file, "rb") as f: 
            b64 = base64.b64encode(f.read()).decode('utf-8')
        try:
            res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_file, "base64": b64}, timeout=30).json()
            doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), f"https://drive.google.com/uc?id={res.get('id')}"], index=2)
        except: pass

    if TELEGRAM_BOT_TOKEN:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", 
                      files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "[HYEOKS] AI 심층 리서치 보고서"})

    print(f"✅ 리포트 발행 완료: {pdf_file}")

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
