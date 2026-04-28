import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# ==========================================
# 1. 환경 설정 및 인증
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
KST = datetime.timezone(datetime.timedelta(hours=9))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

print("🤖 [HYEOKS 리서치 센터] 수석 애널리스트 봇 가동 (전체 기능 완벽 복구본)...")

try: 
    genai.configure(api_key=GEMINI_API_KEY)
    # 💡 404 에러 원인 해결: 구형 패키지 호환성을 위해 '-latest' 태그를 명시합니다.
    model = genai.GenerativeModel('gemini-1.5-pro-latest')
    fast_model = genai.GenerativeModel('gemini-1.5-flash-latest')
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

def safe_generate_content(contents, is_fast=False):
    target_model = fast_model if is_fast else model
    for i in range(5): 
        try: 
            return target_model.generate_content(contents)
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                wait_time = 30 * (i + 1)
                print(f"⚠️ 구글 API 지연. {wait_time}초 대기 후 재시도...")
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
# 3. 구글 시트 연결 및 DB_스캐너 간단 브리핑 업데이트
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

    print("▶ [1단계] DB_스캐너 '대기중' 종목 실전 매매 브리핑 업데이트 (구글 시트 연동)...")
    db_sheet = doc.worksheet("DB_스캐너")
    db_data = db_sheet.get_all_values()
    
    sys_instruction = "기업의 일반적인 소개(무엇을 하는 회사인지 등)는 일절 금지. 차트 지표, 마스터 타점, 수급 데이터를 바탕으로 '현재 기술적 위치'와 '앞으로의 대응 전략'만을 60~70자 내외로 매우 짧고 날카롭게 작성할 것."
    
    for i, row in enumerate(db_data[1:], start=2):
        # DB_스캐너 J열(index 9)에 '대기중' 텍스트 확인
        if len(row) > 9 and "대기중" in str(row[9]):
            stock_name = row[0] if len(row) > 0 else "알수없음"
            print(f" - [{stock_name}] 간단 브리핑 작성 중...")
            
            prompt = f"""
            [{sys_instruction}]
            ■ 종목명: {stock_name}
            ■ 타점 위치: {row[8] if len(row) > 8 else ''}
            ■ 당일 수급: {row[11] if len(row) > 11 else ''}
            위 데이터를 바탕으로 실전 대응 전략을 1~2문장(70자 내외)으로 요약하라.
            """
            try:
                # 간단 브리핑은 1.5-flash-latest 모델 사용
                briefing_text = safe_generate_content(prompt, is_fast=True).text.strip()
                db_sheet.update_cell(i, 10, f"✅ [간단 브리핑] {briefing_text}")
                time.sleep(2) # 구글 API Limit 회피
            except Exception as e:
                print(f"[{stock_name}] 브리핑 에러: {e}")

    # ==========================================
    # 4. 주가데이터_보조에서 150개 풀 스캔 및 알파 종목 발굴
    # ==========================================
    print("\n▶ [2단계] 주가데이터_보조 상위 150개 풀에서 HYEOKS 알파 종목(단기/스윙) 발굴 시작...")
    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange, oil = clean_emojis(macro_data[1][4]), clean_emojis(macro_data[1][6]), clean_emojis(macro_data[1][7])
    news_keywords = clean_emojis("\n".join([f"{r[2]}({r[3]}회)" for r in doc.worksheet("뉴스_키워드").get_all_values()[1:6]]))
    
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]
    liquidity = get_global_liquidity_data()
    
    cands_list = []
    for r in tech_data:
        if len(r) < 21: continue
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        curr_p, chg, score_str, tajeom = str(r[2]).strip(), str(r[3]).strip(), str(r[8]).strip(), str(r[9]).strip()
        prog = str(r[20]).strip()
        
        try: num_score = int(re.findall(r'-?\d+', score_str)[0])
        except: num_score = 0
        
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자', tajeom): continue 
        
        info = f"종목:{name}({code}) | 현재가:{curr_p}원({chg}) | 퀀트점수:{num_score}점 | 타점:{tajeom} | 수급:{prog}"
        cands_list.append({'name': name, 'code': code, 'score': num_score, 'info': info, 'curr_p': int(curr_p.replace(',',''))})

    # HYEOKS 퀀트 점수 30점 이상 종목만 먼저 추림
    high_score_cands = [c for c in cands_list if c['score'] >= 30]
    
    if len(high_score_cands) < 10:
        cands_list.sort(key=lambda x: x['score'], reverse=True)
        pool_150 = cands_list[:150]
    else:
        high_score_cands.sort(key=lambda x: x['score'], reverse=True)
        pool_150 = high_score_cands[:150]

    pool_str = "\n".join([c['info'] for c in pool_150])

    pick_prompt = f"""
    당신은 대한민국 최고의 주식 트레이더이자 HYEOKS 퀀트 분석가입니다.
    아래는 HYEOKS 퀀트 점수가 검증된 최상위 150개 종목 리스트입니다.
    
    이 중에서 제미나이 AI의 직관과 종합적인 판단(숨겨진 모멘텀, 테마 강도, 수급)을 활용해 
    최고의 단기 1종목, 스윙 1종목을 과감히 발굴해 내십시오.

    1. 단기 슈팅 공략주: 오늘 수급이 몰리며 전고점 돌파를 목전에 둔 파괴력 있는 종목 1개.
    2. 스윙 플랫폼 공략주: 바닥에서 에너지를 응축하고 턴어라운드를 시도하는 안정적인 종목 1개.

    [상위 150개 종목 리스트]
    {pool_str}
    
    [출력 형식]
    반드시 아래 JSON 형식으로만 응답하세요. 다른 설명은 절대 추가하지 마세요.
    {{
        "short_term_code": "종목코드6자리",
        "swing_code": "종목코드6자리"
    }}
    """
    
    result_text = safe_generate_content(pick_prompt).text
    cleaned_text = result_text.replace('```json', '').replace('```', '').strip()
    picks_json = json.loads(cleaned_text)
    
    code_short = picks_json.get('short_term_code', '')
    code_mid = picks_json.get('swing_code', '')
    
    best_short = next((c for c in pool_150 if c['code'] == code_short), pool_150[0] if pool_150 else None)
    best_mid = next((c for c in pool_150 if c['code'] == code_mid), pool_150[1] if len(pool_150)>1 else best_short)

    print(f"🔥 최종 발굴 완료 -> 단기: {best_short['name'] if best_short else '없음'} / 스윙: {best_mid['name'] if best_mid else '없음'}\n")

    # ==========================================
    # 5. 시황 및 딥리딩 PDF 리포트 본문 생성
    # ==========================================
    print("▶ [3단계] 딥리딩 분석 및 PDF 리포트/텔레그램 발송을 시작합니다...")
    today_korean = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    status_txt = "코스피/코스닥 지지 (공격적 운영 가능)" 

    macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
아래 데이터를 바탕으로 '오늘의 시황 및 매크로 브리핑'을 1페이지 분량으로 상세히 작성하십시오. 정중한 존댓말(하십시오체)을 사용하십시오.
작성일: {today_korean}
매크로: 나스닥 {nasdaq}, 환율 {exchange}, 유가 {oil}, 국내증시 {status_txt}
유동성: {liquidity}
뉴스 키워드: {news_keywords}
(종목 추천 없이 시황과 트레이더의 스탠스만 서술하십시오.)"""
    
    market_summary = safe_generate_content(macro_prompt).text

    def generate_deep_report(st_type, best_cand):
        if not best_cand: return "", None
        
        vip = get_vip_deep_dive_data(best_cand['code'], KIS_TOKEN)
        news = get_target_stock_news(best_cand['code'])
        sub_title_prefix = "매물대 진공 구간 돌파 및 단기 슈팅 공략" if st_type == "short" else "에너지 응축 후 플랫폼 탈출 스윙 전략"

        detail_prompt = f"""귀하는 대한민국 최상위 1% 실전 트레이더들을 위한 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성하십시오. 한 리포트 내에서 말투가 바뀌지 않도록 정중한 존댓말(하십시오체)로 통일하십시오.

[입력 데이터]
종목 및 스캐너 판독: {best_cand['info']}
펀더멘털: {vip}
최신 뉴스: {news}

[HYEOKS 딥리딩 절대 지침 - 명심하십시오]
1. 분량 및 깊이 (매우 중요): 귀하의 전문적인 통찰력을 발휘하여 충분히 길고 논리적으로 1.5~2페이지 분량이 나오도록 상세히 서술하십시오. 
2. 시각적 차트 딥리딩 허용: 첨부된 차트 이미지를 면밀히 판독하여, 의미 있는 지지선/저항선, 매물대 등을 구체적인 '가격(원)'으로 과감하게 제시하십시오.
3. 실전 액션 플랜 강화: 구체적인 '진입 타점'과 명확한 '손절가'를 반드시 명시하십시오.
4. 가상계좌 규칙: 리포트 마지막 줄에만 [DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'} 형식으로 출력하십시오.

[출력 양식 (마크다운 유지)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{best_cand['name']} ({best_cand['code']})</p>
<p class="subtitle">{sub_title_prefix}: (소제목 작성)</p>
</div>

<div class="summary-box">
<strong>💡 HYEOKS 핵심 모멘텀 요약</strong><br><br>
(기업이 무엇을 하는 회사인지 등 일반적인 개요는 절대 쓰지 마십시오. 오직 차트 타점, 수급, 지지/저항 라인에 근거한 상승 모멘텀만 60~70자 내외의 1문장으로 작성하십시오.)
</div>

## 1. 매크로 유동성 및 내러티브 고찰
## 2. 시각적 차트 판독 및 스마트머니 딥리딩
## 3. 실전 타점 시나리오 및 리스크 관리 전략
[DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'}
"""
        img_path = f"temp_{best_cand['code']}.png"
        try:
            res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_cand['code']}.png", headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            with open(img_path, 'wb') as f: f.write(res.content)
            report_txt = safe_generate_content([detail_prompt, PIL.Image.open(img_path)]).text
            os.remove(img_path)
        except:
            report_txt = safe_generate_content(detail_prompt).text

        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', report_txt)
        if match:
            pick_data = {'name': best_cand['name'], 'code': best_cand['code'], 'target': int(match.group(1).replace(',', '')), 'stop': int(match.group(2).replace(',', '')), 'split': match.group(3) == 'O', 'curr': best_cand['curr_p']}
            report_txt = re.sub(r'\[DATA\].*', '', report_txt, flags=re.DOTALL).strip()
            
        # 💡 최종 2종목도 DB_스캐너에 요약본 업데이트
        try:
            briefing_summary = "✅ [리포트 발송 완료] "
            summary_match = re.search(r'<div class="summary-box">(.*?)</div>', report_txt, re.DOTALL)
            if summary_match:
                clean_text = re.sub(r'<[^>]+>', '', summary_match.group(1)).replace("💡 HYEOKS 핵심 모멘텀 요약", "").strip()
                briefing_summary += clean_text[:80] + "..." if len(clean_text) > 80 else clean_text
            else:
                briefing_summary += "텔레그램에서 상세 분석 리포트를 확인하십시오."

            for i, r in enumerate(db_data[1:], start=2):
                if len(r) > 9 and best_cand['code'] in str(r[2]):
                    db_sheet.update_cell(i, 10, briefing_summary)
                    break
        except Exception as e:
            print(f"⚠️ {best_cand['name']} 브리핑 덮어쓰기 실패: {e}")

        return report_txt, pick_data

    report_short, pick_short = generate_deep_report("short", best_short)
    if best_short: time.sleep(15)
    report_mid, pick_mid = generate_deep_report("mid", best_mid)

    # ==========================================
    # 6. 가상계좌 업데이트
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
    # 7. HTML 조립 및 PDF 생성 -> 구글 드라이브 -> 텔레그램
    # ==========================================
    css = "<style>body{font-family:'NanumGothic',sans-serif;line-height:1.8;padding:30px;color:#222;font-size:110%;}.broker-name{color:#1a365d;font-weight:bold;font-size:22px;margin-bottom:15px;border-bottom:3px solid #1a365d;padding-bottom:10px;}.stock-title{font-size:32px;font-weight:900;margin:0;}.subtitle{font-size:18px;color:#2b6cb0;font-weight:bold;}.summary-box{background:#f8fafc;padding:20px;border-left:5px solid #1a365d;margin:20px 0;border-radius:5px;}h2{color:#1a365d;border-bottom:2px solid #edf2f7;margin-top:30px;padding-bottom:8px;}p{margin-bottom:15px;word-break:keep-all;}img{width:100%;height:auto;border:1px solid #cbd5e0;border-radius:8px;}.chart-container{text-align:center;margin-top:40px;page-break-inside:avoid;}.page-break{page-break-before:always;}.alert-box{background:#fff5f5;padding:15px;border-left:5px solid #e53e3e;margin-bottom:20px;color:#c53030;font-weight:bold;}</style>"
    
    html = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>"
    html += "<div class='broker-name'>HYEOKS SECURITIES | DAILY MARKET REPORT</div>"
    html += f"<h2>글로벌 매크로 및 시황 요약</h2>{markdown.markdown(market_summary)}"

    if best_short:
        html += f"<div class='page-break'></div>{markdown.markdown(report_short)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_short['code']}.png'></div>"
        
    if best_mid:
        html += f"<div class='page-break'></div>{markdown.markdown(report_mid)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_mid['code']}.png'></div>"

    html += "</body></html>"

    pdf_file = f"HYEOKS_Daily_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    if GAS_WEB_APP_URL:
        print("▶ 구글 드라이브 업로드 진행 중...")
        with open(pdf_file, "rb") as f: 
            b64 = base64.b64encode(f.read()).decode('utf-8')
        try:
            res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_file, "base64": b64}, timeout=30).json()
            doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), f"https://drive.google.com/uc?id={res.get('id')}"], index=2)
            print("✅ 리포트_게시 시트 업데이트 완료!")
        except Exception as e: 
            print(f"⚠️ 구글 드라이브 업로드 실패: {e}")

    if TELEGRAM_BOT_TOKEN:
        print("▶ 텔레그램 발송 진행 중...")
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", 
                      files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "[HYEOKS] AI 심층 리서치 보고서"})
        print("✅ 텔레그램 발송 완료!")

    print(f"🎉 모든 작업이 성공적으로 완료되었습니다: {pdf_file}")

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
