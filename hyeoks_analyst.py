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

print("🤖 [HYEOKS 리서치 센터] 매크로 융합 2.5-Pro V7.6 최종본 가동...")

try: 
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e: 
    print(f"❌ API 초기화 실패: {e}"); exit(1)

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
        return "\n".join(news_list) if news_list else "당일 해당 종목의 개별 특징주 뉴스는 없습니다."
    except Exception: return "개별 뉴스 데이터를 수집하지 못했습니다."

def get_vip_deep_dive_data(code, kis_token):
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET): return "📊 PER: N/A / PBR: N/A"
    req = requests.Session()
    headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"}
    try:
        headers["tr_id"] = "FHKST01010100"
        res = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                      headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
        if res.get("rt_cd") == "0":
            out = res.get("output", {})
            return f"📊 PER: {out.get('per', 'N/A')} / PBR: {out.get('pbr', 'N/A')}"
    except: pass
    return "📊 PER: N/A / PBR: N/A"

def safe_generate_content(contents):
    for i in range(5): 
        try: return client.models.generate_content(model='gemini-2.5-pro', contents=contents)
        except Exception as e:
            time.sleep(10 * (i + 1))
    raise Exception("❌ 구글 서버 응답 실패")

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
                trend = f"🔺 증가 (+{diff:,.2f})" if diff > 0 else (f"🔻 감소 ({diff:,.2f})" if diff < 0 else "➖ 변동없음")
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

    # KIS 토큰 확보
    KIS_TOKEN = ""
    try:
        setting_rows = doc.worksheet("⚙️설정").get_all_values()
        for row in setting_rows:
            if len(row) >= 2 and row[0] == "KIS_TOKEN": KIS_TOKEN = row[1]; break
    except: pass

    # 시장 상황 로드
    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange, oil = macro_data[1][4], macro_data[1][6], macro_data[1][7]
    news_keywords = "\n".join([f"{r[2]}({r[3]}회)" for r in doc.worksheet("뉴스_키워드").get_all_values()[1:6]])
    
    # 주가 데이터 로드 및 필터링
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:40]
    liquidity = get_global_liquidity_data()
    
    valid_short, valid_mid = [], []
    is_down = False 

    for r in tech_data:
        if len(r) < 21: continue
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        curr_p, chg, score, tajeom = str(r[2]).strip(), str(r[3]).strip(), str(r[8]).strip(), str(r[9]).strip()
        shadow, vol, prog = str(r[14]).strip(), str(r[18]).strip(), str(r[20]).strip()
        
        if "주의장세" in tajeom: is_down = True
        
        # 제외 조건: 상하한가, 매매제한, 재무불량(시장대안 방지)
        if re.search(r'상한가|하한가|29\.|30\.', chg): continue
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족', tajeom): continue 

        info = f"종목:{name}({code}), 현재가:{curr_p}원 ({chg}), 타점:{tajeom}, 퀀트점수:{score}, 테마:{r[19].strip()}, 프로그램:{prog}, 거래량:{vol}"
        cand = {'name': name, 'code': code, 'tajeom': tajeom, 'info': info, 'curr_p': int(curr_p.replace(',',''))}

        if "플랫폼" in tajeom or "눌림" in tajeom or "수급 유입" in tajeom or "관심" in tajeom or "방어" in tajeom:
            valid_mid.append(cand)
        elif "핵심" in tajeom or "주도주" in tajeom or "돌파" in tajeom:
            valid_short.append(cand)

    status_txt = "코스피/코스닥 20일선 이탈 (보수적 접근)" if is_down else "코스피/코스닥 지지 (공격적 운영 가능)"

    # ==========================================
    # 4. 리포트 본문 생성 엔진
    # ==========================================
    
    # [1단계] 마켓 요약 생성
    macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 애널리스트입니다. 
글로벌 매크로 지표(나스닥:{nasdaq}, 환율:{exchange}, 유가:{oil})와 유동성 데이터\n{liquidity}\n, 뉴스 키워드\n{news_keywords}\n를 분석하십시오.
현재 한국 증시 상황({status_txt})을 고려하여 오늘 트레이더가 취해야 할 최적의 포지션을 정중한 하십시오체로 요약하십시오."""
    market_summary = safe_generate_content(macro_prompt).text

    # [2단계] 전략별 개별 분석 함수
    def generate_strategy_section(st_type, cands):
        if not cands:
            msg = f"## 🚨 [{st_type.upper()}] 금일 매수 기준 부합 종목 부재\n\n현재 시스템의 엄격한 변동성 및 수급 필터를 통과한 {st_type} 전략 종목이 존재하지 않습니다. 무리한 진입보다는 현금을 보존하며 주도 테마의 거래량 실림을 대기하십시오. '기다림 또한 위대한 매매'임을 잊지 마십시오."
            return msg, "000000", None

        # 최적 1종목 선정
        pick_prompt = f"아래 종목 리스트 중 {st_type} 전략에 가장 적합한 대장주 1개의 '6자리 종목코드'만 출력하십시오.\n" + "\n".join([c['info'] for c in cands])
        target_code = re.search(r'\d{6}', safe_generate_content(pick_prompt).text).group()
        best = next((c for c in cands if c['code'] == target_code), cands[0])
        
        vip = get_vip_deep_dive_data(best['code'], KIS_TOKEN)
        news = get_target_stock_news(best['code'])
        
        detail_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 애널리스트입니다. 
[입력 데이터] {best['info']}
[펀더멘털] {vip}
[뉴스] {news}

위 데이터를 바탕으로 {st_type} 전략 리포트를 작성하십시오.
1. 모든 문장은 격조 있는 존댓말(하십시오체)을 사용하십시오.
2. 프로그램 수급과 거래량 판독 데이터를 융합하여 세력의 의도를 날카롭게 분석하십시오.
3. 리포트 마지막에만 반드시 [DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'} 형식을 포함하십시오."""

        img_path = f"temp_{best['code']}.png"
        try:
            res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best['code']}.png", headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            with open(img_path, 'wb') as f: f.write(res.content)
            report_txt = safe_generate_content([detail_prompt, PIL.Image.open(img_path)]).text
            os.remove(img_path)
        except:
            report_txt = safe_generate_content(detail_prompt).text

        # 가상계좌 데이터 파싱
        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', report_txt)
        if match:
            pick_data = {'name': best['name'], 'code': best['code'], 'target': int(match.group(1).replace(',', '')), 'stop': int(match.group(2).replace(',', '')), 'split': match.group(3) == 'O', 'curr': best['curr_p']}
            report_txt = re.sub(r'\[DATA\].*', '', report_txt, flags=re.DOTALL).strip()
            
        return report_txt, best['code'], pick_data

    # 실행
    report_short, code_short, data_short = generate_strategy_section("short", valid_short)
    time.sleep(10) # 속도 제한 방지
    report_mid, code_mid, data_mid = generate_strategy_section("mid", valid_mid)

    # ==========================================
    # 5. 가상계좌 업데이트
    # ==========================================
    def update_portfolio(picks):
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")
        today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        
        # 1. 기존 보유 종목 업데이트 및 매도 처리
        rows = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
        new_rows, closed_rows = [], []
        
        for r in rows[1:]:
            if len(r) < 10 or not r[0]: continue
            name, code = r[0], r[1].replace("'", "").strip().zfill(6)
            buy_p, amt, t_p, s_p = int(float(r[2].replace(',',''))), int(float(r[3].replace(',',''))), int(float(r[7].replace(',',''))), int(float(r[8].replace(',','')))
            
            try:
                curr_p = int(requests.get(f"https://m.stock.naver.com/api/stock/{code}/basic", verify=False, timeout=3).json()['closePrice'].replace(',',''))
            except: curr_p = buy_p
            
            rtn = (curr_p - buy_p) / buy_p
            reason = ""
            if curr_p >= t_p: reason = "🎯 목표가 도달"
            elif curr_p <= s_p: reason = "📉 손절가 이탈"
            elif str(r[9]).strip() == "매도": reason = "수동매도"
            
            if reason:
                closed_rows.append([name, buy_p, curr_p, f"{rtn*100:.2f}%", today, f"{'승리' if rtn>0 else '패배'} ({reason})"])
            else:
                new_rows.append([name, f"'{code}", buy_p, amt, curr_p, f"{rtn*100:.2f}%", r[6], t_p, s_p, ""])

        # 2. 신규 픽 추가
        for p in picks:
            if not p or p['code'] == "000000": continue
            # 이미 있으면 분할매수 처리, 없으면 신규
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
            for cr in closed_rows: closed_sheet.append_row(cr)

    update_portfolio([data_short, data_mid])

    # ==========================================
    # 6. PDF 생성 및 발송
    # ==========================================
    css = "<style>body{font-family:'NanumGothic',sans-serif;line-height:1.8;padding:30px;color:#222;}.broker-name{color:#1a365d;font-weight:bold;font-size:22px;border-bottom:3px solid #1a365d;padding-bottom:10px;margin-bottom:20px;}.market-box{background:#f8fafc;padding:20px;border-radius:10px;border:1px solid #cbd5e0;margin-bottom:30px;}.strategy-title{font-size:24px;color:#1e40af;font-weight:bold;margin-top:40px;border-left:6px solid #1e40af;padding-left:15px;margin-bottom:15px;}img{max-width:95%;border-radius:10px;border:1px solid #ddd;display:block;margin:20px auto;}.page-break{page-break-before:always;}</style>"
    
    html = f"""<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>
    <div class='broker-name'>HYEOKS SECURITIES | MARKET & QUANT REPORT</div>
    <div class='market-box'>
    <h3>🌐 글로벌 매크로 및 시장 인사이트</h3>
    {markdown.markdown(market_summary)}
    </div>
    
    <div class='strategy-title'>1. SHORT-TERM STRATEGY (단기 돌파)</div>
    {markdown.markdown(report_short)}
    """
    if code_short != "000000":
        html += f"<img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_short}.png'>"
        
    html += f"<div class='page-break'></div><div class='strategy-title'>2. MID-TERM STRATEGY (중기 스윙)</div>{markdown.markdown(report_mid)}"
    
    if code_mid != "000000":
        html += f"<img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_mid}.png'>"
    
    html += "</body></html>"

    pdf_file = f"HYEOKS_Daily_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    # 가스 웹앱 업로드
    if GAS_WEB_APP_URL:
        with open(pdf_file, "rb") as f: 
            b64 = base64.b64encode(f.read()).decode('utf-8')
        try:
            res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_file, "base64": b64}, timeout=30).json()
            doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), f"https://drive.google.com/uc?id={res.get('id')}"], index=2)
        except: pass

    # 텔레그램 발송
    if TELEGRAM_BOT_TOKEN:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", 
                      files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS] 시황 집중 AI 리서치 보고서"})

    print(f"✅ 리포트 발행 완료: {pdf_file}")

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
