import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
warnings.filterwarnings("ignore")

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
KST = datetime.timezone(datetime.timedelta(hours=9))

FRED_API_KEY = "eed13162f33f0ad6547783b9bb27190b"

print("🤖 [HYEOKS 리서치 센터] 매크로 융합 2.5-Pro 무한 돌파(Zombie) 엔진 가동...")

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"❌ API 초기화 실패: {e}")
    exit(1)

def get_target_stock_news(code):
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_list = []
        for a_tag in soup.select('.title a')[:3]:  
            news_list.append(f"- {a_tag.text.strip()}")
        return "\n".join(news_list) if news_list else "당일 개별 특징주 뉴스 없음"
    except Exception:
        return "개별 뉴스 수집 실패"
        
def safe_generate_content(contents):
    for i in range(10): 
        try:
            return client.models.generate_content(model='gemini-2.5-pro', contents=contents)
        except Exception as e:
            err_str = str(e).lower()
            print(f"⚠️ [돌파 시도 {i+1}/10] 서버 응답: {e}")
            if "503" in err_str or "unavailable" in err_str or "429" in err_str or "quota" in err_str:
                wait_time = 30 * (i + 1)
                print(f"🚨 서버 혼잡. {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
            else: 
                raise e 
    raise Exception("❌ 구글 서버 응답 불가")

def get_global_liquidity_data():
    print("🌐 글로벌 유동성(FRED) 데이터 수집 중...")
    indicators = {
        "WTREGEN": "TGA 잔고", 
        "RRPONTSYD": "역레포 잔고", 
        "BAMLH0A0HYM2": "하이일드 스프레드", 
        "WALCL": "연준 총자산", 
        "M2SL": "M2 통화량" 
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
                diff = latest_val - prev_val
                trend = f"🔺 증가 (+{diff:,.2f})" if diff > 0 else (f"🔻 감소 ({diff:,.2f})" if diff < 0 else "➖ 변동없음")
                formatted_val = f"{latest_val:,.2f}%" if series_id == "BAMLH0A0HYM2" else f"{latest_val:,.1f}"
                liquidity_report.append(f"- {name}: {formatted_val} ({trend})")
        except Exception:
            pass
    return "\n".join(liquidity_report) if liquidity_report else "유동성 데이터 수집 불가"

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)

    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange_rate, wti_oil = macro_sheet[1][4], macro_sheet[1][6], macro_sheet[1][7]
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    
    liquidity_data = get_global_liquidity_data()
    valid_short_candidates, valid_mid_candidates = [], []
    is_korean_market_down = False 

    for r in tech_data:
        if len(r) < 10: continue
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        # 💡 r[2] (실제 원화 가격)을 current_price 변수로 뽑아냅니다.
        current_price, change_rate, score_str, tajeom = str(r[2]), str(r[3]), str(r[8]), str(r[9])
        shadow_status = str(r[14]) if len(r)>14 else ""
        nxt_rate = str(r[20]) if len(r)>20 else "확인불가"
        program_rate = str(r[21]) if len(r)>21 else "확인불가"
        if "주의장세" in tajeom: is_korean_market_down = True
        if "상한가" in tajeom or "29." in change_rate or "30." in change_rate: continue 
        if "윗꼬리 위험" in shadow_status or "윗꼬리" in tajeom: continue 
        if re.search(r'매수금지|자본잠식|딱지|관망|데이터 부족', tajeom): continue 
            
        # 💡 현재가 란에 "13,820원 (+22.30%)" 형태로 완벽하게 꽂아줍니다!
        cand_info = f"종목:{name}({code}), 현재가:{current_price}원 ({change_rate}), 타점:{tajeom}, 퀀트점수:{score_str}, 테마:{r[19] if len(r)>19 else ''}, 시간외:{nxt_rate}, 프로그램:{program_rate}"
        cand_data = {'name': name, 'code': code, 'tajeom': tajeom, 'info': cand_info}
        
        if "상한가" in tajeom or "주도주" in tajeom or "신고가" in tajeom or "나홀로" in tajeom:
            valid_short_candidates.append(cand_data)
        elif "관망" in tajeom or "대기" in tajeom or "후발주" in tajeom or "눌림" in tajeom or "수급 유입" in tajeom:
            valid_mid_candidates.append(cand_data)
        else:
            if "돌파" in score_str: valid_short_candidates.append(cand_data)
            else: valid_mid_candidates.append(cand_data)

    if not valid_mid_candidates and valid_short_candidates:
        print("⚠️ 전 종목 단기 급등 상태! 하위 종목을 스윙으로 차출합니다.")
        valid_mid_candidates = valid_short_candidates[-3:] 
        valid_short_candidates = valid_short_candidates[:-3]
        
    if not valid_short_candidates and valid_mid_candidates:
        print("⚠️ 전 종목 눌림목 상태! 상위 종목을 단기로 차출합니다.")
        valid_short_candidates = valid_mid_candidates[:3]
        valid_mid_candidates = valid_mid_candidates[3:]

    market_status_text = "코스피/코스닥 20일선 이탈 (하락 변동성 장세)" if is_korean_market_down else "코스피/코스닥 안정화 (추세 추종 베팅 가능)"

    def generate_hyeoks_report(st_type):
        if st_type == "short":
            if not valid_short_candidates: raise Exception("단기 조건 부합 종목 없음.")
            c_str = "\n".join([c['info'] for c in valid_short_candidates])
            s_msg = "주도 테마의 심장부에서 전고점 매물대를 완벽히 소화해 낸 최고의 단기 돌파 1종목"
        else:
            if not valid_mid_candidates: raise Exception("스윙 조건 부합 종목 없음.")
            c_str = "\n".join([c['info'] for c in valid_mid_candidates])
            s_msg = "매크로 불안 속에서도 악성 매도가 씨마른 완벽한 눌림목 스윙 1종목"

        pick_prompt = f"너는 실전 트레이더의 직감을 가진 수석 퀀트 애널리스트야. 매크로(나스닥:{nasdaq}, 환율:{exchange_rate}, 한국증시:{market_status_text})와 [유동성]\n{liquidity_data}\n를 분석해. 유동성이 축소 중이면 보수적 방어종목을, 공급 중이면 폭발적 주도주를 골라. [후보군] {c_str} [지시] 후보 중 '{s_msg}'을 딱 1개만 골라서 '6자리 종목코드 숫자'만 출력해."
        
        raw_code = safe_generate_content(pick_prompt).text
        code_match = re.search(r'\d{6}', raw_code)
        target_code = code_match.group() if code_match else (valid_short_candidates[0]['code'] if st_type == "short" else valid_mid_candidates[0]['code'])

        target_specific_news = get_target_stock_news(target_code)

        best_pick = next((item for item in (valid_short_candidates if st_type=="short" else valid_mid_candidates) if item["code"] == target_code), (valid_short_candidates[0] if st_type=="short" else valid_mid_candidates[0]))
        target_name = best_pick['name']
        print(f"🎯 [{st_type.upper()}] 최종 픽: {target_name} ({target_code})")

        img_path = f"temp_{target_code}.png"
        
        try:
            img_res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            with open(img_path, 'wb') as f: f.write(img_res.content)
            img = PIL.Image.open(img_path)
            img.thumbnail((800, 800))
        except Exception as e:
            print(f"⚠️ 차트 이미지 로드 에러 발생 (빈 이미지로 대체하여 시스템 다운 방어): {e}")
            img = PIL.Image.new('RGB', (800, 800), color=(255, 255, 255))
            img.save(img_path)

        warn = "\n[필수 경고] 고공권 판정. 비중을 절반으로 줄이고 칼손절 요망." if "고공권" in best_pick['tajeom'] else ("\n[필수 경고] 주의 장세. 오버나잇 비중 축소 요망." if is_korean_market_down else "")

        base_prompt = f"""너는 대한민국 최상위 1% 실전 트레이더들의 감각을 가진 퀀트 애널리스트야.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성해라. 

[입력 데이터]
종목: {best_pick['info']}
🔥 당일 타겟 종목 최신 뉴스:
{target_specific_news}
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}, 국내증시 {market_status_text}
글로벌 유동성 현황:
{liquidity_data}
{warn}

[HYEOKS 딥리딩 절대 지침 - 명심해라]
1. 분량 자유도: 1번, 2번 항목은 너의 전문적인 통찰력을 발휘하여 충분히 길고 논리적으로 서술해라. 
2. 가격 창조 금지 (매우 중요): 본문에서 특정 가격을 언급할 때는 반드시 [입력 데이터]에 제공된 '현재가'만을 사용해라. 절대 임의의 주가(예: 2,700원 등)를 상상해서 적지 마라.
3. 전략의 일관성: {st_type} 전략에 맞게 서술해라. 돌파 매매면 돌파만, 눌림목이면 눌림목만 서술하고 두 전략을 섞지 마라.
4. 가상계좌 규칙: 리포트 마지막 줄에만 [DATA] 목표가:00000, 손절가:00000, 분할매수:O 형식 출력.
5. 대장주/후발주 팩트체크: 후발주일 경우 짝짓기 매매의 한계를 지적해라.
6. 익일 시가 갭 대응: 제공된 [시간외] 데이터를 바탕으로 익일 갭 상승/하락에 대한 구체적인 타점을 제시할 것.
7. 프로그램 수급 연계: 제공된 [프로그램] 매수/매도 현황을 분석하여, 기관/외인의 알고리즘 매수가 해당 종목의 주가 상승(또는 지지력)을 어떻게 뒷받침하고 있는지 반드시 리포트 본문에 서술할 것.
[DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'}

[출력 양식 (마크다운 유지)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{target_name} ({target_code})</p>
<p class="subtitle">{'매물대 진공 구간 돌파 및 5분봉 리테스트 공략' if st_type=='short' else 'VCP 거래량 급감 및 5일선 변곡점 종가베팅'}: (소제목 작성)</p>
</div>

<div class="summary-box">
<strong>💡 Company Brief | HYEOKS 퀀트 데스크</strong><br><br>
(기업 펀더멘털 압축 요약)
</div>

## 1. 펀더멘털 및 매크로 유동성 심층 고찰
(FRED 지표 흐름 해석 및 당일 뉴스를 바탕으로 숨겨진 진짜 모멘텀을 심층 분석)

## 2. 시각적 차트 판독 및 거래량 딥리딩
(주요 매물대, 이평선 이격도, 최근 거래량 증감 해부)

## 3. 실전 타점 시나리오 및 리스크 관리 전략
(시간외 데이터를 반영한 익일 시가 갭 대응 시나리오, 1차/2차 진입 가격, 목표가/손절가를 매우 상세하게 작성할 것)
"""
        response = safe_generate_content([base_prompt, img])
        img.close()
        os.remove(img_path)

        raw_report = response.text
        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', raw_report)
        if match:
            pick_data = {'name': target_name, 'code': target_code, 'target': int(match.group(1).replace(',', '')), 'stop': int(match.group(2).replace(',', '')), 'split': match.group(3) == 'O'}
            raw_report = re.sub(r'\[DATA\].*', '', raw_report, flags=re.DOTALL).strip()
        
        return raw_report, target_code, pick_data

    report_short, code_short, pick_short = generate_hyeoks_report("short")
    print("⏳ 단기 리포트 완료! 20초 대기...")
    time.sleep(20)
    report_mid, code_mid, pick_mid = generate_hyeoks_report("mid")

    def update_portfolio(picks):
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")
        hold_data = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
        if len(hold_data) <= 1 or hold_data[0][0] != "종목명":
            hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers]); hold_data = [headers]
        
        today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        new_hold, closed = [], []
        req = requests.Session(); req.headers.update({'User-Agent': 'Mozilla/5.0'})

        for r in hold_data[1:]:
            if len(r) < 10 or not r[0]: continue
            name, code, avg_p, inv_amt, _, _, b_date, t_p, s_p, manual = r[:10]
            avg_p, inv_amt, t_p, s_p = int(float(str(avg_p).replace(',',''))), int(float(str(inv_amt).replace(',',''))), int(float(str(t_p).replace(',',''))), int(float(str(s_p).replace(',','')))
            
            clean_code = str(code).replace("'", "").strip().zfill(6)
            try: curr_p = int(req.get(f"https://m.stock.naver.com/api/stock/{clean_code}/basic", timeout=3).json()['closePrice'].replace(',',''))
            except: curr_p = avg_p 

            rtn = (curr_p - avg_p) / avg_p if avg_p > 0 else 0
            reason = "수동매도" if str(manual).strip() == "매도" else ("🎯 목표가 도달" if curr_p >= t_p else ("📉 손절가 이탈" if curr_p <= s_p else ""))

            if reason: closed.append([name, avg_p, curr_p, f"{rtn*100:.2f}%", today, f"{'승리' if rtn>0 else '패배'} ({reason})"])
            else: new_hold.append([name, f"'{clean_code}", avg_p, inv_amt, curr_p, f"{rtn*100:.2f}%", b_date, t_p, s_p, ""])

        for p in picks:
            if not p: continue
            try: curr_p = int(req.get(f"https://m.stock.naver.com/api/stock/{p['code']}/basic", timeout=3).json()['closePrice'].replace(',',''))
            except: continue
            idx = next((i for i, v in enumerate(new_hold) if v[0] == p['name']), -1)
            if idx != -1:
                if p['split']:
                    new_avg = int((new_hold[idx][3] + 1000000) / ((new_hold[idx][3]/new_hold[idx][2]) + (1000000/curr_p)))
                    new_hold[idx][2], new_hold[idx][3], new_hold[idx][4] = new_avg, new_hold[idx][3] + 1000000, curr_p
                    new_hold[idx][5] = f"{(curr_p - new_avg) / new_avg * 100:.2f}%"
            else: new_hold.append([p['name'], f"'{p['code']}", curr_p, 1000000, curr_p, "0.00%", today, p['target'], p['stop'], ""])

        hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers] + new_hold, value_input_option="USER_ENTERED")
        if closed:
            if not closed_sheet.get_all_values(): closed_sheet.update(range_name="A1", values=[["종목명", "매입단가", "매도단가", "수익률", "매도일자", "결과"]])
            for row in closed: closed_sheet.append_row(row)

    update_portfolio([pick_short, pick_mid])

    # 💡 [레이아웃 패치] 강제 페이지 분할 로직 제거. 자연스럽게 내용이 이어지도록 원상 복구!
    css = "<style>body{font-family:'NanumGothic',sans-serif;line-height:1.8;padding:30px;color:#222;font-size:110%;}.broker-name{color:#1a365d;font-weight:bold;font-size:22px;margin-bottom:15px;border-bottom:3px solid #1a365d;padding-bottom:10px;}.stock-title{font-size:32px;font-weight:900;margin:0;}.subtitle{font-size:18px;color:#2b6cb0;font-weight:bold;}.summary-box{background:#f8fafc;padding:20px;border-left:5px solid #1a365d;margin:20px 0;border-radius:5px;}h2{color:#1a365d;border-bottom:2px solid #edf2f7;margin-top:30px;padding-bottom:8px;}p{margin-bottom:15px;word-break:keep-all;}img{max-width:90%;border:1px solid #cbd5e0;border-radius:8px;}.chart-container{text-align:center;margin-top:40px;page-break-inside:avoid;}.page-break{page-break-before:always;}</style>"
    
    html = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>{markdown.markdown(report_short)}<div class='chart-container'><h3>📊 차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_short}.png'></div><div class='page-break'></div>{markdown.markdown(report_mid)}<div class='chart-container'><h3>📊 차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_mid}.png'></div></body></html>"

    pdf_file = f"HYEOKS_Report_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    if GAS_WEB_APP_URL.startswith("http"):
        with open(pdf_file, "rb") as f: pdf_b64 = base64.b64encode(f.read()).decode('utf-8')
        for _ in range(3):
            try:
                res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_file, "base64": pdf_b64}, timeout=30)
                if res.status_code == 200:
                    doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), f"https://drive.google.com/uc?id={res.json().get('id')}"], index=2)
                    break
            except: time.sleep(5)

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS] 매크로 융합 AI 심층 리포트"})

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
