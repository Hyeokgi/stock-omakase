import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
warnings.filterwarnings("ignore")

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
KST = datetime.timezone(datetime.timedelta(hours=9))

print("🤖 [HYEOKS 리서치 센터] 2.5-Pro 무한 돌파(Zombie) 엔진 가동...")

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"❌ API 초기화 실패: {e}")
    exit(1)

# 💡 [최종 패치] 유일한 생명줄인 2.5-pro 하나만 물고 늘어지는 10연속 돌파 로직
def safe_generate_content(contents):
    for i in range(10): # 포기하지 않고 10번까지 재시도
        try:
            return client.models.generate_content(model='gemini-2.5-pro', contents=contents)
        except Exception as e:
            err_str = str(e).lower()
            print(f"⚠️ [돌파 시도 {i+1}/10] 서버 응답: {e}")
            
            # 503(과부하) 또는 429(트래픽 초과) 시, 죽지 않고 대기 시간을 점진적으로 늘리며 존버
            if "503" in err_str or "unavailable" in err_str or "429" in err_str or "quota" in err_str:
                wait_time = 30 * (i + 1)
                print(f"🚨 서버 혼잡. {wait_time}초 동안 숨을 고른 후 다시 문을 두드립니다...")
                time.sleep(wait_time)
            else: 
                raise e # 503이나 429가 아닌 진짜 치명적 에러면 시스템 중지
    raise Exception("❌ 10번의 재시도에도 구글 서버가 문을 열어주지 않습니다. 30분 뒤에 다시 실행해 주세요.")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)

    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange_rate, wti_oil = macro_sheet[1][4], macro_sheet[1][6], macro_sheet[1][7]
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    
    valid_short_candidates, valid_mid_candidates = [], []
    is_korean_market_down = False 

    for r in tech_data:
        if len(r) < 10: continue
        name = str(r[0]).strip()
        code = str(r[1]).replace("'", "").strip().zfill(6)
        change_rate, score_str, tajeom = str(r[3]), str(r[8]), str(r[9])
        shadow_status = str(r[14]) if len(r)>14 else ""
        
        if "주의장세" in tajeom: is_korean_market_down = True
        if "상한가" in tajeom or "29." in change_rate or "30." in change_rate: continue 
        if "윗꼬리 위험" in shadow_status or "윗꼬리" in tajeom: continue 
        if re.search(r'매수금지|자본잠식|딱지|관망|데이터 부족', tajeom): continue 
            
        cand_info = f"종목:{name}({code}), 현재가:{change_rate}, 타점:{tajeom}, 퀀트점수:{score_str}, 테마:{r[19] if len(r)>19 else ''}"
        cand_data = {'name': name, 'code': code, 'tajeom': tajeom, 'info': cand_info}
        
        if "돌파" in score_str or "주도주" in tajeom: valid_short_candidates.append(cand_data)
        else: valid_mid_candidates.append(cand_data)

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

        pick_prompt = f"너는 실전 트레이더의 직감을 가진 수석 퀀트 애널리스트야. 매크로(나스닥:{nasdaq}, 환율:{exchange_rate}, 유가:{wti_oil}, 증시:{market_status_text})와 아래 데이터를 분석해. 단순히 점수를 맹신하지 말고 테마, 고공권 여부, 거래량 응축 상태를 판단해. [후보군] {c_str} [지시] 후보 중 '{s_msg}'을 딱 1개만 골라서 '6자리 종목코드 숫자'만 출력해."
        
        raw_code = safe_generate_content(pick_prompt).text
        code_match = re.search(r'\d{6}', raw_code)
        target_code = code_match.group() if code_match else (valid_short_candidates[0]['code'] if st_type == "short" else valid_mid_candidates[0]['code'])

        best_pick = next((item for item in (valid_short_candidates if st_type=="short" else valid_mid_candidates) if item["code"] == target_code), (valid_short_candidates[0] if st_type=="short" else valid_mid_candidates[0]))
        target_name = best_pick['name']
        print(f"🎯 [{st_type.upper()}] 최종 픽: {target_name} ({target_code})")

        img_path = f"temp_{target_code}.png"
        img_res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png", headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f: f.write(img_res.content)
        img = PIL.Image.open(img_path)
        img.thumbnail((800, 800))

        warn = "\n[필수 경고] 고공권 판정. 비중을 절반으로 줄이고 칼손절 요망." if "고공권" in best_pick['tajeom'] else ("\n[필수 경고] 주의 장세. 오버나잇 비중 축소 요망." if is_korean_market_down else "")

        # 💡 [분량 다이어트 패치] 중복된 서술을 압축하여 차트 포함 정확히 1종목당 2페이지에 안착시키도록 텍스트 볼륨 조절
        base_prompt = f"""너는 대한민국 최상위 1% 실전 트레이더들의 피 말리는 호가창 감각과 'HYEOKS 리서치'의 정교한 펀더멘털/매크로 분석력을 완벽하게 결합한 수석 퀀트 애널리스트야.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성해라. 

[입력 데이터]
종목: {best_pick['info']}
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}, 국내증시 {market_status_text}
{warn}

[HYEOKS 딥리딩 절대 지침 - 명심해라]
1. 분량 초정밀 제어 (1종목당 차트 포함 딱 2페이지): 분석은 깊게 하되, 중복되는 설명이나 늘어지는 문장을 압축해라. 각 섹션의 핵심만 밀도 있게 타격하여, 리포트 하단에 첨부될 '차트 이미지'까지 포함했을 때 정확히 2페이지 분량으로 딱 떨어지도록 텍스트 길이를 절묘하게 맞춰라.
2. 은어 금지: 리포트 본문에는 'SS급', '단타용', '개미털기' 등 저급한 은어나 이모지를 절대 쓰지 마라. 냉철하고 품격 있는 문어체를 유지해라.
3. 3D 입체 분석: 
   - [매크로]: 환율/유가/지수가 이 종목에 미치는 영향.
   - [펀더멘털/테마]: 이 기업이 영위하는 사업의 잠재력(신사업 등)과 현재 엮인 테마의 강도.
   - [수급]: 거래량 폭증과 스마트 머니(세력/기관/외인) 유입의 인과관계.
4. 실전 야수의 타점 설계: 분석은 학자처럼 깊게 하되, 타점은 야수처럼 날카롭게 잡아라.
   - 단기 돌파: 익일 장 초반 9시~9시 30분 사이 출회되는 '아침 투매(Morning Shakeout)' 공략 등 구체적인 가격대와 시간대 시나리오 제시.
   - 스윙 눌림목: VCP(거래량 급감), 5일선 변곡점, 도지 캔들 등 하방 경직성을 시각적으로 확인한 후 진입하는 '종가 베팅' 타점 제시.
5. 가상계좌 연동 시스템 규칙: 리포트 맨 마지막 줄에는 오직 아래 형식만 출력. (숫자에 콤마 생략 가능)
# 💡 [신규 추가 패치] 2등주를 대장으로 착각하는 대참사를 막는 '짝짓기 매매' 로직
6. 대장주/후발주 팩트체크 (가장 중요): 입력 데이터의 '타점' 항목에 [후발주] 또는 [2등주]라는 뉘앙스가 있다면, 이 종목을 절대 시장을 주도하는 1등 대장주처럼 과대포장하지 마라.
   - 전략 수정: "이 종목은 테마의 후발주이므로 독자적인 돌파를 기대하기보다, 대장주(동일 테마 1등)의 수급이 꺾이지 않는 것을 실시간으로 확인하며 짧은 수익률만 챙기고 나오는 '짝짓기 단기 매매'로 접근해야 한다"고 명확한 한계를 지적해라.
   - 후발주 타점: 대장주보다 변동성이 크므로 목표가는 보수적으로(짧게), 손절가는 더 타이트하게(칼같이) 잡아라.
[DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'}

[출력 양식 (마크다운 유지, 불필요한 공백 제거, 밀도 있는 서술)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{target_name} ({target_code})</p>
<p class="subtitle">{'매물대 진공 구간 돌파 및 오전장 투매 공략' if st_type=='short' else 'VCP 거래량 급감 및 5일선 변곡점 종가베팅'}: (여기에 통찰력 있는 소제목 작성)</p>
</div>

<div class="summary-box">
<strong>💡 Company Brief | HYEOKS 퀀트 데스크</strong><br><br>
(기업의 펀더멘털, 핵심 테마, 모멘텀을 3~4문장으로 밀도 있게 압축 요약.)
</div>

## 1. 펀더멘털 및 매크로 테마 주도력 심층 고찰
(현재 매크로 환경 속에서 해당 종목의 턴어라운드 가능성, 테마 강도 등을 핵심만 찌르며 상세히 서술하되 중복된 설명은 피할 것.)

## 2. 시각적 차트 판독 및 거래량 딥리딩
(주요 매물대, 이평선의 이격도, 최근 거래량 증감(VCP 등)의 특징을 시각적 관점에서 정밀하게 해부할 것.)

## 3. 실전 타점 시나리오 및 리스크 관리 전략
(위에서 지시한 '실전 야수의 타점 설계' 적용. 1차/2차 진입 가격, 구체적 시나리오, 목표가 및 칼 같은 손절가를 논리적으로 명확하게 서술할 것.)
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
            name, code, avg_p, inv_amt, _, _, b_date, t_p, s_p, manual = r
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
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS] AI 심층 리포트"})

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
