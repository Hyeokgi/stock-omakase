import os
import re
import time
import base64
import warnings
import datetime
import requests
import markdown
import pdfkit
import gspread
import PIL.Image 
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai

warnings.filterwarnings("ignore")

# ==========================================
# ⚙️ HYEOKS 인프라 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"

KST = datetime.timezone(datetime.timedelta(hours=9))

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 비전(Vision) 심층 분석 엔진 가동...")

def safe_generate_content(model, prompt_data):
    for i in range(5):
        try:
            return model.generate_content(prompt_data)
        except Exception as e:
            if "429" in str(e):
                wait_time = 30 * (i + 1)
                print(f"⚠️ API 할당량 초과. 숨 고르기를 위해 {wait_time}초 대기합니다... ({i+1}/5)")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 에러")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            vol_status = r[18] if len(r)>18 else ''
            stock_candidates += f"종목:{r[0]}({code}), 현재가:{r[2]}({r[3]}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{r[9]}, 20일이격도:{r[16] if len(r)>16 else ''}, 이력:{r[17] if len(r)>17 else ''}, 거래량상태:{vol_status}\n"

    def generate_hyeoks_report(st_type):
        sys_msg = "내일 당장 급등할 단기 폭발" if st_type == "short" else "직장인 스윙 종가베팅"
        pick_prompt = f"너는 HYEOKS 수석 애널리스트야. 다음 데이터 중 '{sys_msg}' 유망주로 가장 완벽한 1종목을 골라. 다른 말은 절대 하지 말고 '오직 6자리 종목코드 숫자'만 출력해.\n데이터: {stock_candidates}"
        
        raw_code = safe_generate_content(model, pick_prompt).text
        target_code = re.search(r'\d{6}', raw_code).group()

        # 💡 [가상계좌 연동] 종목명 추출
        target_name = "Unknown"
        for r in tech_data:
            if target_code in r[1]:
                target_name = r[0]
                break

        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)
            
        img = PIL.Image.open(img_path)

        if st_type == "short":
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 단기 폭발 유망주 심층 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항]
            1. 금지어 철저 배제: 'S급' 등 내부 시스템 은어 및 등급 기호를 절대 사용하지 말 것.
            2. 분량 및 깊이: 방대한 깊이를 확보할 것.
            3. 시각적 차트 및 거래량 판독 (방배동선수 룰) 최우선 분석.
            4. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가를 논리적으로 계산할 것.
            5. 가상계좌 연동을 위해, 리포트 맨 마지막 줄에 오직 아래 형식으로만 한 줄을 추가할 것. (목표가와 손절가는 숫자만 기입)
               [DATA] 목표가:00000, 손절가:00000, 분할매수:X
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 단기 트레이딩 데스크</strong><br><br>
                (요약)
            </div>

            ## 1. 단기 수급 및 테마 모멘텀 심층 고찰
            (상세 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 타점 시나리오
            (상세 서술)
            """
        else:
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 스윙 심층 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항]
            1. 금지어 철저 배제.
            2. 분량 및 깊이: 펀더멘털 스토리 풍부하게 서술.
            3. 시각적 차트 및 거래량 판독 (방배동선수 룰) 최우선 분석.
            4. 🛡️ 오버나잇 리스크 관리: 1차 진입 비중 제한 및 갭하락 대비 시나리오 포함.
            5. 매수 타점, 목표가, 손절가 논리적 계산.
            6. 가상계좌 연동을 위해, 리포트 맨 마지막 줄에 오직 아래 형식으로만 한 줄을 추가할 것. (목표가와 손절가는 숫자만 기입)
               [DATA] 목표가:00000, 손절가:00000, 분할매수:O
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">직장인 대시세 눌림목 종가베팅: (소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
                (요약)
            </div>

            ## 1. 펀더멘털 및 턴어라운드 스토리
            (상세 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 분할 매수 전략
            (상세 서술)
            """
            
        response = safe_generate_content(model, [final_prompt, img])
        img.close()
        os.remove(img_path)
        
        raw_report_text = response.text
        
        # 💡 [핵심 패치] AI가 출력한 매매 기준 추출 (가상계좌용)
        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', raw_report_text)
        if match:
            pick_data = {
                'name': target_name,
                'code': target_code,
                'target': int(match.group(1).replace(',', '')),
                'stop': int(match.group(2).replace(',', '')),
                'split': match.group(3) == 'O'
            }
            # 실제 리포트 본문에서는 [DATA] 줄을 깔끔하게 삭제하여 숨김
            raw_report_text = re.sub(r'\[DATA\].*', '', raw_report_text, flags=re.DOTALL).strip()

        return raw_report_text, target_code, pick_data

    print("🧠 [HYEOKS 수석 애널리스트] 비전 데이터 심층 분석 중...")
    report_short, code_short, pick_short = generate_hyeoks_report("short")
    
    print("⏳ 단기 리포트 완료! API 과부하 방지를 위해 30초 휴식합니다...")
    time.sleep(30)
    
    report_mid, code_mid, pick_mid = generate_hyeoks_report("mid")

    # ==========================================
    # 💰 가상계좌 실전 퀀트 시뮬레이션 엔진 (Python)
    # ==========================================
    def update_virtual_portfolio(picks):
        print("💰 [HYEOKS 퀀트 데스크] 가상계좌 시뮬레이션 가동 중...")
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")
        
        hold_data = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
        
        # 시트 포맷 초기화 (기존 잘못된 헤더가 있으면 덮어쓰기)
        if len(hold_data) <= 1 or hold_data[0][0] != "종목명" or len(hold_data[0]) < 10:
            hold_sheet.clear()
            hold_sheet.update(range_name="A1", values=[headers])
            hold_data = [headers]
            
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        new_hold_list = []
        closed_list = []
        
        req_session = requests.Session()
        req_session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        # 1. 기존 보유 종목 검증 및 매도 처리
        for row in hold_data[1:]:
            if len(row) < 10 or not row[0]: continue
            name, code, avg_price, invest_amt, _, _, buy_date, t_price, s_price, manual_sell = row
            
            avg_price = int(float(str(avg_price).replace(',', '')))
            invest_amt = int(float(str(invest_amt).replace(',', '')))
            t_price = int(float(str(t_price).replace(',', '')))
            s_price = int(float(str(s_price).replace(',', '')))
            
            # 실시간 현재가 가져오기
            try:
                # [오류 수정 완료] f-string 내부의 역슬래시 사용 제거
                clean_code = str(code).replace("'", "").zfill(6)
                api_url = f"https://m.stock.naver.com/api/stock/{clean_code}/basic"
                curr_price = int(req_session.get(api_url, timeout=3).json()['closePrice'].replace(',', ''))
            except:
                curr_price = avg_price # 통신 오류시 기존가 유지
                
            return_rate = (curr_price - avg_price) / avg_price if avg_price > 0 else 0
            
            sell_reason = ""
            if str(manual_sell).strip() == "매도":
                sell_reason = "사용자 수동 중도 매도"
            elif curr_price >= t_price:
                sell_reason = "🎯 AI 목표가 도달 (전량 익절)"
            elif curr_price <= s_price:
                sell_reason = "📉 AI 손절가 이탈 (전량 손절)"
                
            if sell_reason:
                result_str = "승리" if return_rate > 0 else "패배"
                closed_list.append([name, avg_price, curr_price, f"{return_rate*100:.2f}%", today_str, f"{result_str} ({sell_reason})"])
            else:
                clean_code2 = str(code).replace("'", "").zfill(6)
                new_hold_list.append([name, f"'{clean_code2}", avg_price, invest_amt, curr_price, f"{return_rate*100:.2f}%", buy_date, t_price, s_price, ""])
                
        # 2. 신규 리포트 종목 편입 (기존 보유 확인 후 물타기/신규 진입)
        for pick in picks:
            if not pick: continue
            name, code, t_price, s_price, is_split = pick['name'], pick['code'], pick['target'], pick['stop'], pick['split']
            
            try:
                curr_price = int(req_session.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3).json()['closePrice'].replace(',', ''))
            except: continue
                
            existing_idx = next((i for i, r in enumerate(new_hold_list) if r[0] == name), -1)
            
            if existing_idx != -1:
                # 이미 보유중인데 리포트에서 "분할매수:O" 라면 100만원 추가 베팅 진행
                if is_split:
                    old_avg = new_hold_list[existing_idx][2]
                    old_invest = new_hold_list[existing_idx][3]
                    add_invest = 1000000 # 100만원 고정 매수
                    
                    new_invest = old_invest + add_invest
                    old_qty = old_invest / old_avg
                    add_qty = add_invest / curr_price
                    new_avg = int(new_invest / (old_qty + add_qty)) # 평단가 재조정
                    
                    new_hold_list[existing_idx][2] = new_avg
                    new_hold_list[existing_idx][3] = new_invest
                    new_hold_list[existing_idx][4] = curr_price
                    new_hold_list[existing_idx][5] = f"{(curr_price - new_avg) / new_avg * 100:.2f}%"
            else:
                # 겹치지 않는 신규 종목일 경우 편입 (초기 진입 100만원)
                new_hold_list.append([name, f"'{code}", curr_price, 1000000, curr_price, "0.00%", today_str, t_price, s_price, ""])
                
        # 시트 데이터 덮어쓰기 (기존 내용 싹 밀고 새 리스트로 갱신)
        hold_sheet.clear()
        hold_sheet.update(range_name="A1", values=[headers] + new_hold_list, value_input_option="USER_ENTERED")
        
        # 매도된 종목이 있다면 가상계좌_종료 시트에 누적
        if closed_list:
            # 헤더 없으면 생성
            if not closed_sheet.get_all_values():
                closed_sheet.update(range_name="A1", values=[["종목명", "최종평단가", "매도단가", "최종수익률", "매도일자", "결과(승/패)"]])
            for row in closed_list:
                closed_sheet.append_row(row)
                
        print("✅ 실전 가상계좌 리밸런싱 및 익/손절 처리 완료!")

    # 🚀 생성된 리포트 데이터를 바탕으로 가상계좌 업데이트 실행
    update_virtual_portfolio([pick_short, pick_mid])


    # ==========================================
    # 📝 리포트 HTML 조립 및 PDF 생성
    # ==========================================
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.9; padding: 40px; color: #222; font-size: 110%; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 22px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 35px; padding-bottom: 15px; }
        .stock-title { font-size: 38px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 21px; color: #2b6cb0; font-weight: bold; margin-top: 8px; line-height: 1.4; }
        .summary-box { background-color: #f8fafc; padding: 25px; border-left: 5px solid #1a365d; margin-top: 25px; margin-bottom: 35px; border-radius: 6px; font-size: 105%; }
        h2 { color: #1a365d; border-bottom: 2px solid #edf2f7; margin-top: 45px; margin-bottom: 25px; padding-bottom: 10px; font-size: 130%; }
        p { margin-bottom: 20px; text-align: justify; word-break: keep-all; }
        ul, ol { margin-bottom: 25px; padding-left: 25px; }
        li { margin-bottom: 12px; }
        strong { color: #1a365d; }
        .chart-container { text-align: center; margin-top: 50px; page-break-inside: avoid; }
        .chart-container h3 { color: #4a5568; font-size: 110%; margin-bottom: 15px; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border-radius: 8px; }
        .page-break { page-break-before: always; }
    </style>"""

    def make_chart_html(code, title):
        return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)} {make_chart_html(code_short, "[야수들의 단기 돌파 전략] AI 분석 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart_html(code_mid, "[직장인 스윙 종가베팅 전략] AI 분석 일봉 차트")}
    </body></html>"""

    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    if GAS_WEB_APP_URL.startswith("http"):
        print("📂 구글 드라이브 업로드 진행 중...")
        with open(pdf_filename, "rb") as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        for attempt in range(3):
            try:
                res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_filename, "base64": pdf_base64}, timeout=30)
                if res.status_code == 200 and "success" in res.text:
                    file_id = res.json().get("id")
                    report_link = f"https://drive.google.com/uc?id={file_id}"
                    doc.worksheet("리포트_게시").append_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), report_link])
                    print("✅ 앱시트 연동 완료!")
                    break  
                else:
                    print(f"⚠️ 드라이브 업로드 응답 오류 (시도 {attempt+1}/3)")
            except Exception as e:
                print(f"⚠️ 드라이브 에러 (시도 {attempt+1}/3): {e}")
                time.sleep(5) 

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] AI 펀더멘털 및 시각적 차트 판독 심층 리포트\n\n💰 (가상계좌 포트폴리오 연동 완료)"})
            if response.status_code == 200:
                print("✅ 텔레그램 전송 성공!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
