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
import PIL.Image  # 💡 차트 이미지를 AI에게 먹이기 위해 추가된 라이브러리!
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai

warnings.filterwarnings("ignore")

# ==========================================
# ⚙️ HYEOKS 인프라 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

GEMINI_API_KEY = "여기에_제미나이_API_키_입력"
TELEGRAM_BOT_TOKEN = "여기에_텔레그램_봇_토큰_입력"
TELEGRAM_CHAT_ID = "여기에_텔레그램_CHAT_ID_입력"

GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 비전(Vision) 엔진 가동...")

def safe_generate_content(model, prompt_data):
    for i in range(3):
        try:
            return model.generate_content(prompt_data)
        except Exception as e:
            if "429" in str(e):
                print(f"⚠️ API 할당량 초과. 10초 대기 후 재시도합니다... ({i+1}/3)")
                time.sleep(10)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 에러")

try:
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    # 2. AI 모델 설정
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    # 3. 데이터 로드 (현재가, 5일선, 20일선 등 완벽 포함)
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]}({code}), 현재가:{r[2]}({r[3]}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{r[9]}, 20일이격도:{r[16] if len(r)>16 else ''}, 이력:{r[17] if len(r)>17 else ''}\n"

    # 4. 💡 투-스텝 지능형 리포트 작성 엔진 (이미지 분석 포함)
    def generate_hyeoks_report(st_type):
        print(f"🔍 [{st_type.upper()}] 최적의 1종목 타겟팅 중...")
        
        # [Step 1] AI에게 데이터만 주고 '종목코드' 딱 1개만 뽑아내라고 명령
        sys_msg = "내일 당장 급등할 단기 폭발" if st_type == "short" else "직장인 스윙 종가베팅"
        pick_prompt = f"너는 HYEOKS 수석 애널리스트야. 다음 데이터 중 '{sys_msg}' 유망주로 가장 완벽한 1종목을 골라. 다른 말은 절대 하지 말고 '오직 6자리 종목코드 숫자'만 출력해.\n데이터: {stock_candidates}"
        
        raw_code = safe_generate_content(model, pick_prompt).text
        match = re.search(r'\d{6}', raw_code)
        
        if not match:
            raise Exception("종목 코드를 추출하지 못했습니다.")
        target_code = match.group()
        print(f"🎯 타겟 종목 포착: {target_code} / 차트 이미지 스캔 중...")

        # [Step 2] 포착된 종목의 차트 이미지 몰래 다운로드
        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)
            
        img = PIL.Image.open(img_path)

        # [Step 3] 텍스트 데이터 + 차트 이미지를 동시에 던지며 본 리포트 작성 지시!
        if st_type == "short":
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 단기 전략 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항]
            1. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 제공된 [현재가], [5일선], [20일선] 데이터를 바탕으로 상식적으로 산출할 것.
            2. 👁️ 시각적 차트 판독 포함: '## 2. 기술적 타점 분석' 목차에 반드시 네가 첨부된 사진을 직접 보고 느낀 캔들의 형태, 이평선 밀집도, 지지/저항 패턴을 생생하게 묘사할 것.
            3. 여백 확보 및 스마트폰 가독성 최적화.

            [출력 양식 (마크다운 및 HTML 구조 유지)]
            <div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (1~2줄의 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 단기 트레이딩 데스크</strong><br><br>
                (이곳에 종목 요약)
            </div>

            ## 1. 단기 수급 및 테마 모멘텀 심층 고찰
            (상세 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 타점 분석
            * **차트 정밀 판독:** (이미지를 직접 보고 분석한 캔들과 이평선 패턴 서술)
            * **매수 타점 및 목표가:** (구체적인 가격대와 논리)
            * **손절 라인:** (가격과 이탈 시 논리)
            """
        else:
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 직장인 스윙 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항]
            1. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 제공된 [현재가], [5일선], [20일선] 데이터를 바탕으로 상식적으로 산출할 것.
            2. 👁️ 시각적 차트 판독 포함: '## 2. 이평선 밀집 및 타점 전략' 목차에 첨부된 차트를 보고 바닥권 탈출 형태인지, 20일선 수렴 상태인지를 시각적으로 묘사할 것.
            3. 여백 확보 및 스마트폰 가독성 최적화.

            [출력 양식 (마크다운 및 HTML 구조 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">직장인 대시세 눌림목 종가베팅: (1~2줄의 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
                (종목 요약)
            </div>

            ## 1. 펀더멘털 및 턴어라운드 스토리
            (상세 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 분할 매수 타점
            가. 차트 패턴 분석: (이미지를 직접 보고 분석한 바닥권 흐름 및 이평선 밀집 묘사)
            나. 중기 스윙 모아가기 분할 타점: (가격대 제시)
            다. 중기 목표가 및 손절 전략: (가격대 제시)
            """
            
        # 프롬프트 텍스트와 이미지(img)를 한 번에 넘깁니다!
        response = safe_generate_content(model, [final_prompt, img])
        img.close()
        os.remove(img_path) # 사용 끝난 사진 파일 삭제
        
        return response.text, target_code

    print("🧠 [HYEOKS 수석 애널리스트] 비전 데이터 분석 중...")
    report_short, code_short = generate_hyeoks_report("short")
    time.sleep(2)
    report_mid, code_mid = generate_hyeoks_report("mid")

    # 5. HTML 및 차트 결합 (보존된 CSS 디자인)
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

    # 이미지 태그를 리포트 하단에 깔끔하게 붙여줍니다.
    def make_chart_html(code, title):
        return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)} {make_chart_html(code_short, "[야수들의 단기 돌파 전략] AI 분석 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart_html(code_mid, "[직장인 스윙 종가베팅 전략] AI 분석 일봉 차트")}
    </body></html>"""

    # 6. PDF 변환
    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    # 7. 구글 드라이브 업로드 & 앱시트 기록
    if GAS_WEB_APP_URL.startswith("http"):
        print("📂 구글 드라이브 업로드 진행 중...")
        with open(pdf_filename, "rb") as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        try:
            res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_filename, "base64": pdf_base64})
            if res.status_code == 200 and "success" in res.text:
                file_id = res.json().get("id")
                report_link = f"https://drive.google.com/uc?id={file_id}"
                doc.worksheet("리포트_게시").append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
                print("✅ 앱시트 연동 완료!")
        except Exception as e:
            print(f"⚠️ 드라이브 에러: {e}")

    # 8. 텔레그램 전송
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] AI 차트 비전(Vision) 분석이 탑재된 심층 리포트"})
            if response.status_code == 200:
                print("✅ 텔레그램 전송 성공!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
