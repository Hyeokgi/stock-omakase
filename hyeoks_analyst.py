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

# 💡 깃허브 시크릿(Secrets)에서 불러오도록 복구
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 비전(Vision) 엔진 가동...")

def safe_generate_content(model, prompt_data):
    for i in range(3):
        try:
            return model.generate_content(prompt_data)
        except Exception as e:
            if "429" in str(e):
                print(f"⚠️ API 할당량 초과. 10초 대기... ({i+1}/3)")
                time.sleep(10)
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
            stock_candidates += f"종목:{r[0]}({code}), 현재가:{r[2]}({r[3]}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{r[9]}, 20일이격도:{r[16] if len(r)>16 else ''}, 이력:{r[17] if len(r)>17 else ''}\n"

    def generate_hyeoks_report(st_type):
        sys_msg = "내일 당장 급등할 단기 폭발" if st_type == "short" else "직장인 스윙 종가베팅"
        pick_prompt = f"너는 HYEOKS 수석 애널리스트야. 다음 데이터 중 '{sys_msg}' 유망주로 가장 완벽한 1종목을 골라. 다른 말은 절대 하지 말고 '오직 6자리 종목코드 숫자'만 출력해.\n데이터: {stock_candidates}"
        
        raw_code = safe_generate_content(model, pick_prompt).text
        target_code = re.search(r'\d{6}', raw_code).group()

        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)
            
        img = PIL.Image.open(img_path)

        # 💡 [핵심 패치] 분량 엄수 및 개조식 작성 강제
        base_prompt_rules = """
        [특별 지시사항 - 1페이지 압축 룰]
        1. 분량 통제: 절대 서술형으로 길게 쓰지 말 것. 모든 문장은 기관 애널리스트처럼 '개조식(~함, ~전망, ~임)'으로 짧고 명료하게 작성할 것. 
        2. 차트 분석: 첨부된 차트 이미지를 보고 캔들과 이평선 상태를 1~2줄로 예리하게 짚어낼 것.
        3. 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 [현재가], [5일선], [20일선]을 기준으로 상식적으로 계산할 것.
        """

        if st_type == "short":
            final_prompt = f"""
            너는 HYEOKS 증권 수석 애널리스트야. 이미지와 아래 데이터를 보고 '{target_code}' 단기 리포트를 써.
            데이터: {stock_candidates}
            {base_prompt_rules}
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 분석: (1줄 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief</strong><br>
                (핵심 모멘텀 2줄 요약)
            </div>

            <div class="chart-box">
                <img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png">
            </div>

            ## 1. 👁️ AI 시각적 차트 판독 & 단기 수급
            * **차트 흐름:** (차트 사진을 본 결과를 1~2줄 브리핑)
            * **수급/테마:** (핵심만 짧게)
            
            ## 2. 기술적 타점 및 시나리오
            * **매수 타점:** (가격과 논리)
            * **목표가:** (가격)
            * **손절 라인:** (가격)
            """
        else:
            final_prompt = f"""
            너는 HYEOKS 증권 수석 애널리스트야. 이미지와 아래 데이터를 보고 '{target_code}' 스윙 리포트를 써.
            데이터: {stock_candidates}
            {base_prompt_rules}
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">스윙 종가베팅: (1줄 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief</strong><br>
                (턴어라운드 요약 2줄)
            </div>

            <div class="chart-box">
                <img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png">
            </div>

            ## 1. 👁️ AI 시각적 차트 판독 & 모멘텀
            * **차트 흐름:** (차트 사진을 본 결과를 1~2줄 브리핑)
            * **모멘텀:** (핵심만 짧게)
            
            ## 2. 이평선 밀집 및 분할 타점
            * **1차 진입:** (가격)
            * **2차 진입:** (가격)
            * **목표가 및 손절가:** (가격)
            """
            
        response = safe_generate_content(model, [final_prompt, img])
        img.close()
        os.remove(img_path)
        return response.text

    print("🧠 [HYEOKS 수석 애널리스트] 비전 데이터 분석 중...")
    report_short = generate_hyeoks_report("short")
    time.sleep(2)
    report_mid = generate_hyeoks_report("mid")

    # 💡 1페이지 가독성 극대화 CSS 디자인
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.6; padding: 30px; color: #222; font-size: 105%; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 20px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }
        .header { border-bottom: 3px solid #1a365d; margin-bottom: 20px; padding-bottom: 10px; }
        .stock-title { font-size: 32px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 18px; color: #2b6cb0; font-weight: bold; margin-top: 5px; }
        .summary-box { background-color: #f8fafc; padding: 15px; border-left: 4px solid #1a365d; margin-bottom: 20px; border-radius: 4px; }
        .chart-box { text-align: center; margin-bottom: 20px; }
        .chart-box img { max-width: 80%; border: 1px solid #e2e8f0; border-radius: 6px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
        h2 { color: #1a365d; border-bottom: 1px solid #edf2f7; padding-bottom: 5px; font-size: 120%; margin-top: 15px; margin-bottom: 10px; }
        ul { margin-bottom: 15px; padding-left: 20px; }
        li { margin-bottom: 8px; }
        strong { color: #1a365d; }
        .page-break { page-break-before: always; }
    </style>"""

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)}
    </body></html>"""

    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

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

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] AI 차트 비전(Vision) 분석 심층 리포트"})
            if response.status_code == 200:
                print("✅ 텔레그램 전송 성공!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
