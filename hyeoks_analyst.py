import os
import re
import time
import warnings
import datetime
import requests
import markdown
import pdfkit
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai

warnings.filterwarnings("ignore")

# ==========================================
# ⚙️ HYEOKS 인프라 설정 (텔레그램 전송 전용)
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 엔진 가동 (텔레그램 전용 모드)...")

def safe_generate_content(model, prompt):
    """할당량 초과(429) 에러 발생 시 대기 후 재시도하는 안전 함수"""
    for i in range(3):
        try:
            return model.generate_content(prompt)
        except Exception as e:
            if "429" in str(e):
                print(f"⚠️ API 할당량 초과. 10초 대기 후 재시도합니다... ({i+1}/3)")
                time.sleep(10)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 할당량이 부족합니다.")

try:
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    # 2. AI 모델 설정
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    # 3. 데이터 로드
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        # 시트에 추가된 '20일선 이격도', '거래량비율' 데이터가 있다면 여기서 함께 읽어옵니다.
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            # 예시: r[10]이 20일선 이격도, r[11]이 거래량 비율이라고 가정 (시트 세팅에 맞게 수정)
            stock_candidates += f"종목:{r[0]} ({code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    # 4. 듀얼 전략 프롬프트 (단기 모멘텀 vs 직장인 20일선 스윙)
    def generate_hyeoks_report(st_type):
        if st_type == "short":
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            [데이터]를 바탕으로 내일 당장 '갭 상승'이나 '강력한 슈팅'이 나올 단기 폭발 유망주 1종목을 선정해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
            
            <div class="broker-name">HYEOKS Securities | Short-Term Strategy</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (1줄 소제목)</p>
            </div>
            ## 1. 재료 분석 및 추세 연속성
            ## 2. 거래량 및 타점 고찰
            ## 3. 단기 대응 시나리오
            """
        else:
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            [데이터]를 바탕으로 직장인이 마음 편히 종가에 매수하여 '대시세(20~50% 반등)'를 노릴 수 있는 스윙 유망주 1종목을 골라. (단기 종목과 겹치지 않게 할 것)
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [직장인 스윙 특화 지시사항] 
            1. 대장주의 조건: 최근 1~2달 내 상한가 또는 막대한 거래대금으로 폭등했던 이력(엔벨로프 상단 터치) 확인.
            2. 10~20일 거래량 급감 조정: 고점 이후 10일~20일간 거래량이 완벽히 마르며 조정을 받은 상태 분석.
            3. 20일선 이평선 밀집: 주가가 20일선 부근까지 내려와 5, 10, 20일선이 수렴하며 반등(도지, 첫 양봉)을 보이는지 분석.
            
            <div class="broker-name">HYEOKS Securities | Swing & Closing Bet Strategy</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">직장인 대시세 눌림목 종가베팅: (1줄 소제목)</p>
            </div>
            ## 1. 폭등 이력 및 대장주 명분
            ## 2. 시간 조정 및 거래량(씨마름) 분석
            ## 3. 직장인 매수 타점 및 대응 시나리오 (자동감시주문 기준가 제시)
            """
        return safe_generate_content(model, prompt).text

    print("🧠 [HYEOKS 수석 애널리스트] 2.5-flash 심층 분석 중...")
    report_short = generate_hyeoks_report("short")
    time.sleep(2)
    report_mid = generate_hyeoks_report("mid")

    # 5. HTML 및 차트 결합
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.8; padding: 40px; color: #222; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 20px; text-transform: uppercase; letter-spacing: 1px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 25px; padding-bottom: 10px; }
        .stock-title { font-size: 34px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 19px; color: #2b6cb0; font-weight: bold; margin-top: 5px; }
        h2 { color: #1a365d; border-bottom: 1px solid #ddd; margin-top: 35px; }
        .chart-container { text-align: center; margin-top: 40px; page-break-inside: avoid; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .page-break { page-break-before: always; }
    </style>"""

    def make_chart(text, title):
        match = re.search(r'\((\d{6})\)', text)
        if match:
            code = match.group(1)
            return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'
        return ""

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)} {make_chart(report_short, "[야수들의 단기 돌파 전략] 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart(report_mid, "[직장인 스윙 종가베팅 전략] 일봉 차트")}
    </body></html>"""

    # 6. PDF 변환 및 텔레그램 전송
    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(pdf_filename, 'rb') as f:
        requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 직장인 스윙 & 단기 심층 리포트"})
    print("✅ 모든 프로세스 완료!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
