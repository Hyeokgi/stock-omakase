import os
import re
import warnings
import datetime
import requests
import markdown
import pdfkit
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

warnings.filterwarnings("ignore")

# ==========================================
# ⚙️ 설정 및 보안 연동 (GitHub Secrets)
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
# 구글 드라이브 폴더 ID (본인의 폴더 ID로 교체 또는 Secrets 등록 가능)
FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "1VSc9UyOKxYMNffAvPZfRXeeSfKE0ZHiT") 

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

print("🤖 [HYEOKS 리서치 센터] 인프라 가동 및 데이터 수집 시작...")

try:
    # 1. AI 모델 탐색 및 설정
    genai.configure(api_key=GEMINI_API_KEY)
    available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
    target_model_name = next((m.replace('models/', '') for m in ['models/gemini-1.5-pro', 'models/gemini-1.5-flash'] if m in available_models), available_models[0].replace('models/', ''))
    model = genai.GenerativeModel(target_model_name)
    print(f"✅ 엔진 장착 완료: [{target_model_name}]")

    # 2. 구글 서비스(시트, 드라이브) 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    drive_service = build('drive', 'v3', credentials=creds)

    # 3. 데이터 로드 및 가공
    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange_rate = macro_data[1][4], macro_data[1][6]
    
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    print("🧠 [HYEOKS 수석 애널리스트] 단기/중기 전략 심층 분석 및 미래 예측 중...")

    # 4. 듀얼 리포트 생성 (단기/중기)
    def generate_report(strategy_type):
        strategy_prompt = "단기 시세 분출(1-5일) 돌파/깃발형" if strategy_type == "short" else "중기(1주-1달) 이평선 밀집/턴어라운드 모아가기"
        prompt = f"""너는 HYEOKS 증권 수석 애널리스트야. [데이터]를 기반으로 {strategy_prompt} 전략의 Top Pick 1종목을 선정해 깊이 있게 분석해. 
        데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}
        규칙: 종목 코드는 반드시 제공된 6자리 숫자를 쓸 것. 미래 예측적 관점을 포함할 것.
        양식: <div class="broker-name">HYEOKS Securities | {strategy_type.upper()}</div>
        <div class="header"><p class="stock-title">종목명 (코드)</p><p class="subtitle">소제목</p></div>
        ## 투자 포인트 / ## 기술적 분석 / ## 리스크 관리"""
        res = model.generate_content(prompt)
        return res.text

    report_short_text = generate_report("short")
    report_mid_text = generate_report("mid")

    # 5. HTML/CSS 및 차트 결합
    def get_chart_html(text, title):
        match = re.search(r'\((\d{6})\)', text)
        if match:
            code = match.group(1)
            return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'
        return ""

    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.8; padding: 40px; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 20px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 25px; }
        .stock-title { font-size: 32px; font-weight: 900; margin: 0; }
        .info-box { background: #f7fafc; padding: 20px; border-left: 5px solid #1a365d; margin: 20px 0; }
        .chart-container { text-align: center; margin-top: 30px; page-break-inside: avoid; }
        .chart-container img { max-width: 90%; border: 1px solid #ddd; padding: 5px; }
        .page-break { page-break-before: always; }
    </style>"""

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short_text)} {get_chart_html(report_short_text, "[단기 전략] 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid_text)} {get_chart_html(report_mid_text, "[중기 전략] 일봉 차트")}
    </body></html>"""

    # 6. PDF 생성 및 업로드 (드라이브 & 앱시트)
    pdf_file = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료.")

    # 드라이브 업로드
    media = MediaFileUpload(pdf_file, mimetype='application/pdf')
    uploaded = drive_service.files().create(body={'name': pdf_file, 'parents': [FOLDER_ID]}, media_body=media, fields='id').execute()
    file_id = uploaded.get('id')
    
    # 앱시트 시트 기록 (날짜, 파일링크)
    publish_sheet = doc.worksheet("리포트_게시")
    report_link = f"https://drive.google.com/uc?id={file_id}"
    publish_sheet.append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
    print("✅ 앱시트 게시판 업데이트 완료.")

    # 7. 텔레그램 전송
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(pdf_file, 'rb') as f:
        requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 오늘의 듀얼 전략 PDF 리포트"})
    print("✅ 텔레그램 전송 완료!")

except Exception as e:
    print(f"❌ 에러 발생: {e}")
    exit(1)
