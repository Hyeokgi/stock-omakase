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
# ⚙️ HYEOKS 인프라 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
# 💡 여기에 실제 폴더 ID를 정확히 입력하세요 (또는 GitHub Secrets 이용)
FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "1VSc9UyOKxYMNffAvPZfRXeeSfKE0ZHiT") 

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

print("🤖 [HYEOKS 리서치 센터] 인프라 가동 및 데이터 수집 시작...")

try:
    # 1. 인증 및 서비스 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    
    # 구글 시트 & 드라이브 API 연결
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # 2. AI 모델 설정 (자동 탐색)
    genai.configure(api_key=GEMINI_API_KEY)
    available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
    target_model_name = next((m.replace('models/', '') for m in ['models/gemini-1.5-pro', 'models/gemini-1.5-flash'] if m in available_models), available_models[0].replace('models/', ''))
    model = genai.GenerativeModel(target_model_name)
    print(f"✅ 엔진 장착 완료: [{target_model_name}]")

    # 3. 시장 데이터 수집
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    print("🧠 [HYEOKS 수석 애널리스트] 시장의 미래를 예측하는 심층 듀얼 분석 중...")

    # 4. 리포트 생성 함수 (단기/중기)
    def generate_hyeoks_report(st_type):
        st_name = "단기 시세 분출(1-5일) 돌파/깃발형" if st_type == "short" else "중기(1주-1달) 이평선 밀집/턴어라운드 모아가기"
        prompt = f"""너는 HYEOKS 증권의 수석 애널리스트야. [데이터] 기반 {st_name} 전략 종목을 선정해.
        데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}
        규칙: 2페이지가 넘어도 좋으니 깊이 있게 서술할 것. 코드는 반드시 제공된 6자리 숫자를 쓸 것.
        양식: <div class="broker-name">HYEOKS Securities | {st_type.upper()}</div>
        <div class="header"><p class="stock-title">종목명 (코드)</p><p class="subtitle">미래 예측적 소제목</p></div>
        ## 투자 포인트 및 고찰 / ## 기술적 분석 / ## 리스크 관리"""
        return model.generate_content(prompt).text

    report_short = generate_hyeoks_report("short")
    report_mid = generate_hyeoks_report("mid")

    # 5. 차트 매칭 및 HTML 변환
    def make_chart_section(text, title):
        match = re.search(r'\((\d{6})\)', text)
        if match:
            code = match.group(1)
            return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'
        return ""

    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.8; padding: 40px; color: #222; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 20px; text-transform: uppercase; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 25px; padding-bottom: 10px; }
        .stock-title { font-size: 34px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 19px; color: #2b6cb0; font-weight: bold; margin-top: 5px; }
        h2 { color: #1a365d; border-bottom: 1px solid #ddd; margin-top: 35px; }
        .chart-container { text-align: center; margin-top: 40px; page-break-inside: avoid; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .page-break { page-break-before: always; }
    </style>"""

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)} {make_chart_section(report_short, "[단기 전략] 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart_section(report_mid, "[중기 전략] 일봉 차트")}
    </body></html>"""

    # 6. PDF 저장 및 업로드 (수정된 로직)
    pdf_filename = f"HYEOKS_Research_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    
    print(f"📂 HYEOKS 리서치 드라이브 업로드 시도... (폴더: {FOLDER_ID})")
    media = MediaFileUpload(pdf_filename, mimetype='application/pdf')
    file_metadata = {'name': pdf_filename, 'parents': [FOLDER_ID]}
    
    # 💡 [핵심 수정] supportsAllDrives=True 옵션을 추가하여 서비스 계정의 용량 제한을 우회합니다.
    uploaded_file = drive_service.files().create(
        body=file_metadata, 
        media_body=media, 
        fields='id',
        supportsAllDrives=True  # 이 옵션이 공유 폴더 업로드의 핵심입니다!
    ).execute()
    
    file_id = uploaded_file.get('id')
    
    # 7. 앱시트 시트 기록
    publish_sheet = doc.worksheet("리포트_게시")
    report_link = f"https://drive.google.com/uc?id={file_id}"
    publish_sheet.append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
    print("✅ 앱시트 및 드라이브 게시 완료!")

    # 8. 텔레그램 발송
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(pdf_filename, 'rb') as f:
        requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 오늘의 심층 리포트 (단기/중기)"})
    print("✅ 텔레그램 전송 완료!")

except Exception as e:
    print(f"❌ 에러 발생: {e}")
    exit(1)
