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
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

warnings.filterwarnings("ignore")

# ==========================================
# ⚙️ HYEOKS 인프라 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "1VSc9UyOKxYMNffAvPZfRXeeSfKE0ZHiT") 

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 엔진 가동 중...")

def safe_generate_content(model, prompt):
    """할당량 초과(429) 에러 발생 시 재시도하는 안전 함수"""
    for i in range(3):  # 최대 3번 재시도
        try:
            return model.generate_content(prompt)
        except Exception as e:
            if "429" in str(e):
                print(f"⚠️ 할당량 초과 발생. 10초 대기 후 재시도합니다... ({i+1}/3)")
                time.sleep(10)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 할당량이 부족합니다.")

try:
    # 1. 인증 및 서비스 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # 2. AI 모델 고정 설정
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    print("✅ 엔진 장착 완료: [gemini-2.5-flash]")

    # 3. 데이터 로드
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    # 4. 리포트 생성 (재시도 로직 적용)
    def generate_hyeoks_report(st_type):
        st_name = "단기 시세 분출(1-5일) 돌파/깃발형" if st_type == "short" else "중기(1주-1달) 이평선 밀집/턴어라운드 모아가기"
        prompt = f"""너는 HYEOKS 증권의 수석 애널리스트야. [데이터] 기반 {st_name} 전략 종목을 선정해 깊이 있게 분석해.
        데이터: {stock_candidates}
        양식: <div class="broker-name">HYEOKS Securities | {st_type.upper()}</div>
        <div class="header"><p class="stock-title">종목명 (코드)</p><p class="subtitle">미래 예측적 소제목</p></div>
        ## 투자 포인트 및 고찰 / ## 기술적 분석 / ## 리스크 관리"""
        response = safe_generate_content(model, prompt)
        return response.text

    print("🧠 [HYEOKS 수석 애널리스트] 2.5-flash 심층 분석 중...")
    report_short = generate_hyeoks_report("short")
    time.sleep(2) # 모델 부하 분산
    report_mid = generate_hyeoks_report("mid")

    # 5. HTML/PDF 생성 (가독성 중시 레이아웃)
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.8; padding: 40px; color: #222; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 20px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 25px; padding-bottom: 10px; }
        .stock-title { font-size: 34px; font-weight: 900; margin: 0; }
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
        {markdown.markdown(report_short)} {make_chart(report_short, "[단기 전략] 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart(report_mid, "[중기 전략] 일봉 차트")}
    </body></html>"""

    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    # 6. 구글 드라이브 업로드 (강력한 권한 옵션 적용)
    print(f"📂 드라이브 업로드 시도 중...")
    media = MediaFileUpload(pdf_filename, mimetype='application/pdf', resumable=True)
    uploaded_file = drive_service.files().create(
        body={'name': pdf_filename, 'parents': [FOLDER_ID]},
        media_body=media,
        fields='id',
        supportsAllDrives=True  # 공유 권한 에러 해결 핵심
    ).execute()
    file_id = uploaded_file.get('id')
    
    # 7. 앱시트 시트 기록
    report_link = f"https://drive.google.com/uc?id={file_id}"
    doc.worksheet("리포트_게시").append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
    print("✅ 드라이브 및 앱시트 게시 성공!")

    # 8. 텔레그램 발송
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(pdf_filename, 'rb') as f:
        requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 2.5-flash 심층 리포트"})
    print("✅ 모든 프로세스 완료!")

except Exception as e:
    print(f"❌ 최종 에러 발생: {e}")
    exit(1)
