import os
import warnings
warnings.filterwarnings("ignore")

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import google.generativeai as genai
import markdown
import pdfkit
import requests
import datetime

# ==========================================
# ⚙️ 설정: 깃허브 Secrets에서 API 키 불러오기 (보안 유지)
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

# PC에서 로컬 테스트할 때는 하드코딩하되, 깃허브에 올릴 땐 아래처럼 환경변수 처리해야 합니다!
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "여기에_임시로_키_입력")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "여기에_임시로_토큰_입력")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "여기에_임시로_챗ID_입력")

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro') 

print("🤖 [오마카세 리서치 센터] 데이터 수집 및 분석 시작...")

try:
    # 1. 구글 시트 데이터 수집 (이전과 동일)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    client = gspread.authorize(creds)
    doc = client.open_by_url(SHEET_URL)

    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, sp500, exchange_rate, wti_oil = macro_data[1][4], macro_data[1][5], macro_data[1][6], macro_data[1][7]

    raw_data = doc.worksheet("수급_Raw").get_all_values()[1:300] 
    theme_history = "\n".join([f"날짜:{r[0]}, 테마:{r[2]}, 대장주:{r[3]}, 등락률:{r[5]}%, 거래대금:{r[6]}억" for r in raw_data if len(r) >= 7])

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30] 
    stock_candidates = "\n".join([f"종목:{r[0]}, 현재가:{r[2]}, 타점:{r[9]}" for r in tech_data if len(r) >= 10])

    # 2. AI 프롬프트 (증권사 리포트 형식 강제)
    prompt = f"""
    너는 메리츠증권이나 키움증권의 최고 수석 퀀트 애널리스트야. 
    다음 데이터를 바탕으로 오늘 시장의 핵심을 짚고, 가장 유망한 Top Pick 1종목을 골라 한 장짜리 증권사 리포트(마크다운 형식)를 작성해.

    [데이터]
    - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
    - 테마흐름: {theme_history}
    - 타점후보: {stock_candidates}

    [작성 양식] (반드시 아래 구조를 지켜서 전문적인 리포트 톤으로 작성할 것)
    # [오마카세 리서치] 오늘의 Top Pick: (선정된 종목명)
    
    ## 1. 투자 요약 (Investment Summary)
    (매크로 환경과 테마 순환매를 바탕으로 이 종목을 선정한 핵심 논리 3줄 요약)
    
    ## 2. 기업 개요 및 실적 모멘텀 (Company Overview & Earnings)
    (이 기업이 어떤 비즈니스를 하는지 개요를 적고, 최근 실적 턴어라운드나 시장에서 기대하는 모멘텀이 무엇인지 애널리스트의 시각으로 서술할 것)
    
    ## 3. 기술적 분석 및 매매 전략 (Technical Analysis)
    (캔들, 거래량, 깃발형 패턴 등을 근거로 한 매수 타점 분석 및 손절 라인 제시)
    """

    response = model.generate_content(prompt)
    ai_report_md = response.text
    print("✅ AI 텍스트 리포트 생성 완료!")

    # 3. 마크다운 -> 증권사 리포트 스타일의 HTML로 변환
    html_content = markdown.markdown(ai_report_md, extensions=['tables', 'fenced_code'])
    
    # 🎨 PDF 디자인 CSS (실제 리포트처럼 깔끔하게)
    css_style = """
    <style>
        body { font-family: 'NanumGothic', 'Malgun Gothic', sans-serif; color: #333; line-height: 1.6; padding: 30px; }
        h1 { color: #004080; border-bottom: 2px solid #004080; padding-bottom: 10px; font-size: 24px; text-align: center; }
        h2 { color: #0059b3; margin-top: 25px; font-size: 18px; border-left: 4px solid #004080; padding-left: 10px; }
        p { font-size: 13px; text-align: justify; }
        .footer { text-align: center; font-size: 10px; color: #999; margin-top: 40px; border-top: 1px solid #ddd; padding-top: 10px; }
    </style>
    """
    
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8">{css_style}</head>
    <body>
        {html_content}
        <div class="footer">본 리포트는 오마카세 AI 퀀트 시스템에 의해 자동 생성된 투자 참고용 자료입니다. (생성일: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})</div>
    </body>
    </html>
    """

    # 4. HTML을 PDF 파일로 굽기 (wkhtmltopdf 사용)
    pdf_filename = "Omakase_Daily_Report.pdf"
    
    # 깃허브 액션 등 리눅스 환경에 맞춘 옵션
    options = {
        'page-size': 'A4',
        'margin-top': '0.75in',
        'margin-right': '0.75in',
        'margin-bottom': '0.75in',
        'margin-left': '0.75in',
        'encoding': "UTF-8",
        'no-outline': None
    }
    
    pdfkit.from_string(full_html, pdf_filename, options=options)
    print("✅ PDF 리포트 파일 생성 완료!")

    # 5. 텔레그램으로 PDF 파일 전송하기!
    if TELEGRAM_BOT_TOKEN != "여기에_임시로_토큰_입력" and TELEGRAM_CHAT_ID != "여기에_임시로_챗ID_입력":
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        
        with open(pdf_filename, 'rb') as pdf_file:
            files = {'document': (pdf_filename, pdf_file, 'application/pdf')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 오늘의 [오마카세 리서치] 심층 분석 PDF 리포트가 도착했습니다!"}
            requests.post(url, files=files, data=data)
        print("✅ 텔레그램 첨부파일 전송 완료!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
