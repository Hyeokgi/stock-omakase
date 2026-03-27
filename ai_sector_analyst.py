import os
import requests
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import markdown
import pdfkit
import datetime

# ==========================================
# ⚙️ 깃허브 Secrets 보안 연동
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

# 깃허브에 올릴 때는 보안을 위해 os.environ 사용!
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "여기에_로컬테스트용_키입력")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "여기에_로컬테스트용_토큰입력")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "여기에_로컬테스트용_챗ID입력")

print("🤖 [오마카세 리서치 센터] 데이터 수집 및 분석 시작...")

try:
    # 1. 구글 시트 데이터 수집
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

    print("🧠 [AI 수석 애널리스트] 다이렉트 통신망으로 1.5-flash 엔진 접속 중...")

    # 2. 증권사 리포트 형식 강제 프롬프트
    prompt = f"""
    너는 메리츠증권의 최고 수석 퀀트 애널리스트 '오마카세 AI'야. 
    다음 데이터를 바탕으로 오늘 시장의 핵심을 짚고, 가장 유망한 Top Pick 1종목을 골라.

    [데이터]
    - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
    - 테마흐름: {theme_history}
    - 타점후보: {stock_candidates}

    [작성 규칙] (반드시 아래 마크다운 양식을 지켜서 전문적인 애널리스트 톤으로 작성)
    
    <div class="broker-name">OMAKASE Securities</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">여기에 시선을 끄는 강력한 1줄 소제목 작성 (예: 1Q 서프라이즈, 여기도 기판이 완판)</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | 퀀트 분석팀</b><br>
        현재 매크로(환율, 유가 등) 환경과 테마 순환매를 바탕으로 이 종목을 지금 매수해야 하는 핵심 논리를 3줄로 요약.
    </div>

    ## 1. 기업 개요 및 실적 모멘텀 (Company Overview & Earnings)
    이 기업의 비즈니스 모델 개요와 최근 시장에서 기대하고 있는 실적 턴어라운드나 모멘텀, 뉴스를 애널리스트의 시각으로 서술.
    
    ## 2. 기술적 분석 및 매매 전략 (Technical Analysis)
    세력의 깃발형 패턴, 거래량 급감, 이평선 지지 등을 근거로 한 정확한 매수 타점 분석.
    
    ## 3. 리스크 팩터 및 손절 라인 (Risk Management)
    시나리오 이탈 시나 거시경제 악화 시 대응할 수 있는 칼 같은 손절 기준.
    """

    # 3. 무적의 다이렉트 API 통신 (SDK 에러 영구 차단)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {'Content-Type': 'application/json'}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    
    response = requests.post(url, headers=headers, json=data)
    response_data = response.json()
    
    if response.status_code != 200:
        print(f"❌ AI 통신 에러: {response_data}")
        exit(1)
        
    ai_report_md = response_data['candidates'][0]['content']['parts'][0]['text']
    print("✅ AI 텍스트 리포트 생성 완료!")

    # 4. 마크다운 -> 증권사 리포트 스타일의 HTML로 변환
    html_content = markdown.markdown(ai_report_md)
    
    # 🎨 업로드하신 메리츠증권 PDF 스타일 완벽 복제 CSS
    css_style = """
    <style>
        body { font-family: 'NanumGothic', 'Malgun Gothic', sans-serif; color: #222; line-height: 1.7; padding: 40px; margin: 0; }
        .broker-name { color: #800000; font-weight: 900; font-size: 22px; margin-bottom: 20px; font-style: italic; }
        .header { border-bottom: 4px solid #800000; padding-bottom: 15px; margin-bottom: 25px; }
        .stock-title { font-size: 36px; font-weight: 900; margin: 0; color: #000; }
        .subtitle { font-size: 20px; color: #004080; margin-top: 8px; font-weight: bold; }
        .info-box { border-left: 5px solid #800000; background: #f4f4f4; padding: 15px 20px; margin: 25px 0; font-size: 14px; }
        h2 { color: #800000; font-size: 18px; margin-top: 35px; border-bottom: 1px solid #ddd; padding-bottom: 5px; text-transform: uppercase; }
        p { font-size: 14px; text-align: justify; word-break: keep-all; }
        .footer { text-align: center; font-size: 11px; color: #999; margin-top: 50px; border-top: 1px solid #eee; padding-top: 15px; }
    </style>
    """
    
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8">{css_style}</head>
    <body>
        {html_content}
        <div class="footer">본 리포트는 OMAKASE AI 퀀트 시스템에 의해 자동 생성된 투자 참고용 자료입니다. (생성일: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})</div>
    </body>
    </html>
    """

    # 5. HTML을 PDF 파일로 굽기
    pdf_filename = f"Omakase_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    
    options = {
        'page-size': 'A4',
        'margin-top': '0.7in', 'margin-right': '0.7in', 'margin-bottom': '0.7in', 'margin-left': '0.7in',
        'encoding': "UTF-8", 'enable-local-file-access': None
    }
    pdfkit.from_string(full_html, pdf_filename, options=options)
    print("✅ 고급 PDF 리포트 파일 렌더링 완료!")

    # 6. 텔레그램으로 PDF 파일 전송
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "여기에_로컬테스트용_토큰입력":
        print("❌ [경고] 텔레그램 토큰이 설정되지 않았습니다.")
    else:
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as pdf_file:
            files = {'document': (pdf_filename, pdf_file, 'application/pdf')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 오늘의 [오마카세 리서치] 1장짜리 핵심 요약 PDF가 도착했습니다!"}
            
            response = requests.post(url, files=files, data=data)
            if response.status_code == 200:
                print("✅ 텔레그램 첨부파일 전송 성공!")
            else:
                print(f"❌ 텔레그램 전송 실패: {response.text}")
                exit(1)

except Exception as e:
    print(f"\n❌ 심각한 에러 발생: {e}")
    exit(1)
