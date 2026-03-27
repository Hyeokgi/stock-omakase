import os
import re
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
# ⚙️ 깃허브 Secrets 보안 연동
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "여기에_로컬테스트용_키입력")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "여기에_로컬테스트용_토큰입력")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "여기에_로컬테스트용_챗ID입력")

print("🤖 [HYEOKS 리서치 센터] 데이터 수집 및 분석 시작...")

try:
    # 💡 자동 탐색 엔진 장착
    genai.configure(api_key=GEMINI_API_KEY)
    available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
    
    target_model_name = None
    priority_models = ['models/gemini-1.5-pro', 'models/gemini-1.5-pro-latest', 'models/gemini-1.5-flash', 'models/gemini-pro']
    for m in priority_models:
        if m in available_models:
            target_model_name = m.replace('models/', '')
            break
    if not target_model_name:
        target_model_name = available_models[0].replace('models/', '')
        
    print(f"✅ 모델 탐색 완료! [{target_model_name}] 엔진 장착.")
    model = genai.GenerativeModel(target_model_name)

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
    
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            real_code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({real_code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    print("🧠 [HYEOKS 수석 애널리스트] 듀얼 전략(단기/중기) 분석을 시작합니다...")

    # ==========================================
    # 📝 전략 1: 단기 모멘텀 & 돌파 (Short-Term)
    # ==========================================
    prompt_short = f"""
    너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
    다음 데이터를 바탕으로 '단기적인 시세 분출(1일~5일)'이 기대되는 전고점 돌파 또는 단기 깃발형 패턴의 Top Pick 1종목을 골라.

    [데이터]
    - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
    - 테마흐름: {theme_history}
    - 타점후보 (이름과 진짜 코드 매칭 완료): 
    {stock_candidates}

    [작성 규칙] 
    - ⚠️ 제목의 종목 코드는 반드시 [데이터]에 제공된 진짜 6자리 숫자를 사용할 것.
    
    <div class="broker-name">HYEOKS Securities | Short-Term Strategy</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">단기 모멘텀 집중 분석: (1줄 소제목)</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | HYEOKS 단기 트레이딩 데스크</b><br>
        현재 수급과 차트 흐름상 단기 슈팅이 임박한 핵심 논리를 명확히 요약.
    </div>

    ## 1. 단기 수급 및 테마 모멘텀 (Momentum)
    현재 이 종목에 왜 단기적인 자금이 쏠리고 있는지, 뉴스나 재료를 기반으로 서술.
    
    ## 2. 기술적 타점 분석 (Technical Analysis)
    전고점 돌파, 깃발형 단기 수렴 등 캔들과 거래량 기반의 정확한 단기 매수 타점 분석.
    """

    response_short = model.generate_content(prompt_short)
    html_short = markdown.markdown(response_short.text)
    
    chart_html_short = ""
    match_short = re.search(r'\((\d{6})\)', response_short.text) 
    if match_short:
        code_s = match_short.group(1)
        chart_html_short = f'<div class="chart-container"><h3>📊 [단기 전략] 일봉 캔들 차트</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_s}.png"></div>'

    # ==========================================
    # 📝 전략 2: 중기 모아가기 (Mid-Term)
    # ==========================================
    prompt_mid = f"""
    너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
    다음 데이터를 바탕으로 '1주~1달 간 천천히 모아갈 수 있는' 중기 스윙 Top Pick 1종목을 골라. (단기 추천 종목과 겹치지 않게 다른 종목을 고를 것)

    [데이터]
    - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
    - 테마흐름: {theme_history}
    - 타점후보: 
    {stock_candidates}

    [작성 규칙] 
    - ⚠️ 탄탄한 기업이 주목을 받았다가 충분한 기간/가격 조정을 거치고, 이평선이 밀집되며 이제 막 반등의 기미가 보이는 종목을 찾아낼 것.
    - 제목의 종목 코드는 반드시 [데이터]에 제공된 6자리 숫자를 사용할 것.
    
    <div class="broker-name">HYEOKS Securities | Mid-Term Strategy</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">중기 스윙 모아가기 전략: (1줄 소제목)</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | HYEOKS 밸류에이션 데스크</b><br>
        실적 기반의 탄탄한 종목이 조정을 끝내고 턴어라운드 할 조짐을 보이는 논리 요약.
    </div>

    ## 1. 실적 모멘텀 및 턴어라운드 (Fundamentals)
    우량한 펀더멘털과 향후 1주~1달간 지속될 중기 모멘텀 분석.
    
    ## 2. 이평선 밀집 및 모아가기 전략 (Accumulation Strategy)
    기간/가격 조정을 거친 후 이평선 수렴 상태에서의 분할 매수 전략 및 중기 목표가/손절가 제시.
    """

    response_mid = model.generate_content(prompt_mid)
    html_mid = markdown.markdown(response_mid.text)
    
    chart_html_mid = ""
    match_mid = re.search(r'\((\d{6})\)', response_mid.text) 
    if match_mid:
        code_m = match_mid.group(1)
        chart_html_mid = f'<div class="chart-container"><h3>📊 [중기 전략] 일봉 캔들 차트</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_m}.png"></div>'

    print("✅ 단기 & 중기 듀얼 리포트 텍스트 및 차트 생성 완료!")

    # ==========================================
    # 🎨 PDF 통합 렌더링
    # ==========================================
    css_style = """
    <style>
        body { font-family: 'NanumGothic', 'Malgun Gothic', sans-serif; color: #222; line-height: 1.6; padding: 40px; margin: 0; }
        .broker-name { color: #1a365d; font-weight: 900; font-size: 20px; margin-bottom: 20px; text-transform: uppercase; }
        .header { border-bottom: 4px solid #1a365d; padding-bottom: 15px; margin-bottom: 25px; }
        .stock-title { font-size: 34px; font-weight: 900; margin: 0; color: #000; }
        .subtitle { font-size: 18px; color: #2b6cb0; margin-top: 8px; font-weight: bold; }
        .info-box { border-left: 5px solid #1a365d; background: #f7fafc; padding: 15px 20px; margin: 25px 0; font-size: 14px; }
        h2 { color: #1a365d; font-size: 17px; margin-top: 35px; border-bottom: 1px solid #e2e8f0; padding-bottom: 5px; }
        p { font-size: 14px; text-align: justify; word-break: keep-all; }
        .chart-container { text-align: center; margin-top: 40px; page-break-inside: avoid; }
        .chart-container h3 { color: #2d3748; font-size: 15px; margin-bottom: 15px; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; background: #fff; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .page-break { page-break-before: always; }
        .footer { text-align: center; font-size: 11px; color: #a0aec0; margin-top: 40px; border-top: 1px solid #edf2f7; padding-top: 15px; }
    </style>
    """
    
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8">{css_style}</head>
    <body>
        {html_short}
        {chart_html_short}
        
        <div class="page-break"></div>
        
        {html_mid}
        {chart_html_mid}
        
        <div class="footer">본 리포트는 HYEOKS AI 퀀트 시스템에 의해 자동 생성된 투자 참고용 자료입니다. (생성일: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})</div>
    </body>
    </html>
    """

    pdf_filename = f"HYEOKS_Research_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    
    options = {
        'page-size': 'A4',
        'margin-top': '0.7in', 'margin-right': '0.7in', 'margin-bottom': '0.7in', 'margin-left': '0.7in',
        'encoding': "UTF-8", 'enable-local-file-access': None
    }
    pdfkit.from_string(full_html, pdf_filename, options=options)
    print("✅ HYEOKS 리서치 듀얼 PDF 파일 렌더링 완료!")

    # ==========================================
    # 📲 텔레그램 발송 (심플한 메시지 처리)
    # ==========================================
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "여기에_로컬테스트용_토큰입력":
        print("❌ [경고] 텔레그램 토큰을 확인하세요!")
        exit(1)
    else:
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as pdf_file:
            files = {'document': (pdf_filename, pdf_file, 'application/pdf')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': "🤖 [HYEOKS 리서치] 일일 딥 리서치 PDF 리포트"}
            
            response = requests.post(url, files=files, data=data)
            if response.status_code == 200:
                print("✅ 텔레그램 첨부파일 전송 성공!")
            else:
                print(f"❌ 텔레그램 전송 실패! [에러코드: {response.status_code}]")
                exit(1)

except Exception as e:
    print(f"\n❌ 심각한 에러 발생: {e}")
    exit(1)
