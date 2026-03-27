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

    print("🧠 [HYEOKS 수석 애널리스트] 시장의 미래를 예측하는 심층 듀얼 분석 중... (시간이 조금 더 소요될 수 있습니다)")

    # ==========================================
    # 📝 전략 1: 단기 모멘텀 & 돌파 (Short-Term) - 심층 예측 버전
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
    - ⚠️ 분량 제한 없음. 1페이지를 가득 채우거나 넘어가도 좋으니, 단순 묘사를 넘어 현상의 이면을 분석하고 '미래를 예측'하는 통찰력을 보여줄 것.
    - 제목의 종목 코드는 반드시 [데이터]에 제공된 진짜 6자리 숫자를 사용할 것.
    
    <div class="broker-name">HYEOKS Securities | Short-Term Strategy</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">단기 모멘텀 집중 분석: (1줄 소제목)</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | HYEOKS 단기 트레이딩 데스크</b><br>
        현재 수급과 차트 흐름상 단기 슈팅이 임박한 핵심 논리를 명확히 요약.
    </div>

    ## 1. 단기 수급 및 테마 모멘텀 심층 고찰 (Momentum & Predictive Analysis)
    현재 이 종목에 왜 단기적인 자금이 쏠리고 있는지 단순 나열을 넘어, 거시경제 지표(환율/유가 등)와 연계하여 세력의 의도와 향후 며칠간의 테마 흐름을 예측하여 서술할 것.
    
    ## 2. 기술적 타점 분석 및 대응 시나리오 (Technical Analysis)
    전고점 돌파, 깃발형 단기 수렴 등 캔들과 거래량 기반의 정확한 매수 타점 분석. 돌파 성공 시의 1차 목표가와 실패 시의 칼 같은 손절 라인을 제시.
    """

    response_short = model.generate_content(prompt_short)
    html_short = markdown.markdown(response_short.text)
    
    chart_html_short = ""
    match_short = re.search(r'\((\d{6})\)', response_short.text) 
    if match_short:
        code_s = match_short.group(1)
        chart_html_short = f'<div class="chart-container"><h3>📊 [단기 전략] 일봉 캔들 차트</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_s}.png"></div>'

    # ==========================================
    # 📝 전략 2: 중기 모아가기 (Mid-Term) - 심층 밸류에이션 버전
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
    - 분량 제한 없이 각 항목에 대해 매우 깊이 있고 입체적인 분석을 제공할 것.
    - 제목의 종목 코드는 반드시 [데이터]에 제공된 6자리 숫자를 사용할 것.
    
    <div class="broker-name">HYEOKS Securities | Mid-Term Strategy</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">중기 스윙 모아가기 전략: (1줄 소제목)</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | HYEOKS 밸류에이션 데스크</b><br>
        실적 기반의 우량주가 조정을 끝내고 턴어라운드 할 조짐을 보이는 핵심 논리 요약.
    </div>

    ## 1. 펀더멘털 및 턴어라운드 스토리 (Fundamentals & Future Outlook)
    왜 이 기업이 단순한 테마주가 아니라 중기로 끌고 갈 만한 우량한 실적 모멘텀을 가지고 있는지, 향후 1주~1달간 지속될 시장의 모멘텀을 상세히 분석할 것.
    
    ## 2. 이평선 밀집 및 모아가기 타점 전략 (Accumulation Strategy)
    충분한 기간/가격 조정을 거친 후 현재 이평선 수렴 상태에서 어떻게 분할 매수를 진행해야 하는지(예: N분할 매수, 눌림목 공략 등) 구체적인 전략과 중기 목표가를 제시할 것.
    """

    response_mid = model.generate_content(prompt_mid)
    html_mid = markdown.markdown(response_mid.text)
    
    chart_html_mid = ""
    match_mid = re.search(r'\((\d{6})\)', response_mid.text) 
    if match_mid:
        code_m = match_mid.group(1)
        chart_html_mid = f'<div class="chart-container"><h3>📊 [중기 전략] 일봉 캔들 차트</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code_m}.png"></div>'

    print("✅ 단기 & 중기 듀얼 심층 리포트 생성 완료!")

    # ==========================================
    # 🎨 PDF 통합 렌더링 (강제 페이지 넘김 완벽 적용)
    # ==========================================
    css_style = """
    <style>
        body { font-family: 'NanumGothic', 'Malgun Gothic', sans-serif; color: #222; line-height: 1.8; padding: 40px; margin: 0; }
        .broker-name { color: #1a365d; font-weight: 900; font-size: 20px; margin-bottom: 20px; text-transform: uppercase; letter-spacing: 1px; }
        .header { border-bottom: 4px solid #1a365d; padding-bottom: 15px; margin-bottom: 25px; }
        .stock-title { font-size: 34px; font-weight: 900; margin: 0; color: #000; }
        .subtitle { font-size: 18px; color: #2b6cb0; margin-top: 8px; font-weight: bold; }
        .info-box { border-left: 5px solid #1a365d; background: #f7fafc; padding: 20px 25px; margin: 25px 0; font-size: 14.5px; box-shadow: 2px 2px 5px rgba(0,0,0,0.03); }
        h2 { color: #1a365d; font-size: 18px; margin-top: 40px; border-bottom: 1px solid #e2e8f0; padding-bottom: 8px; }
        p { font-size: 14.5px; text-align: justify; word-break: keep-all; margin-bottom: 15px; }
        .chart-container { text-align: center; margin-top: 40px; page-break-inside: avoid; }
        .chart-container h3 { color: #2d3748; font-size: 15px; margin-bottom: 15px; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; background: #fff; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        
        /* 💡 핵심: 어떤 상황에서도 다음 리포트는 완벽하게 새 페이지에서 시작하도록 강제 분리 */
        .page-break { page-break-before: always; display: block; height: 0; margin: 0; padding: 0; }
        
        .footer { text-align: center; font-size: 11px; color: #a0aec0; margin-top: 40px; border-top: 1px solid #edf2f7; padding-top: 15px; }
    </style>
    """
    
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8">{css_style}</head>
    <body>
        <div class="report-section">
            {html_short}
            {chart_html_short}
        </div>
        
        <div class="page-break"></div>
        
        <div class="report-section">
            {html_mid}
            {chart_html_mid}
        </div>
        
        <div class="footer">본 리포트는 HYEOKS AI 퀀트 시스템에 의해 자동 생성된 투자 참고용 자료입니다. (생성일: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})</div>
    </body>
    </html>
    """

    pdf_filename = f"HYEOKS_Research_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    
    options = {
        'page-size': 'A4',
        'margin-top': '0.75in', 'margin-right': '0.75in', 'margin-bottom': '0.75in', 'margin-left': '0.75in',
        'encoding': "UTF-8", 'enable-local-file-access': None
    }
    pdfkit.from_string(full_html, pdf_filename, options=options)
    print("✅ HYEOKS 리서치 듀얼 PDF 파일 렌더링 완료!")

    # ==========================================
    # 📲 텔레그램 발송 
    # ==========================================
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "여기에_로컬테스트용_토큰입력":
        print("❌ [경고] 텔레그램 토큰을 확인하세요!")
        exit(1)
    else:
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as pdf_file:
            files = {'document': (pdf_filename, pdf_file, 'application/pdf')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': "🤖 [HYEOKS 리서치] 일일 딥 리서치 PDF (단기 & 중기 전략)"}
            
            response = requests.post(url, files=files, data=data)
            if response.status_code == 200:
                print("✅ 텔레그램 첨부파일 전송 성공!")
            else:
                print(f"❌ 텔레그램 전송 실패! [에러코드: {response.status_code}]")
                exit(1)

except Exception as e:
    print(f"\n❌ 심각한 에러 발생: {e}")
    exit(1)
