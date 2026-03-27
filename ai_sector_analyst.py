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
    # 💡 자동 탐색 엔진
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
    
    # 💡 [핵심 패치] AI가 코드를 환각으로 지어내지 못하도록 진짜 코드(r[1])를 매칭해서 던져줍니다!
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            # 구글 시트에 작은따옴표('005930)가 붙어있을 수 있으므로 깔끔하게 제거하고 6자리로 맞춥니다.
            real_code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({real_code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    print("🧠 [AI 수석 애널리스트] 입체적 데이터 융합 및 분석 중...")

    # 2. 강력한 환각 방지 프롬프트 적용
    prompt = f"""
    너는 메리츠증권의 최고 수석 퀀트 애널리스트 'HYEOKS AI'야. 
    다음 데이터를 바탕으로 오늘 시장의 핵심을 짚고, 가장 유망한 Top Pick 1종목을 골라.

    [데이터]
    - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
    - 테마흐름: {theme_history}
    - 타점후보 (이름과 진짜 코드 매칭 완료): 
    {stock_candidates}

    [작성 규칙] 
    - ⚠️ 아주 중요: 제목의 종목 코드는 반드시 [데이터]에 제공된 진짜 6자리 숫자를 사용해라. 절대 임의로 지어내지 마라!
    - 2페이지 분량이 되어도 좋으니, 수석 애널리스트의 깊이 있는 통찰력과 구체적인 고찰을 풍부하게 서술할 것.
    
    <div class="broker-name">HYEOKS Securities</div>
    <div class="header">
        <p class="stock-title">종목명 (종목코드)</p>
        <p class="subtitle">여기에 시선을 끄는 강력한 1줄 소제목 작성</p>
    </div>
    
    <div class="info-box">
        <b>Company Brief | 퀀트 분석팀</b><br>
        현재 매크로(환율, 유가 등) 환경과 테마 순환매를 바탕으로 이 종목을 지금 매수해야 하는 핵심 논리를 요약.
    </div>

    ## 1. 투자 포인트 및 실적 모멘텀 (Investment Points)
    이 기업의 비즈니스 모델 개요와 최근 시장에서 기대하고 있는 실적 턴어라운드, 모멘텀, 뉴스를 깊이 있게 고찰하여 서술.
    
    ## 2. 매크로 및 테마 순환매 고찰 (Macro & Theme Analysis)
    현재 지수와 환율 상황 속에서 왜 하필 이 테마(섹터)로 돈이 몰리고 있는지, 시간 조정을 어떻게 거쳤는지 상세히 분석.
    
    ## 3. 기술적 분석 및 매매 전략 (Technical Analysis)
    세력의 깃발형 패턴, 거래량 급감, 이평선 지지 등을 근거로 한 정확하고 구체적인 매수 타점 분석. 왜 지금이 최적의 손익비 자리인지 논증.
    
    ## 4. 리스크 팩터 및 손절 라인 (Risk Management)
    시나리오 이탈 시나 거시경제 악화 시 대응할 수 있는 리스크 관리 방안과 손절 기준.
    """

    # 3. AI 리포트 텍스트 생성
    response = model.generate_content(prompt)
    ai_report_md = response.text
    print("✅ AI 텍스트 리포트 생성 완료!")

    # 4. 차트 이미지 로드
    chart_html = ""
    match = re.search(r'\((\d{6})\)', ai_report_md) 
    if match:
        stock_code = match.group(1)
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{stock_code}.png"
        chart_html = f"""
        <div class="chart-container">
            <h3>📊 [기술적 분석] 일봉 캔들 차트</h3>
            <img src="{chart_url}" alt="Stock Chart">
        </div>
        """
        print(f"✅ 일봉 차트 캡처 완료! (정확한 종목코드: {stock_code})")
    else:
        print("⚠️ 종목 코드를 찾지 못해 차트를 생략합니다.")

    # 5. 마크다운 -> HTML 변환 (차트 첨부)
    html_content = markdown.markdown(ai_report_md)
    
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
        .chart-container { text-align: center; margin-top: 50px; page-break-inside: avoid; }
        .chart-container h3 { color: #333; font-size: 16px; margin-bottom: 15px; }
        .chart-container img { max-width: 90%; border: 1px solid #ccc; padding: 10px; background: #fff; box-shadow: 2px 2px 8px rgba(0,0,0,0.1); }
        .footer { text-align: center; font-size: 11px; color: #999; margin-top: 50px; border-top: 1px solid #eee; padding-top: 15px; }
    </style>
    """
    
    full_html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8">{css_style}</head>
    <body>
        {html_content}
        {chart_html} 
        <div class="footer">본 리포트는 OMAKASE AI 퀀트 시스템에 의해 자동 생성된 투자 참고용 자료입니다. (생성일: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')})</div>
    </body>
    </html>
    """

    # 6. HTML을 PDF 파일로 굽기
    pdf_filename = f"Omakase_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    
    options = {
        'page-size': 'A4',
        'margin-top': '0.7in', 'margin-right': '0.7in', 'margin-bottom': '0.7in', 'margin-left': '0.7in',
        'encoding': "UTF-8", 'enable-local-file-access': None
    }
    pdfkit.from_string(full_html, pdf_filename, options=options)
    print("✅ 고급 PDF 리포트 파일 렌더링 완료!")

    # 7. 텔레그램으로 PDF 전송
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "여기에_로컬테스트용_토큰입력":
        print("❌ [경고] 텔레그램 토큰을 확인하세요!")
        exit(1)
    else:
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as pdf_file:
            files = {'document': (pdf_filename, pdf_file, 'application/pdf')}
            data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 오늘의 [오마카세 리서치] 딥 리서치 PDF (오류 수정 완료)가 도착했습니다!"}
            
            response = requests.post(url, files=files, data=data)
            if response.status_code == 200:
                print("✅ 텔레그램 첨부파일 전송 성공!")
            else:
                print(f"❌ 텔레그램 전송 실패! [에러코드: {response.status_code}]")
                exit(1)

except Exception as e:
    print(f"\n❌ 심각한 에러 발생: {e}")
    exit(1)
