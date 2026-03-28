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
    for i in range(3):  # 최대 3번 재시도
        try:
            return model.generate_content(prompt)
        except Exception as e:
            if "429" in str(e):
                print(f"⚠️ API 할당량 초과. 10초 대기 후 재시도합니다... ({i+1}/3)")
                time.sleep(10)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 할당량이 부족합니다. 잠시 후 다시 시도하세요.")

try:
    # 1. 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    # 2. AI 모델 설정 (2.5-flash 고정)
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    print("✅ 엔진 장착 완료: [gemini-2.5-flash]")

    # 3. 데이터 로드 (매크로 및 주가 데이터)
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            stock_candidates += f"종목:{r[0]} ({code}), 현재가:{r[2]}, 타점:{r[9]}\n"

    # 4. 💡 심층 분석 프롬프트 분리 적용 (단기 vs 중기/스윙)
    def generate_hyeoks_report(st_type):
        if st_type == "short":
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            다음 데이터를 바탕으로 내일 당장 '갭 상승'을 띄우거나 '강력한 추가 슈팅'이 나올 확률이 가장 높은 단기 폭발 유망주 1종목을 골라.

            [데이터]
            - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
            - 타점후보: {stock_candidates}

            [심층 분석 지시사항] 
            1. 뉴스 및 모멘텀 분석: 이 종목을 둘러싼 최근 뉴스나 재료의 크기를 가늠하고, 이 상승 추세가 내일도 이어질 만큼 파급력이 있는지 정확히 추측할 것.
            2. 기술적 폭발력: 전고점 돌파나 거래량 터진 도지 등, 세력이 자금을 밀어 넣고 있는 흔적을 분석하여 단기 상승 여력을 논증할 것.
            3. 제목의 종목 코드는 반드시 [데이터]에 제공된 6자리 숫자를 사용할 것.
            
            <div class="broker-name">HYEOKS Securities | Short-Term Strategy</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (1줄 소제목)</p>
            </div>
            
            ## 1. 재료 분석 및 추세 연속성 (News & Momentum)
            현재 상승을 만든 뉴스와 테마의 강도를 분석하고, 내일 추가 상승(갭업 등)이 가능한 이유를 서술.
            
            ## 2. 거래량 및 타점 고찰 (Volume & Technicals)
            터진 거래량과 캔들의 의미를 해석하고, 세력의 이탈이 아닌 추가 돌파로 보는 근거 서술.
            
            ## 3. 단기 대응 시나리오 (Trading Plan)
            돌파 성공 시 목표가와, 추세가 꺾일 때의 칼 같은 손절 기준 제시.
            """
        else:
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            다음 데이터를 바탕으로 직장인이 마음 편히 종가에 매수하여 '대시세(20~50% 반등)'를 노릴 수 있는 최고의 스윙 유망주 1종목을 골라.

            [데이터]
            - 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}
            - 타점후보: {stock_candidates}

            [직장인 스윙 특화 지시사항] 
            1. 대장주의 조건: 최근 1~2달 내에 상한가, 장대양봉, 또는 막대한 거래대금(시가총액 이상)을 동반하며 폭등했던 주도주 이력이 있는지 확인할 것 (엔벨로프 20,40 상단 터치 급의 파괴력).
            2. 10~20일 거래량 급감 조정: 고점 형성 후 최소 10일~20일 동안 거래량이 완벽하게 마르며 조정을 받은 상태인지 분석할 것.
            3. 이평선 밀집 & 20일선 지지 타점: 현재 주가가 20일 이동평균선 부근까지 내려와 5, 10, 20일선이 수렴(밀집)하고 있으며, 여기서 반등의 기미(도지, 첫 양봉 등)를 보이는지 고찰할 것.
            
            <div class="broker-name">HYEOKS Securities | Swing & Closing Bet Strategy</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">대시세 눌림목 종가베팅: (1줄 소제목)</p>
            </div>
            
            ## 1. 폭등 이력 및 대장주 명분 (Market Leader Catalyst)
            이 종목이 과거 어떤 재료와 거래대금으로 폭등했었는지, 왜 시장의 주도주로 볼 수 있는지 강력한 근거 서술.
            
            ## 2. 시간 조정 및 거래량 분석 (Time Correction & Volume)
            고점 이후 10~20일간의 가격/기간 조정 흐름과 거래량 급감 상태를 분석하여 세력이 아직 이탈하지 않았다는 증거를 제시.
            
            ## 3. 직장인 매수 타점 및 50% 대반등 시나리오 (Trading Plan)
            현재 20일선 부근의 이평선 밀집 지역이 왜 가장 안전한 종가베팅 타점인지 설명. 분할 매수 전략과 함께, 직장인이 여유롭게 끌고 갈 수 있는 목표 수익률(대반등 시나리오) 및 칼같은 손절 라인 제시.
            """
        
        response = safe_generate_content(model, prompt)
        return response.text

    print("🧠 [HYEOKS 수석 애널리스트] 2.5-flash 심층 분석 중...")
    report_short = generate_hyeoks_report("short")
    time.sleep(2) # API 부하 분산
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
        {markdown.markdown(report_short)} {make_chart(report_short, "[단기 전략] 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart(report_mid, "[직장인 스윙/종가베팅 전략] 일봉 차트")}
    </body></html>"""

    # 6. PDF 변환
    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    # 7. 텔레그램 전송 (드라이브 업로드 제거, 텔레그램 직배송)
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ [경고] 텔레그램 토큰 또는 챗 ID가 설정되지 않았습니다!")
        exit(1)
        
    print("📲 텔레그램으로 PDF 발송 중...")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    with open(pdf_filename, 'rb') as f:
        response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 직장인 스윙 & 단기 심층 리포트가 도착했습니다!"})
        
        if response.status_code == 200:
            print("✅ 텔레그램 첨부파일 전송 성공!")
        else:
            print(f"❌ 텔레그램 전송 실패! [에러코드: {response.status_code}]")
            exit(1)

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
