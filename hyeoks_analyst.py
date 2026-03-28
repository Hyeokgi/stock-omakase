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
            # 💡 시트에 새로 추가된 20일이격도(16번째), 대장주이력(17번째) 칼럼을 AI에게 먹여줍니다.
            stock_candidates += f"종목:{r[0]} ({code}), 타점:{r[9]}, 20일이격도:{r[16]}, 이력:{r[17]}\n"

    # 4. 💡 심층 분석 프롬프트 분리 적용 (단기 vs 중기/스윙)
    def generate_hyeoks_report(st_type):
        if st_type == "short":
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            [데이터]를 바탕으로 내일 당장 '갭 상승'이나 '강력한 슈팅'이 나올 단기 폭발 유망주 1종목을 선정해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항 - 반드시 지킬 것]
            1. 여백 및 단락: 스마트폰에서 읽기 편하도록 각 단락(문단) 사이에는 반드시 한 줄 이상의 빈 줄(엔터)을 넣어 넉넉한 여백을 확보할 것. 문장은 너무 길지 않게 끊어 칠 것.
            2. 용어 순화: 'SS급', '대장주(O)', '깃발 0일차' 같은 시스템 내부 기호나 은어를 절대 출력하지 말 것. 직관적이고 우아하게 풀어서 설명할 것.
            
            [출력 양식 (마크다운 및 HTML 구조 완벽 유지)]
            <div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (1~2줄의 강력한 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 단기 트레이딩 데스크</strong><br><br>
                (이곳에 종목의 현재 테마 상황과 강력한 매수 근거를 3~4문장으로 요약)
            </div>

            ## 1. 단기 수급 및 테마 모멘텀 심층 고찰
            (거시경제 및 유동성 연계 분석, 테마 강세 및 세력의 의도 서술)
            
            ## 2. 기술적 타점 분석 및 대응 시나리오
            * **캔들 및 매수 타점 분석:** (상세 서술)
            * **기준봉 출현 및 추가 상승 동력:** (상세 서술)
            * **대응 시나리오:**
              - **매수 타점:** (구체적인 진입 가격대와 논리)
              - **목표가:** (구체적인 가격대와 논리)
              - **손절 라인:** (칼같은 손절 가격과 이탈 시의 논리)
            """
        else:
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            [데이터]를 바탕으로 직장인이 마음 편히 종가에 매수하여 '대시세(20~50% 반등)'를 노릴 수 있는 스윙 유망주 1종목을 골라. (단기 종목과 겹치지 않게 할 것)
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항 - 반드시 지킬 것]
            1. 여백 및 단락: 스마트폰에서 읽기 편하도록 각 단락(문단) 사이에는 반드시 한 줄 이상의 빈 줄(엔터)을 넣어 넉넉한 여백을 확보할 것. 문장은 너무 길지 않게 끊어 칠 것.
            2. 용어 순화: '대장주(O)', 'A급', 'B급', '4음 1양' 같은 시스템 내부 은어를 절대 쓰지 말 것. 전문적이고 매끄럽게 풀어서 쓸 것.
            
            [출력 양식 (마크다운 및 HTML 구조 완벽 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">직장인 대시세 눌림목 종가베팅: (1~2줄의 강력한 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
                (이곳에 종목의 펀더멘털과 턴어라운드 기대감을 3~4문장으로 요약)
            </div>

            ## 1. 펀더멘털 및 턴어라운드 스토리
            가. 기업 개요 및 잃어버린 모멘텀 분석: (서술)
            나. 턴어라운드 시그널 포착: (서술)
            다. 향후 1주~1달 간 지속될 핵심 모멘텀: (상세 서술)
            
            ## 2. 이평선 밀집 및 모아가기 타점 전략
            가. 기술적 분석 (이평선 밀집과 추세 전환): (서술)
            나. 중기 스윙 모아가기 분할 타점:
              - **1차 진입 (초기 포지션 구축):** (시점 및 논리)
              - **2차 진입 (눌림목 활용):** (시점 및 논리)
              - **3차 진입 (추세 확인 후):** (시점 및 논리)
            다. 중기 목표가 및 손절 전략:
              - **목표가:** (1차, 2차 분할 설정)
              - **손절가:** (추세 이탈 기준)
            """
        return safe_generate_content(model, prompt).text

    print("🧠 [HYEOKS 수석 애널리스트] 2.5-flash 심층 분석 중...")
    report_short = generate_hyeoks_report("short")
    time.sleep(2)
    report_mid = generate_hyeoks_report("mid")

    # 5. HTML 및 차트 결합 (💡 가독성 극대화 CSS 디자인 패치)
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.9; padding: 40px; color: #222; font-size: 110%; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 22px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 35px; padding-bottom: 15px; }
        .stock-title { font-size: 38px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 21px; color: #2b6cb0; font-weight: bold; margin-top: 8px; line-height: 1.4; }
        
        /* 💡 추가된 요약 박스 디자인 */
        .summary-box { background-color: #f8fafc; padding: 25px; border-left: 5px solid #1a365d; margin-top: 25px; margin-bottom: 35px; border-radius: 6px; font-size: 105%; }
        
        h2 { color: #1a365d; border-bottom: 2px solid #edf2f7; margin-top: 45px; margin-bottom: 25px; padding-bottom: 10px; font-size: 130%; }
        
        /* 💡 문단 및 리스트 여백 대폭 확대 */
        p { margin-bottom: 20px; text-align: justify; word-break: keep-all; }
        ul, ol { margin-bottom: 25px; padding-left: 25px; }
        li { margin-bottom: 12px; }
        strong { color: #1a365d; }
        
        .chart-container { text-align: center; margin-top: 50px; page-break-inside: avoid; }
        .chart-container h3 { color: #4a5568; font-size: 110%; margin-bottom: 15px; }
        .chart-container img { max-width: 90%; border: 1px solid #cbd5e0; padding: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border-radius: 8px; }
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
