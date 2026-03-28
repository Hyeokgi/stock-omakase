import os
import re
import time
import base64
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
# ⚙️ HYEOKS 인프라 설정
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# 💡 웹 앱 URL 유지
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 엔진 가동...")

def safe_generate_content(model, prompt):
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
    
    # 3. 데이터 로드 (💡 1차 수술: 현재가, 5일선, 20일선 데이터를 다시 넘겨줍니다!)
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            # 현재가[2], 등락률[3], 5일선[4], 20일선[5] 정보 추가
            stock_candidates += f"종목:{r[0]}({code}), 현재가:{r[2]}({r[3]}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{r[9]}, 20일이격도:{r[16] if len(r)>16 else ''}, 이력:{r[17] if len(r)>17 else ''}\n"

    # 4. 💡 심층 분석 프롬프트 분리 적용 (단기 vs 중기/스윙)
    def generate_hyeoks_report(st_type):
        if st_type == "short":
            prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            [데이터]를 바탕으로 내일 당장 '갭 상승'이나 '강력한 슈팅'이 나올 단기 폭발 유망주 1종목을 선정해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항 - 반드시 지킬 것]
            1. 분량 및 깊이: 각 목차별로 최소 2~3개의 상세한 단락을 작성하여, 기관용 리포트 수준의 방대한 분량과 전문적인 깊이를 확보할 것.
            2. 여백 및 단락: 스마트폰에서 읽기 편하도록 각 단락(문단) 사이에는 반드시 한 줄 이상의 빈 줄(엔터)을 넣어 넉넉한 여백을 확보할 것. 문장은 너무 길지 않게 끊어 칠 것.
            3. 용어 순화: 'SS급', '대장주(O)', '깃발 0일차' 같은 시스템 내부 기호나 은어를 절대 출력하지 말 것. 직관적이고 우아하게 풀어서 설명할 것.
            4. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 내가 제공한 [현재가], [5일선], [20일선] 데이터를 기준으로 수학적이고 논리적으로 산출할 것. 현재가보다 낮은 목표가를 제시하는 등 상식에 어긋나는 소설을 절대 쓰지 말 것.
            
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
            1. 분량 및 깊이: 각 목차별로 최소 2~3개의 상세한 단락을 작성하여, 기관용 리포트 수준의 방대한 분량과 전문적인 깊이를 확보할 것.
            2. 여백 및 단락: 스마트폰에서 읽기 편하도록 각 단락(문단) 사이에는 반드시 한 줄 이상의 빈 줄(엔터)을 넣어 넉넉한 여백을 확보할 것. 문장은 너무 길지 않게 끊어 칠 것.
            3. 용어 순화: '대장주(O)', 'A급', 'B급', '4음 1양' 같은 시스템 내부 은어를 절대 쓰지 말 것. 전문적이고 매끄럽게 풀어서 쓸 것.
            4. 분석 초점: 과거 상한가나 폭등 이력이 있는 종목이 거래량이 마르며 20일선에 수렴한 상황을 집중 조명할 것.
            5. 절대적인 가격 규칙: 매수 타점, 1차/2차 목표가, 손절가는 반드시 내가 제공한 [현재가], [5일선], [20일선] 데이터를 기준으로 수학적이고 논리적으로 산출할 것. 현재가보다 낮은 목표가를 제시하는 등 상식에 어긋나는 소설을 절대 쓰지 말 것.
            
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
            가. 기업 개요 및 잃어버린 모멘텀 분석: (상세 서술)
            나. 턴어라운드 시그널 포착: (상세 서술)
            다. 향후 1주~1달 간 지속될 핵심 모멘텀: (상세 서술)
            
            ## 2. 이평선 밀집 및 모아가기 타점 전략
            가. 기술적 분석 (이평선 밀집과 추세 전환): (상세 서술)
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

    # 5. HTML 및 차트 결합 (보존된 CSS 가독성 디자인)
    css = """<style>
        body { font-family: 'NanumGothic', sans-serif; line-height: 1.9; padding: 40px; color: #222; font-size: 110%; }
        .broker-name { color: #1a365d; font-weight: bold; font-size: 22px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; }
        .header { border-bottom: 4px solid #1a365d; margin-bottom: 35px; padding-bottom: 15px; }
        .stock-title { font-size: 38px; font-weight: 900; margin: 0; }
        .subtitle { font-size: 21px; color: #2b6cb0; font-weight: bold; margin-top: 8px; line-height: 1.4; }
        .summary-box { background-color: #f8fafc; padding: 25px; border-left: 5px solid #1a365d; margin-top: 25px; margin-bottom: 35px; border-radius: 6px; font-size: 105%; }
        h2 { color: #1a365d; border-bottom: 2px solid #edf2f7; margin-top: 45px; margin-bottom: 25px; padding-bottom: 10px; font-size: 130%; }
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

    # 6. PDF 변환
    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    # 7. 🚀 구글 드라이브 업로드 (GAS 터널 방식) & 앱시트 기록
    if GAS_WEB_APP_URL.startswith("http"):
        print("📂 구글 드라이브(앱시트용) 업로드 진행 중...")
        with open(pdf_filename, "rb") as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        payload = {"filename": pdf_filename, "base64": pdf_base64}
        try:
            res = requests.post(GAS_WEB_APP_URL, json=payload)
            if res.status_code == 200 and "success" in res.text:
                file_id = res.json().get("id")
                # 앱시트용 바로가기 링크 생성
                report_link = f"https://drive.google.com/uc?id={file_id}"
                doc.worksheet("리포트_게시").append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
                print("✅ 구글 드라이브 저장 및 앱시트(리포트_게시) 연동 완료!")
            else:
                print(f"⚠️ 드라이브 업로드 실패: {res.text}")
        except Exception as e:
            print(f"⚠️ 드라이브 통신 에러: {e}")

    # 8. 텔레그램 전송
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램으로 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] 직장인 스윙 & 단기 심층 리포트"})
            if response.status_code == 200:
                print("✅ 텔레그램 첨부파일 전송 성공!")
            else:
                print(f"❌ 텔레그램 전송 실패! [에러코드: {response.status_code}]")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
