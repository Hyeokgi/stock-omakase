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
import PIL.Image 
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
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"

print("🤖 [HYEOKS 리서치 센터] 2.5-flash 비전(Vision) 심층 분석 엔진 가동...")

def safe_generate_content(model, prompt_data):
    for i in range(5):
        try:
            return model.generate_content(prompt_data)
        except Exception as e:
            if "429" in str(e):
                wait_time = 30 * (i + 1)
                print(f"⚠️ API 할당량 초과. 숨 고르기를 위해 {wait_time}초 대기합니다... ({i+1}/5)")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 에러")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')
    
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    stock_candidates = ""
    for r in tech_data:
        if len(r) >= 10:
            code = r[1].replace("'", "").strip().zfill(6)
            # 💡 [핵심 패치] 오마카세 시트에서 '거래량상태' 데이터를 읽어옴 (인덱스 18번)
            vol_status = r[18] if len(r)>18 else ''
            stock_candidates += f"종목:{r[0]}({code}), 현재가:{r[2]}({r[3]}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{r[9]}, 20일이격도:{r[16] if len(r)>16 else ''}, 이력:{r[17] if len(r)>17 else ''}, 거래량상태:{vol_status}\n"

    def generate_hyeoks_report(st_type):
        sys_msg = "내일 당장 급등할 단기 폭발" if st_type == "short" else "직장인 스윙 종가베팅"
        pick_prompt = f"너는 HYEOKS 수석 애널리스트야. 다음 데이터 중 '{sys_msg}' 유망주로 가장 완벽한 1종목을 골라. 다른 말은 절대 하지 말고 '오직 6자리 종목코드 숫자'만 출력해.\n데이터: {stock_candidates}"
        
        raw_code = safe_generate_content(model, pick_prompt).text
        target_code = re.search(r'\d{6}', raw_code).group()

        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)
            
        img = PIL.Image.open(img_path)

        # 💡 예전의 '심층 분석' 프롬프트 부활 + 방배동선수/은어배제/오버나잇 로직 추가
        if st_type == "short":
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 단기 폭발 유망주 심층 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항 - 반드시 지킬 것]
            1. 금지어 철저 배제: 'S급', 'A급', 'B급', '깃발', '0일차' 등 내부 시스템 은어 및 등급 기호를 절대 사용하지 말 것. 실제 증권사 리포트처럼 정제되고 고급스러운 금융 용어로만 서술할 것.
            2. 분량 및 깊이: 절대 짧게 요약하지 말고, 기관용 리포트처럼 각 목차별로 최소 2~3개의 상세한 단락을 작성하여 방대한 깊이를 확보할 것.
            3. 👁️ 시각적 차트 및 거래량 판독 (방배동선수 룰): 차트를 분석할 때 단순히 이평선 위치만 보지 말고, 데이터에 제공된 [거래량상태]를 바탕으로 "주가가 눌림을 줄 때 거래량이 의미 있게 급감하며 투매가 멈추었는지"를 최우선으로 분석하여 서술할 것.
            4. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 [현재가], [5일선], [20일선]을 기준으로 논리적으로 계산할 것.
            5. 가독성: 문단 사이에 빈 줄(엔터)을 넉넉히 넣을 것.
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">단기 모멘텀 집중 분석: (1~2줄 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 단기 트레이딩 데스크</strong><br><br>
                (종목의 현재 테마 상황과 매수 근거를 3~4문장으로 요약)
            </div>

            ## 1. 단기 수급 및 테마 모멘텀 심층 고찰
            (거시경제 연계 분석 및 테마 강세 이유를 매우 상세히 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 타점 시나리오
            * **차트 정밀 판독:** (이미지와 [거래량상태] 데이터를 종합하여 생생하게 서술)
            * **매수 타점:** (가격과 논리)
            * **목표가:** (가격)
            * **손절 라인:** (가격)
            """
        else:
            final_prompt = f"""
            너는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 스윙 심층 리포트를 작성해.
            데이터: {stock_candidates} / 매크로: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}

            [특별 지시사항 - 반드시 지킬 것]
            1. 금지어 철저 배제: 'S급', 'A급', 'B급', '깃발', '0일차' 등 내부 시스템 은어 및 등급 기호를 절대 사용하지 말 것. 실제 증권사 리포트처럼 정제되고 고급스러운 금융 용어로만 서술할 것.
            2. 분량 및 깊이: 절대 짧게 요약하지 말고, 기관용 리포트처럼 각 목차별로 최소 2~3개의 상세한 단락을 작성하여 펀더멘털 스토리를 풍부하게 풀어낼 것.
            3. 👁️ 시각적 차트 및 거래량 판독 (방배동선수 룰): 차트를 분석할 때 단순히 이평선 위치만 보지 말고, 데이터에 제공된 [거래량상태]를 바탕으로 "주가가 눌림을 줄 때 거래량이 의미 있게 급감하며 투매가 멈추었는지"를 최우선으로 분석하여 서술할 것.
            4. 🛡️ 스윙 리포트 필수 요건 (오버나잇 리스크 관리): "현재 매크로 변동성을 감안하여, 1차 종가 진입 비중은 30% 이하로 엄격히 제한하고, 익일 미국장 이슈로 인한 갭하락 시 20일선 부근에서 2차 매수하는 보수적 시나리오를 권장함." 이라는 맥락을 반드시 리포트에 녹여낼 것.
            5. 절대적인 가격 규칙: 매수 타점, 목표가, 손절가는 반드시 [현재가], [5일선], [20일선]을 기준으로 논리적으로 계산할 것.
            6. 가독성: 문단 사이에 빈 줄(엔터)을 넉넉히 넣을 것.
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">종목명 (종목코드)</p>
                <p class="subtitle">직장인 대시세 눌림목 종가베팅: (1~2줄 소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
                (종목의 펀더멘털과 턴어라운드 기대감을 3~4문장으로 요약)
            </div>

            ## 1. 펀더멘털 및 턴어라운드 스토리
            (기업 개요 및 향후 지속될 핵심 모멘텀을 매우 상세히 서술)
            
            ## 2. 👁️ AI 시각적 차트 판독 및 분할 매수 전략
            * **차트 정밀 판독:** (이미지와 [거래량상태] 데이터를 종합하여 거래량 급감 관점에서 생생하게 서술)
            * **1차 진입 / 2차 진입:** (가격, 분할 논리 및 오버나잇 비중 조절)
            * **목표가 및 손절가:** (가격)
            """
            
        response = safe_generate_content(model, [final_prompt, img])
        img.close()
        os.remove(img_path)
        return response.text, target_code

    print("🧠 [HYEOKS 수석 애널리스트] 비전 데이터 심층 분석 중...")
    report_short, code_short = generate_hyeoks_report("short")
    
    # 💡 [핵심 패치] 429 에러(8분 딜레이) 방지를 위해 휴식 시간을 30초로 넉넉하게 연장
    print("⏳ 단기 리포트 완료! API 과부하 방지를 위해 30초 휴식합니다...")
    time.sleep(30)
    
    report_mid, code_mid = generate_hyeoks_report("mid")

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

    def make_chart_html(code, title):
        return f'<div class="chart-container"><h3>📊 {title}</h3><img src="https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{code}.png"></div>'

    full_html = f"""<!DOCTYPE html><html><head><meta charset="utf-8">{css}</head><body>
        {markdown.markdown(report_short)} {make_chart_html(code_short, "[야수들의 단기 돌파 전략] AI 분석 일봉 차트")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart_html(code_mid, "[직장인 스윙 종가베팅 전략] AI 분석 일봉 차트")}
    </body></html>"""

    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    if GAS_WEB_APP_URL.startswith("http"):
        print("📂 구글 드라이브 업로드 진행 중...")
        with open(pdf_filename, "rb") as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')
        
        # 💡 [핵심 패치] 네트워크 끊김 방지를 위한 3회 재시도 및 타임아웃 방어 로직
        for attempt in range(3):
            try:
                res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_filename, "base64": pdf_base64}, timeout=30)
                if res.status_code == 200 and "success" in res.text:
                    file_id = res.json().get("id")
                    report_link = f"https://drive.google.com/uc?id={file_id}"
                    doc.worksheet("리포트_게시").append_row([datetime.datetime.now().strftime('%Y-%m-%d'), report_link])
                    print("✅ 앱시트 연동 완료!")
                    break  # 성공 시 반복문 탈출
                else:
                    print(f"⚠️ 드라이브 업로드 응답 오류 (시도 {attempt+1}/3)")
            except Exception as e:
                print(f"⚠️ 드라이브 에러 (시도 {attempt+1}/3): {e}")
                time.sleep(5) # 실패 시 5초 대기 후 재시도

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] AI 펀더멘털 및 시각적 차트 판독 심층 리포트"})
            if response.status_code == 200:
                print("✅ 텔레그램 전송 성공!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
