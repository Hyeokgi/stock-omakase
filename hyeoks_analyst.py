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

KST = datetime.timezone(datetime.timedelta(hours=9))

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

    # 매크로 지표 수집
    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    
    # ==========================================
    # 🔍 후보군 추출 및 국내장 매크로 분위기 자동 판독
    # 상한가, 윗꼬리, 매매금지 등 '절대 불가' 종목만 컷오프 하고 AI에게 선택권을 넘깁니다.
    # ==========================================
    valid_short_candidates = []
    valid_mid_candidates = []
    is_korean_market_down = False # 코스닥 하락장 여부 판독기

    for r in tech_data:
        if len(r) < 10: continue
        
        name = str(r[0]).strip()
        code = str(r[1]).replace("'", "").strip().zfill(6)
        change_rate = str(r[3])
        score_str = str(r[8])  
        tajeom = str(r[9])     
        shadow_status = str(r[14]) if len(r)>14 else ""
        
        # 💡 시스템이 '주의장세' 꼬리표를 달아놨다면, 현재 국내 증시(코스닥)가 20일선 아래의 하락장임을 뜻함
        if "주의장세" in tajeom:
            is_korean_market_down = True
            
        # ❌ [안전망: 무조건 탈락 조건]
        if "상한가" in tajeom or "29." in change_rate or "30." in change_rate: continue 
        if "윗꼬리 위험" in shadow_status or "윗꼬리" in tajeom: continue 
        if re.search(r'매수금지|자본잠식|딱지|관망|데이터 부족', tajeom): continue 
            
        cand_info = f"종목:{name}({code}), 현재가:{r[2]}({change_rate}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{tajeom}, 퀀트점수:{score_str}, 거래량:{r[18] if len(r)>18 else ''}, 테마:{r[19] if len(r)>19 else '개별주'}"
        
        cand_data = {'name': name, 'code': code, 'tajeom': tajeom, 'info': cand_info}
        
        # 트랙 분류 (돌파/주도주는 단기, 나머지는 스윙)
        if "돌파" in score_str or "주도주" in tajeom:
            valid_short_candidates.append(cand_data)
        else:
            valid_mid_candidates.append(cand_data)

    market_status_text = "코스피/코스닥 20일선 이탈 (하락 변동성 장세 - 방어적 트레이딩 요망)" if is_korean_market_down else "코스피/코스닥 안정화 (추세 추종 및 비중 베팅 가능)"

    def generate_hyeoks_report(st_type):
        # ==========================================
        # 🧠 [AI 선택권 부활] AI가 매크로와 후보군을 입체적으로 분석하여 최고 1종목을 직접 발탁!
        # ==========================================
        if st_type == "short":
            if not valid_short_candidates: raise Exception("단기 돌파 조건에 부합하는 안전한 종목이 없습니다.")
            candidates_str = "\n".join([c['info'] for c in valid_short_candidates])
            sys_msg = "대한민국 증시를 지배하는 주도 테마의 심장부에서, 거래대금이 폭발하며 전고점 매물대를 완벽히 소화해 낸 '단기 폭발(Short-term Breakout)' 최고의 1종목"
        else:
            if not valid_mid_candidates: raise Exception("스윙 눌림 조건에 부합하는 안전한 종목이 없습니다.")
            candidates_str = "\n".join([c['info'] for c in valid_mid_candidates])
            sys_msg = "매크로 불안 속에서도 메이저 스마트 머니가 굳건하게 방어해주며, 악성 매도 물량이 씨가 마른(거래량 급감) 완벽한 '스윙 눌림목(Mid-term Swing)' 최고의 1종목"

        pick_prompt = f"""
        너는 전설적인 실전 트레이더들(방배동선수, 강창권 등)의 호가창/차트 판독 능력을 딥러닝하고, 거시경제 통찰력까지 갖춘 HYEOKS 리서치의 최고 AI 수석 퀀트 애널리스트야.
        현재 매크로 상황과 아래의 [안전망을 통과한 후보 종목 데이터]를 입체적으로 분석해라.

        📊 [현재 글로벌 및 국내 매크로 환경]
        - 나스닥: {nasdaq} / 원달러 환율: {exchange_rate} / WTI유가: {wti_oil}
        - 국내 증시 상태: {market_status_text}

        📋 [후보 종목 데이터 (퀀트 점수 및 기술적 지표)]
        {candidates_str}

        [행동 지침]
        단순히 퀀트 점수가 높은 순으로 맹신하지 마라. 현재의 매크로 흐름(유가, 환율 등)과 엮일 수 있는 테마인지, 단기 고점 리스크(고공권)는 없는지, 거래량의 응축 상태는 완벽한지 인간 최고수 트레이더의 직감으로 평가해라.
        위 후보들 중 {sys_msg}을 단 1개만 찾아라.
        다른 설명은 절대 하지 말고, 네가 선택한 1개 종목의 '6자리 종목코드 숫자'만 정확히 출력해.
        """
        
        raw_code = safe_generate_content(model, pick_prompt).text
        code_match = re.search(r'\d{6}', raw_code)
        if not code_match:
            # AI가 형식을 어길 경우를 대비한 안전장치 (가장 점수 높은 1등 강제 편입)
            target_code = valid_short_candidates[0]['code'] if st_type == "short" else valid_mid_candidates[0]['code']
        else:
            target_code = code_match.group()

        # 선택된 종목의 데이터 매핑
        if st_type == "short":
            best_pick = next((item for item in valid_short_candidates if item["code"] == target_code), valid_short_candidates[0])
        else:
            best_pick = next((item for item in valid_mid_candidates if item["code"] == target_code), valid_mid_candidates[0])

        target_name = best_pick['name']
        print(f"🎯 [{st_type.upper()}] AI 수석 애널리스트의 최종 픽: {target_name} ({target_code})")

        # 차트 이미지 캡처
        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)

        img = PIL.Image.open(img_path)

        # 🚨 [리스크 맞춤형 프롬프트 주입] (이모지 제거 및 텍스트 정제)
        warning_msg = ""
        if "고공권" in best_pick['tajeom']:
            warning_msg = "\n[필수 경고] 이 종목은 최근 단기 급등하여 '고공권' 판정을 받았습니다. 모멘텀은 최고조이나, 단기 이격 리스크가 있으므로 '비중을 평소의 절반으로 줄이고 -3% 이탈 시 기계적 손절을 집행하는 철저한 방어적 트레이딩'을 권고하는 문장을 반드시 포함하십시오."
        elif is_korean_market_down:
            warning_msg = "\n[필수 경고] 현재 국내 증시가 20일선을 이탈한 '주의 장세'입니다. 내일 아침 갭하락 리스크를 피하기 위해 '오버나잇 비중을 축소하고, 익일 장 초반 지지선 방어를 확인한 뒤 진입'하라는 보수적 가이드를 반드시 포함하십시오."

        # ==========================================
        # 📝 [리포트 작성 프롬프트] 들여쓰기 완벽 제거 및 HYEOKS 독자 전략 주입
        # ==========================================
        if st_type == "short":
            final_prompt = f"""너는 압도적인 모멘텀 분석과 실전 호가창의 심리를 꿰뚫는 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 단기 돌파(Short-term) 심층 리포트를 작성해.

[입력 데이터] 
{best_pick['info']} 
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}, 국내증시 {market_status_text}
{warning_msg}

[작성 지침 - HYEOKS Master Mode]
1. 은어 배제: 특정인의 이름이나 'SS급', '단타용' 등 시스템 이모지 및 은어를 리포트 본문에 절대 노출하지 말 것.
2. 통찰력: 이 종목이 현재 매크로(지수/유가/환율) 흐름 속에서 왜 시장의 뭉칫돈(스마트 머니)을 빨아들이고 있는지 서술할 것.
3. 차트 판독: 첨부된 차트의 매물대 소화 과정, 전고점 돌파 여부, 거래량의 발자국을 심층 분석할 것.
4. 절대 규칙: 논리적인 매수 타점, 목표가, 손절가를 계산할 것.
5. 가상계좌 연동 필수 형식 (리포트 맨 마지막 줄에 오직 이 형식으로만 작성, 숫자에 콤마 생략 가능):
[DATA] 목표가:00000, 손절가:00000, 분할매수:X

[출력 양식 (마크다운 유지 - 반드시 들여쓰기 없이 좌측에 붙여서 출력할 것)]
<div class="broker-name">HYEOKS SECURITIES | SHORT-TERM STRATEGY</div>
<div class="header">
<p class="stock-title">{target_name} ({target_code})</p>
<p class="subtitle">단기 모멘텀 및 스마트 머니 유입 분석: (소제목)</p>
</div>

<div class="summary-box">
<strong>💡 Company Brief | HYEOKS 트레이딩 데스크</strong><br><br>
(요약)
</div>

## 1. 매크로 연동성 및 테마 주도력 고찰
(상세 서술)

## 2. 차트/거래량 딥리딩 및 타점 시나리오
(상세 서술)
"""
        else:
            final_prompt = f"""너는 철저한 리스크 관리와 모멘텀 분석을 완벽하게 융합한 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 직장인 스윙(Mid-term) 심층 리포트를 작성해.

[입력 데이터] 
{best_pick['info']} 
매크로 환경: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}, 국내증시 {market_status_text}
{warning_msg}

[작성 지침 - HYEOKS Master Mode]
1. 은어 배제: 특정인의 이름이나 내부 시스템 은어 및 이모지를 절대 사용하지 말 것. 정제된 애널리스트 어조 유지.
2. 통찰력: 하락장 또는 변동성 장세 속에서도 이 종목이 왜 지지선을 방어하며 턴어라운드를 준비하고 있는지 서술할 것.
3. 거래량 및 VCP 판독: 음봉 거래량의 극감(악성 매도 물량의 씨마름), 주요 이평선(20일선 등)에서의 방어 여부를 시각적으로 분석할 것.
4. 리스크 관리 1 (확인 매매): 단순히 지지선에 닿았다고 예측하여 매수하는 것이 아니라, 하락하던 5일선이 수평으로 눕거나 위로 고개를 드는(턴어라운드) 흐름을 '눈으로 확인한 뒤 진입'하는 보수적 타점을 제시할 것.
5. 리스크 관리 2 (비중 조절): 1차 매수가 부근에서 횡보 시 무의미한 추가 매수(물타기)를 엄격히 금지하고, 추세가 확실히 위로 방향을 틀 때 추가 비중을 싣는 직장인 맞춤형 스윙 전략을 서술할 것. 갭하락 대비 시나리오도 포함.
6. 가상계좌 연동 필수 형식 (리포트 맨 마지막 줄에 오직 이 형식으로만 작성, 숫자에 콤마 생략 가능):
[DATA] 목표가:00000, 손절가:00000, 분할매수:O

[출력 양식 (마크다운 유지 - 반드시 들여쓰기 없이 좌측에 붙여서 출력할 것)]
<div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
<div class="header">
<p class="stock-title">{target_name} ({target_code})</p>
<p class="subtitle">대시세 눌림목 종가베팅 전략: (소제목)</p>
</div>

<div class="summary-box">
<strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
(요약)
</div>

## 1. 펀더멘털 및 매크로 방어력
(상세 서술)

## 2. 거래량 딥리딩 및 직장인 스윙 타점 전략
(상세 서술)
"""
        else:
            final_prompt = f"""
            너는 전설적인 실전 트레이더들의 매매 기법(거래량 씨마름, VCP 등)을 체화한 HYEOKS 증권의 최고 수석 퀀트 애널리스트야. 
            내가 첨부한 '일봉 차트 이미지'와 아래 [데이터]를 바탕으로 종목코드 '{target_code}'에 대한 직장인 스윙(Mid-term) 심층 리포트를 작성해.

            [입력 데이터] 
            {best_pick['info']} 
            매크로 환경: 나스닥 {nasdaq}, 환율 {exchange_rate}, 유가 {wti_oil}, 국내증시 {market_status_text}
            {warning_msg}

            [작성 지침 - Apex Trader Mode]
            1. 은어 배제: 내부 시스템 은어 및 이모지를 절대 사용하지 말 것.
            2. 통찰력: 하락장 또는 변동성 장세 속에서도 이 종목이 왜 지지선을 방어하며 턴어라운드를 준비하고 있는지, 기업의 펀더멘털과 엮어서 통찰력 있게 서술할 것.
            3. 차트 판독: 음봉 거래량의 급감(악성 매도 물량의 씨마름), 주요 이평선(20일선 등)에서의 방어 여부를 시각적으로 분석할 것.
            4. 리스크 관리: 직장인 투자자가 오버나잇(Overnight) 리스크를 견딜 수 있도록 1차 진입 비중 제한 및 갭하락 대비 분할 매수 시나리오를 제시할 것.
            5. 가상계좌 연동 필수 형식 (리포트 맨 마지막 줄에 오직 이 형식으로만 작성, 숫자에 콤마 생략 가능):
               [DATA] 목표가:00000, 손절가:00000, 분할매수:O
            
            [출력 양식 (마크다운 유지)]
            <div class="broker-name">HYEOKS SECURITIES | MID-TERM STRATEGY</div>
            <div class="header">
                <p class="stock-title">{target_name} ({target_code})</p>
                <p class="subtitle">대시세 눌림목 종가베팅 전략: (소제목)</p>
            </div>
            
            <div class="summary-box">
                <strong>💡 Company Brief | HYEOKS 밸류에이션 데스크</strong><br><br>
                (요약)
            </div>

            ## 1. 펀더멘털 및 매크로 방어력
            (상세 서술)
            
            ## 2. 👁️ 거래량/차트 딥리딩 및 직장인 분할 매수 전략
            (상세 서술)
            """

        response = safe_generate_content(model, [final_prompt, img])
        img.close()
        os.remove(img_path)

        raw_report_text = response.text

        # 💡 [핵심 패치] AI가 출력한 매매 기준 추출 (가상계좌용)
        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', raw_report_text)
        if match:
            pick_data = {
                'name': target_name,
                'code': target_code,
                'target': int(match.group(1).replace(',', '')),
                'stop': int(match.group(2).replace(',', '')),
                'split': match.group(3) == 'O'
            }
            # 실제 리포트 본문에서는 [DATA] 줄을 깔끔하게 삭제하여 숨김
            raw_report_text = re.sub(r'\[DATA\].*', '', raw_report_text, flags=re.DOTALL).strip()

        return raw_report_text, target_code, pick_data

    print("🧠 [HYEOKS 수석 애널리스트] 매크로 및 비전 데이터 심층 분석 중...")
    report_short, code_short, pick_short = generate_hyeoks_report("short")

    print("⏳ 단기 리포트 완료! API 과부하 방지를 위해 30초 휴식합니다...")
    time.sleep(30)

    report_mid, code_mid, pick_mid = generate_hyeoks_report("mid")

    # ==========================================
    # 💰 가상계좌 실전 퀀트 시뮬레이션 엔진 (Python)
    # ==========================================
    def update_virtual_portfolio(picks):
        print("💰 [HYEOKS 퀀트 데스크] 가상계좌 시뮬레이션 가동 중...")
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")

        hold_data = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]

        if len(hold_data) <= 1 or hold_data[0][0] != "종목명" or len(hold_data[0]) < 10:
            hold_sheet.clear()
            hold_sheet.update(range_name="A1", values=[headers])
            hold_data = [headers]

        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        new_hold_list = []
        closed_list = []

        req_session = requests.Session()
        req_session.headers.update({'User-Agent': 'Mozilla/5.0'})

        for row in hold_data[1:]:
            if len(row) < 10 or not row[0]: continue
            name, code, avg_price, invest_amt, _, _, buy_date, t_price, s_price, manual_sell = row

            avg_price = int(float(str(avg_price).replace(',', '')))
            invest_amt = int(float(str(invest_amt).replace(',', '')))
            t_price = int(float(str(t_price).replace(',', '')))
            s_price = int(float(str(s_price).replace(',', '')))

            try:
                clean_code = str(code).replace("'", "").zfill(6)
                api_url = f"https://m.stock.naver.com/api/stock/{clean_code}/basic"
                curr_price = int(req_session.get(api_url, timeout=3).json()['closePrice'].replace(',', ''))
            except:
                curr_price = avg_price 

            return_rate = (curr_price - avg_price) / avg_price if avg_price > 0 else 0

            sell_reason = ""
            if str(manual_sell).strip() == "매도":
                sell_reason = "사용자 수동 중도 매도"
            elif curr_price >= t_price:
                sell_reason = "🎯 AI 목표가 도달 (전량 익절)"
            elif curr_price <= s_price:
                sell_reason = "📉 AI 손절가 이탈 (전량 손절)"

            if sell_reason:
                result_str = "승리" if return_rate > 0 else "패배"
                closed_list.append([name, avg_price, curr_price, f"{return_rate*100:.2f}%", today_str, f"{result_str} ({sell_reason})"])
            else:
                clean_code2 = str(code).replace("'", "").zfill(6)
                new_hold_list.append([name, f"'{clean_code2}", avg_price, invest_amt, curr_price, f"{return_rate*100:.2f}%", buy_date, t_price, s_price, ""])

        for pick in picks:
            if not pick: continue
            name, code, t_price, s_price, is_split = pick['name'], pick['code'], pick['target'], pick['stop'], pick['split']

            try:
                curr_price = int(req_session.get(f"https://m.stock.naver.com/api/stock/{code}/basic", timeout=3).json()['closePrice'].replace(',', ''))
            except: continue

            existing_idx = next((i for i, r in enumerate(new_hold_list) if r[0] == name), -1)

            if existing_idx != -1:
                if is_split:
                    old_avg = new_hold_list[existing_idx][2]
                    old_invest = new_hold_list[existing_idx][3]
                    add_invest = 1000000 

                    new_invest = old_invest + add_invest
                    old_qty = old_invest / old_avg
                    add_qty = add_invest / curr_price
                    new_avg = int(new_invest / (old_qty + add_qty)) 

                    new_hold_list[existing_idx][2] = new_avg
                    new_hold_list[existing_idx][3] = new_invest
                    new_hold_list[existing_idx][4] = curr_price
                    new_hold_list[existing_idx][5] = f"{(curr_price - new_avg) / new_avg * 100:.2f}%"
            else:
                new_hold_list.append([name, f"'{code}", curr_price, 1000000, curr_price, "0.00%", today_str, t_price, s_price, ""])

        hold_sheet.clear()
        hold_sheet.update(range_name="A1", values=[headers] + new_hold_list, value_input_option="USER_ENTERED")

        if closed_list:
            if not closed_sheet.get_all_values():
                closed_sheet.update(range_name="A1", values=[["종목명", "최종평단가", "매도단가", "최종수익률", "매도일자", "결과(승/패)"]])
            for row in closed_list:
                closed_sheet.append_row(row)

        print("✅ 실전 가상계좌 리밸런싱 및 익/손절 처리 완료!")

    update_virtual_portfolio([pick_short, pick_mid])


    # ==========================================
    # 📝 리포트 HTML 조립 및 PDF 생성
    # ==========================================
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
        {markdown.markdown(report_short)} {make_chart_html(code_short, "[트레이더의 관점] AI 시각적 일봉 차트 판독")}
        <div class="page-break"></div>
        {markdown.markdown(report_mid)} {make_chart_html(code_mid, "[직장인 종가베팅] AI 시각적 일봉 차트 판독")}
    </body></html>"""

    pdf_filename = f"HYEOKS_Report_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(full_html, pdf_filename, options={'encoding': "UTF-8", 'enable-local-file-access': None})
    print("✅ PDF 렌더링 완료!")

    if GAS_WEB_APP_URL.startswith("http"):
        print("📂 구글 드라이브 업로드 진행 중...")
        with open(pdf_filename, "rb") as f:
            pdf_base64 = base64.b64encode(f.read()).decode('utf-8')

        for attempt in range(3):
            try:
                res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_filename, "base64": pdf_base64}, timeout=30)
                if res.status_code == 200 and "success" in res.text:
                    file_id = res.json().get("id")
                    report_link = f"https://drive.google.com/uc?id={file_id}"
                    doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), report_link], index=2)
                    print("✅ 앱시트 연동 완료!")
                    break  
                else:
                    print(f"⚠️ 드라이브 업로드 응답 오류 (시도 {attempt+1}/3)")
            except Exception as e:
                print(f"⚠️ 드라이브 에러 (시도 {attempt+1}/3): {e}")
                time.sleep(5) 

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        print("📲 텔레그램 PDF 발송 중...")
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        with open(pdf_filename, 'rb') as f:
            response = requests.post(url, files={'document': f}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "📊 [HYEOKS 리서치] AI 펀더멘털 및 시각적 차트 판독 심층 리포트\n\n💰 (가상계좌 포트폴리오 연동 완료)"})
            if response.status_code == 200:
                print("✅ 텔레그램 전송 성공!")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")
    exit(1)
