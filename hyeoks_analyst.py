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
from google import genai

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

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
    # 💡 [원상 복구] 수석님의 지시대로 압도적 차트 판독을 위해 다시 2.5-flash로 엔진 격상!
    MODEL_ID = 'gemini-2.5-flash'
except Exception as e:
    print(f"❌ Gemini API 초기화 실패: {e}")
    exit(1)

# 💡 [핵심 패치 3] 무한 대기를 막고, 진짜 에러 이유를 깃허브 로그에 찍어주는 스마트 디버거
def safe_generate_content(contents):
    for i in range(3): # 피 말리는 대기 시간을 줄이기 위해 3번만 시도
        try:
            return client.models.generate_content(
                model=MODEL_ID,
                contents=contents
            )
        except Exception as e:
            err_str = str(e).lower()
            print(f"⚠️ [디버그 원문] 구글 서버 응답: {e}") # 깃허브 액션 로그에서 정확한 이유를 보기 위함
            
            if "429" in err_str or "quota" in err_str or "exhausted" in err_str:
                wait_time = 30 * (i + 1)
                print(f"⏳ 토큰/할당량 초과. {wait_time}초 숨 고르기... ({i+1}/3)")
                time.sleep(wait_time)
            else:
                raise e
    raise Exception("❌ 재시도 횟수 초과: 구글 API 에러 (위의 '디버그 원문' 로그를 확인하세요)")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)

    macro_sheet = doc.worksheet("시장요약").get_all_values()
    nasdaq = macro_sheet[1][4]
    exchange_rate = macro_sheet[1][6]
    wti_oil = macro_sheet[1][7]

    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:30]
    
    valid_short_candidates = []
    valid_mid_candidates = []
    is_korean_market_down = False 

    for r in tech_data:
        if len(r) < 10: continue
        
        name = str(r[0]).strip()
        code = str(r[1]).replace("'", "").strip().zfill(6)
        change_rate = str(r[3])
        score_str = str(r[8])  
        tajeom = str(r[9])     
        shadow_status = str(r[14]) if len(r)>14 else ""
        
        if "주의장세" in tajeom:
            is_korean_market_down = True
            
        if "상한가" in tajeom or "29." in change_rate or "30." in change_rate: continue 
        if "윗꼬리 위험" in shadow_status or "윗꼬리" in tajeom: continue 
        if re.search(r'매수금지|자본잠식|딱지|관망|데이터 부족', tajeom): continue 
            
        cand_info = f"종목:{name}({code}), 현재가:{r[2]}({change_rate}), 5일선:{r[4]}, 20일선:{r[5]}, 타점:{tajeom}, 퀀트점수:{score_str}, 거래량:{r[18] if len(r)>18 else ''}, 테마:{r[19] if len(r)>19 else '개별주'}"
        cand_data = {'name': name, 'code': code, 'tajeom': tajeom, 'info': cand_info}
        
        if "돌파" in score_str or "주도주" in tajeom:
            valid_short_candidates.append(cand_data)
        else:
            valid_mid_candidates.append(cand_data)

    market_status_text = "코스피/코스닥 20일선 이탈 (하락 변동성 장세 - 방어적 트레이딩 요망)" if is_korean_market_down else "코스피/코스닥 안정화 (추세 추종 및 비중 베팅 가능)"

    def generate_hyeoks_report(st_type):
        if st_type == "short":
            if not valid_short_candidates: raise Exception("단기 돌파 조건에 부합하는 안전한 종목이 없습니다.")
            candidates_str = "\n".join([c['info'] for c in valid_short_candidates])
            sys_msg = "대한민국 증시를 지배하는 주도 테마의 심장부에서, 거래대금이 폭발하며 전고점 매물대를 완벽히 소화해 낸 '단기 폭발(Short-term Breakout)' 최고의 1종목"
        else:
            if not valid_mid_candidates: raise Exception("스윙 눌림 조건에 부합하는 안전한 종목이 없습니다.")
            candidates_str = "\n".join([c['info'] for c in valid_mid_candidates])
            sys_msg = "매크로 불안 속에서도 메이저 스마트 머니가 굳건하게 방어해주며, 악성 매도 물량이 씨가 마른(거래량 급감) 완벽한 '스윙 눌림목(Mid-term Swing)' 최고의 1종목"

        pick_prompt = f"""
        너는 전설적인 실전 트레이더들의 호가창/차트 판독 능력을 딥러닝하고, 거시경제 통찰력까지 갖춘 HYEOKS 리서치의 최고 AI 수석 퀀트 애널리스트야.
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
        
        raw_code = safe_generate_content(pick_prompt).text
        code_match = re.search(r'\d{6}', raw_code)
        if not code_match:
            target_code = valid_short_candidates[0]['code'] if st_type == "short" else valid_mid_candidates[0]['code']
        else:
            target_code = code_match.group()

        if st_type == "short":
            best_pick = next((item for item in valid_short_candidates if item["code"] == target_code), valid_short_candidates[0])
        else:
            best_pick = next((item for item in valid_mid_candidates if item["code"] == target_code), valid_mid_candidates[0])

        target_name = best_pick['name']
        print(f"🎯 [{st_type.upper()}] AI 수석 애널리스트의 최종 픽: {target_name} ({target_code})")

        img_path = f"temp_chart_{target_code}.png"
        chart_url = f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{target_code}.png"
        img_res = requests.get(chart_url, headers={'User-Agent': 'Mozilla/5.0'})
        with open(img_path, 'wb') as f:
            f.write(img_res.content)

        img = PIL.Image.open(img_path)
        
        # 💡 [핵심 패치 2] 토큰 과식 방지: 차트 해상도를 적당히 압축하여 구글 서버 부담 완화
        img.thumbnail((800, 800))

        warning_msg = ""
        if "고공권" in best_pick['tajeom']:
            warning_msg = "\n[필수 경고] 이 종목은 최근 단기 급등하여 '고공권' 판정을 받았습니다. 모멘텀은 최고조이나, 단기 이격 리스크가 있으므로 '비중을 평소의 절반으로 줄이고 -3% 이탈 시 기계적 손절을 집행하는 철저한 방어적 트레이딩'을 권고하는 문장을 반드시 포함하십시오."
        elif is_korean_market_down:
            warning_msg = "\n[필수 경고] 현재 국내 증시가 20일선을 이탈한 '주의 장세'입니다. 내일 아침 갭하락 리스크를 피하기 위해 '오버나잇 비중을 축소하고, 익일 장 초반 지지선 방어를 확인한 뒤 진입'하라는 보수적 가이드를 반드시 포함하십시오."

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

        response = safe_generate_content([final_prompt, img])
        img.close()
        os.remove(img_path)

        raw_report_text = response.text

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
            raw_report_text = re.sub(r'\[DATA\].*', '', raw_report_text, flags=re.DOTALL).strip()

        return raw_report_text, target_code, pick_data

    print("🧠 [HYEOKS 수석 애널리스트] 매크로 및 비전 데이터 심층 분석 중...")
    report_short, code_short, pick_short = generate_hyeoks_report("short")

    print("⏳ 단기 리포트 완료! 구글 API 토큰 리미트 방어를 위해 30초간 휴식합니다...")
    time.sleep(30)

    report_mid, code_mid, pick_mid = generate_hyeoks_report("mid")

    # ==========================================
    # 💰 가상계좌 실전 퀀트 시뮬레이션 엔진
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
        .summary-box { background-color: #f8fafc; padding: 25px; border-left: 5px solid #
