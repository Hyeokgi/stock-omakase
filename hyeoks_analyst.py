import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
import urllib3
import json

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# ==========================================
# 1. 환경 설정 및 인증
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"
GAS_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbxyuSEjPmg8rZPjLlG-YKck07QYxmZm0HtxvWAumvV2zp7RRpVaKDo6D-CiQ6pLqKFm/exec"
KST = datetime.timezone(datetime.timedelta(hours=9))

KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

now_kst = datetime.datetime.now(KST)
current_hour = now_kst.hour

print(f"🤖 [HYEOKS 리서치 센터] 봇 가동 (현재 KST {now_kst.strftime('%H:%M:%S')})")

try: 
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e: 
    print(f"❌ API 초기화 실패: {e}"); exit(1)

def clean_emojis(text):
    emojis = ['🚨','💡','💎','🔥','📊','📈','📉','🎯','🛡️','⏰','⏸️','🐎','🌟','🔒','🔴','🔵','⚪','🟢','🟡','👑','⚡','🚀','👀','⏳','🔻','🔺','➖', '🛢️', '💵', '🇺🇸']
    for e in emojis: text = text.replace(e, '')
    return text.replace('  ', ' ').strip()

def safe_generate_content(contents, is_fast=False):
    model_name = 'gemini-2.5-flash' if is_fast else 'gemini-2.5-pro'
    for i in range(5): 
        try: 
            return client.models.generate_content(model=model_name, contents=contents)
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                wait_time = 10 * (i + 1)
                print(f"⚠️ 구글 API 지연. {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
            else: raise e 
    raise Exception("❌ 구글 서버 할당량 초과 또는 무응답으로 최종 실패")

def parse_ai_json(text):
    """제미나이가 반환한 JSON 문자열을 딕셔너리로 안전하게 파싱합니다."""
    try:
        # 💡 마크다운 복사 오류 방지를 위한 안전한 문자열 치환
        clean_text = text.replace('`'*3 + 'json', '').replace('`'*3, '').strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"JSON 파싱 에러 (정규식 대체 시도): {e}")
        try:
            t_match = re.search(r'"target_price"\s*:\s*(\d+)', text)
            s_match = re.search(r'"stop_loss"\s*:\s*(\d+)', text)
            b_match = re.search(r'"briefing"\s*:\s*"([^"]+)"', text)
            return {
                "briefing": b_match.group(1) if b_match else "분석 결과 텍스트 오류",
                "target_price": int(t_match.group(1)) if t_match else 0,
                "stop_loss": int(s_match.group(1)) if s_match else 0
            }
        except:
            return {"briefing": "응답 오류", "target_price": 0, "stop_loss": 0}

def get_target_stock_news(code):
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_list = [f"- {a.text.strip()}" for a in soup.select('.title a')[:3]]
        return clean_emojis("\n".join(news_list)) if news_list else "개별 뉴스 없음"
    except: return "뉴스 수집 실패"

def get_vip_deep_dive_data(code, kis_token):
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET): return "PER: N/A / PBR: N/A"
    try:
        headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P", "tr_id": "FHKST01010100"}
        res = requests.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                          headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
        out = res.get("output", {})
        return f"PER: {out.get('per', 'N/A')} / PBR: {out.get('pbr', 'N/A')}"
    except: return "데이터 수집 실패"

# ==========================================
# 2. 구글 시트 연결 및 모드별 작동
# ==========================================
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    db_sheet = doc.worksheet("DB_스캐너")
    db_rows = db_sheet.get_all_values()
    
    KIS_TOKEN = ""
    try:
        for row in doc.worksheet("⚙️설정").get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN": KIS_TOKEN = row[1]; break
    except: pass

    sys_instruction = "기업의 일반적인 소개(무엇을 하는 회사인지 등)는 일절 금지. 차트 지표, 타점, 수급 데이터를 바탕으로 '현재 기술적 위치'와 '앞으로의 대응 전략'만을 60~70자 내외로 매우 짧고 날카롭게 작성할 것."

    # 🟢 [모드 1] 아침 7시: 브리핑 및 목표가/손절가 완전 초기화
    if current_hour == 7:
        print("▶ [오전 7시 모드] DB_스캐너 데이터를 'AI 브리핑 대기중' 및 '계산 대기'로 초기화합니다.")
        for i in range(2, len(db_rows) + 1):
            if len(db_rows[i-1]) > 9:
                db_sheet.update_cell(i, 10, "AI 브리핑 대기중")
                db_sheet.update_cell(i, 15, "계산 대기")  # O열 (목표가)
                db_sheet.update_cell(i, 16, "계산 대기")  # P열 (손절가)
        print("✅ 초기화 완료. 프로그램 종료.")
        exit(0)

    # 🟡 [모드 2] 오전장, 저녁장: 간단 브리핑 + 목표가/손절가 산출 업데이트
    if current_hour != 15:
        print(f"▶ [{current_hour}시 모드] 메인 리포트 시간이 아니므로, 대기 중인 종목의 브리핑 및 가격 산출을 진행합니다.")
        for i, row in enumerate(db_rows[1:], start=2):
            # 💡 '대기중' 조건 삭제 -> '리포트 발송 완료'가 아닌 모든 종목 매번 새로 갱신!
            if len(row) > 9 and "리포트 발송 완료" not in str(row[9]):
                stock_name = row[0] if len(row) > 0 else "알수없음"
                print(f" - [{stock_name}] AI 전략 및 가격 산출 중...")
                prompt = f"""
                당신은 천상계 트레이더의 수석 퀀트 애널리스트입니다.
                [{sys_instruction}]
                
                ■ 종목명: {stock_name}
                ■ 현재가: {row[3] if len(row) > 3 else ''}
                ■ 타점 위치: {row[8] if len(row) > 8 else ''}
                ■ 당일 수급: {row[11] if len(row) > 11 else ''}
                ■ 52주 고가: {row[12] if len(row) > 12 else ''}
                ■ 테마: {row[5] if len(row) > 5 else ''}
                ■ 파이썬 1차 계산 목표가: {row[14] if len(row) > 14 else ''}
                ■ 파이썬 1차 계산 손절가: {row[15] if len(row) > 15 else ''}
                
                💡 [시간대별 실전 지침 및 AI 자율 가격 설정 룰 (현재 KST 시간: {current_hour}시)]
                - [핵심 가격 룰] 위 '파이썬 1차 계산 가격'을 뼈대로 하되, 실전 트레이딩에 맞게 '손익비(Risk/Reward)'를 교정하십시오.
                  1) 손절가는 현재가 대비 **최소 -4% ~ -8%** 아래의 의미 있는 지지선으로 넉넉히 설정하여 세력의 휩쏘(흔들기)를 방어하십시오. (수 백원 차이의 타이트한 손절가 절대 금지!)
                  2) 스윙/눌림 타점의 목표가는 현재가 대비 **최소 +8% ~ +15% 이상**의 다음 매물대 저항선으로 높여 잡으십시오. (2~3% 수준의 의미 없는 단타 목표가 절대 금지!)
                - [시간 룰] 11시~13시(마의 구간): 가짜 돌파 확률이 높으므로 "오후장까지 관망하며 지지선 확인" 권고.
                - [시간 룰] 9시~10시(오전장): 당일 주도주의 '오전장 눌림목' 또는 '돌파' 타점을 가장 긍정적으로 평가.
                - [시간 룰] 14시~15시(오후장): 내일을 대비하는 '종가베팅' 수급 유입에 초점.
                - [출력 형식] 위 데이터를 바탕으로 현재 시간에 맞는 실전 대응 전략을 1~2문장(70자 내외)으로 매우 짧고 날카롭게 작성하십시오.
                
                반드시 아래의 엄격한 JSON 형식으로만 대답하십시오. 마크다운이나 다른 설명은 절대 금지합니다.
                {{
                    "briefing": "여기에 전략 요약 작성",
                    "target_price": 150000,
                    "stop_loss": 135000
                }}
                """
                try:
                    res_text = safe_generate_content(prompt, is_fast=True).text
                    parsed_data = parse_ai_json(res_text)
                    
                    briefing_text = parsed_data.get("briefing", "브리핑 생성 에러")
                    if not briefing_text.startswith("✅"): briefing_text = f"✅ [간단 브리핑] {briefing_text}"
                    
                    target_val = f"{int(parsed_data.get('target_price', 0)):,}원"
                    stop_val = f"{int(parsed_data.get('stop_loss', 0)):,}원"
                    
                    db_sheet.update_cell(i, 10, briefing_text)
                    db_sheet.update_cell(i, 15, target_val)
                    db_sheet.update_cell(i, 16, stop_val)
                    time.sleep(3.5)
                except Exception as e:
                    print(f"[{stock_name}] 브리핑/가격 산출 에러 발생 (건너뜀): {e}")
        print(f"🌅 {current_hour}시 전략 브리핑 완료! 프로그램 종료.")
        exit(0)

    # 🔴 [모드 3] 15시 모드 (메인 리포트 생성 및 풀 코스)
    print("\n▶ [15시 메인 리포트 모드] 주가데이터_보조 상위 150개 풀에서 HYEOKS 알파 종목 발굴 시작...")
    
    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange, oil = clean_emojis(macro_data[1][4]), clean_emojis(macro_data[1][6]), clean_emojis(macro_data[1][7])
    news_keywords = clean_emojis("\n".join([f"{r[2]}({r[3]}회)" for r in doc.worksheet("뉴스_키워드").get_all_values()[1:6]]))
    
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]
    
    cands_list = []
    for r in tech_data:
        if len(r) < 21: continue
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        curr_p, chg, score_str, tajeom = str(r[2]).strip(), str(r[3]).strip(), str(r[8]).strip(), str(r[9]).strip()
        prog = str(r[20]).strip()
        
        try: num_score = int(re.findall(r'-?\d+', score_str)[0])
        except: num_score = 0
        
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자', tajeom): continue 
        
        info = f"종목:{name}({code}) | 현재가:{curr_p}원({chg}) | 퀀트점수:{num_score}점 | 타점:{tajeom} | 수급:{prog}"
        cands_list.append({'name': name, 'code': code, 'score': num_score, 'info': info, 'curr_p': int(curr_p.replace(',',''))})

    high_score_cands = [c for c in cands_list if c['score'] >= 30]
    
    if len(high_score_cands) < 10:
        cands_list.sort(key=lambda x: x['score'], reverse=True)
        pool_150 = cands_list[:150]
    else:
        high_score_cands.sort(key=lambda x: x['score'], reverse=True)
        pool_150 = high_score_cands[:150]

    pool_str = "\n".join([c['info'] for c in pool_150])

    pick_prompt = f"""
    당신은 대한민국 최고의 주식 트레이더이자 HYEOKS 퀀트 분석가입니다.
    아래는 HYEOKS 퀀트 점수가 검증된 최상위 150개 종목 리스트입니다.
    
    이 중에서 제미나이 2.5 모델의 직관과 종합적인 판단(숨겨진 모멘텀, 테마 강도, 수급)을 활용해 
    최고의 단기 1종목, 스윙 1종목을 과감히 발굴해 내십시오.

    1. 단기 슈팅 공략주: 오늘 수급이 몰리며 전고점 돌파를 목전에 둔 파괴력 있는 종목 1개.
    2. 스윙 플랫폼 공략주: 바닥에서 에너지를 응축하고 턴어라운드를 시도하는 안정적인 종목 1개.

    [상위 150개 종목 리스트]
    {pool_str}
    
    [출력 형식]
    반드시 아래 JSON 형식으로만 응답하세요. 다른 설명은 절대 추가하지 마세요.
    {{
        "short_term_code": "종목코드6자리",
        "swing_code": "종목코드6자리"
    }}
    """
    
    result_text = safe_generate_content(pick_prompt).text
    # 💡 마크다운 복사 오류 방지를 위한 안전한 문자열 치환
    cleaned_text = result_text.replace('`'*3 + 'json', '').replace('`'*3, '').strip()
    picks_json = json.loads(cleaned_text)
    
    code_short = picks_json.get('short_term_code', '')
    code_mid = picks_json.get('swing_code', '')
    
    best_short = next((c for c in pool_150 if c['code'] == code_short), pool_150[0] if pool_150 else None)
    best_mid = next((c for c in pool_150 if c['code'] == code_mid), pool_150[1] if len(pool_150)>1 else best_short)

    print(f"🔥 최종 발굴 완료 -> 단기: {best_short['name'] if best_short else '없음'} / 스윙: {best_mid['name'] if best_mid else '없음'}\n")

    # ==========================================
    # 5. 시황 및 딥리딩 PDF 리포트 본문 생성
    # ==========================================
    print("▶ [2단계] 딥리딩 분석 및 PDF 리포트 본문 생성 (약 3~5분 소요)...")
    today_korean = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    status_txt = "코스피/코스닥 지지 (공격적 운영 가능)" 

    macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
아래 데이터를 바탕으로 '오늘의 시황 및 매크로 브리핑'을 1페이지 분량으로 상세히 작성하십시오. 정중한 존댓말(하십시오체)을 사용하십시오.
작성일: {today_korean}
매크로: 나스닥 {nasdaq}, 환율 {exchange}, 국내증시 {status_txt}
뉴스 키워드: {news_keywords}
(종목 추천 없이 시황과 트레이더의 스탠스만 서술하십시오.)"""
    
    market_summary = safe_generate_content(macro_prompt).text

    def generate_deep_report(st_type, best_cand):
        if not best_cand: return "", None
        
        vip = get_vip_deep_dive_data(best_cand['code'], KIS_TOKEN)
        news = get_target_stock_news(best_cand['code'])
        sub_title_prefix = "매물대 진공 구간 돌파 및 단기 슈팅 공략" if st_type == "short" else "에너지 응축 후 플랫폼 탈출 스윙 전략"

        detail_prompt = f"""귀하는 대한민국 최상위 1% 실전 트레이더들을 위한 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성하십시오. 한 리포트 내에서 말투가 바뀌지 않도록 정중한 존댓말(하십시오체)로 통일하십시오.

[입력 데이터]
종목 및 스캐너 판독: {best_cand['info']}
★확정 현재가: {best_cand['curr_p']}원
펀더멘털: {vip}
최신 뉴스: {news}

[HYEOKS 딥리딩 절대 지침 - 명심하십시오]
1. 분량 및 깊이: 귀하의 전문적인 통찰력을 발휘하여 충분히 길고 논리적으로 1.5~2페이지 분량이 나오도록 상세히 서술하십시오. 
2. 🚨 [할루시네이션(거짓 정보) 엄격 금지]: 차트를 판독하여 지지/저항선을 제시할 때, 반드시 위 [입력 데이터]에 제공된 ★확정 현재가({best_cand['curr_p']}원)를 기준으로 상/하단 가격을 계산하십시오. 1차 진입가는 현재가 부근으로 설정하십시오.
3. 실전 액션 플랜 강화: 구체적인 '진입 타점'과 명확한 '손절가'를 반드시 명시하십시오.
4. 가상계좌 규칙: 리포트 마지막 줄에만 [DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'} 형식으로 출력하십시오.

[출력 양식 (마크다운 유지)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{best_cand['name']} ({best_cand['code']})</p>
<p class="subtitle">{sub_title_prefix}: (소제목 작성)</p>
</div>

<div class="summary-box">
<strong>💡 HYEOKS 핵심 모멘텀 요약</strong><br><br>
(기업이 무엇을 하는 회사인지 등 일반적인 개요는 절대 쓰지 마십시오. 오직 차트 타점, 수급, 지지/저항 라인에 근거한 상승 모멘텀만 60~70자 내외의 1문장으로 작성하십시오.)
</div>

## 1. 매크로 유동성 및 내러티브 고찰
## 2. 시각적 차트 판독 및 스마트머니 딥리딩
## 3. 실전 타점 시나리오 및 리스크 관리 전략
[DATA] 목표가:00000, 손절가:00000, 분할매수:{'X' if st_type=='short' else 'O'}
"""
        img_path = f"temp_{best_cand['code']}.png"
        try:
            res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_cand['code']}.png", headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
            with open(img_path, 'wb') as f: f.write(res.content)
            report_txt = safe_generate_content([detail_prompt, PIL.Image.open(img_path)]).text
            os.remove(img_path)
        except:
            report_txt = safe_generate_content(detail_prompt).text

        pick_data = None
        match = re.search(r'\[DATA\]\s*목표가\s*:\s*([0-9,]+).*?손절가\s*:\s*([0-9,]+).*?분할매수\s*:\s*([OX])', report_txt)
        if match:
            pick_data = {'name': best_cand['name'], 'code': best_cand['code'], 'target': int(match.group(1).replace(',', '')), 'stop': int(match.group(2).replace(',', '')), 'split': match.group(3) == 'O', 'curr': best_cand['curr_p']}
            report_txt = re.sub(r'\[DATA\].*', '', report_txt, flags=re.DOTALL).strip()
            
        return report_txt, pick_data

    report_short, pick_short = generate_deep_report("short", best_short)
    if best_short: time.sleep(15)
    report_mid, pick_mid = generate_deep_report("mid", best_mid)


    # ==========================================
    # 6. 3시 마감 최신 DB_스캐너 동기화 및 브리핑 일괄 덮어쓰기
    # ==========================================
    print("\n▶ [3단계] 최신 DB_스캐너 동기화 및 리포트 종목/나머지 종목 갱신...")
    latest_db_data = db_sheet.get_all_values()

    def extract_summary(report_text):
        if not report_text: return ""
        briefing_summary = "✅ [리포트 발송 완료] "
        summary_match = re.search(r'<div class="summary-box">(.*?)</div>', report_text, re.DOTALL)
        if summary_match:
            clean_text = re.sub(r'<[^>]+>', '', summary_match.group(1)).replace("💡 HYEOKS 핵심 모멘텀 요약", "").strip()
            briefing_summary += clean_text[:80] + "..." if len(clean_text) > 80 else clean_text
        else:
            briefing_summary += "텔레그램에서 상세 분석 리포트를 확인하십시오."
        return briefing_summary

    short_summary = extract_summary(report_short) if best_short else ""
    mid_summary = extract_summary(report_mid) if best_mid else ""

    for i, r in enumerate(latest_db_data[1:], start=2):
        if len(r) > 9:
            code = str(r[2]).replace("'", "").strip().zfill(6)
            stock_name = r[0] if len(r) > 0 else "알수없음"

            # 1) 리포트 타겟 종목: 리포트의 목표가/손절가를 그대로 업데이트
            if best_short and code == best_short['code']:
                print(f" - [{stock_name}] 리포트 정보 및 가격 시트에 업데이트 중...")
                db_sheet.update_cell(i, 10, short_summary)
                if pick_short:
                    db_sheet.update_cell(i, 15, f"{pick_short['target']:,}원")
                    db_sheet.update_cell(i, 16, f"{pick_short['stop']:,}원")
                time.sleep(3.5)
                continue
            
            if best_mid and code == best_mid['code']:
                print(f" - [{stock_name}] 리포트 정보 및 가격 시트에 업데이트 중...")
                db_sheet.update_cell(i, 10, mid_summary)
                if pick_mid:
                    db_sheet.update_cell(i, 15, f"{pick_mid['target']:,}원")
                    db_sheet.update_cell(i, 16, f"{pick_mid['stop']:,}원")
                time.sleep(3.5)
                continue
            
            # 2) 나머지 종목들은 다시 JSON 프롬프트로 글+숫자 동시 산출 (리포트 제외 전체)
            if "리포트 발송 완료" not in str(r[9]):
                print(f" - [{stock_name}] AI 전략 및 가격 산출 중...")
                prompt = f"""
                당신은 천상계 트레이더의 수석 퀀트 애널리스트입니다.
                [{sys_instruction}]
                
                ■ 종목명: {stock_name}
                ■ 현재가: {r[3] if len(r) > 3 else ''}
                ■ 타점 위치: {r[8] if len(r) > 8 else ''}
                ■ 당일 수급: {r[11] if len(r) > 11 else ''}
                ■ 52주 고가: {r[12] if len(r) > 12 else ''}
                ■ 테마: {r[5] if len(r) > 5 else ''}
                ■ 파이썬 1차 계산 목표가: {r[14] if len(r) > 14 else ''}
                ■ 파이썬 1차 계산 손절가: {r[15] if len(r) > 15 else ''}
                
                💡 [시간대별 실전 지침 및 AI 자율 가격 설정 룰 (현재 KST 시간: {current_hour}시)]
                - [핵심 가격 룰] 위 '파이썬 1차 계산 가격'을 뼈대로 하되, 실전 트레이딩에 맞게 '손익비(Risk/Reward)'를 교정하십시오.
                  1) 손절가는 현재가 대비 **최소 -4% ~ -8%** 아래의 의미 있는 지지선으로 넉넉히 설정하여 세력의 휩쏘(흔들기)를 방어하십시오. (수 백원 차이의 타이트한 손절가 절대 금지!)
                  2) 스윙/눌림 타점의 목표가는 현재가 대비 **최소 +8% ~ +15% 이상**의 다음 매물대 저항선으로 높여 잡으십시오. (2~3% 수준의 의미 없는 단타 목표가 절대 금지!)
                - [시간 룰] 11시~13시(마의 구간): 가짜 돌파 확률이 높으므로 "오후장까지 관망하며 지지선 확인" 권고.
                - [시간 룰] 9시~10시(오전장): 당일 주도주의 '오전장 눌림목' 또는 '돌파' 타점을 가장 긍정적으로 평가.
                - [시간 룰] 14시~15시(오후장): 내일을 대비하는 '종가베팅' 수급 유입에 초점.
                - [출력 형식] 위 데이터를 바탕으로 현재 시간에 맞는 실전 대응 전략을 1~2문장(70자 내외)으로 매우 짧고 날카롭게 작성하십시오.
                
                반드시 아래의 엄격한 JSON 형식으로만 대답하십시오. 마크다운이나 다른 설명은 절대 금지합니다.
                {{
                    "briefing": "여기에 전략 요약 작성",
                    "target_price": 150000,
                    "stop_loss": 135000
                }}
                """
                try:
                    res_text = safe_generate_content(prompt, is_fast=True).text
                    parsed_data = parse_ai_json(res_text)
                    
                    briefing_text = parsed_data.get("briefing", "브리핑 생성 에러")
                    if not briefing_text.startswith("✅"): briefing_text = f"✅ [간단 브리핑] {briefing_text}"
                    
                    target_val = f"{int(parsed_data.get('target_price', 0)):,}원"
                    stop_val = f"{int(parsed_data.get('stop_loss', 0)):,}원"
                    
                    db_sheet.update_cell(i, 10, briefing_text)
                    db_sheet.update_cell(i, 15, target_val)
                    db_sheet.update_cell(i, 16, stop_val)
                    time.sleep(3.5)
                except Exception as e:
                    print(f"[{stock_name}] 브리핑/가격 산출 에러 발생 (건너뜀): {e}")

    # ==========================================
    # 7. 가상계좌 업데이트
    # ==========================================
    print("\n▶ [4단계] 가상계좌 업데이트 및 PDF/텔레그램 발송...")
    def update_portfolio(picks):
        hold_sheet = doc.worksheet("가상계좌_보유")
        closed_sheet = doc.worksheet("가상계좌_종료")
        today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        
        rows = hold_sheet.get_all_values()
        headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
        if len(rows) <= 1 or rows[0][0] != "종목명":
            hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers]); rows = [headers]

        new_rows, closed_rows = [], []
        for r in rows[1:]:
            if len(r) < 10 or not r[0]: continue
            name, code = r[0], r[1].replace("'", "").strip().zfill(6)
            buy_p, amt, t_p, s_p = int(float(r[2].replace(',',''))), int(float(r[3].replace(',',''))), int(float(r[7].replace(',',''))), int(float(r[8].replace(',','')))
            try: curr_p = int(requests.get(f"https://m.stock.naver.com/api/stock/{code}/basic", verify=False, timeout=3).json()['closePrice'].replace(',',''))
            except: curr_p = buy_p
            
            rtn = (curr_p - buy_p) / buy_p
            reason = ""
            if curr_p >= t_p: reason = "목표가 도달"
            elif curr_p <= s_p: reason = "손절가 이탈"
            elif str(r[9]).strip() == "매도": reason = "수동매도"
            
            if reason: closed_rows.append([name, buy_p, curr_p, f"{rtn*100:.2f}%", today, f"{'승리' if rtn>0 else '패배'} ({reason})"])
            else: new_rows.append([name, f"'{code}", buy_p, amt, curr_p, f"{rtn*100:.2f}%", r[6], t_p, s_p, ""])

        for p in picks:
            if not p or p['code'] == "000000": continue
            idx = next((i for i, v in enumerate(new_rows) if v[0] == p['name']), -1)
            if idx != -1:
                if p['split']:
                    total_amt = new_rows[idx][3] + 1000000
                    avg_p = int(total_amt / ((new_rows[idx][3]/new_rows[idx][2]) + (1000000/p['curr'])))
                    new_rows[idx][2], new_rows[idx][3], new_rows[idx][4] = avg_p, total_amt, p['curr']
            else:
                new_rows.append([p['name'], f"'{p['code']}", p['curr'], 1000000, p['curr'], "0.00%", today, p['target'], p['stop'], ""])

        hold_sheet.clear(); hold_sheet.update(range_name="A1", values=[headers] + new_rows, value_input_option="USER_ENTERED")
        if closed_rows:
            if not closed_sheet.get_all_values(): closed_sheet.update(range_name="A1", values=[["종목명", "매입단가", "매도단가", "수익률", "매도일자", "결과"]])
            for cr in closed_rows: closed_sheet.append_row(cr)

    update_portfolio([pick_short, pick_mid])

    # ==========================================
    # 8. HTML 조립 및 PDF 생성 -> 구글 드라이브 -> 텔레그램
    # ==========================================
    css = "<style>body{font-family:'NanumGothic',sans-serif;line-height:1.8;padding:30px;color:#222;font-size:110%;}.broker-name{color:#1a365d;font-weight:bold;font-size:22px;margin-bottom:15px;border-bottom:3px solid #1a365d;padding-bottom:10px;}.stock-title{font-size:32px;font-weight:900;margin:0;}.subtitle{font-size:18px;color:#2b6cb0;font-weight:bold;}.summary-box{background:#f8fafc;padding:20px;border-left:5px solid #1a365d;margin:20px 0;border-radius:5px;}h2{color:#1a365d;border-bottom:2px solid #edf2f7;margin-top:30px;padding-bottom:8px;}p{margin-bottom:15px;word-break:keep-all;}img{width:100%;height:auto;border:1px solid #cbd5e0;border-radius:8px;}.chart-container{text-align:center;margin-top:40px;page-break-inside:avoid;}.page-break{page-break-before:always;}.alert-box{background:#fff5f5;padding:15px;border-left:5px solid #e53e3e;margin-bottom:20px;color:#c53030;font-weight:bold;}</style>"
    
    html = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{css}</head><body>"
    html += "<div class='broker-name'>HYEOKS SECURITIES | DAILY MARKET REPORT</div>"
    html += f"<h2>글로벌 매크로 및 시황 요약</h2>{markdown.markdown(market_summary)}"

    if best_short:
        html += f"<div class='page-break'></div>{markdown.markdown(report_short)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_short['code']}.png'></div>"
        
    if best_mid:
        html += f"<div class='page-break'></div>{markdown.markdown(report_mid)}"
        html += f"<div class='chart-container'><h3>차트 판독</h3><img src='https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_mid['code']}.png'></div>"

    html += "</body></html>"

    pdf_file = f"HYEOKS_Daily_{datetime.datetime.now(KST).strftime('%Y%m%d')}.pdf"
    pdfkit.from_string(html, pdf_file, options={'encoding': "UTF-8", 'enable-local-file-access': None})

    if GAS_WEB_APP_URL:
        print("▶ 구글 드라이브 업로드 진행 중...")
        with open(pdf_file, "rb") as f: 
            b64 = base64.b64encode(f.read()).decode('utf-8')
        try:
            res = requests.post(GAS_WEB_APP_URL, json={"filename": pdf_file, "base64": b64}, timeout=30).json()
            doc.worksheet("리포트_게시").insert_row([datetime.datetime.now(KST).strftime('%Y-%m-%d'), f"https://drive.google.com/uc?id={res.get('id')}"], index=2)
            print("✅ 리포트_게시 시트 업데이트 완료!")
        except Exception as e: 
            print(f"⚠️ 구글 드라이브 업로드 실패: {e}")

    if TELEGRAM_BOT_TOKEN:
        print("▶ 텔레그램 발송 진행 중...")
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", 
                      files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "[HYEOKS] AI 심층 리서치 보고서"})
        print("✅ 텔레그램 발송 완료!")

    print(f"🎉 모든 작업이 성공적으로 완료되었습니다: {pdf_file}")

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
