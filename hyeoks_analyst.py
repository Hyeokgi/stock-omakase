# -*- coding: utf-8 -*-
import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, PIL.Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
from google.genai import types
import urllib3
import json
import xml.etree.ElementTree as ET
import concurrent.futures

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
    # 💡 [핵심 픽스] deadline 1s 에러를 유발하는 일체의 timeout 옵션 삭제. 기본값으로 맡김.
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"❌ API 초기화 실패: {e}"); exit(1)

def clean_emojis(text):
    emojis = ['🚨','💡','💎','🔥','📊','📈','📉','🎯','🛡️','⏰','⏸️','🐎','🌟','🔒','🔴','🔵','⚪','🟢','🟡','👑','⚡','🚀','👀','⏳','🔻','🔺','➖', '🛢️', '💵', '🇺🇸', '🌱']
    for e in emojis: text = text.replace(e, '')
    return text.replace('  ', ' ').strip()

def safe_generate_content(contents, is_fast=False):
    model_name = 'gemini-1.5-flash' if is_fast else 'gemini-1.5-pro'
    for i in range(3):
        try:
            return client.models.generate_content(
                model=model_name,
                contents=contents
            )
        except Exception as e:
            err = str(e).lower()
            if "503" in err or "429" in err or "quota" in err or "timeout" in err:
                wait_time = 5 * (i + 1)
                print(f"⚠️ API 지연({i+1}/3). {wait_time}초 대기 후 재시도... ({err[:30]})")
                time.sleep(wait_time)
            else:
                print(f"⚠️ API 에러: {err[:50]}")
                time.sleep(3)
                
    # 💡 [핵심 픽스] 3번 다 실패해도 프로그램이 죽지 않도록 가짜(Dummy) 텍스트를 던져줌 (J열 오류 복원용)
    class DummyResponse:
        text = '{"briefing": "⚠️ AI 분석 지연 (수동 확인 요망)", "target_price": 0, "stop_loss": 0}'
    return DummyResponse()

def parse_ai_json(text):
    try:
        clean_text = text.replace('```json', '').replace('```', '').strip()
        return json.loads(clean_text)
    except Exception as e:
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
            return {"briefing": "응답 파싱 오류", "target_price": 0, "stop_loss": 0}

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
        res = requests.get("https://openapi.koreainvestment.com:9443/uapi/dynamic-stock/v1/quotations/inquire-price", 
                          headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
        out = res.get("output", {})
        return f"PER: {out.get('per', 'N/A')} / PBR: {out.get('pbr', 'N/A')}"
    except: return "데이터 수집 실패"

def cleanup_and_reorder(doc, sheet_name, sort_col_idx):
    try:
        sheet = doc.worksheet(sheet_name)
        data = sheet.get_all_values()
        if len(data) <= 2: return
        
        header = data[0]
        rows = [r for r in data[1:] if len(r) > sort_col_idx and str(r[sort_col_idx]).strip()]
        
        def parse_date(val):
            val = str(val).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y. %m. %d"):
                try: return datetime.datetime.strptime(val, fmt)
                except: continue
            return datetime.datetime(1970, 1, 1)
            
        rows.sort(key=lambda x: parse_date(x[sort_col_idx]), reverse=True)
        
        sheet.batch_clear(['A2:Z'])
        sheet.update(range_name="A2", values=[header] + rows, value_input_option="USER_ENTERED")
        print(f"✅ [{sheet_name}] 최신순 정렬 및 청소 완료")
    except Exception as e:
        print(f"⚠️ [{sheet_name}] 정렬 실패: {e}")

# 💡 오킨스전자 등 억울한 탈락 방지 기준 (500억/300억)
def validate_stock_historical_dna(cand, raw_theme_daily_map, is_warning_market):
    code = cand['code']
    name = cand['name']
    
    min_tv_threshold = 30_000_000_000 if is_warning_market else 50_000_000_000
    
    local_session = requests.Session()
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        res = local_session.get(url, verify=False, timeout=3)
        root = ET.fromstring(res.text)
        items = root.findall(".//item")
        
        has_qualified_day = False
        for item in items:
            data = item.get("data").split("|")
            close_p = int(data[4])
            vol = int(data[5])
            day_tv_krw = close_p * vol
            if day_tv_krw >= min_tv_threshold:
                has_qualified_day = True
                break
                    
        return cand, has_qualified_day
    except Exception as e:
        return cand, True

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
    
    cleanup_and_reorder(doc, "접속로그", 1)
    cleanup_and_reorder(doc, "DB_중장기", 0)

    KIS_TOKEN = ""
    try:
        for row in doc.worksheet("⚙️설정").get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN": KIS_TOKEN = row[1]; break
    except: pass

    market_summary_data = doc.worksheet("시장요약").get_all_values()
    korean_market_status = clean_emojis(market_summary_data[1][8]) if len(market_summary_data) > 1 and len(market_summary_data[1]) > 8 else "확인불가"
    is_warning_market = "하락" in korean_market_status or "이탈" in korean_market_status

    sys_instruction = "기업의 일반적인 소개(무엇을 하는 회사인지 등)는 일절 금지. 차트 지표, 타점, 수급 데이터를 바탕으로 '현재 기술적 위치'와 '앞으로의 대응 전략'만을 60~70자 내외로 매우 짧고 날카롭게 작성할 것."

    if current_hour == 7:
        print("▶ [오전 7시 모드] DB_스캐너 데이터를 'AI 브리핑 대기중' 및 '계산 대기'로 초기화합니다.")
        updates = []
        for i in range(2, len(db_rows) + 1):
            if len(db_rows[i-1]) > 9:
                updates.append({'range': f'J{i}', 'values': [['AI 브리핑 대기중']]})
                updates.append({'range': f'O{i}', 'values': [['AI 데이터 계산중']]})
                updates.append({'range': f'P{i}', 'values': [['AI 데이터 계산중']]})
        if updates:
            db_sheet.batch_update(updates)
        print(f"✅ {len(updates) // 3}개 종목 초기화 완료 (batch_update 1회). 프로그램 종료.")
        exit(0)

    def get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, is_warning_market):
        is_seed = "🌱" in tajeom_badge or "모아가기" in tajeom_badge or "DB_중장기" in tajeom_badge
        is_active_buy = "외인집중" in tajeom_badge
        market_context = "🚨 현재 시장은 변동성이 큰 하락/횡보장입니다. 안정성을 최우선으로 고려하십시오." if is_warning_market else "현재 시장은 정상적인 스윙/돌파가 가능한 장세입니다."
        
        if is_active_buy:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 외인 집중배팅(Non-Program) 역발상 전략]
            {market_context}
            1. 프로그램 매도에도 외국인 액티브 자금이 유입 중이라는 역발상 논리를 씁니다.
            2. 매집 단가를 유추해 넉넉한 손절가와 분할 매수 타점을 명시합니다.
            """
        elif is_seed:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 중장기 모아가기(Accumulation)]
            {market_context}
            1. 차트 하단 지지선을 유추해 단단한 손절가를 제시하십시오.
            2. 현재가 1차, 손절가 위 2차 분할 매수 시나리오를 작성하십시오.
            """
        else:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 단기/스윙 히트앤런 전략]
            {market_context}
            1. 리스크 과다 시 관망을 지시하고 타점을 0으로 처리하십시오.
            2. 하락장일 경우 손익비를 타이트하게 끊어치는 보수적인 타점을 제시하십시오.
            """

        return f"""
        당신은 수석 퀀트 애널리스트입니다. [{sys_instruction}]
        
        ■ 종목명: {stock_name}
        ■ 현재가: {curr_p}
        ■ 타점 위치: {tajeom_badge}
        ■ 수급: {sugeup}
        ■ 테마: {theme}
        ■ 임시 기준가: 목표가 {target_sys} / 손절가 {stop_sys}
        
        {guide_text}
        
        반드시 JSON 형식으로만 대답하십시오:
        {{ "briefing": "전략 요약", "target_price": 150000, "stop_loss": 135000 }}
        """

    # 🟡 [모드 2] 오전장, 저녁장: 간단 브리핑 산출 로직
    # (유료 계정의 파워 + batch_update 조합으로 속도 극대화, 리포트 종목만 살리고 전부 갱신)
    if current_hour != 15:
        print(f"▶ [{current_hour}시 모드] 실시간 브리핑 초고속 갱신 모드 가동...")
        batch_updates = []
        
        for i, row in enumerate(db_rows[1:], start=2):
            if len(row) > 9:
                status = str(row[9]).strip()
                # 💡 수석님의 지시: 리포트는 유지하고, 나머지는 무조건 새로 쓴다!
                if "리포트 발송 완료" in status:
                    continue
                
                stock_name = row[0] if len(row) > 0 else "알수없음"
                print(f" - [{stock_name}] AI 초고속 분석 중...")
                
                curr_p = row[3] if len(row) > 3 else '0'
                tajeom_badge = row[8] if len(row) > 8 else ''
                sugeup = row[11] if len(row) > 11 else ''
                high_52 = row[12] if len(row) > 12 else ''
                theme = row[5] if len(row) > 5 else ''
                target_sys = row[14] if len(row) > 14 else ''
                stop_sys = row[15] if len(row) > 15 else ''
                
                prompt = get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, is_warning_market)
                
                res_text = safe_generate_content(prompt, is_fast=True).text
                parsed_data = parse_ai_json(res_text)
                
                briefing_text = parsed_data.get("briefing", "⚠️ 텍스트 생성 오류")
                if not briefing_text.startswith("✅") and not briefing_text.startswith("⚠️"):
                    briefing_text = f"✅ [간단 브리핑] {briefing_text}"
                
                raw_target = str(parsed_data.get('target_price', '0')).replace(',', '').replace('원', '')
                raw_stop   = str(parsed_data.get('stop_loss',   '0')).replace(',', '').replace('원', '')
                target_val = f"{int(raw_target):,}원" if raw_target.isdigit() and int(raw_target) > 0 else "관망"
                stop_val   = f"{int(raw_stop):,}원"   if raw_stop.isdigit()   and int(raw_stop)   > 0 else "관망"
                
                batch_updates.append({'range': f'J{i}', 'values': [[briefing_text]]})
                batch_updates.append({'range': f'O{i}', 'values': [[target_val]]})
                batch_updates.append({'range': f'P{i}', 'values': [[stop_val]]})
                # 유료 API 한도(1000 RPM)이므로 time.sleep(3.5) 완전 삭제 (0.1초만 둠)
                time.sleep(0.1) 
                
        if batch_updates:
            db_sheet.batch_update(batch_updates)
            print(f"✅ 총 {len(batch_updates)//3}개 종목 초고속 일괄 업데이트 완료!")
            
        print(f"🌅 {current_hour}시 브리핑 완료! 종료.")
        exit(0)

    # 🔴 [모드 3] 15시 모드 (메인 리포트 생성 및 풀 코스)
    print("\n▶ [15시 메인 리포트 모드] 주가데이터_보조 상위 150개 풀에서 HYEOKS 알파 종목 발굴 시작...")
    
    macro_data = doc.worksheet("시장요약").get_all_values()
    nasdaq, exchange, oil = clean_emojis(macro_data[1][4]), clean_emojis(macro_data[1][6]), clean_emojis(macro_data[1][7])
    news_keywords = clean_emojis("\n".join([f"{r[2]}({r[3]}회)" for r in doc.worksheet("뉴스_키워드").get_all_values()[1:6]]))
    
    raw_theme_daily_map = {}
    try:
        raw_sheet = doc.worksheet("수급_Raw")
        raw_values = raw_sheet.get_all_values()
        if len(raw_values) > 1:
            header = raw_values[0]
            date_idx = header.index('날짜') if '날짜' in header else 0
            theme_idx = header.index('테마명') if '테마명' in header else 2
            val_idx = header.index('거래대금(억원)') if '거래대금(억원)' in header else 6
            
            for row in raw_values[1:]:
                if len(row) > max(date_idx, theme_idx, val_idx):
                    r_date = str(row[date_idx]).strip()
                    r_theme = str(row[theme_idx]).split(' (대장:')[0].strip()
                    try:
                        r_val = int(str(row[val_idx]).replace(',', '').strip())
                        raw_theme_daily_map[(r_date, r_theme)] = raw_theme_daily_map.get((r_date, r_theme), 0) + r_val
                    except: pass
    except: pass

    helper_data = doc.worksheet("주가데이터_보조").get_all_values()
    tech_data_headers = [h.strip() for h in helper_data[0]]
    tech_data = helper_data[1:]
    
    def find_column_index(keywords, default_idx):
        for kw in keywords:
            for idx, h in enumerate(tech_data_headers):
                if kw in h: return idx
        return default_idx

    name_idx = find_column_index(["종목명"], 0)
    code_idx = find_column_index(["종목코드", "코드"], 1)
    price_idx = find_column_index(["현재가"], 2)
    change_idx = find_column_index(["등락률"], 3)
    theme_idx = find_column_index(["테마명", "테마"], 19)
    prog_idx = find_column_index(["프로그램"], 20)
    seed_idx = find_column_index(["종목쿼터", "쿼터", "유형"], 25)
    
    cands_list = []
    for r in tech_data:
        if len(r) < max(name_idx, code_idx, price_idx, change_idx, theme_idx, prog_idx, seed_idx) + 1: continue
        
        name = str(r[name_idx]).strip()
        code = str(r[code_idx]).replace("'", "").strip().zfill(6)
        curr_p = str(r[price_idx]).strip()
        chg = str(r[change_idx]).strip()
        theme_name = str(r[theme_idx]).strip()
        prog = str(r[prog_idx]).strip()
        seed_tag = str(r[seed_idx]).strip() if seed_idx < len(r) else "NORMAL"
        
        val_col8 = str(r[8]).strip() if len(r) > 8 else ""
        val_col9 = str(r[9]).strip() if len(r) > 9 else ""
        
        score_display = val_col8 if '점' in val_col8 else (val_col9 if '점' in val_col9 else "0점")
        tajeom_raw = val_col9 if '점' in val_col8 else (val_col8 if '점' in val_col9 else val_col9)
        
        try: num_score = int(re.findall(r'-?\d+', score_display)[0])
        except: num_score = 0
        
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자|스코어 미달|과거 주도주 이력 미달', tajeom_raw): continue 
        
        tajeom_clean = tajeom_raw.split('⚠️')[0].strip().split('🎯')[0].strip()
        clean_curr_p = re.sub(r'[^0-9]', '', curr_p)
        int_curr_p = int(clean_curr_p) if clean_curr_p else 0
        
        info = f"종목:{name}({code}) | 현재가:{curr_p}원({chg}) | 퀀트점수:{num_score}점 | 타점:{tajeom_clean} | 수급:{prog} | 유형:{seed_tag}"
        cands_list.append({
            'name': name, 'code': code, 'score': num_score, 'info': info, 'curr_p': int_curr_p, 'type': seed_tag, 'theme_name': theme_name
        })

    high_score_cands = [c for c in cands_list if c['score'] >= 30]
    if len(high_score_cands) < 15:
        cands_list.sort(key=lambda x: x['score'], reverse=True)
        pre_pool = cands_list[:100]
    else:
        high_score_cands.sort(key=lambda x: x['score'], reverse=True)
        pre_pool = high_score_cands[:100]

    print(f"🧬 후보군 {len(pre_pool)}개 종목의 역사적 수급 DNA(개별 최고액) 검증 돌입...")
    validated_pool = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_dna = {executor.submit(validate_stock_historical_dna, c, raw_theme_daily_map, is_warning_market): c for c in pre_pool}
        for future in concurrent.futures.as_completed(future_to_dna):
            cand, is_qualified = future.result()
            if is_qualified: validated_pool.append(cand)

    validated_pool.sort(key=lambda x: x['score'], reverse=True)
    pool_150 = validated_pool[:150]
    pool_str = "\n".join([c['info'] for c in pool_150])

    pick_prompt = f"""
    당신은 세계 최고의 애널리스트 집단이 검증하는 HYEOKS 퀀트 분석가입니다.
    현재 시장 국면은 {'하락장' if is_warning_market else '상승/보합장'}입니다.
    최고의 단기 1종목, 중장기 스윙 1종목을 엄선하십시오. 

    [상위 150개 종목 리스트]
    {pool_str}
    
    [출력 형식] JSON block only
    {{
        "short_term_code": "종목코드6자리",
        "swing_code": "종목코드6자리"
    }}
    """
    
    result_text = safe_generate_content(pick_prompt).text
    cleaned_text = result_text.replace('```json', '').replace('```', '').strip()
    picks_json = json.loads(cleaned_text)
    
    code_short = picks_json.get('short_term_code', '')
    code_mid = picks_json.get('swing_code', '')
    
    best_short = next((c for c in pool_150 if c['code'] == code_short), None)
    best_mid = next((c for c in pool_150 if c['code'] == code_mid), None)

    print(f"🔥 최종 발굴 완료 -> 단기: {best_short['name'] if best_short else '없음'} / 스윙: {best_mid['name'] if best_mid else '없음'}\n")

    print("▶ [2단계] 딥리딩 분석 및 PDF 리포트 본문 생성...")
    today_korean = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    status_txt = "코스닥 20일선 이탈 (보수적 방어매매 요망)" if is_warning_market else "코스피/코스닥 지지 (공격적 운영 가능)" 

    macro_prompt = f"""오늘의 시황 및 매크로 브리핑을 1페이지 분량으로 작성하십시오. (종목 추천 금지)
작성일: {today_korean}
매크로: 나스닥 {nasdaq}, 환율 {exchange}, 국내증시 {status_txt}
뉴스 키워드: {news_keywords}"""
    
    market_summary = safe_generate_content(macro_prompt).text

    def generate_deep_report(st_type, best_cand, is_warning_market):
        if not best_cand: return "", None
        
        vip = get_vip_deep_dive_data(best_cand['code'], KIS_TOKEN)
        news = get_target_stock_news(best_cand['code'])
        market_context = "하락장" if is_warning_market else "상승장"
        sub_title_prefix = "단기 슈팅 공략" if st_type == "short" else "중장기 스윙 전략"

        detail_prompt = f"""세계 최고의 수석 퀀트 애널리스트로서 심층 리포트를 작성하십시오.
[입력 데이터]
종목 및 스캐너 판독: {best_cand['info']}
★확정 현재가: {best_cand['curr_p']}원
펀더멘털: {vip}
최신 뉴스: {news}

[출력 양식 (마크다운 유지)]
<div class="broker-name">HYEOKS SECURITIES | {'SHORT-TERM' if st_type=='short' else 'MID-TERM'} STRATEGY</div>
<div class="header">
<p class="stock-title">{best_cand['name']} ({best_cand['code']})</p>
<p class="subtitle">{sub_title_prefix}</p>
</div>
<div class="summary-box">
<strong>[HYEOKS 핵심 모멘텀 요약]</strong><br><br>
(상승 모멘텀 1문장 요약)
</div>

## 1. 매크로 환경 및 내러티브 고찰
## 2. 시각적 차트 판독 및 스마트머니 딥리딩
## 3. 실전 타점 시나리오 및 방어적 리스크 관리 전략
[DATA] 목표가:00000, 손절가:00000, 분할매수:{'O' if st_type=='mid' else 'X'}
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

    report_short, pick_short = generate_deep_report("short", best_short, is_warning_market)
    report_mid, pick_mid = generate_deep_report("mid", best_mid, is_warning_market)

    print("\n▶ [3단계] 최신 DB_스캐너 초고속 동기화 (Batch Update)...")
    latest_db_data = db_sheet.get_all_values()

    def extract_summary(report_text):
        if not report_text: return ""
        briefing_summary = "✅ [리포트 발송 완료] "
        summary_match = re.search(r'<div class="summary-box">(.*?)</div>', report_text, re.DOTALL)
        if summary_match:
            clean_text = re.sub(r'<[^>]+>', '', summary_match.group(1)).replace("[HYEOKS 핵심 모멘텀 요약]", "").strip()
            briefing_summary += clean_text[:80] + "..." if len(clean_text) > 80 else clean_text
        else:
            briefing_summary += "텔레그램에서 상세 분석 리포트를 확인하십시오."
        return briefing_summary

    short_summary = extract_summary(report_short) if best_short else ""
    mid_summary = extract_summary(report_mid) if best_mid else ""

    # 💡 [핵심 픽스] 15시 모드에서도 update_cell의 병목(40분)을 없애고 1초만에 덮어쓰도록 batch_update 사용
    batch_updates_15 = []
    
    for i, r in enumerate(latest_db_data[1:], start=2):
        if len(r) > 9:
            code       = str(r[2]).replace("'", "").strip().zfill(6)
            stock_name = r[0] if len(r) > 0 else "알수없음"
            status     = str(r[9]).strip()

            if best_short and code == best_short['code']:
                batch_updates_15.append({'range': f'J{i}', 'values': [[short_summary]]})
                if pick_short:
                    batch_updates_15.append({'range': f'O{i}', 'values': [[f"{pick_short['target']:,}원"]]})
                    batch_updates_15.append({'range': f'P{i}', 'values': [[f"{pick_short['stop']:,}원"]]})
                continue

            if best_mid and code == best_mid['code']:
                batch_updates_15.append({'range': f'J{i}', 'values': [[mid_summary]]})
                if pick_mid:
                    batch_updates_15.append({'range': f'O{i}', 'values': [[f"{pick_mid['target']:,}원"]]})
                    batch_updates_15.append({'range': f'P{i}', 'values': [[f"{pick_mid['stop']:,}원"]]})
                continue

            # 수석님의 지시: 리포트는 유지하고 나머지는 전부 새로 작성
            if "리포트 발송 완료" in status:
                continue
                
            print(f" - [{stock_name}] AI 브리핑 작성 중...")
            curr_p       = r[3]  if len(r) > 3  else '0'
            tajeom_badge = r[8]  if len(r) > 8  else ''
            sugeup       = r[11] if len(r) > 11 else ''
            high_52      = r[12] if len(r) > 12 else ''
            theme        = r[5]  if len(r) > 5  else ''
            target_sys   = r[14] if len(r) > 14 else ''
            stop_sys     = r[15] if len(r) > 15 else ''
            
            prompt = get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, is_warning_market)
            
            res_text    = safe_generate_content(prompt, is_fast=True).text
            parsed_data = parse_ai_json(res_text)
            briefing_text = parsed_data.get("briefing", "⚠️ 브리핑 생성 에러")
            
            if not briefing_text.startswith("✅") and not briefing_text.startswith("⚠️"):
                briefing_text = f"✅ [간단 브리핑] {briefing_text}"
            
            raw_target = str(parsed_data.get('target_price', '0')).replace(',', '').replace('원', '')
            raw_stop   = str(parsed_data.get('stop_loss',   '0')).replace(',', '').replace('원', '')
            target_val = f"{int(raw_target):,}원" if raw_target.isdigit() and int(raw_target) > 0 else "관망"
            stop_val   = f"{int(raw_stop):,}원"   if raw_stop.isdigit()   and int(raw_stop)   > 0 else "관망"
            
            batch_updates_15.append({'range': f'J{i}', 'values': [[briefing_text]]})
            batch_updates_15.append({'range': f'O{i}', 'values': [[target_val]]})
            batch_updates_15.append({'range': f'P{i}', 'values': [[stop_val]]})
            
            time.sleep(0.1) # 유료 API 1000 RPM 한도이므로 초고속 통과

    if batch_updates_15:
        db_sheet.batch_update(batch_updates_15)
        print(f"✅ DB_스캐너 초고속 일괄 업데이트 완료!")

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

    try:
        print("▶ 백테스트 로그 스냅샷 기록 중...")
        bt_sheet = doc.worksheet("백테스트_로그")
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
        
        bt_data = bt_sheet.get_all_values()
        already_logged = any(today_str in str(r[0]) for r in bt_data if r)
        
        if not already_logged:
            log_rows = []
            for cand in pool_150[:5]:
                tajeom_match = re.search(r'타점:(.*?)\|', cand['info'])
                tajeom_str = tajeom_match.group(1).strip() if tajeom_match else "분석대기"
                
                log_rows.append([
                    today_str, 
                    cand['name'], 
                    f"'{cand['code']}", 
                    "수동확인요망", 
                    f"{cand['curr_p']:,}원", 
                    tajeom_str, 
                    f"{cand['score']}점", 
                    "", ""
                ])
            
            bt_sheet.append_rows(log_rows, value_input_option="USER_ENTERED")
            print("✅ 오늘자 상위 5개 종목 백테스트 로그 기록 완료!")
    except Exception as e:
        print(f"⚠️ 백테스트 로그 기록 에러: {e}")
        
    print(f"🎉 모든 작업이 성공적으로 완료되었습니다: {pdf_file}")

except Exception as e:
    print(f"\n❌ 시스템 에러: {e}")
    exit(1)
