# -*- coding: utf-8 -*-
import os, re, time, base64, warnings, datetime, requests, markdown, pdfkit, gspread, json
from PIL import Image 
from bs4 import BeautifulSoup  
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
import urllib3
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
 
print(f"🤖 [HYEOKS 리서치 센터] 3단계 세이프티 가드 엔진 가동 (현재 KST {now_kst.strftime('%H:%M:%S')})")
 
try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"❌ API 초기화 실패: {e}"); exit(1)
 
def clean_emojis(text):
    emojis = ['🚨','💡','💎','🔥','📊','📈','📉','🎯','🛡️','⏰','⏸️','🐎','🌟','🔒','🔴','🔵','⚪','🟢','🟡','👑','⚡','🚀','👀','⏳','🔻','🔺','➖', '🛢️', '💵', '🇺🇸', '🌱']
    for e in emojis: text = text.replace(e, '')
    return text.replace('  ', ' ').strip()
 
class GeminiUnreachableError(Exception):
    """🆕 [회로차단기] 제미나이가 실제로 다운/불통 상태로 판단될 때 던지는 전용 예외.
       일반 예외와 구분해서, 이걸 잡은 상위 로직이 '오늘은 AI 없이 안전하게 종료'를 선택할 수 있게 함."""
    pass

# 🆕 [회로차단기] DART 수집기 때와 동일한 패턴 — 최근 N회 호출 중 실패율이 임계치를 넘으면
#    "일시적 지연"이 아니라 "진짜 불통"으로 판단하고, 헛되이 재시도로 시간을 낭비하는 대신
#    즉시 포기하고 다음 예약 실행에서 자연스럽게 복구되게 함.
_gemini_recent_results = []
GEMINI_ROLLING_WINDOW = 20
GEMINI_FAIL_RATE_THRESHOLD = 0.6
_gemini_consecutive_fails = 0
GEMINI_CONSECUTIVE_FAIL_LIMIT = 5

def _record_gemini_result(ok):
    global _gemini_consecutive_fails
    _gemini_recent_results.append(ok)
    if len(_gemini_recent_results) > GEMINI_ROLLING_WINDOW:
        _gemini_recent_results.pop(0)
    _gemini_consecutive_fails = 0 if ok else _gemini_consecutive_fails + 1
    if _gemini_consecutive_fails >= GEMINI_CONSECUTIVE_FAIL_LIMIT:
        raise GeminiUnreachableError(f"제미나이 연속 {_gemini_consecutive_fails}회 실패 — 완전 불통으로 판단")
    if len(_gemini_recent_results) >= 10:
        fail_rate = 1 - (sum(_gemini_recent_results) / len(_gemini_recent_results))
        if fail_rate >= GEMINI_FAIL_RATE_THRESHOLD:
            raise GeminiUnreachableError(f"제미나이 최근 {len(_gemini_recent_results)}회 중 실패율 {fail_rate:.0%} — 간헐적 불통으로 판단")


def safe_generate_content(contents, is_fast=False):
    model_name = 'gemini-2.5-flash' if is_fast else 'gemini-2.5-pro'
    for i in range(5): 
        try: 
            result = client.models.generate_content(model=model_name, contents=contents)
            _record_gemini_result(True)
            return result
        except GeminiUnreachableError:
            raise  # 회로차단기가 이미 발동한 거라 더 재시도하지 않고 바로 위로 전파
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                _record_gemini_result(False)  # 회로차단기 임계치 넘으면 여기서 GeminiUnreachableError로 즉시 중단됨
                wait_time = 10 * (i + 1)
                print(f"⚠️ 구글 API 지연. {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
            else:
                _record_gemini_result(False)
                raise e 
    _record_gemini_result(False)
    raise Exception("❌ 구글 서버 할당량 초과 또는 무응답으로 최종 실패")
 
def parse_ai_json(text):
    try:
        clean_text = text.replace('```json', '').replace('```', '').strip()
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
        except Exception as e:
            print(f"⚠️ [parse_ai_json 폴백 파싱도 실패] {e}")
            return {"briefing": "응답 오류", "target_price": 0, "stop_loss": 0}
 
def get_target_stock_news(code):
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='cp949')
        news_list = [f"- {a.text.strip()}" for a in soup.select('.title a')[:3]]
        return clean_emojis("\n".join(news_list)) if news_list else "개별 뉴스 없음"
    except Exception as e:
        print(f"⚠️ [개별 뉴스 수집 실패 {code}] {e}")
        return "뉴스 수집 실패"
 
def get_vip_deep_dive_data(code, kis_token):
    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET): return "PER: N/A / PBR: N/A"
    try:
        headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P", "tr_id": "FHKST01010100"}
        res = requests.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                          headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
        out = res.get("output", {})
        return f"PER: {out.get('per', 'N/A')} / PBR: {out.get('pbr', 'N/A')}"
    except Exception as e:
        print(f"⚠️ [KIS 재무지표 조회 실패 {code}] {e}")
        return "데이터 수집 실패"
 
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
                except Exception: continue
            return datetime.datetime(1970, 1, 1)
            
        rows.sort(key=lambda x: parse_date(x[sort_col_idx]), reverse=True)
        
        sheet.batch_clear(['A2:Z'])
        sheet.update(range_name="A2", values=[header] + rows, value_input_option="USER_ENTERED")
        print(f"✅ [{sheet_name}] 최신순 정렬 및 청소 완료")
    except Exception as e:
        print(f"⚠️ [{sheet_name}] 정렬 실패: {e}")
 
def validate_stock_historical_dna(cand, raw_theme_daily_map):
    code = cand['code']
    name = cand['name']
    theme_raw = cand.get('theme_name', '')
    clean_theme = theme_raw.replace("🆕[당일]", "").replace("🕰️[과거]", "").split(' (대장:')[0].strip()
    
    local_session = requests.Session()
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=250&requestType=0"
        res = local_session.get(url, verify=False, timeout=3)
        root = ET.fromstring(res.text)
        items = root.findall(".//item")
        
        has_qualified_day = False
        for item in items:
            data = item.get("data").split("|")
            f_date_raw = data[0]
            f_date = f"{f_date_raw[:4]}-{f_date_raw[4:6]}-{f_date_raw[6:8]}"
            close_p = int(data[4])
            vol = int(data[5])
            
            day_tv_krw = close_p * vol
            if day_tv_krw >= 70_000_000_000:
                theme_val_eok = raw_theme_daily_map.get((f_date, clean_theme), 0)
                if theme_val_eok >= 2000 or theme_val_eok == 0:
                    has_qualified_day = True
                    break
                    
        return cand, has_qualified_day
    except Exception as e:
        print(f"⚠️ [{name}] 역사적 DNA 검증 인프라 오류 (안전을 위해 풀에서 배제): {e}")
        return cand, False

def generate_deep_report(st_type, best_cand, is_warning_market=False, KIS_TOKEN="", client=None):
    if not best_cand: 
        return "", None
        
    vip = get_vip_deep_dive_data(best_cand['code'], KIS_TOKEN)
    news = get_target_stock_news(best_cand['code'])
    
    strategy_instruction = ""
    if is_warning_market:
        strategy_instruction = "🚨 현재 국내 증시는 보수적 운영 및 방어적 매매가 요망되는 하락/조정 장세입니다. 리스크 관리를 극대화하는 관점으로 서술하십시오."
    else:
        strategy_instruction = "✨ 현재 국내 증시는 공격적 운영이 가능한 지지 장세입니다. 주도주 돌파 및 적극적인 수익 극대화 관점으로 서술하십시오."

    eng_strategy = "AGGRESSIVE TREND MOMENTUM STRATEGY" if st_type == "short" else "DEFENSIVE PLATFORM ACCUMULATION SWING"
    sub_title_prefix = "매물대 진공 구간 돌파 및 단기 슈팅 공략" if st_type == "short" else "에너지 응축 후 플랫폼 탈출 스윙 전략"

    # 🆕 [V3 반영] 장기 추세추종형(바닥·분할매수/코어픽) 중기 픽에 실적점수(V3)가 있으면,
    #    이 종목 스토리의 핵심 근거로 실적 추이를 적극 서술하도록 명시적으로 지시.
    #    (과매도·역배팅형은 기술적 반등이 핵심이라 실적 서술을 강제하지 않음)
    is_structural_pick = st_type == "mid" and best_cand.get('v3_score') is not None and \
        (("바닥 · 분할매수" in best_cand.get('info', '')) or ("코어픽" in best_cand.get('info', '')))
    # 🆕 [단기/중기 목표·손절 폭 분리] 실측 결과 두 채널의 목표폭이 사실상 같았다(단기 +14.7% / 중기 +14.5%,
    #    차이 0.2%p). 보유기간이 다른데 가격 밴드가 같으면 '단기'라는 구분 자체가 무의미해진다.
    #    · 단기는 며칠 내 도달 가능한 좁은 밴드여야 회전이 돌고, 손절도 타이트해야 손실이 짧게 끊긴다.
    #    · 중기는 분할매수를 전제로 손절을 넉넉히 둬야 흔들림에 털리지 않고, 목표도 그만큼 멀어야 한다.
    #    프롬프트에 폭을 수치로 못 박아 AI가 둘을 실제로 다르게 잡도록 강제한다.
    price_band_instruction = (
        "\n4. 🚨 [목표가·손절가 폭 — 단기 전용 규칙]: 이 픽은 며칠 내 승부를 보는 단기 슈팅입니다.\n"
        "   · 목표가는 확정 현재가 대비 +7~12% 범위에서 정하십시오. 며칠 안에 닿지 못할 먼 목표는 금지입니다.\n"
        "   · 손절가는 -6~8% 범위로 잡으십시오. 그보다 더 조이면 국내 증시 일중 변동성에 그냥 잘려 나갑니다.\n"
        "   · 손익비는 최소 1.2 이상이 되게 맞추십시오."
        if st_type == 'short' else
        "\n4. 🚨 [목표가·손절가 폭 — 중기 전용 규칙]: 이 픽은 분할매수로 몇 주간 끌고 가는 스윙입니다.\n"
        "   · 목표가는 확정 현재가 대비 +15~30% 범위에서 정하십시오. 단기 시세가 아니라 추세 목표여야 합니다.\n"
        "   · 손절가는 -8~15% 범위로 넉넉히 잡으십시오. 차트상 의미 있는 하단(기준봉 시가·60일선·쌍바닥 저점)에 두어\n"
        "     일상적인 흔들림에 털리지 않게 하십시오.\n"
        "   · 손익비는 최소 1.8 이상이 되게 맞추십시오."
    )

    earnings_instruction = (
        "\n4. 🆕 [실적 추세 적극 반영]: 이 종목은 장기 추세추종형 픽이며 실적점수(V3)가 확인됩니다. "
        "매출액·영업이익이 최근 분기 들어 어떻게 개선되고 있는지를 이 종목 투자 스토리의 핵심 근거로 삼아 "
        "서술에 적극 반영하십시오 — 단순 차트 언급에 그치지 말고, '실적이 뒷받침되는 구조적 성장'이라는 점을 명확히 짚어주십시오."
        if is_structural_pick else ""
    )

    detail_prompt = f"""당신은 대한민국 최상위 1% 실전 트레이더들을 위한 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다.
제공된 일봉 차트(Vision)와 데이터를 바탕으로 심층 리포트를 작성하십시오. 한 리포트 내에서 말투가 바뀌지 않도록 정중한 존댓말(하십시오체)로 통일하십시오.

[입력 데이터]
종목 및 스캐너 판독: {best_cand['info']}
★확정 현재가: {best_cand['curr_p']}원
펀더멘털: {vip}
최신 뉴스: {news}
{strategy_instruction}

[HYEOKS 딥리딩 절대 지침 - 명심하십시오]
1. 분량 및 깊이: 귀하의 최고 수준의 통찰력을 발휘하여 논리적으로 서술하되, 전체 분량이 A4 최대 2페이지를 넘지 않도록 하십시오.
2. 🚨 [할루시네이션(거짓 정보) 엄격 금지]: 차트를 판독하여 지지/저항선을 제시할 때, 반드시 위 [입력 데이터]에 제공된 ★확정 현재가({best_cand['curr_p']}원)를 기준으로 상/하단 가격을 논리적으로 계산하십시오.
3. 가상계좌 규칙: 리포트 마지막 줄에만 [DATA] 목표가:00000, 손절가:00000, 분할매수:{'O' if st_type=='mid' else 'X'} 형식으로 숫자로만 출력하십시오.
{price_band_instruction}{earnings_instruction}

[출력 양식 (마크다운 및 HTML 복합 레이아웃 절대 고수)]

<div class="strategy-eng">{eng_strategy}</div>
<hr>
<h1 class="stock-title">{best_cand['name']} ({best_cand['code']})</h1>
<div class="subtitle">{sub_title_prefix}</div>

<div class="summary-box">
[HYEOKS 핵심 모멘텀 요약]
🚨 [작성 지침]: 첫 문장은 반드시 구두점 포함 '50자 이내의 완결된 딱 한 문장'으로, 핵심 호재와 차트 위치만 압축한 헤드라인으로 작성하십시오
(예: "역대급 거래대금 동반 장기 박스권 상단 돌파로 단기 슈팅 초입 국면 판독."). 이 첫 문장은 다른 시스템에서 그대로 발췌되니 반드시 이 한 문장 안에서 완결되어야 합니다.
그 뒤에 이어서, 진짜 증권사 리포트의 총평처럼 2~3문장을 추가로 덧붙여 이 종목의 투자 포인트를 조금 더 풍부하게 설명하십시오
(핵심 근거 한 문장 + 리스크 요인 한 문장 정도의 균형 잡힌 총평). 전체 문단이 5문장을 넘지는 마십시오.
</div>

## 1. 펀더멘털 및 매크로 유동성 심층 고찰
(FRED 지표 흐름 해석을 바탕으로 숨겨진 진짜 모멘텀을 심층 분석. 뉴스·이슈는 아래 2번 섹션에서 별도로 다루니 여기선 매크로·펀더멘털에 집중하십시오.)

## 2. 뉴스 및 이슈 분석
(위 [입력 데이터]의 최신 뉴스를 바탕으로, 이 종목과 직접 관련된 이슈·재료가 주가에 미치는 영향을 애널리스트 관점으로 분석. 뉴스가 빈약하거나 무관하면 "특기할 만한 개별 뉴스 없음. 수급·차트 동력 중심의 접근이 유효"처럼 정직하게 명시하고 억지로 서술을 늘리지 마십시오.)

## 3. 시각적 차트 판독 및 거래량 딥리딩
(주요 매물대, 이평선 이격도, 최근 스마트머니의 거래량 증감 해부)

## 4. 실전 타점 시나리오 및 리스크 관리 전략
(시간외 데이터를 반영한 익일 시가 갭 대응 시나리오, 1차/2차 진입 가격대, 목표가/손절가를 매우 상세하게 작성할 것)

[DATA] 목표가:00000, 손절가:00000, 분할매수:{'O' if st_type=='mid' else 'X'} """

    img_path = f"temp_{best_cand['code']}.png"
    try:
        res = requests.get(f"https://ssl.pstatic.net/imgfinance/chart/item/candle/day/{best_cand['code']}.png", headers={'User-Agent': 'Mozilla/5.0'}, verify=False)
        with open(img_path, 'wb') as f: 
            f.write(res.content)
        
        model_name = 'gemini-2.5-pro'
        report_txt = client.models.generate_content(model=model_name, contents=[detail_prompt, Image.open(img_path)]).text
        os.remove(img_path)
    except Exception as e:
        print(f"⚠️ 비전 파싱 실패 Fallback 구동: {e}")
        model_name = 'gemini-2.5-pro'
        report_txt = client.models.generate_content(model=model_name, contents=detail_prompt).text

    pick_data = None
    if report_txt:
        match = re.search(r'\[DATA\]\s*목표가:(\d+),\s*손절가:(\d+),\s*분할매수:([OX])', report_txt)
        if match:
            pick_data = {
                'code': best_cand['code'],
                'name': best_cand['name'],
                'curr_p': best_cand['curr_p'],
                'curr': best_cand['curr_p'],
                'target': int(match.group(1)),
                'stop': int(match.group(2)),
                'split': match.group(3)
            }
            
    return report_txt, pick_data
 
# ==========================================
# 2. 구글 시트 연결 및 마켓 리스크 단계 판독
# ==========================================
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    db_sheet = doc.worksheet("DB_스캐너")
    db_rows = db_sheet.get_all_values()

    # 🕰️ [락 분리 안전판]: 리포트 lock을 omakase와 분리하면서 유일하게 남는 좁은 충돌 창구 —
    # 07:00 리포트 트리거가 omakase의 DB_스캐너 리셋 시간대(07:00~08:50)와 겹치면 빈/리셋 직후 데이터를 읽을 수 있음.
    # 데이터가 비정상적으로 적으면(리셋 직후로 추정) 최대 2회, 20초 간격으로 짧게 재조회.
    _retry_left = 2
    while len(db_rows) <= 2 and _retry_left > 0:
        print(f"⚠️ [DB_스캐너 데이터 부족 감지] omakase 리셋 시간대와 겹쳤을 가능성 → 20초 후 재조회 ({_retry_left}회 남음)")
        time.sleep(20)
        db_rows = db_sheet.get_all_values()
        _retry_left -= 1

    cleanup_and_reorder(doc, "접속로그", 1)
    cleanup_and_reorder(doc, "DB_중장기", 0)
 
    # 실시간 토큰 주입 파트
    KIS_TOKEN = ""
    last_published_date = ""  # 🛡️ [예외 재실행 안전장치] 오늘 이미 발행됐는지 확인용
    try:
        for row in doc.worksheet("⚙️설정").get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN": KIS_TOKEN = row[1]
            if len(row) >= 2 and row[0] == "마지막_리포트_발행일": last_published_date = row[1].strip()
    except Exception as e:
        print(f"⚠️ [⚙️설정 로드 실패 — KIS 토큰/마지막 발행일 확인 불가] {e}")
    FORCE_RESEND = os.environ.get("FORCE_RESEND", "false").strip().lower() == "true"
 
    market_summary_data = doc.worksheet("시장요약").get_all_values()
    korean_market_status = clean_emojis(market_summary_data[1][8]) if len(market_summary_data) > 1 and len(market_summary_data[1]) > 8 else ""

    # 🔧 [수정] "시장요약" 탭은 더 이상 운영되지 않아 항상 빈 칸이었음 — 그런데 예전 로직은 이 칸이 비어있기만
    #    해도 "확인불가"로 보고 무조건 STAGE 2(경고)로 진입시켰음. 그 결과 실제 코스피가 +4.4%로 강세인
    #    날에도 "하락 장세"라며 전종목 매수 보류가 뜨는 오판이 매일 발생하고 있었음(2026-07-23 확인된 사고).
    #    → 이제 실시간 코스피·코스닥 등락률(살아있는 소스) 조회를 우선 신뢰하고, 시장요약 텍스트는 실제로
    #    "하락"/"이탈" 문구가 있을 때만 보조 신호로 씀. 둘 다 실시간 조회에 실패했을 때만 "확인불가" 처리.
    kospi_rate_fallback, kosdaq_rate_fallback = 0.0, 0.0
    kospi_fetch_ok, kosdaq_fetch_ok = False, False
    try:
        _k = requests.get(f"https://m.stock.naver.com/api/index/KOSPI/basic?_={int(time.time()*1000)}", headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=3).json()
        kospi_rate_fallback = float(str(_k.get("fluctuationsRatio", "0")).replace(',', ''))
        kospi_fetch_ok = True
    except Exception as _e:
        print(f"⚠️ 코스피 등락률 실시간 조회 실패: {_e}")
    try:
        _q = requests.get(f"https://m.stock.naver.com/api/index/KOSDAQ/basic?_={int(time.time()*1000)}", headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=3).json()
        kosdaq_rate_fallback = float(str(_q.get("fluctuationsRatio", "0")).replace(',', ''))
        kosdaq_fetch_ok = True
    except Exception as _e:
        print(f"⚠️ 코스닥 등락률 실시간 조회 실패: {_e}")

    _live_rates = [r for r, ok in [(kospi_rate_fallback, kospi_fetch_ok), (kosdaq_rate_fallback, kosdaq_fetch_ok)] if ok]
    both_fetch_failed = len(_live_rates) == 0
    worst_live_rate = min(_live_rates) if _live_rates else 0.0
    status_is_blank = both_fetch_failed  # 실시간 조회가 둘 다 실패했을 때만 "진짜 확인불가"로 취급

    is_warning_market = ("하락" in korean_market_status) or ("이탈" in korean_market_status) or status_is_blank or (worst_live_rate <= -1.0)

    market_stage = 1
    stage_text = "STAGE 1 (정상 장세 - 공격형 추세 매매 가동)"
    if is_warning_market:
        market_stage = 2
        stage_text = "STAGE 2 (주의 장세 - 방어형/바닥주 SEED 포지션 제한)"
        if status_is_blank:
            stage_text = "STAGE 2 (시장상태 미확인 → 안전 기본값 방어 모드)"
    if (bool(korean_market_status.strip()) and any(kw in korean_market_status for kw in ["패닉", "붕괴", "투매", "하락장 위험"])) or (worst_live_rate <= -3.0):
        # 🔧 [수정] "검은"·"폭락"·"쇼크" 등은 시스템이 다른 곳(HALLUC_KW)에서 이미 오독 위험이 크다고 판단해
        #    걸러내는 단어들인데, 정작 여기 STAGE 3 판정에는 그대로 남아있었음. 만약 "시장요약" 탭에
        #    과거 폭락장 때 적힌 낡은 문구("검은 금요일 장세" 등)가 안 지워진 채 남아있다면, 오늘 실제로는
        #    폭등장이어도 이 텍스트 하나 때문에 전 종목이 강제로 말살(000000)될 위험이 있었음.
        #    → 오독 위험이 큰 단어는 제거, 텍스트 조건은 시장요약이 실제로 채워져 있을 때만 적용,
        #    실시간 수치는 코스피 단독이 아니라 코스피·코스닥 중 더 나쁜 쪽(worst_live_rate)으로 판단.
        market_stage = 3
        stage_text = "STAGE 3 (패닉 장세 - 서킷 위험 임계점 돌파,전원 사격 중지)"
    print(f"📡 [실시간 시장 위험도 연산 판독 완료]: {stage_text} (상태: {korean_market_status or '미확인'} / 코스피:{kospi_rate_fallback:.2f}%)")
 
    # ── [복원] 주요일정 이슈브리핑 — 굵직한 일정을 골라 AI가 섹터영향/체크포인트 분석 (주 1회) ──
    #    2026-07-04 도입 → 2026-07-11 커밋에서 통째로 삭제되며 중단됐던 기능을 되살림.
    #    (마지막 생성분이 7/6 실행분이라 시트가 7/16 일정에서 멈춰 있었다)
    def generate_schedule_briefings(doc):
        try:
            now = datetime.datetime.now(KST)
            # 🔧 [주기 변경] 예전엔 '매주 월요일 1회'였는데, 회당 8건 상한과 겹쳐 밀린 일정을 따라잡는 데
            #    몇 주가 걸렸다(앞으로 2주치 27건이면 3주 소요). 아래 중복키 때문에 이미 분석된 일정은
            #    절대 재생성되지 않으므로, 매일 아침 회차에 돌려도 다 채워진 뒤에는 API 호출이 0이 된다.
            #    → 하루 1회(7시 회차)로 바꿔 밀린 분량을 며칠 안에 소화. 즉시 채우고 싶으면 FORCE_BRIEFING=true.
            force_brief = os.environ.get("FORCE_BRIEFING", "false").strip().lower() == "true"
            if not force_brief and current_hour != 7:
                print(f"ℹ️ [이슈브리핑] 이번 회차({current_hour}시)는 생성 대상 아님 (매일 7시 회차 또는 FORCE_BRIEFING=true)")
                return

            sched_rows = doc.worksheet("주요일정").get_all_values()[1:]
            today_str = now.strftime('%Y-%m-%d')
            horizon_str = (now + datetime.timedelta(days=14)).strftime('%Y-%m-%d')

            # 📅 자동수집(당일) = 뉴스 스크래핑 결과라 제외. 미리 정해진 굵직한 일정만 대상.
            EXCLUDE_CATS = {"📅 자동수집(당일)"}
            candidates = []
            for row in sched_rows:
                if len(row) < 3:
                    continue
                title, cat = str(row[1]).strip(), str(row[2]).strip()
                if not title or cat in EXCLUDE_CATS:
                    continue
                parts = str(row[0]).strip().replace('. ', '-').replace('.', '-').strip('-').split('-')
                if len(parts) != 3:
                    continue
                date_norm = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                if today_str <= date_norm <= horizon_str:
                    candidates.append({'date': date_norm, 'title': title, 'cat': cat})

            if not candidates:
                print("ℹ️ [이슈브리핑] 향후 14일 내 분석 대상 일정 없음")
                return

            try:
                brief_sheet = doc.worksheet("이슈브리핑")
            except Exception:
                brief_sheet = doc.add_worksheet(title="이슈브리핑", rows="500", cols="4")
                brief_sheet.append_row(["날짜", "일정내용", "테마구분", "AI분석"])

            existing_keys = set(f"{r[0]}|{r[1]}" for r in brief_sheet.get_all_values()[1:] if len(r) >= 2)

            new_rows, failed = [], 0
            for c in candidates[:8]:      # 한 번에 최대 8건 (비용 관리, 남으면 다음 주에 이어서)
                key = f"{c['date']}|{c['title']}"
                if key in existing_keys:
                    continue
                prompt = f"""아래 증시 일정 하나를 분석해서, 정해진 형식으로만 한국어로 답변하세요.
일정: {c['date']} - {c['title']} (분류: {c['cat']})

다음 형식을 정확히 지켜 작성하십시오 (군더더기 설명·서론 없이 바로):
[핵심]
- (이 일정이 왜 중요한지 2줄 이내)
[주식시장 영향]
- (국내 증시에 미칠 영향 2~3줄)
[주목 섹터]
- (관련 섹터/테마 2~4개, 쉼표로 나열)
[체크포인트]
- (투자자가 확인해야 할 변수 2~3개)

확인되지 않은 구체적 수치나 사실을 지어내지 말고, 일정의 성격과 일반적으로 알려진 배경지식에 기반한 분석만 작성하십시오."""
                try:
                    analysis = safe_generate_content(prompt, is_fast=True).text.strip()
                except GeminiUnreachableError:
                    # 제미나이 자체가 불통이면 나머지도 전부 실패한다 → 조용히 반복하지 말고 즉시 중단
                    print("🚨 [이슈브리핑] 제미나이 불통 감지 — 남은 일정 생성을 중단합니다(다음 주 재시도).")
                    break
                except Exception as e:
                    failed += 1
                    print(f"⚠️ [이슈브리핑 생성 실패] {c['date']} {c['title']}: {e}")
                    continue
                new_rows.append([c['date'], c['title'], c['cat'], analysis])
                existing_keys.add(key)

            if new_rows:
                brief_sheet.append_rows(new_rows, value_input_option="USER_ENTERED")
                print(f"✅ [이슈브리핑] {len(new_rows)}건 신규 생성 완료" + (f" (실패 {failed}건)" if failed else ""))
            else:
                print(f"⏭ [이슈브리핑] 신규 생성 없음 (기존 분석 보유 또는 전부 실패 {failed}건)")
        except Exception as e:
            # 보조 기능이므로 메인 리포트를 막지 않는다. 다만 사유는 반드시 남긴다.
            print(f"❌ [이슈브리핑 전체 에러 — 메인 리포트는 계속 진행] {type(e).__name__}: {e}")

    generate_schedule_briefings(doc)

    sys_instruction = "기업의 일반적인 소개는 일절 금지. 차트 지표, 타점, 수급 데이터를 바탕으로 '현재 기술적 위치'와 '앞으로의 대응 전략'만을 60~70자 내외로 매우 짧고 날카롭게 작성할 것."
 
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
        print(f"✅ {len(updates) // 3}개 종목 초기화 완료 (batch_update 1회). 프로그램 종료."); exit(0)
 
    def get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, market_stage, stage_text):
        is_seed = any(kw in tajeom_badge for kw in ["🌱", "모아가기", "DB_중장기"])
        is_active_buy = any(kw in tajeom_badge or kw in sugeup for kw in ["외인집중", "외인대량", "🔥", "👑", "💎", "🔴"])
        
        if market_stage == 3:
            market_context = "🚨 [비상 국면] 국내 증시는 현재 무차별 패닉 투매가 발생하는 극단적 고위험 상태입니다."
            veto_template = "⚠️ [매수 보류] 시장 패닉셀 국면 진입으로 인해 전 종목 매수 보류 및 현금 100% 관망을 강력 권고합니다."
        elif market_stage == 2:
            market_context = "⚠️ [주의 국면] 국내 증시는 현재 변동성이 큰 하락/횡보 장세입니다."
            veto_template = "⚠️ [매수 보류] 하락 장세로 인한 시장 리스크 과다 및 단기 상승 동력 부족으로 관망 권장"
        else:
            market_context = "🟢 [정상 국면] 국내 증시는 현재 정상적인 추세 매매 및 돌파 랠리가 가능한 양호한 장세입니다."
            veto_template = "⚠️ [매수 보류] 지수 장세는 양호하나, 본 종목의 독자적인 단기 기술적 과열(이격 과다) 또는 상단 매물 저항으로 인해 관망을 권장합니다."
 
        if market_stage == 3:
            guide_text = f"""
            🚨🚨 [EMERGENCY: 시스템 전원 사격 중지 명령] 🚨🚨
            현재 시장은 {stage_text} 상태입니다. 기술적 지표나 개별 종목의 모멘텀 유무와 상관없이 무차별 연쇄 패닉 투매가 발생하는 고위험 국면입니다.
            1. 어떠한 낙관론이나 억지 매수 타점 시나리오도 전개하지 마십시오. 무조건 강력한 '매수 보류(Veto)' 조치를 집행해야 합니다.
            2. briefing 본문은 반드시 토씨 하나 틀리지 않고 정확하게 다음 문장으로만 출력하십시오: "{veto_template}"
            3. target_price와 stop_loss는 어떠한 계산값도 출력하지 말고 반드시 0으로 처리하십시오.
            """
        elif is_active_buy:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 외인 집중배팅 역발상 전략]
            {market_context}
            🚨 귀하는 세계 최고의 월스트리트 퀀트 애널리스트 집단입니다. 
            1. 이 종목은 기계적인 프로그램 매도 폭탄 속에서도 외국인 액티브 자금이 강력하게 '개별 종목으로 집중 매집'하고 있는 보석 같은 종목입니다.
            2. 지수 하락에 흔들리지 말고, 세력의 매집 단가를 유추하여 손절가를 넉넉하게 잡고, 1차/2차 분할 매수 타점을 제시하십시오.
            3. "프로그램 매도에도 불구하고 찐외인 수급이 유입 중"이라는 역발상 논리를 브리핑에 반드시 포함하십시오.
            """
        elif is_seed:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 중장기 모아가기 & DB_중장기 픽 전략]
            {market_context}
            🚨 귀하는 세계 최고의 월스트리트 퀀트 애널리스트 집단입니다. 
            1. 이 종목은 현재 고점 대비 조정을 받고 거래량이 마른 '씨앗(SEED)' 종목입니다. 시스템 기준가에 얽매이지 마십시오.
            2. 손절가 설정: 차트 상의 아주 넉넉하고 의미 있는 하단 바운더리(예: 이전 거대한 기준봉의 시가, 60일선, 쌍바닥 최저점)를 유추하여 단단하게 설정하십시오.
            3. 매수 전략: 한 번에 몰빵하는 것이 아니라 "현재가 부근 1차 매수 후, ~원 부근(손절가 위)에서 2차 분할 매수"하는 시나리오를 브리핑에 포함하십시오.
            """
        else:
            guide_text = f"""
            💡 [AI 매매 보류(Veto) 및 가격 결정 가이드: 단기/스윙 주도주 전략]
            {market_context}
            🚨 귀하는 세계 최고의 월스트리트 퀀트 애널리스트 집단입니다. 
            1. 제공된 데이터를 분석했을 때, 단기 모멘텀이 빠르게 소멸할 위험이 있거나, 윗꼬리가 너무 길어 리스크가 크다고 판단되면 과감히 관망(Veto)을 지시하십시오.
               - 🚨 [중요]: 만약 매수 보류(Veto)를 선언할 경우, briefing 문구는 반드시 다음 규칙을 준수하여 장세 판독과 모순되지 않게 작성하십시오:
                 - 장세가 정상(STAGE 1)일 때 보류하는 경우: "{veto_template}"
                 - 장세가 하락/주의(STAGE 2)일 때 보류하는 경우: "⚠️ [매수 보류] 하락 장세로 인한 시장 리스크 과다 및 단기 상승 동력 부족으로 관망 권장"
               - 이 경우 target_price와 stop_loss는 반드시 0으로 처리하십시오.
            2. 가격 튜닝: 진입이 가능하다고 판단될 경우 손절을 매우 타이트하게 잡고, 익절(목표가) 역시 짧게 끊어치는 보수적인 타점을 제시하십시오.
            """
 
        return f"""
        당신은 세계 최고의 헤지펀드를 이끄는 수석 퀀트 애널리스트입니다.
        [{sys_instruction}]
        
        ■ 종목명: {stock_name}
        ■ 현재가: {curr_p}
        ■ 타점 위치(배지): {tajeom_badge}
        ■ 수급강도 및 프로그램: {sugeup}
        | 시장 상황 컨텍스트: {market_context}
        ■ 52주 고가: {high_52}
        ■ 테마: {theme}
        ■ 🤖 [시스템 임시 기준가]: 목표가 {target_sys} / 손절가 {stop_sys}
        
        {guide_text}
        
        반드시 아래 JSON 형식으로만 대답하십시오.
        {{
            "briefing": "여기에 전략 요약 작성",
            "target_price": 150000,
            "stop_loss": 135000
        }}
        """
 
    # ==========================================
    # 📡 [장중 스냅샷 실시간 업데이트 루프 - 15시 외 가동]
    # ==========================================
    if current_hour != 15 and not FORCE_RESEND:
        print(f"▶ [{current_hour}시 모드] 메인 리포트 시간이 아니므로, 실시간 대기 종목의 정밀 요격 브리핑을 개시합니다.")
        for i, row in enumerate(db_rows[1:], start=2):
            if len(row) > 9 and not any(key in str(row[9]) for key in ["리포트 발송 완료", "리포트 작성 완료"]):  
                stock_name = row[0] if len(row) > 0 else "알수없음"
                code = str(row[2]).replace("'", "").strip().zfill(6)
                
                curr_p = row[3] if len(row) > 3 else ''
                tajeom_badge = row[8] if len(row) > 8 else ''
                sugeup = row[11] if len(row) > 11 else ''  
                high_52 = row[12] if len(row) > 12 else ''  
                theme = row[5] if len(row) > 5 else ''
                target_sys = row[14] if len(row) > 14 else ''
                stop_sys = row[15] if len(row) > 15 else ''
                
                prompt = get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, market_stage, stage_text)
                
                try:
                    res_text = safe_generate_content(prompt, is_fast=True).text
                    parsed_data = parse_ai_json(res_text)
                    
                    briefing_text = parsed_data.get("briefing", "브리핑 생성 에러")
                    if not briefing_text.startswith("✅") and not briefing_text.startswith("⚠️"): 
                        briefing_text = f"✅ [간단 브리핑] {briefing_text}"
                    
                    raw_target = str(parsed_data.get('target_price', '0')).replace(',', '').replace('원', '')
                    raw_stop = str(parsed_data.get('stop_loss', '0')).replace(',', '').replace('원', '')
                    
                    target_val = f"{int(raw_target):,}원" if raw_target.isdigit() and int(raw_target) > 0 else "관망"
                    stop_val = f"{int(raw_stop):,}원" if raw_stop.isdigit() and int(raw_stop) > 0 else "관망"
                    
                    current_db_snapshot = db_sheet.get_all_values()
                    real_row_idx = -1
                    for idx, r_row in enumerate(current_db_snapshot, start=1):
                        if len(r_row) > 2 and str(r_row[2]).replace("'", "").strip().zfill(6) == code:
                            real_row_idx = idx; break
                    
                    if real_row_idx != -1:
                        if any(key in str(current_db_snapshot[real_row_idx-1][9]) for key in ["리포트 발송 완료", "리포트 작성 완료"]): continue
                            
                        db_sheet.update_cell(real_row_idx, 10, briefing_text)
                        db_sheet.update_cell(real_row_idx, 15, target_val)
                        db_sheet.update_cell(real_row_idx, 16, stop_val)
                        
                        try:
                            helper_sheet = doc.worksheet("주가데이터_보조")
                            helper_snapshot = helper_sheet.get_all_values()
                            for h_idx, h_row in enumerate(helper_snapshot, start=1):
                                if len(h_row) > 1 and str(h_row[1]).replace("'", "").strip().zfill(6) == code:
                                    helper_sheet.update_cell(h_idx, 10, briefing_text)    
                                    helper_sheet.update_cell(h_idx, 24, target_val)      
                                    helper_sheet.update_cell(h_idx, 25, stop_val)        
                                    break
                        except Exception as ex:
                            print(f"⚠️ 시간외 주가데이터_보조 보조 타격 실패: {ex}")
                        
                    time.sleep(3.5)
                except Exception as e:
                    print(f"[{stock_name}] 브리핑/가격 산출 에러 발생 (건너뜀): {e}")
                    
        print(f"🌅 {current_hour}시 시간외 마감 정제 브리핑 완료! 프로그램 종료."); exit(0)
 
    # ==========================================
    # 🔴 [메인 15시 리포트 발급 마스터 파이프라인]
    # ==========================================
    if FORCE_RESEND and current_hour != 15:
        print(f"\n🛡️ [강제 재실행] 현재 {current_hour}시지만 FORCE_RESEND=true로 시간 게이트를 우회해 메인 리포트 파이프라인을 실행합니다.")
    print("\n▶ [15시 메인 리포트 모드] 주가데이터_보조 상위 150개 풀에서 HYEOKS 알파 종목 발굴 시작...")
    
    macro_data = doc.worksheet("시장요약").get_all_values()
    # 🔧 [중대 버그픽스] 시장요약은 1행이 '라벨'(🇺🇸 나스닥 (NASDAQ) 등)이고 2행이 '값'(26,363.44)이다.
    #    그런데 여태 1행을 읽어서, AI 매크로 프롬프트에 숫자가 아니라 라벨 문자열이 그대로 들어갔다.
    #    ("나스닥: 나스닥 (NASDAQ)") — 시황 브리핑이 늘 알맹이 없이 원론적이었던 근본 원인.
    def _macro_cell(col):
        """라벨 행을 피해 실제 수치가 있는 행에서 값을 집는다(레이아웃이 밀려도 견디게)."""
        for ri in (2, 1, 3):
            if len(macro_data) > ri and len(macro_data[ri]) > col:
                v = clean_emojis(str(macro_data[ri][col])).strip()
                if v and any(ch.isdigit() for ch in v):   # 숫자가 있어야 값으로 인정
                    return v
        return ""
    nasdaq, sp500 = _macro_cell(4), _macro_cell(5)
    exchange, oil = _macro_cell(6), _macro_cell(7)

    # 🆕 [시황 내용 보강] 뉴스 '언급 빈도'만으로는 실제 자금이 어디로 갔는지 알 수 없다.
    #    수급_실시간의 테마별 거래대금(=실제 돈)을 상위 5개만 뽑아 매크로 브리핑 근거로 제공.
    money_themes = ""
    try:
        _rt = doc.worksheet("수급_실시간").get_all_values()
        if len(_rt) > 1:
            _hd = _rt[0]
            _ti = _hd.index('테마명') if '테마명' in _hd else 3
            _vi = _hd.index('거래대금(억원)') if '거래대금(억원)' in _hd else 7
            _agg = {}
            for _r in _rt[1:]:
                if len(_r) > max(_ti, _vi):
                    _tn = str(_r[_ti]).split(' (대장:')[0].strip()
                    # 거래대금은 '903억원'처럼 단위가 붙어 오므로 숫자만 추출(그냥 int()면 전부 실패)
                    _vd = re.sub(r'[^0-9]', '', str(_r[_vi]))
                    if not _vd: continue
                    _vv = int(_vd)
                    if _tn:
                        _agg[_tn] = _agg.get(_tn, 0) + _vv
            _top = sorted(_agg.items(), key=lambda x: -x[1])[:5]
            money_themes = " / ".join(f"{k} {v:,}억" for k, v in _top)
    except Exception as e:
        print(f"⚠️ [주도 테마 자금흐름 집계 실패 — 매크로 브리핑에서 생략] {e}")
    # [환각 차단]: '검은(금요일/월요일)' 등 폭락·색깔·모호 단어가 키워드에 섞이면 AI가 가짜 테마('검은 반도체')로 창작함 → 매크로 입력 전 제거
    HALLUC_KW = {"검은", "블랙", "패닉", "폭락", "쇼크", "투매", "붕괴", "공포", "급락", "빨간", "파란"}
    _kw_rows = doc.worksheet("뉴스_키워드").get_all_values()[1:6]
    news_keywords = clean_emojis("\n".join([f"{r[2]}({r[3]}회)" for r in _kw_rows if str(r[2]).strip() not in HALLUC_KW]))
    
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
                    except Exception: pass
    except Exception as e:
        print(f"⚠️ 역사적 주도 테마 대금 연산 보조맵 생성 누락: {e}")
 
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]

    # 🆕 [V3 연결] DB_실적(hyeoks_earnings_collector.py가 채워둔 실적점수)을 종목코드 기준으로 미리 읽어둠.
    #    아직 DB_중장기·DB_스캐너 종목만 커버하므로, 데이터가 없는 종목은 "모른다"로 두고 불이익 주지 않음(fail-open).
    v3_map = {}
    try:
        earn_rows = doc.worksheet("DB_실적").get_all_values()[1:]
        for row in earn_rows:
            if len(row) > 8 and row[0].strip():
                try: v3_map[str(row[0]).strip().zfill(6)] = int(row[8])
                except Exception: pass
    except Exception as e:
        print(f"⚠️ [DB_실적 읽기 실패, V3 없이 진행] {e}")

    cands_list = []
    for r in tech_data:
        if len(r) < 21: continue
        name, code = str(r[0]).strip(), str(r[1]).replace("'", "").strip().zfill(6)
        curr_p, chg = str(r[2]).strip(), str(r[3]).strip()
        tajeom_raw = str(r[8]).strip()
        theme_name = str(r[19]).strip()
        prog = str(r[20]).strip()
        seed_tag = str(r[25]).strip() if len(r) > 25 else "NORMAL"
 
        try: v1_score = int(r[29]) if len(r) > 29 else 0
        except Exception: v1_score = 0
        try: v2_score = int(r[31]) if len(r) > 31 else 0
        except Exception: v2_score = 0
        # 🆕 [RS등급] 전종목 상대강도 백분위(1~99) — 표본 부족한 날은 빈 칸이라 그런 경우엔 프롬프트에서 아예 생략
        rs_grade_raw = str(r[33]).strip() if len(r) > 33 else ""
        try: rs_grade = int(rs_grade_raw) if rs_grade_raw else None
        except Exception: rs_grade = None
        combo_score = max(v1_score, v2_score)
        
        if re.search(r'매매제한|매수금지|자본잠식|딱지|데이터 부족|3년적자|스코어 미달|과거 주도주 이력 미달', tajeom_raw): continue

        v3_score = v3_map.get(code)
        # 🔧 [수정] "SEED" 안에 성격이 다른 두 전략이 섞여 있음 — ①바닥 확인 후 모아가기(장기 추세추종형)는
        #    실적 뒷받침이 핵심이지만, ②과매도 역배팅(단기 반등/역배팅형)은 기술적 반등이 목적이라 실적과 무관하게
        #    기회가 될 수 있음. 그래서 V3 필터는 ①에만 걸고, ②는 실적 점수와 무관하게 통과시킴.
        is_structural_seed = ("바닥 · 분할매수" in tajeom_raw) or ("코어픽" in tajeom_raw)
        if is_structural_seed and v3_score is not None and v3_score < 20:
            continue
        
        tajeom_clean = tajeom_raw.split('⚠️')[0].strip()
        tajeom_clean = tajeom_clean.split('🎯')[0].strip()

        # ⑤ 추세 위상(trend phase)을 AI에 명시적으로 전달 (omakase 칼만 시크릿 텍스트에서 추출)
        if any(k in tajeom_raw for k in ["📉", "하락 전환", "3파 익절", "고점 리스크", "반등 미확인", "하락장"]):
            trend_phase_txt = "하락/고점주의"
        elif any(k in tajeom_raw for k in ["가속", "2파"]):
            trend_phase_txt = "상승가속"
        elif any(k in tajeom_raw for k in ["전환", "1파"]):
            trend_phase_txt = "상승전환초기"
        elif "추세 유지" in tajeom_raw:
            trend_phase_txt = "상승유지"
        else:
            trend_phase_txt = "중립"

        v3_info_txt = f" | 실적점수(V3):{v3_score}점" if v3_score is not None else ""
        rs_info_txt = f" | RS등급(상대강도):{rs_grade}" if rs_grade is not None else ""
        info = (
            f"종목:{name}({code}) | 현재가:{curr_p}원({chg}) | 차트점수(V1):{v1_score}점 | 수급점수(V2):{v2_score}점{v3_info_txt}{rs_info_txt} | "
            f"타점:{tajeom_clean} | 추세:{trend_phase_txt} | 수급강도:{prog} | 유형:{seed_tag} | 테마:{theme_name}"
        )
        # 🔧 [수정] 현재가가 빈 문자열이거나 이상한 값인 종목이 하나라도 섞이면 전체 리포트 생성이
        #    죽어버리던 문제 — 안전장치 없이 바로 int() 변환하다 터졌음. 이제 그 한 종목만 조용히 건너뜀.
        try:
            curr_p_int = int(curr_p.replace(',', '').replace('원', ''))
            if curr_p_int <= 0: continue
        except (ValueError, AttributeError):
            continue

        cands_list.append({
            'name': name, 'code': code, 'score': combo_score, 'v1_score': v1_score, 'v2_score': v2_score, 'v3_score': v3_score,
            'rs_grade': rs_grade, 'info': info, 'curr_p': curr_p_int, 'type': seed_tag, 'theme_name': theme_name
        })
 
    high_score_cands = [c for c in cands_list if c['score'] >= 30]
    if len(high_score_cands) < 15:
        cands_list.sort(key=lambda x: x['score'], reverse=True)
        pre_pool = cands_list[:150]  # 🔧 [수정] 100→150으로 확장 — 프롬프트가 "상위 150개"라 주장하면서 실제론 100개만 넘기던 불일치 해소
    else:
        high_score_cands.sort(key=lambda x: x['score'], reverse=True)
        pre_pool = high_score_cands[:150]
 
    print(f"🧬 후보군 {len(pre_pool)}개 종목의 역사적 수급 DNA 검증 돌입...")
    validated_pool = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        future_to_dna = {executor.submit(validate_stock_historical_dna, c, raw_theme_daily_map): c for c in pre_pool}
        for future in concurrent.futures.as_completed(future_to_dna):
            cand, is_qualified = future.result()
            if is_qualified: validated_pool.append(cand)
            else: print(f"❌ [{cand['name']}] 역대 최고거래대금 기준 미달로 최종 리포트 및 스캐너 풀에서 완전 배제")
 
    # ⑦ [대량 실패 가드]: 검증 인프라(네이버 등)가 전면 다운되면 전 종목이 동시 탈락 → 빈 풀이 조용히 넘어가는 것을 차단.
    # 탈락률 80% 이상이면 개별 부적격이 아니라 인프라 장애로 간주하고 점수순 폴백(우회)한다.
    fail_rate = 1 - (len(validated_pool) / max(1, len(pre_pool)))
    if len(pre_pool) >= 10 and fail_rate >= 0.8:
        print(f"🚨 [DNA 검증 인프라 대량 실패 {fail_rate:.0%}] 개별 부적격이 아닌 장애로 판단 → 검증 게이트 우회, 점수순 폴백 적용")
        validated_pool = list(pre_pool)

    validated_pool.sort(key=lambda x: x['score'], reverse=True)
    pool_150 = validated_pool[:150]
    pool_str = "\n".join([c['info'] for c in pool_150])

    def get_recent_performance_summary(doc):
        """🆕 [AI Memory] 모델을 재학습시키는 게 아니라, 최근 리포트 채널(단기/중기)의 실제 성과를
           매번 프롬프트에 참고자료로 띄워주는 가벼운 방식. 표본이 적으면 그 사실도 같이 명시해서
           Gemini가 노이즈를 신호로 오인하지 않도록 함(자동 가중치 조정은 아직 표본이 부족해 보류)."""
        try:
            bt_data = doc.worksheet("백테스트_로그").get_all_values()
            if len(bt_data) < 2:
                return ""
            rows = bt_data[1:]

            def summarize_channel(channel_name):
                matched = [r for r in rows if len(r) > 18 and str(r[2]).strip() == channel_name]
                matched = matched[-20:] if len(matched) > 20 else matched  # 최근 20건만 — 오래된 표본 과의존 방지
                returns = []
                for r in matched:
                    for idx in (18, 17, 16):  # T+5 우선, 없으면 T+3, T+1 순으로 대체
                        val = str(r[idx]).strip() if len(r) > idx else ""
                        if val:
                            try:
                                returns.append(float(val.replace('%', '')))
                            except Exception:
                                pass
                            break
                if not returns:
                    return None
                n = len(returns)
                return {"n": n, "avg": sum(returns) / n, "win_rate": sum(1 for x in returns if x > 0) / n * 100}

            lines = []
            for ch, label in [("리포트TOP2_단기", "단기"), ("리포트TOP2_중기", "중기")]:
                stat = summarize_channel(ch)
                if stat:
                    note = "" if stat["n"] >= 15 else " (표본 적어 참고만)"
                    lines.append(f"- {label} 픽 최근 {stat['n']}건: 평균수익률 {stat['avg']:+.1f}%, 승률 {stat['win_rate']:.0f}%{note}")
            return "\n".join(lines)
        except Exception as e:
            print(f"⚠️ [AI Memory 성과 요약 실패, 참고자료 없이 진행] {e}")
            return ""

    def get_dominant_sector_summary(doc):
        """🆕 [국면 판별 확장] STAGE(변동성 기반)만으로는 '오늘이 어떤 장세인지'를 다 설명하지 못함.
           최근 며칠간 수급_Raw의 테마 순위를 보고, 특정 섹터가 계속 1~2위를 지키고 있으면
           '주도 섹터 장세'로 태깅해서 참고자료로 얹음. 새 데이터 소스 없이 기존 데이터만 사용."""
        try:
            raw = doc.worksheet("수급_Raw").get_all_values()[1:]
            if not raw:
                return ""
            by_date = {}
            for row in raw:
                if len(row) < 3: continue
                date_str, rank_str, theme = str(row[0]).strip(), str(row[1]).strip(), str(row[2]).strip()
                if not date_str or not theme: continue
                try:
                    rank = int(rank_str)
                except Exception:
                    continue
                if rank in (1, 2):
                    by_date.setdefault(date_str, {})[rank] = theme

            recent_dates = sorted(by_date.keys())[-5:]  # 최근 5거래일
            if len(recent_dates) < 3:
                return ""  # 데이터가 며칠치 안 쌓였으면 판단 보류

            theme_count = {}
            for d in recent_dates:
                for rank in (1, 2):
                    t = by_date[d].get(rank)
                    if t:
                        theme_count[t] = theme_count.get(t, 0) + 1

            if not theme_count:
                return ""
            top_theme, top_count = max(theme_count.items(), key=lambda x: x[1])
            if top_count >= 3:  # 최근 5일 중 3일 이상 1~2위를 지켰으면 "주도 섹터"로 판단
                return f"- 최근 {len(recent_dates)}거래일 중 {top_count}일 동안 '{top_theme}' 테마가 1~2위를 유지 → 지금은 이 섹터가 시장을 주도하는 장세일 가능성이 높습니다. 이 테마와 무관한 종목을 추천할 땐 조금 더 신중하게 판단하십시오."
            return ""
        except Exception as e:
            print(f"⚠️ [국면 판별 확장 실패, 참고자료 없이 진행] {e}")
            return ""

    recent_perf_str = get_recent_performance_summary(doc)
    recent_perf_block = f"""
    [📈 최근 성과 참고자료 — AI Memory]
    아래는 최근 실제로 선정됐던 픽들의 사후 성과입니다. 이건 규칙이 아니라 참고 정보이니,
    표본이 적다고 표시된 경우 특정 테마·유형을 과도하게 회피하거나 맹신하지 말고 어디까지나 보조 판단 자료로만 쓰십시오.
    {recent_perf_str}
    """ if recent_perf_str else ""

    dominant_sector_str = get_dominant_sector_summary(doc)
    dominant_sector_block = f"""
    [🧭 시장 국면 참고자료 — 주도 섹터 판별]
    {dominant_sector_str}
    """ if dominant_sector_str else ""

    pick_prompt = f"""
    당신은 세계 최고의 애널리스트 집단이 검증하는 HYEOKS 퀀트 분석가입니다.
     아래는 HYEOKS 퀀트 점수와 역사적 주도주 DNA 검증이 끝난 최상위 {len(pool_150)}개 종목 리스트입니다.
    현재 시장 리스크 매트릭스는 [{stage_text}] 단계입니다.
    {recent_perf_block}
    {dominant_sector_block}
    [🚨 국면별 종목 선정 제약 지침]
    - 만약 현재 시장이 STAGE 2(주의 장세)라면 단기 슈팅 종목을 극도로 보수적으로 판단하고, 애매하면 단기 픽 자리에 "000000"을 출력하십시오.
    - 만약 현재 시장이 STAGE 3(패닉 장세)라면, 자산을 사수하기 위해 단기(short_term_code) 및 중기(swing_code)를 불문하고 억지로 추천을 내지 말고 둘 다 무조건 "000000"을 반환해야 합니다.
 
    [종목 선정 기준]
    1. 단기 슈팅 공략주 (short_term_code): 유형:NORMAL 종목 중 파괴력 있는 종목 1개 선별. (없으면 "000000")
    2. 중장기 모아가기주 (swing_code): 유형:SEED 종목 중 과열 배지가 없는 바닥 확인형 1개 선별.
       단, SEED 안에는 성격이 다른 두 유형이 섞여 있으니 구분해서 판단하십시오:
       - 타점이 '바닥 · 분할매수' 또는 '코어픽' 배지인 종목(장기 추세추종형): 실적점수(V3)를 적극 반영하십시오.
         실적점수가 높을수록(매출·영업이익이 여러 분기 꾸준히 개선 중일수록) 우선순위를 높게 두고,
         실적점수가 낮게 명시된 종목은 기술적으로 좋아 보여도 피하십시오.
       - 타점이 '과매도 · 역배팅'인 종목(단기 반등/역배팅형): 이건 기술적 반등을 노리는 전략이라
         실적점수가 낮거나 표시가 없어도 배제하지 마십시오 — 실적과 무관하게 기존 기준(반등 확인 여부 등)대로 판단하십시오.
       실적점수 표시 자체가 없는 종목은 아직 데이터가 없는 것이니 기존 기준대로 판단하십시오.
       (없으면 "000000")
    3. 🚨 [추세 절대 거부권 - 최우선 규칙]: '추세:하락/고점주의'이거나 타점에 '📉 / 3파 익절 / 하락 전환 / 반등 미확인'이 포함된 종목은 점수가 아무리 높아도 절대 선정하지 마십시오. 추세 반전이 확인되지 않은 '떨어지는 칼'은 반드시 "000000"으로 회피하십시오.
    4. 🆕 [RS등급 활용 — 참고용 우선순위, 절대 기준 아님]: RS등급(상대강도, 1~99 백분위)은 전종목 대비 상대적 강도를 나타냅니다.
       다른 조건(타점·추세·V1·V2)이 비슷한 후보가 여럿이라면 RS등급이 높은 쪽을 우선하십시오.
       단, RS등급이 낮거나 표시가 없다는 이유만으로 다른 조건이 확실히 좋은 종목을 배제하지는 마십시오 — 어디까지나 동점자 처리용 참고 지표입니다.
 
    [🆕 판단 절차 — 반드시 이 순서를 지키십시오]
    ① 먼저 단기·중기 각각 후보를 2~3개씩 내부적으로 추려내고, 위 1~4번 기준으로 각 후보의 장단점을 스스로 비교하십시오.
    ② 그중 최종 1개씩을 선택하되, 선택 직전에 "이 종목이 3번 추세 거부권에 걸리지는 않는가?", "선정 기준에서 요구하는 조건을 실제로 만족하는가?"를 항목별로 다시 한번 점검하십시오.
    ③ 점검 결과 어느 하나라도 애매하면 그 후보는 버리고 차순위 후보로 넘어가거나, 마땅한 후보가 없다면 "000000"을 반환하십시오.
    ④ 🚨 중요: "000000"을 반환하는 것은 실패가 아니라 성공적인 판단입니다. 확신이 서지 않는 종목을 억지로 채워 넣는 것보다,
       조건을 확실히 만족하는 종목이 없다는 걸 정직하게 인정하는 쪽이 훨씬 낫습니다. 애매하면 채우지 말고 비우십시오.
    ⑤ 최종 응답에는 reasoning_short/reasoning_mid에 "왜 이 종목을 골랐는지"를 위 1~4번 기준 중 어떤 근거를 썼는지 구체적으로 1~2문장으로 남기십시오
       (예: "V1 74점·RS등급 88로 상위권이며 타점이 코어픽이고 추세가 상승유지라 선정" 처럼 근거를 구체적으로 명시).
       "000000"을 반환하는 경우에도 왜 마땅한 후보가 없었는지 1문장으로 남기십시오.
    ⑥ 종목코드는 반드시 아래 리스트에 있는 6자리 코드를 정확히 그대로(오타 없이) 복사해서 사용하십시오. 리스트에 없는 코드를 만들어내지 마십시오.
 
    [상위 {len(pool_150)}개 종목 리스트]
    {pool_str}
    
    반드시 아래 JSON 형식으로만 응답하세요. 다른 설명은 일절 배제하십시오.
    {{
        "short_term_code": "종목코드6자리 또는 000000",
        "reasoning_short": "단기 픽 선정/보류 근거 1~2문장",
        "swing_code": "종목코드6자리 또는 000000",
        "reasoning_mid": "중기 픽 선정/보류 근거 1~2문장"
    }}
    """
    
    if market_stage == 3:
        print("🚨 [CRITICAL ALERT] STAGE 3 대피 패닉 장세가 발동되었습니다. 억지 종목 매수를 차단하기 위해 AI 픽을 전면 전면 취소(Zero Pick)합니다.")
        picks_json = {"short_term_code": "000000", "swing_code": "000000", "reasoning_short": "STAGE 3 패닉 장세로 인한 자동 제로픽", "reasoning_mid": "STAGE 3 패닉 장세로 인한 자동 제로픽"}
    else:
        result_text = safe_generate_content(pick_prompt).text
        cleaned_text = result_text.replace('```json', '').replace('```', '').strip()
        picks_json = json.loads(cleaned_text)
    
    code_short = picks_json.get('short_term_code', '')
    code_mid = picks_json.get('swing_code', '')
    # 🆕 [근거 로그] Gemini가 왜 이 종목을(또는 왜 000000을) 골랐는지 콘솔에 남겨서, 나중에 픽 품질을
    #    복기할 때 "이유를 알 수 없는 블랙박스" 상태가 아니라 근거를 추적할 수 있게 함.
    print(f"🧠 [단기 픽 근거] {picks_json.get('reasoning_short', '(근거 없음)')}")
    print(f"🧠 [중기 픽 근거] {picks_json.get('reasoning_mid', '(근거 없음)')}")
    
    best_short = next((c for c in pool_150 if c['code'] == code_short), None) if code_short != "000000" else None
    best_mid = next((c for c in pool_150 if c['code'] == code_mid), None) if code_mid != "000000" else None
 
    print(f"🔥 최종 발굴 결과 -> 단기 리포트 대상: {best_short['name'] if best_short else '없음(000000)'} / 중기 스윙 대상: {best_mid['name'] if best_mid else '없음(000000)'}\n")
 
    # ==========================================
    # 5. 시황 및 딥리딩 PDF 리포트 본문 생성
    # ==========================================
    print("▶ [2단계] 딥리딩 분석 및 PDF 리포트 본문 생성...")
    today_korean = datetime.datetime.now(KST).strftime('%Y년 %m월 %d일')
    status_txt = "코스닥 20일선 이탈 (보수적 운영 요망)" if is_warning_market else "코스피/코스닥 지지 (공격적 운영 가능)" 
    if market_stage == 3: status_txt = "🚨 역대급 패닉셀 투매 장세 돌입 (전원 사격 중지 및 현금 100% 관망 요망)"
 
    if market_stage == 3:
        macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 최고 심의위원이자 수석 애널리스트입니다.
        현재 국내 증시는 역대급 패닉 폭락 장세인 [{korean_market_status}] 상태입니다.
        자산을 사수하기 위한 강력한 경고 메시지와 전원 사격 중지(현금 100% 관망)의 당위성을 거시 매크로 분석과 함께 1페이지 분량으로 묵직하게 작성하십시오. 정중한 하십시오체를 사용하십시오. 작성일: {today_korean}"""
    else:
        # 🔧 [수정] 예전엔 "1페이지 분량으로"라는 느슨한 지시만 있어서 실제로는 자주 2페이지를 넘겨
        #    추천주 리포트가 3페이지부터 시작하는 문제가 있었음. 열린 산문 대신 불릿·문장수 상한을 강제하는
        #    구조로 바꿔서, 진짜로 "그날의 엑기스"만 한눈에 들어오도록 압축.
        macro_prompt = f"""귀하는 HYEOKS 리서치 센터의 수석 퀀트 애널리스트입니다. 아래 데이터를 바탕으로 '오늘의 시황 브리핑'을 작성하십시오. 작성일: {today_korean}

[입력 데이터]
· 해외: 나스닥 {nasdaq} / S&P500 {sp500} / WTI 유가 {oil}
· 환율: 원/달러 {exchange}
· 국내 증시 판정: {status_txt}
· 뉴스 언급 빈도(참고): {news_keywords}
· 실제 자금이 몰린 테마(거래대금 상위): {money_themes if money_themes else "집계 없음"}

[🚨 분량 규칙]
전체 A4 1.5페이지를 넘지 마십시오. 각 섹션의 문장 수 하한과 상한을 모두 지키십시오.
분량을 채우려 같은 말을 바꿔 쓰거나, 원론적 교과서 설명("분산투자가 중요합니다" 등)을 넣지 마십시오.
문장 하나하나가 오늘 데이터에 근거한 새로운 정보를 담아야 합니다.

[🚨 데이터 해석 규칙 — 중요]
1. '뉴스 언급 빈도'는 기사에 몇 번 나왔는지일 뿐 검증된 주도 테마가 아닙니다. 반면 '실제 자금이 몰린 테마'는
   진짜 거래대금입니다. 둘이 어긋나면(예: 뉴스는 A인데 돈은 B로) 그 괴리 자체를 반드시 짚어 주십시오.
   이 괴리가 오늘 브리핑에서 가장 값어치 있는 통찰입니다.
2. 의미가 불명확한 단일 단어(색깔·형용사·추상어)를 근거로 실재하지 않는 테마나 종목군을 창작·명명하지
   마십시오. 출처가 불분명한 키워드는 해석을 보류하거나 아예 언급하지 않습니다.
3. 수치를 인용할 때는 방향(상승/하락)과 그 파급 경로(무엇에 어떤 영향)를 함께 쓰십시오. 숫자 나열 금지.

[출력 양식 — 아래 HTML/마크다운 구조를 그대로 따르십시오]

<div class="summary-box">
<b>[오늘의 3줄 요약]</b>
<ul>
<li>(해외 매크로 핵심 한 줄 — 30자 이내)</li>
<li>(국내 증시·수급 핵심 한 줄 — 30자 이내)</li>
<li>(오늘의 투자 시사점 핵심 한 줄 — 30자 이내)</li>
</ul>
</div>

## 1. 해외 매크로
(나스닥과 S&P500의 흐름이 위험선호 심리에 주는 신호, 유가가 시사하는 인플레·원자재 방향, 원/달러 환율이
외국인 수급에 미치는 영향을 각각 짚어 4~5문장. 지표 간 상충 신호가 있으면 그것도 명시.)

## 2. 국내 증시 및 수급
(국내 증시 판정의 의미, 지수의 기술적 위치, 외국인·기관 수급의 방향성을 4~5문장. 어제와 달라진 점이
있다면 그 변화를 중심으로 서술.)

## 3. 자금 흐름과 주도 테마
(거래대금 상위 테마에서 오늘 돈이 어디로 갔는지 3~4문장. 뉴스 빈도와 실제 자금의 괴리가 있으면 반드시 지적.
쏠림이 심한지 분산돼 있는지도 판단해 주십시오.)

## 4. 오늘의 전략 시사점
(위 1~3을 종합한 실전 대응 방향을 2~3문장. 공격/중립/방어 중 어느 스탠스인지 명확히.)

## 5. 리스크 체크
(오늘 판단이 틀릴 수 있는 요인, 또는 경계해야 할 신호를 1~2문장. 낙관 일변도로 끝내지 마십시오.)"""
    
    market_summary = safe_generate_content(macro_prompt).text
 
    report_short, pick_short = generate_deep_report("short", best_short, is_warning_market, KIS_TOKEN, client)
    if best_short: time.sleep(15)
    report_mid, pick_mid = generate_deep_report("mid", best_mid, is_warning_market, KIS_TOKEN, client)
 
    # ==========================================
    # 👑 [양방향 연동 완벽 개편 구역]: 주가데이터_보조 J열 고착 해결 레이어
    # ==========================================
    print("\n▶ [3단계] 최신 DB_스캐너 동기화 및 리포트 종목/나머지 종목 갱신...")
    latest_db_data = db_sheet.get_all_values()
    helper_sheet = doc.worksheet("주가데이터_보조")
 
    def extract_summary(report_text):
        if not report_text: return ""
        briefing_summary = "✅ [리포트 작성 완료] "
        summary_match = re.search(r'<div class="summary-box">(.*?)</div>', report_text, re.DOTALL)
        if summary_match:
            clean_text = re.sub(r'<[^>]+>', '', summary_match.group(1)).replace("[HYEOKS 핵심 모멘텀 요약]", "").strip()
            
            # 👑 [가독성 슬라이싱 혁신]: 주절주절 길어지는 것을 방지하기 위해 마침표(.) 기준 첫 문장만 정밀 분리
            sentences = [s.strip() for s in re.split(r'(?<=[.])', clean_text) if s.strip()]
            if sentences:
                clean_text = sentences[0]
            
            # 👑 [안전 하드 가드레일]: 만약 첫 문장 자체가 65자를 초과할 경우 지저분한 절단을 방지하고 강제 클리핑
            if len(clean_text) > 65:
                clean_text = clean_text[:62] + "..."
            briefing_summary += clean_text
        else: briefing_summary += "텔레그램에서 상세 분석 리포트를 확인하십시오."
        return briefing_summary
 
    short_summary = extract_summary(report_short) if best_short else ""
    mid_summary = extract_summary(report_mid) if best_mid else ""
 
    for i, r_legacy in enumerate(latest_db_data[1:], start=2):
        if len(r_legacy) > 9:
            code = str(r_legacy[2]).replace("'", "").strip().zfill(6)
            stock_name = r_legacy[0] if len(r_legacy) > 0 else "알수없음"
            
            current_db_snapshot = db_sheet.get_all_values()
            real_row_idx = -1
            for idx, r_row in enumerate(current_db_snapshot, start=1):
                if len(r_row) > 2 and str(r_row[2]).replace("'", "").strip().zfill(6) == code:
                    real_row_idx = idx; break
            
            if real_row_idx == -1: continue
 
            if best_short and code == best_short['code']:
                db_sheet.update_cell(real_row_idx, 10, short_summary)
                if pick_short:
                    db_sheet.update_cell(real_row_idx, 15, f"{pick_short['target']:,}원")
                    db_sheet.update_cell(real_row_idx, 16, f"{pick_short['stop']:,}원")
                
                helper_snapshot = helper_sheet.get_all_values()
                for h_idx, h_row in enumerate(helper_snapshot, start=1):
                    if len(h_row) > 1 and str(h_row[1]).replace("'", "").strip().zfill(6) == code:
                        helper_sheet.update_cell(h_idx, 10, short_summary)
                        if pick_short:
                            helper_sheet.update_cell(h_idx, 24, f"{pick_short['target']:,}원")
                            helper_sheet.update_cell(h_idx, 25, f"{pick_short['stop']:,}원")
                        break
                time.sleep(3.5); continue
            
            if best_mid and code == best_mid['code']:
                db_sheet.update_cell(real_row_idx, 10, mid_summary)
                if pick_mid:
                    db_sheet.update_cell(real_row_idx, 15, f"{pick_mid['target']:,}원")
                    db_sheet.update_cell(real_row_idx, 16, f"{pick_mid['stop']:,}원")
                
                helper_snapshot = helper_sheet.get_all_values()
                for h_idx, h_row in enumerate(helper_snapshot, start=1):
                    if len(h_row) > 1 and str(h_row[1]).replace("'", "").strip().zfill(6) == code:
                        helper_sheet.update_cell(h_idx, 10, mid_summary)
                        if pick_mid:
                            helper_sheet.update_cell(h_idx, 24, f"{pick_mid['target']:,}원")
                            helper_sheet.update_cell(h_idx, 25, f"{pick_mid['stop']:,}원")
                        break
                time.sleep(3.5); continue
            
            if not any(key in str(current_db_snapshot[real_row_idx-1][9]) for key in ["리포트 발송 완료", "리포트 작성 완료"]):
                curr_p = r_legacy[3] if len(r_legacy) > 3 else ''
                tajeom_badge = r_legacy[8] if len(r_legacy) > 8 else ''
                sugeup = r_legacy[11] if len(r_legacy) > 11 else ''
                high_52 = r_legacy[12] if len(r_legacy) > 12 else ''
                theme = r_legacy[5] if len(r_legacy) > 5 else ''
                target_sys = r_legacy[14] if len(r_legacy) > 14 else ''
                stop_sys = r_legacy[15] if len(r_legacy) > 15 else ''
                
                prompt = get_ai_prompt_for_briefing(stock_name, curr_p, tajeom_badge, sugeup, high_52, theme, target_sys, stop_sys, market_stage, stage_text)
                
                try:
                    res_text = safe_generate_content(prompt, is_fast=True).text
                    parsed_data = parse_ai_json(res_text)
                    briefing_text = parsed_data.get("briefing", "브리핑 생성 에러")
                    if not briefing_text.startswith("✅") and not briefing_text.startswith("⚠️"): 
                        briefing_text = f"✅ [간단 브리핑] {briefing_text}"
                    
                    raw_target = str(parsed_data.get('target_price', '0')).replace(',', '').replace('원', '')
                    raw_stop = str(parsed_data.get('stop_loss', '0')).replace(',', '').replace('원', '')
                    target_val = f"{int(raw_target):,}원" if raw_target.isdigit() and int(raw_target) > 0 else "관망"
                    stop_val = f"{int(raw_stop):,}원" if raw_stop.isdigit() and int(raw_stop) > 0 else "관망"
                    
                    db_sheet.update_cell(real_row_idx, 10, briefing_text)
                    db_sheet.update_cell(real_row_idx, 15, target_val)
                    db_sheet.update_cell(real_row_idx, 16, stop_val)
                    
                    helper_snapshot = helper_sheet.get_all_values()
                    for h_idx, h_row in enumerate(helper_snapshot, start=1):
                        if len(h_row) > 1 and str(h_row[1]).replace("'", "").strip().zfill(6) == code:
                            helper_sheet.update_cell(h_idx, 10, briefing_text)
                            helper_sheet.update_cell(h_idx, 24, target_val)
                            helper_sheet.update_cell(h_idx, 25, stop_val)
                            break
                    time.sleep(3.5)
                except Exception as e:
                    print(f"[{stock_name}] 브리핑/가격 에러 (건너뜀): {e}")
 
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
            except Exception as e:
                print(f"⚠️ [보유종목 현재가 조회 실패 {code} — 매입가로 대체] {e}")
                curr_p = buy_p
            
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
    
    if market_stage == 3:
        html += "<div class='alert-box'>🚨 [HYEOKS EMERGENCY SYSTEM ALERT] 시장 극단적 패닉 국면 판독으로 인해 전체 투자 알고리즘의 '전원 사격 중지(Ceasefire)' 프로토콜이 발동되었습니다. 현 포지션의 무리한 물타기 및 신규 진입을 전면 금지하며 100% 현금 보존 관망 스탠스를 엄격히 권고합니다.</div>"
        
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
 
    # 🛡️ [예외 재실행 안전장치]: 오늘 이미 발행된 경우, 강제 재발송(FORCE_RESEND=true)이 아니면 중복 텔레그램 발송을 막는다.
    today_pub_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
    already_published_today = (last_published_date == today_pub_str)
    if already_published_today and not FORCE_RESEND:
        print(f"⏭ [중복 발송 차단] 오늘({today_pub_str}) 이미 발행됨. 재발송하려면 워크플로 수동 실행 시 force_resend 입력을 true로 설정하십시오.")
    elif TELEGRAM_BOT_TOKEN:
        if already_published_today and FORCE_RESEND:
            print("⚠️ [강제 재발송 모드] 오늘 이미 발행됐지만 FORCE_RESEND=true로 재실행합니다.")
        print("▶ 텔레그램 발송 진행 중...")
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", 
                      files={'document': open(pdf_file, 'rb')}, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': "[HYEOKS] AI 심층 리서치 비상 보고서" if market_stage==3 else "[HYEOKS] AI 심층 리서치 보고서"})
        print("✅ 텔레그램 발송 완료!")
        try:
            setting_sheet = doc.worksheet("⚙️설정")
            setting_rows = setting_sheet.get_all_values()
            flag_row_idx = next((i for i, r in enumerate(setting_rows) if len(r) >= 1 and r[0] == "마지막_리포트_발행일"), None)
            if flag_row_idx is not None:
                setting_sheet.update(range_name=f"B{flag_row_idx + 1}", values=[[today_pub_str]])
            else:
                setting_sheet.append_row(["마지막_리포트_발행일", today_pub_str])
        except Exception as e:
            print(f"⚠️ 발행일 플래그 기록 실패(다음 실행에 영향 없음): {e}")
 
    # ==========================================
    # 👑 [HYEOKS 백테스트 V5] 리포팅 채널 연동
    # ==========================================
    try:
        print("\n▶ [백테스트 V6 Step1] 리포트 채널 진입 append (신 26열 스키마)...")
        bt_sheet = doc.worksheet("백테스트_로그")
        bt_data = bt_sheet.get_all_values()
        today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')

        # 신 스키마(26열, trade_id 헤더)일 때만 append. 시트 생성·마이그레이션은 omakase가 단독 소유.
        is_new_schema = bool(bt_data) and len(bt_data[0]) >= 26 and str(bt_data[0][0]).strip() == "trade_id"
        if not is_new_schema:
            print("ℹ️ 백테스트_로그가 아직 신 스키마 아님(omakase 마이그레이션 대기) → 리포트 채널 보류.")
        else:
            existing_ids = set(str(row[0]).strip() for row in bt_data[1:] if row and str(row[0]).strip())

            def _idx_close(name):
                try:
                    sym = "KOSDAQ" if str(name).upper() == "KOSDAQ" else "KOSPI"
                    root = ET.fromstring(requests.get(f"https://fchart.stock.naver.com/sise.nhn?symbol={sym}&timeframe=day&count=3&requestType=0", verify=False, timeout=4).text)
                    its = root.findall(".//item")
                    if its: return float(its[-1].get("data").split("|")[4])
                except Exception: pass
                return 0.0

            def _market(code):
                try:
                    j = requests.get(f"https://m.stock.naver.com/api/stock/{code}/basic", headers={'User-Agent': 'Mozilla/5.0'}, verify=False, timeout=3).json()
                    nm = str(j.get("stockExchangeName", "")).upper()
                    return nm if nm in ("KOSPI", "KOSDAQ") else "KOSPI"
                except Exception:
                    return "KOSPI"

            idx_cache = {"KOSPI": _idx_close("KOSPI"), "KOSDAQ": _idx_close("KOSDAQ")}
            # 🔧 [수정] "집중도" 열이 계속 빈 값이던 문제 — 위에서 이미 읽어둔 _kw_rows(뉴스_키워드)로 1위:2위 언급횟수 기록
            try:
                _kw_counts = [int(re.sub(r'[^0-9]', '', str(kr[3]))) for kr in _kw_rows if len(kr) >= 4 and re.sub(r'[^0-9]', '', str(kr[3]))]
                concentration_str = f"'{_kw_counts[0]}:{_kw_counts[1]}" if len(_kw_counts) >= 2 else ""  # 🔧 앞에 ' 붙여 텍스트 강제(시:분으로 오인되던 버그 수정)
            except Exception:
                concentration_str = ""
            # 🔧 [수정] 단기(best_short)·중기(best_mid)는 완전히 다른 투자 논리라 채널명 자체를 분리.
            #    기존엔 둘 다 "리포트TOP2"로 합쳐 기록돼서, 평가 시 서로 다른 성과가 섞여 뭉개지는 문제가 있었음.
            report_picks = [(best_short, "리포트TOP2_단기", pick_short), (best_mid, "리포트TOP2_중기", pick_mid)]
            new_rows = []
            for cand, channel_name, pick_info in report_picks:
                if not cand: continue
                s_code = str(cand['code']).replace("'", "").strip().zfill(6)
                if s_code == "000000": continue
                tid = f"{today_str}_{channel_name}_{s_code}"
                if tid in existing_ids: continue
                bench = _market(s_code)
                # 🆕 목표가·손절가 — 딥리포트가 이미 계산해둔 pick_short/pick_mid의 target/stop을 그대로 사용
                _rt = pick_info.get('target', 0) if pick_info else 0
                _rs = pick_info.get('stop', 0) if pick_info else 0
                new_rows.append([
                    tid, today_str, channel_name, cand['name'], f"'{s_code}", cand.get('theme_name', ''), "", market_stage, concentration_str,
                    cand.get('v1_score', ''), cand.get('v2_score', ''), "", "", bench, cand.get('curr_p', ''), idx_cache.get(bench, 0.0)
                ] + [""] * 16 + [_rt if _rt else "", _rs if _rs else "", "", ""])  # 🔧 [수정] 32열→36열(목표가·손절가·터치 2종) 패딩 확장
                existing_ids.add(tid)
            if new_rows:
                # 🛡️ [수정] omakase.py가 10분마다 같은 시트를 건드리는데, hyeoks_analyst.py(15시)와 락 그룹이
                #    달라서 서로 안 기다려줌 → 거의 매일 15시 정각에 동시 쓰기가 겹칠 수 있음. append 자체는
                #    "성공"으로 찍혀도, 그 직후 다른 스크립트의 쓰기와 충돌해 조용히 덮어써지는 사고(2026-07-16
                #    이후 리포트TOP2_단기/중기 통째로 누락)가 있었음. → 쓰고 나서 실제로 남아있는지 재확인,
                #    누락됐으면 그 종목만 다시 씀(최대 3회).
                pending = list(new_rows)
                verified = False
                for attempt in range(3):
                    try:
                        bt_sheet.append_rows(pending, value_input_option="USER_ENTERED")
                    except Exception as e:
                        print(f"⚠️ [리포트 채널 append 시도 {attempt + 1}/3 실패] {e}")
                        time.sleep(3)
                        continue
                    time.sleep(1.5)  # 구글시트 반영 시차 감안
                    check_ids = set(str(row[0]).strip() for row in bt_sheet.get_all_values()[1:] if row and row[0])
                    pending = [r for r in pending if r[0] not in check_ids]
                    if not pending:
                        verified = True
                        break
                    print(f"⚠️ [리포트 채널 기록 확인 실패, 재시도 {attempt + 1}/3] 누락: {[r[0] for r in pending]}")
                    time.sleep(3)
                if verified:
                    print(f"✅ [백테스트 V6 Step1] 리포트 채널(단기/중기 분리) {len(new_rows)}건 append 및 확인 완료.")
                    # 🆕 [정렬] append_rows는 항상 시트 맨 끝에 붙는다. omakase는 자기 진입행을 넣은 뒤
                    #    정렬하지만, analyst는 그보다 늦게(리포트 생성 후) 쓰기 때문에 리포트 행만 하단에
                    #    고립돼 있었다(다음 날 omakase 진입 적재 전까지 계속). → 여기서도 즉시 재정렬.
                    #    (채널 배경색·수익률 색은 수식 기반 조건부서식이라 행이 움직여도 자동으로 따라옴.
                    #     날짜 구분선만 omakase 다음 회차에서 다시 계산됨.)
                    # 🔒 [레이스 차단] 전체 재작성(read→sort→update A1) 대신 서버측 sortRange 사용.
                    #    analyst(hyeoks-report-lock)와 omakase(hyeoks-sheet-lock)는 동시성 그룹이 달라
                    #    동시에 돌 수 있는데, 양쪽 다 전체 재작성을 하면 늦게 쓰는 쪽이 상대 변경분을
                    #    통째로 날린다. sortRange는 원자적이라 그런 소실이 없다.
                    #    (채널 커스텀 우선순위는 omakase 다음 회차가 적용 — 데이터 안전이 우선)
                    try:
                        _n = len(bt_sheet.get_all_values())
                        if _n >= 3:
                            doc.batch_update({"requests": [{"sortRange": {
                                "range": {"sheetId": bt_sheet.id, "startRowIndex": 1, "endRowIndex": _n,
                                          "startColumnIndex": 0, "endColumnIndex": 36},
                                "sortSpecs": [
                                    {"dimensionIndex": 1, "sortOrder": "DESCENDING"},   # B 진입일(최신 위)
                                    {"dimensionIndex": 2, "sortOrder": "ASCENDING"},    # C 채널(날짜 내 그룹)
                                ]}}]})
                            print(f"🔃 [백테스트_로그] 진입일 최신순 재정렬 완료 ({_n - 1}행, 원자적 sortRange)")

                            # 🔧 [구분선 갱신] 날짜 구분선(테두리)은 '행 위치' 기반이라 정렬로 행이 밀리면
                            #    그대로 어긋난다. 예전엔 omakase가 다음 진입행을 넣을 때(= 다음 거래일 15시)
                            #    에야 다시 그려져서, 리포트 종목이 들어온 뒤 하루 내내 선이 밀려 보였다.
                            #    → 정렬한 쪽이 곧바로 다시 그린다. (omakase의 로직과 동일 규칙)
                            _rows2 = bt_sheet.get_all_values()[1:]
                            _brd = [{"updateBorders": {
                                "range": {"sheetId": bt_sheet.id, "startRowIndex": 1,
                                          "endRowIndex": 1 + len(_rows2), "startColumnIndex": 0, "endColumnIndex": 36},
                                "top": {"style": "NONE"}, "bottom": {"style": "NONE"},
                                "left": {"style": "NONE"}, "right": {"style": "NONE"},
                                "innerHorizontal": {"style": "NONE"}, "innerVertical": {"style": "NONE"}}}]
                            _prev, _cnt = None, 0
                            for _i, _r in enumerate(_rows2):
                                _cur = str(_r[1]) if len(_r) > 1 else ""
                                if _prev is not None and _cur != _prev:
                                    _sr = _i + 1
                                    _brd.append({"updateBorders": {
                                        "range": {"sheetId": bt_sheet.id, "startRowIndex": _sr,
                                                  "endRowIndex": _sr + 1, "startColumnIndex": 0, "endColumnIndex": 36},
                                        "top": {"style": "SOLID_THICK",
                                                "color": {"red": 0.12, "green": 0.12, "blue": 0.12}}}})
                                    _cnt += 1
                                _prev = _cur
                            doc.batch_update({"requests": _brd})
                            print(f"📏 [백테스트_로그] 날짜 구분선 {_cnt}개 재적용")
                    except Exception as _se:
                        print(f"⚠️ [백테스트_로그 정렬/구분선 실패 — 다음 omakase 회차에서 복구됨] {_se}")
                else:
                    print(f"❌ [백테스트 V6 Step1] 3회 재시도 후에도 확인 실패 — 누락: {[r[0] for r in pending]}")
            else:
                print("⏭ [백테스트 V6 Step1] 리포트 채널 — 추가 없음.")
    except Exception as e: print(f"⚠️ [백테스트 V6 Step1] 리포트 채널 기록 에러: {e}")
        
    print(f"🎉 모든 작업이 성공적으로 완료되었습니다: {pdf_file}")
except GeminiUnreachableError as e:
    print(f"\n🚨 [제미나이 회로차단기 발동] {e}")
    print("   → 헛되이 재시도하며 시간을 낭비하지 않고 여기서 안전하게 종료합니다. 다음 예약 실행에서 자연스럽게 복구됩니다.")
    exit(1)
except Exception as e:
    print(f"\n❌ 시스템 에러: {e}"); exit(1)
