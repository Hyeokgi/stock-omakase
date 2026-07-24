# -*- coding: utf-8 -*-
"""
HYEOKS 실적(매출액/영업이익) 수집기 — 금융감독원 OpenDART 연동
──────────────────────────────────────────────────────────
1. corp_code 매핑(종목코드 ↔ DART 고유번호)을 구글시트에 캐시해두고 재사용
2. DB_중장기 + DB_스캐너에 있는 종목들의 최근 분기 매출액/영업이익을 DART에서 가져옴
   (전체 스캔 풀이 아니라 이 두 시트로 시작 — API 호출량을 보수적으로 관리하기 위함.
    안정적으로 잘 돌아가는 게 확인되면 대상을 주가데이터_보조 전체로 넓힐 수 있음)
3. 보고서별 "누적치"를 분기 "단독" 수치로 환산(Q2=반기-Q1, Q3=3분기누적-반기, Q4=연간-3분기누적)
4. 전분기 대비 매출/영업이익 증감률과 "실적개선여부"를 계산해서 DB_실적 시트에 기록
   (V3 실적점수 설계 및 중기 픽 필터링의 원본 데이터로 사용 예정)

필요 패키지: pip install gspread oauth2client requests --break-system-packages
환경변수: DART_API_KEY (OpenDART에서 발급받은 인증키)
"""
import os, re, time, datetime, zipfile, io
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import xml.etree.ElementTree as ET

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
DART_API_KEY = os.environ.get("DART_API_KEY")
KST = datetime.timezone(datetime.timedelta(hours=9))
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

SESSION = requests.Session()

# 보고서 코드: 사업(연간누적) / 반기(H1누적) / 1분기(Q1단독) / 3분기(9개월누적)
REPRT_CODES = [("11013", "Q1"), ("11012", "H1"), ("11014", "9M"), ("11011", "FY")]


def get_doc():
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", SCOPE)
    gc = gspread.authorize(creds)
    return gc.open_by_url(SHEET_URL)


# ──────────────────────────────────────────────
# ① corp_code 매핑 (종목코드 → DART 고유번호), 구글시트에 캐시
# ──────────────────────────────────────────────
def load_or_build_corp_code_map(doc):
    try:
        sheet = doc.worksheet("DB_기업코드매핑")
        rows = sheet.get_all_values()[1:]
        if len(rows) > 1000:  # 정상적으로 채워진 캐시가 있으면 재사용 (매번 새로 받을 필요 없음)
            print(f"♻️ corp_code 매핑 캐시 재사용 ({len(rows)}개 종목)")
            return {r[0].strip(): r[1].strip() for r in rows if len(r) >= 2 and r[0].strip()}
    except Exception:
        sheet = doc.add_worksheet(title="DB_기업코드매핑", rows="4500", cols="3")

    print("🆕 DART corp_code 매핑이 없어 새로 받아옵니다 (최초 1회, 몇 분 걸릴 수 있음)...")
    res = SESSION.get(f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}", timeout=30)
    zf = zipfile.ZipFile(io.BytesIO(res.content))
    xml_bytes = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml_bytes)

    mapping = {}
    rows_to_write = [["종목코드", "DART고유번호", "회사명"]]
    for item in root.findall(".//list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        if stock_code:  # 상장사만 (비상장은 종목코드 칸이 공백으로 옴)
            mapping[stock_code] = corp_code
            rows_to_write.append([stock_code, corp_code, corp_name])

    sheet.clear()
    sheet.update(range_name="A1", values=rows_to_write, value_input_option="RAW")
    print(f"✅ corp_code 매핑 {len(mapping)}개 종목 캐시 완료")
    return mapping


# ──────────────────────────────────────────────
# ② 특정 종목의 최근 N년치 분기 실적 조회 + 단독 수치 환산
# ──────────────────────────────────────────────
class DartUnreachableError(Exception):
    """DART API가 이번 실행에서 아예 응답하지 않는 것으로 판단될 때 던져서 전체 실행을 조기 종료시킴."""
    pass


_consecutive_failures = [0]  # 🆕 [회로차단기] 리스트로 감싸서 여러 함수에서 공유(모듈 전역 카운터)
CIRCUIT_BREAKER_THRESHOLD = 15  # 연속 이 횟수만큼 연결 실패하면 "API 자체가 불통"으로 판단하고 전체 중단
#    (2026-07-?? 사고: DART 연결이 매 요청 10초씩 실패, 134개 종목 전체를 끝까지 시도하느라 4시간 34분 소요.
#     첫 15회 연속 실패만으로도 이미 "이번엔 안 되는 상황"이라고 판단하기 충분함 — 15×10초 ≈ 2.5분 안에 조기 종료)

_recent_results = []  # 🆕 [회로차단기 보강] 최근 요청들의 성공/실패를 순서대로 담아둠(최대 WINDOW개)
ROLLING_WINDOW = 40
ROLLING_FAIL_RATE_THRESHOLD = 0.5  # 연속 실패가 아니어도, 최근 40번 중 절반 이상 실패하면 "간헐적 불통"으로 판단
#    (2026-07-23 이후 사고: DART가 완전히 끊긴 게 아니라 간헐적으로만 실패해서, 연속 카운터는 15에
#     한 번도 안 걸렸지만 누적 지연이 20~30분씩 쌓여 매번 시간 초과로 강제 종료되던 문제 재발 방지)


def _record_result(ok):
    _recent_results.append(ok)
    if len(_recent_results) > ROLLING_WINDOW:
        _recent_results.pop(0)
    if len(_recent_results) >= ROLLING_WINDOW:
        fail_rate = 1 - (sum(_recent_results) / len(_recent_results))
        if fail_rate >= ROLLING_FAIL_RATE_THRESHOLD:
            raise DartUnreachableError(f"최근 {ROLLING_WINDOW}회 요청 중 실패율 {fail_rate*100:.0f}% — 연속은 아니지만 간헐적으로 계속 불안정한 것으로 판단")


def fetch_raw_reports(corp_code, years):
    """여러 연도의 4개 보고서(1분기/반기/3분기/사업) 원본 응답을 그대로 모아옴 (계정 추출은 나중에)."""
    raw = {}  # (year, label) -> DART list 응답
    for year in years:
        for reprt_code, label in REPRT_CODES:
            try:
                url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
                params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": reprt_code}
                res = SESSION.get(url, params=params, timeout=7).json()  # 🔧 10초→7초로 단축(회로차단 반응 속도 개선)
                _consecutive_failures[0] = 0  # 한 번이라도 성공하면 카운터 리셋
                _record_result(True)
                if res.get("status") == "000":
                    raw[(year, label)] = res.get("list", [])
            except DartUnreachableError:
                raise
            except Exception as e:
                print(f"⚠️ [DART fetch {corp_code} {year} {label}] {e}")
                _consecutive_failures[0] += 1
                _record_result(False)
                if _consecutive_failures[0] >= CIRCUIT_BREAKER_THRESHOLD:
                    raise DartUnreachableError(f"DART API 연속 {CIRCUIT_BREAKER_THRESHOLD}회 연결 실패 — 이번 실행에서 API 자체가 응답하지 않는 것으로 판단")
            time.sleep(0.15)  # DART 호출 과다 방지
    return raw


def pick_consistent_fs_div(raw):
    """🔧 [수정] 예전엔 보고서마다 따로 CFS(연결)/OFS(별도)를 골라서, 최근 분기는 아직 연결이
       안 올라와 별도로 잡히고 과거 분기는 연결로 잡히는 식으로 회계 기준이 섞이는 버그가 있었음
       (그 결과 모든 종목에서 실제와 무관하게 -60~70%대의 균일한 '착시 급감'이 나타났음).
       → 이 회사의 전체 조회 기간에 걸쳐 CFS가 다 있으면 CFS로 통일, 하나라도 없으면 OFS로 통일."""
    if not raw:
        return "OFS"
    has_cfs_everywhere = all(
        any(r.get("fs_div") == "CFS" and r.get("sj_div") == "IS" for r in items)
        for items in raw.values()
    )
    return "CFS" if has_cfs_everywhere else "OFS"


def extract_amount(items, fs_div, keywords, exclude=()):
    for r in items:
        if r.get("fs_div") != fs_div or r.get("sj_div") != "IS":
            continue
        name = str(r.get("account_nm", ""))
        if any(ex in name for ex in exclude):
            continue
        if name in keywords or any(kw in name for kw in keywords):
            amt_str = str(r.get("thstrm_amount", "0")).replace(",", "").strip()
            if amt_str.lstrip("-").isdigit():
                return int(amt_str)
    return None


def to_quarterly(year_data):
    """🔧 [수정] 실제 공개된 수치와 대조해서 확인한 DART의 실제 동작:
       반기보고서(11012)·3분기보고서(11014)의 매출액/영업이익은 '누적치'가 아니라
       이미 '그 분기 하나만의 값'으로 나옴 (1분기·사업보고서만 원래 성격대로 1분기단독/연간총합).
       예전엔 이걸 다시 이전 구간을 빼는 역산을 해서, 2·3분기는 너무 작게, 그 여파로
       4분기(연간-3분기값)는 1~3분기가 다 얹혀서 너무 크게 나오는 버그가 있었음.
       → Q1·Q2·Q3는 받은 값을 그대로 쓰고, Q4만 "연간총합 - (Q1+Q2+Q3)"으로 역산."""
    def sub(a, b):
        if a is None or b is None:
            return None
        return a - b

    q1, h1, m9, fy = year_data.get("Q1", {}), year_data.get("H1", {}), year_data.get("9M", {}), year_data.get("FY", {})
    quarters = {}
    if q1.get("revenue") is not None:
        quarters["Q1"] = {"revenue": q1["revenue"], "op_profit": q1.get("op_profit")}
    if h1.get("revenue") is not None:
        quarters["Q2"] = {"revenue": h1["revenue"], "op_profit": h1.get("op_profit")}
    if m9.get("revenue") is not None:
        quarters["Q3"] = {"revenue": m9["revenue"], "op_profit": m9.get("op_profit")}

    if fy.get("revenue") is not None and all(k in quarters and quarters[k].get("revenue") is not None for k in ("Q1", "Q2", "Q3")):
        q1_3_rev_sum = quarters["Q1"]["revenue"] + quarters["Q2"]["revenue"] + quarters["Q3"]["revenue"]
        q4_rev = sub(fy["revenue"], q1_3_rev_sum)

        q4_op = None
        if fy.get("op_profit") is not None and all(quarters[k].get("op_profit") is not None for k in ("Q1", "Q2", "Q3")):
            q1_3_op_sum = quarters["Q1"]["op_profit"] + quarters["Q2"]["op_profit"] + quarters["Q3"]["op_profit"]
            q4_op = sub(fy["op_profit"], q1_3_op_sum)

        quarters["Q4"] = {"revenue": q4_rev, "op_profit": q4_op}
    return quarters


def get_recent_quarters(corp_code, num_years=2):
    """최근 num_years년치를 가져와서 시간순으로 정렬된 분기 리스트로 반환.
       (회사 전체 기간에 걸쳐 동일한 회계 기준을 먼저 확정한 뒤 분기 환산)"""
    this_year = datetime.datetime.now(KST).year
    years = list(range(this_year - num_years, this_year + 1))

    raw = fetch_raw_reports(corp_code, years)
    if not raw:
        return [], "OFS"
    fs_div = pick_consistent_fs_div(raw)

    q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
    all_quarters = []
    for year in years:
        year_data = {}
        for _, label in REPRT_CODES:
            items = raw.get((year, label), [])
            revenue = extract_amount(items, fs_div, {"매출액", "수익(매출액)", "영업수익"})
            op_profit = extract_amount(items, fs_div, {"영업이익"}, exclude=("률", "율"))
            year_data[label] = {"revenue": revenue, "op_profit": op_profit}
        q_map = to_quarterly(year_data)
        for q_name, vals in q_map.items():
            if vals.get("revenue") is not None:
                all_quarters.append({"year": year, "quarter": q_name, "q_sort": q_order[q_name], "revenue": vals["revenue"], "op_profit": vals.get("op_profit")})
    all_quarters.sort(key=lambda x: (x["year"], x["q_sort"]))
    return all_quarters, fs_div


def fetch_consensus_estimates(code, debug=False):
    """네이버금융 종목 페이지의 '기업실적분석' 표에서 애널리스트 컨센서스(추정치) 분기 실적을 가져옴.
       🔒 [개인용 한정] 이 데이터는 금융정보업체가 집계한 상업적 컨센서스 데이터라, 개인·가족 소수 인원
       참고용으로만 쓰고 외부 배포·공개하지 않는 것을 전제로 함. DART 기반 V3와는 완전히 분리해서 저장.
       ⚠️ 네이버 페이지 실제 HTML 구조를 직접 확인 못 하고 작성한 코드라 셀렉터가 안 맞을 수 있음 —
       debug=True인 종목은 각 단계에서 뭘 찾았는지 로그로 남겨서, 다음 실행에서 원인을 바로 알 수 있게 함."""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=7)
        if debug:
            print(f"🔎 [컨센서스 진단 {code}] HTTP 상태: {res.status_code}, 응답 길이: {len(res.text)}자")
        res.encoding = 'utf-8'  # 🔧 [수정] 'euc-kr'로 강제했더니 실제로는 UTF-8 페이지라 한글이 깨져서
        #    "매출액"/"영업이익" 라벨 매칭이 전부 실패하고 있었음(숫자·괄호는 아스키라 안 깨져서 헤더의
        #    "(E)" 표시는 멀쩡히 보였지만, 정작 행 라벨이 깨져서 아무 것도 못 찾았던 것).
        soup = BeautifulSoup(res.text, 'html.parser')

        table = None
        matched_sel = None
        for sel in ["div.cop_analysis table.gHead01", "table.tb_type1_ifrs", "div.section.cop_analysis table"]:
            table = soup.select_one(sel)
            if table:
                matched_sel = sel
                break
        if debug:
            all_tables = soup.select("table")
            print(f"🔎 [컨센서스 진단 {code}] 페이지 내 전체 <table> 개수: {len(all_tables)}개, 매칭된 셀렉터: {matched_sel}")
            for t in all_tables[:10]:
                cls = t.get("class")
                print(f"      - table class={cls} id={t.get('id')}")
        if not table:
            if debug:
                print(f"⚠️ [컨센서스 진단 {code}] 실적분석 표를 못 찾음 — 셀렉터 조정 필요")
            return None

        headers = [th.get_text(strip=True) for th in table.select("thead th")]
        if debug:
            print(f"🔎 [컨센서스 진단 {code}] 헤더: {headers}")

        # 🔧 [수정] "주요재무정보"·"최근 연간 실적" 같은 그룹 라벨이 헤더 앞에 섞여 있어서, 예전처럼
        #    "th 하나만큼 밀림"으로 단순 계산하면 실제 데이터 칸과 어긋남(연간(E) 자리에 분기 실제값이
        #    들어가는 사고 있었음). 실제 "YYYY.MM" 형태의 날짜 헤더만 정규식으로 골라내서, 그 순번이
        #    tbody의 td 순번과 정확히 1:1 대응하도록 다시 짬(그룹 라벨은 데이터 칸이 아니라 자동 제외됨).
        date_pattern = re.compile(r'^\d{4}\.\d{2}(\(E\))?$')
        date_headers = [h for h in headers if date_pattern.match(h)]  # 실제 데이터 컬럼만, 순서 그대로
        estimate_slots = [(i, h) for i, h in enumerate(date_headers) if "(E)" in h]  # (실제 td 인덱스, 헤더명)
        if not estimate_slots:
            if debug:
                print(f"⚠️ [컨센서스 진단 {code}] 날짜 헤더는 {len(date_headers)}개 찾았는데 '(E)' 표시가 있는 칸이 없음")
            return None

        result = {}
        for row in table.select("tbody tr"):
            th = row.select_one("th")
            if not th:
                continue
            label = th.get_text(strip=True)
            if label not in ("매출액", "영업이익", "당기순이익"):
                continue
            tds = row.select("td")
            for td_idx, h in estimate_slots:
                if 0 <= td_idx < len(tds):
                    val_str = tds[td_idx].get_text(strip=True).replace(",", "")
                    try:
                        val = float(val_str)
                    except Exception:
                        continue
                    result.setdefault(h, {})[label] = val
        if debug:
            print(f"🔎 [컨센서스 진단 {code}] 최종 파싱 결과: {result}")
        return result if result else None
    except Exception as e:
        print(f"⚠️ [컨센서스 조회 실패 {code}] {e}")
        return None


def find_same_quarter_last_year(quarters, latest):
    """latest와 같은 분기(Q1/Q2/Q3/Q4)의 1년 전 수치를 찾음 — 계절성을 통제한 전년동기 비교용."""
    target_year, target_q = latest["year"] - 1, latest["quarter"]
    for q in quarters:
        if q["year"] == target_year and q["quarter"] == target_q:
            return q
    return None


def summarize(quarters):
    """가장 최근 분기 기준 전분기(QoQ)와 전년동기(YoY) 증감률을 함께 계산.
       🔧 [수정] 원래 QoQ만 보고 있었는데, 계절적으로 강한 분기(보통 4분기) 바로 다음 분기와 비교하면
       회사가 실제로 안 나빠졌어도 계절성만으로 큰 폭의 '착시 감소'가 나오는 문제가 있었음
       (예: 4분기가 원래 제일 센 회사는 1분기와 QoQ 비교하면 항상 나빠 보임). 이제 전년동기(YoY)를
       실적개선 판단의 주된 근거로 삼고, QoQ는 최근 모멘텀 참고용 보조 지표로만 씀."""
    if len(quarters) < 2:
        return None
    latest, prev = quarters[-1], quarters[-2]

    def pct(a, b):
        if a is None or not b:
            return None
        return round((a - b) / abs(b) * 100, 1)

    rev_growth_qoq = pct(latest["revenue"], prev["revenue"])
    op_growth_qoq = pct(latest.get("op_profit"), prev.get("op_profit"))

    yoy_ref = find_same_quarter_last_year(quarters, latest)
    rev_growth_yoy = pct(latest["revenue"], yoy_ref["revenue"]) if yoy_ref else None
    op_growth_yoy = pct(latest.get("op_profit"), yoy_ref.get("op_profit")) if yoy_ref else None

    # 실적개선 판단은 전년동기(YoY)를 우선 기준으로 삼음. YoY 비교 대상이 아직 안 쌓였으면 QoQ로 대체.
    primary_rev_growth = rev_growth_yoy if rev_growth_yoy is not None else rev_growth_qoq
    is_improving = (primary_rev_growth is not None and primary_rev_growth > 0) and ((latest.get("op_profit") or 0) > 0)

    return {
        "latest_label": f"{latest['year']}{latest['quarter']}",
        "latest_revenue": latest["revenue"],
        "latest_op_profit": latest.get("op_profit"),
        "rev_growth_pct": rev_growth_qoq if rev_growth_qoq is not None else "",
        "op_growth_pct": op_growth_qoq if op_growth_qoq is not None else "",
        "rev_growth_yoy_pct": rev_growth_yoy if rev_growth_yoy is not None else "",
        "op_growth_yoy_pct": op_growth_yoy if op_growth_yoy is not None else "",
        "is_improving": "개선" if is_improving else ""
    }


def compute_v3_score(quarters, summary):
    """V1(차트)/V2(수급)와 같은 0~100 스케일의 '실적' 축 점수.
       🔧 [수정] 계절성 왜곡을 피하려고 전년동기(YoY) 성장률을 주된 근거로 삼고,
       직전분기(QoQ) 흐름은 더 이상 점수에 직접 반영하지 않음(참고 표시로만 남김).
       단일 분기 반짝이 아니라 여러 분기 이어지는 추세를 더 높게 쳐주는 원래 설계 의도는 그대로 유지."""
    if not summary:
        return 0, ""

    rev_growth = summary["rev_growth_yoy_pct"] if summary["rev_growth_yoy_pct"] != "" else summary["rev_growth_pct"]
    op_growth = summary["op_growth_yoy_pct"] if summary["op_growth_yoy_pct"] != "" else summary["op_growth_pct"]
    is_profitable = (summary["latest_op_profit"] or 0) > 0

    # 연속 성장 분기 수도 전년동기(YoY) 기준으로 계산 — 계절성과 무관하게 진짜 추세만 잡기 위함
    consec_rev_growth = 0
    for q in reversed(quarters):
        ref = find_same_quarter_last_year(quarters, q)
        if ref and q["revenue"] is not None and ref["revenue"] and q["revenue"] > ref["revenue"]:
            consec_rev_growth += 1
        else:
            break

    v3 = 15  # 기본점
    if rev_growth != "":
        if rev_growth >= 20: v3 += 30
        elif rev_growth >= 10: v3 += 20
        elif rev_growth > 0: v3 += 10
        elif rev_growth < -10: v3 -= 15

    if op_growth != "":
        if op_growth >= 20: v3 += 25
        elif op_growth >= 0: v3 += 10
        elif op_growth < -20: v3 -= 20

    v3 += 10 if is_profitable else -20  # 적자는 크게 감점

    v3 += min(consec_rev_growth, 4) * 5  # 연속 성장 분기당 가산(최대 4분기 = 20점) — 단발성 반짝 성장과 구분하는 핵심 장치

    v3 = max(0, min(100, int(v3)))
    return v3, (f"{consec_rev_growth}분기 연속(YoY)" if consec_rev_growth > 0 else "0분기 연속")


# ──────────────────────────────────────────────
# ③ 대상 종목 목록 — 우선 DB_중장기 + DB_스캐너로 시작 (API 호출량 보수적 관리)
# ──────────────────────────────────────────────
def get_target_stocks(doc):
    """반환: {종목코드: 종목명} 딕셔너리"""
    names_from_trend = set()
    try:
        rows = doc.worksheet("DB_중장기").get_all_values()[1:]
        for row in rows:
            if len(row) > 4:
                for col_idx in [3, 4]:
                    if len(row) > col_idx and row[col_idx].strip():
                        nm = row[col_idx].split('(')[0].strip()
                        if nm: names_from_trend.add(nm)
    except Exception as e:
        print(f"⚠️ [DB_중장기 읽기 실패] {e}")

    codes_from_scanner = {}  # code -> name(DB_스캐너 자체 표기, 하이퍼링크 수식일 수 있어 보정 필요)
    try:
        rows = doc.worksheet("DB_스캐너").get_all_values()[1:]
        for row in rows:
            if len(row) > 2 and row[2].strip():
                code = str(row[2]).replace("'", "").strip().zfill(6)
                raw_name = str(row[0]).strip()
                m = re.search(r',\s*"([^"]+)"\)', raw_name)  # =HYPERLINK(...,"종목명") 형태 대비
                codes_from_scanner[code] = m.group(1).strip() if m else raw_name
    except Exception as e:
        print(f"⚠️ [DB_스캐너 읽기 실패] {e}")

    result = {}
    try:
        name_to_code = {str(r[0]).strip(): str(r[2]).strip().zfill(6) for r in doc.worksheet("기업정보").get_all_values()[1:] if len(r) >= 3}
        code_to_name = {v: k for k, v in name_to_code.items()}

        for nm in names_from_trend:
            if nm in name_to_code:
                result[name_to_code[nm]] = nm
        for code, nm in codes_from_scanner.items():
            result[code] = code_to_name.get(code, nm)  # 기업정보가 더 정확하면 그걸 우선
    except Exception as e:
        print(f"⚠️ [기업정보 이름→코드 매핑 실패] {e}")
        result = dict(codes_from_scanner)

    return result


if __name__ == "__main__":
    if not DART_API_KEY:
        print("❌ DART_API_KEY 환경변수가 없습니다. GitHub Secrets에 등록해주세요.")
        exit(1)

    doc = get_doc()
    corp_map = load_or_build_corp_code_map(doc)
    target_map = get_target_stocks(doc)  # {종목코드: 종목명}
    print(f"▶️ 총 {len(target_map)}개 종목의 실적 데이터를 수집합니다 (DB_중장기 + DB_스캐너 기준)...")

    try:
        out_sheet = doc.worksheet("DB_실적")
    except Exception:
        out_sheet = doc.add_worksheet(title="DB_실적", rows="1000", cols="12")

    header = ["종목코드", "종목명", "최신분기", "매출액", "영업이익", "매출증감률(YoY,%)", "영업이익증감률(YoY,%)", "매출증감률(QoQ,%)", "영업이익증감률(QoQ,%)", "실적개선여부", "V3(실적점수)", "연속성장", "재무제표기준", "갱신일시"]
    rows_out = [header]
    consensus_header = ["종목코드", "종목명", "추정분기", "추정매출액", "추정영업이익", "추정당기순이익", "갱신일시"]
    consensus_rows_out = [consensus_header]
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    target_codes = list(target_map.keys())
    fs_div_counter = {"CFS": 0, "OFS": 0}
    DEBUG_STOCKS = {"005930", "000660", "035420"}  # 🔎 [진단용] 삼성전자/SK하이닉스/NAVER — 분기별 원본 수치를 그대로 로그에 찍어서 확인

    # 🆕 [시간 예산] 워크플로의 하드 타임아웃(30분)에 강제 종료당하면 그때까지 모은 데이터가
    #    단 한 줄도 저장 안 되는 문제가 있었음 — 그 전에 미리 멈추고 지금까지 모은 것만이라도 저장함.
    SCRIPT_TIME_BUDGET_SEC = 55 * 60  # 하드 타임아웃(60분)보다 5분 여유
    script_start = time.time()
    time_budget_hit = False

    for idx, code in enumerate(target_codes):
        if time.time() - script_start > SCRIPT_TIME_BUDGET_SEC:
            print(f"⏱️ [시간 예산 초과] {SCRIPT_TIME_BUDGET_SEC}초 경과 — 남은 {len(target_codes) - idx}개 종목은 건너뛰고, 지금까지 모은 데이터부터 저장합니다.")
            time_budget_hit = True
            break
        corp_code = corp_map.get(code)
        if not corp_code:
            print(f"⚠️ [{code}] DART corp_code 매핑 없음 (비상장·최근상장 등) — 스킵")
            continue
        try:
            quarters, fs_div = get_recent_quarters(corp_code, num_years=2)

            if code in DEBUG_STOCKS:
                print(f"🔎 [진단] {target_map.get(code, code)}({code}) 분기별 원본 수치 (기준: {fs_div}):")
                for q in quarters:
                    print(f"      {q['year']}{q['quarter']}: 매출액={q['revenue']:,}" + (f", 영업이익={q['op_profit']:,}" if q.get('op_profit') is not None else ""))

            summary = summarize(quarters)
            if not summary:
                print(f"⚠️ [{code}] 실적 데이터 부족 — 스킵")
                continue
            fs_div_counter[fs_div] = fs_div_counter.get(fs_div, 0) + 1
            v3_score, streak_label = compute_v3_score(quarters, summary)
            rows_out.append([
                code, target_map.get(code, ""), summary["latest_label"], summary["latest_revenue"], summary["latest_op_profit"],
                summary["rev_growth_yoy_pct"], summary["op_growth_yoy_pct"], summary["rev_growth_pct"], summary["op_growth_pct"],
                summary["is_improving"], v3_score, streak_label,
                "연결" if fs_div == "CFS" else "별도", now_str
            ])

            # 🆕 [개인용 참고자료] 애널리스트 컨센서스 — DART 확정치(V3)와는 완전히 분리해서 별도 시트에 기록
            consensus = fetch_consensus_estimates(code, debug=(code in DEBUG_STOCKS))
            if consensus:
                for q_label, vals in consensus.items():
                    consensus_rows_out.append([
                        code, target_map.get(code, ""), q_label,
                        vals.get("매출액", ""), vals.get("영업이익", ""), vals.get("당기순이익", ""), now_str
                    ])
            time.sleep(0.2)  # 네이버 호출 과다 방지
        except DartUnreachableError as e:
            print(f"🚨 [회로차단기 발동] {e}")
            print("⏭ 이번 실행은 여기서 조기 종료합니다 — 다음 스케줄 실행에서 다시 시도됩니다.")
            break
        except Exception as e:
            print(f"⚠️ [{code}] 실적 처리 실패: {e}")
            continue

        if (idx + 1) % 20 == 0:
            print(f"   ...{idx + 1}/{len(target_codes)} 진행 중")

    partial_note = " (⏱️ 시간 예산 초과로 일부만 처리됨 — 나머지는 다음 실행에서 이어서 수집됩니다)" if time_budget_hit else ""

    if len(rows_out) > 1:
        out_sheet.clear()
        out_sheet.update(range_name="A1", values=rows_out, value_input_option="RAW")
        print(f"✅ [DB_실적] {len(rows_out) - 1}개 종목 기록 완료 (연결기준 {fs_div_counter.get('CFS',0)}개 / 별도기준 {fs_div_counter.get('OFS',0)}개){partial_note}")
    else:
        print("⚠️ 수집된 실적 데이터가 없습니다.")

    if len(consensus_rows_out) > 1:
        try:
            consensus_sheet = doc.worksheet("DB_컨센서스")
        except Exception:
            consensus_sheet = doc.add_worksheet(title="DB_컨센서스", rows="1000", cols="8")
        consensus_sheet.clear()
        consensus_sheet.update(range_name="A1", values=consensus_rows_out, value_input_option="RAW")
        print(f"✅ [DB_컨센서스 · 개인 참고용] {len(consensus_rows_out) - 1}행 기록 완료{partial_note}")
    else:
        print("⚠️ 컨센서스 데이터가 하나도 안 잡혔습니다 — 네이버 페이지 셀렉터 조정이 필요할 수 있습니다.")
