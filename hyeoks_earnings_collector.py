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
import os, re, sys, time, datetime, zipfile, io
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
    """Verified provider JSON: quarterly IFRS consolidated, estimates only, KRW 100M.
    Errors propagate to the caller; empty coverage is distinct from source failure.
    """
    from naver_sources import consensus_estimates
    return consensus_estimates(code)


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
import earnings_schema as _es
EARNINGS_SCHEMA_VERSION = _es.SCHEMA_VERSION   # 사본을 두지 않는다
EARNINGS_HEADER = ["종목코드", "종목명", "최신분기", "매출액", "영업이익", "매출증감률(YoY,%)",
                   "영업이익증감률(YoY,%)", "매출증감률(QoQ,%)", "영업이익증감률(QoQ,%)",
                   "실적개선여부", "V3(실적점수)", "연속성장", "재무제표기준", "갱신일시"]
STAMP_COL = EARNINGS_HEADER.index("갱신일시")


def _code_of(row):
    return str(row[0]).lstrip("'").strip() if row else ""


def merge_earnings_rows(old, new):
    """🔴 2026-09-18 — DB_실적 은 매 실행 `clear()` 후 통째로 다시 썼다.

    시간 예산(55분)에 걸려 127/143 만 처리한 날, **나머지 16종목의 이전 행이
    시트에서 사라졌다.** 로그는 "나머지는 다음 실행에서 이어서 수집됩니다" 라고
    말했지만 대상 순서가 고정이라 다음 실행도 같은 앞쪽 127개를 돌았다.
    즉 꼬리는 이어받는 게 아니라 **매일 지워지고 영영 안 채워졌다.**

    바로 옆 DB_컨센서스 경로는 이미 merge_consensus_rows 로 올바르게 보존하고
    있었다. 같은 파일 안에서 두 시트가 다르게 동작하고 있었던 것이다.

    이번 실행이 건드리지 못한 종목은 이전 행과 이전 갱신일시를 그대로 남긴다.
    """
    if not new or len(new[0]) != len(EARNINGS_HEADER):
        raise ValueError("실적 출력 스키마가 다르다")
    width = len(new[0])
    if old and [str(c) for c in old[0][:width]] != new[0]:
        raise ValueError("기존 DB_실적 헤더 불일치")
    refreshed = {_code_of(r) for r in new[1:]}
    preserved = [list(r[:width]) + [""] * (width - len(r[:width]))
                 for r in old[1:] if _code_of(r) and _code_of(r) not in refreshed]
    return new + preserved


def stale_first(codes, old):
    """갱신일시가 오래된(없으면 더 오래된 것으로) 종목부터 처리한다.

    순서가 고정이면 시간 예산에 잘린 꼬리는 **영원히** 꼬리다.
    오래된 것부터 돌면 어제 잘린 종목이 오늘 맨 앞에 온다 —
    그래야 "다음 실행에서 이어서" 가 말이 아니라 사실이 된다.
    """
    stamp = {}
    for row in (old or [])[1:]:
        c = _code_of(row)
        if c:
            stamp[c] = str(row[STAMP_COL]).strip() if len(row) > STAMP_COL else ""
    return sorted(codes, key=lambda c: (stamp.get(c, ""), c))


CONSENSUS_CONTROL = ("005930", "000660")   # 대조 종목: 커버리지가 확실한 대형주


def stage_earnings(rows, reason, out_dir="data/earnings_staging"):
    """🔴 2026-09-18 GPT P0-2/P0-3 — 본표 쓰기를 막을 때 오늘 수집분을 버리지 않는다.

    보존이 불가능하거나 스키마를 못 믿으면 시트는 건드리지 않되, 모은 것은 파일로
    남겨 복구 후 병합할 수 있게 한다. "멈추는 오류가 틀린 값보다 낫다" 는 원칙은
    "모은 것을 버린다" 는 뜻이 아니다.
    """
    import csv
    import os
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.datetime.now(KST).strftime("%Y%m%dT%H%M%S")
    path = os.path.join(out_dir, f"{stamp}_earnings.csv")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["# 본표 쓰기 차단", reason])
        writer.writerows(rows)
    return path


def consensus_preflight(fetch, controls=CONSENSUS_CONTROL):
    """🔴 2026-09-18 GPT §13 — 143종목을 다시 때려 보고 원인을 추측하지 않는다.

    대조 종목 두 개로 원천 상태를 먼저 본다. 전부 실패하면 그날 배치를 **시작하지
    않는다.** 이것만으로 DART 예산을 지킬 수 있고, 실패 사유도 두 줄이면 나온다.

    반환: (ok, [(code, 상태, 초, 사유)...])
    """
    detail = []
    for code in controls:
        start = time.time()
        try:
            fetch(code)
            detail.append((code, "ok", round(time.time() - start, 1), ""))
        except Exception as error:
            detail.append((code, "fail", round(time.time() - start, 1),
                           f"{type(error).__name__}: {error}"[:140]))
    return any(d[1] == "ok" for d in detail), detail


def consensus_health(attempted, succeeded):
    """OK / DEGRADED / FAILED. 문턱 숫자는 아직 박지 않는다(원인 조사 후 사전 고정).

    🔴 GPT §6 — 이전에는 1건만 실패해도 성공한 142건을 **저장하기 전에** 종료했다.
    """
    if attempted <= 0:
        return "SKIPPED"
    if succeeded == 0:
        return "FAILED"
    return "OK" if succeeded == attempted else "DEGRADED"


def get_target_stocks(doc):
    """반환: ({종목코드: 종목명}, {시트: "ok"|사유})

    🔴 2026-09-18 — 세 시트 읽기가 전부 로그만 남기고 넘어갔다. 하나가 실패해도
       나머지로 dict 가 만들어지므로 **universe 가 조용히 줄어든다.**
       앞서 나는 `len(target_map) >= 50` 으로 대신했는데 그건 source health 가
       아니다 — DB_중장기가 통째로 실패해도 DB_스캐너에 100종목이 있으면 "ok" 다.
       시트별 성공 여부를 그대로 돌려준다.
    """
    health = {}
    names_from_trend = set()
    try:
        rows = doc.worksheet("DB_중장기").get_all_values()[1:]
        for row in rows:
            if len(row) > 4:
                for col_idx in [3, 4]:
                    if len(row) > col_idx and row[col_idx].strip():
                        nm = row[col_idx].split('(')[0].strip()
                        if nm: names_from_trend.add(nm)
        health["DB_중장기"] = "ok"
    except Exception as e:
        health["DB_중장기"] = f"읽기 실패 {type(e).__name__}"
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
        health["DB_스캐너"] = "ok"
    except Exception as e:
        health["DB_스캐너"] = f"읽기 실패 {type(e).__name__}"
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
        health["기업정보"] = "ok"
    except Exception as e:
        health["기업정보"] = f"매핑 실패 {type(e).__name__}"
        print(f"⚠️ [기업정보 이름→코드 매핑 실패] {e}")
        result = dict(codes_from_scanner)

    return result, health


if __name__ == "__main__":
    # 🔴 2026-09-18 GPT §5 — 워크플로를 갈라 **장애 도메인**을 분리한다.
    #    하나의 워크플로가 빨간불이면 "투자 핵심 데이터가 죽었는지 보조자료가
    #    죽었는지" 로그를 열어봐야 했다. 이제 이름으로 구분된다.
    #      earnings_collector.yml  --phase primary  → DART → DB_실적
    #      consensus_aux.yml       --phase aux      → WiseReport → DB_컨센서스
    #    --phase all 은 기존 동작(한 실행에서 둘 다)이며 수동 점검용으로 남긴다.
    PHASE = "all"
    for _i, _a in enumerate(sys.argv):
        if _a == "--phase" and _i + 1 < len(sys.argv):
            PHASE = sys.argv[_i + 1]
    if PHASE not in ("all", "primary", "aux"):
        print(f"❌ --phase 는 all|primary|aux 다 (받은 값: {PHASE})")
        sys.exit(2)
    RUN_PRIMARY, RUN_AUX = PHASE in ("all", "primary"), PHASE in ("all", "aux")
    print(f"▶️ phase={PHASE} (primary={RUN_PRIMARY} · aux={RUN_AUX})")

    # 🔴 2026-09-18 ⑩ — aux 는 DART 를 전혀 쓰지 않는다. 그런데 DART_API_KEY 가
    #    없으면 여기서 죽었고, corp_code 매핑(DART 전용, 수 MB zip 다운로드)도
    #    같이 돌았다. 분리가 이름만이고 의존성은 그대로였다는 뜻이다.
    if RUN_PRIMARY and not DART_API_KEY:
        print("❌ DART_API_KEY 환경변수가 없습니다. GitHub Secrets에 등록해주세요.")
        sys.exit(1)

    doc = get_doc()
    corp_map = load_or_build_corp_code_map(doc) if RUN_PRIMARY else {}
    target_map, _sheet_health = get_target_stocks(doc)
    # 🔴 입력 시트를 하나라도 못 읽으면 universe 가 줄어든다. 그러면
    #    "전부 처리했다" 가 참이어도 실제로는 일부만 본 것이다.
    #    개수(N>=50)는 **보조 sanity check** 로만 남기고 근거로 쓰지 않는다.
    _bad_sheets = [k for k, v in _sheet_health.items() if v != "ok"]
    _missing_sheets = [k for k in ("DB_중장기", "DB_스캐너", "기업정보")
                       if k not in _sheet_health]
    if _bad_sheets or _missing_sheets:
        _target_health = f"입력 시트 이상 {_bad_sheets + _missing_sheets}"
    elif len(target_map) < 50:
        _target_health = f"sanity: target 수 비정상({len(target_map)})"
    else:
        _target_health = "ok"
    print(f"📋 입력 시트 상태: {_sheet_health} → {_target_health}")
    print(f"▶️ 총 {len(target_map)}개 종목의 실적 데이터를 수집합니다 (DB_중장기 + DB_스캐너 기준)...")

    out_sheet = None
    try:
        # aux 는 DB_실적을 건드리지 않는다 — 없으면 만들지도 않는다
        out_sheet = doc.worksheet("DB_실적") if RUN_PRIMARY else None
    except Exception:
        # 🔴 2026-09-18 GPT P0-4 — cols="12" 였다. 지금 스키마는 14열이다.
        #    기존 운영 시트가 이미 있어서 안 드러났을 뿐, 새/복구/시험 환경에서 깨진다.
        #    (이 12 라는 숫자가 V3 가 index 8 이던 옛 스키마의 화석이다 — P0-1 참조)
        out_sheet = doc.add_worksheet(title="DB_실적", rows="1000",
                                      cols=str(len(EARNINGS_HEADER)))

    rows_out = [list(EARNINGS_HEADER)]   # 사본을 두지 않는다 — 어긋나면 병합이 헛돈다
    consensus_header = ["종목코드", "종목명", "추정분기", "추정매출액", "추정영업이익", "추정당기순이익", "갱신일시"]
    consensus_rows_out = [consensus_header]
    consensus_failures = []
    consensus_targets = []
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    # 🔴 2026-09-18 — 순서가 고정이라 시간 예산에 잘린 꼬리가 다음 실행에도 꼬리였다.
    #    기존 시트의 갱신일시를 읽어 **오래된 것부터** 돈다. 한 번도 못 받은 종목이 맨 앞.
    # 🔴 2026-09-18 GPT P0-2 — 읽기 실패를 빈 시트로 취급하면 안 된다.
    #    못 읽었다고 시트가 비어 있는 건 아니다. 빈 목록으로 병합하면 새 데이터가
    #    위쪽만 덮고 아래쪽 옛 행은 고아로 남는다(batch_clear 가드도 0 > N 이라 안 돈다).
    #    **읽지 못했는데 쓰는 것**이 보존 병합에서 가장 위험한 패턴이다.
    #    보존이 불가능하면 본표 쓰기를 막는다.
    existing_earnings, earnings_readable = [], True
    try:
        existing_earnings = out_sheet.get("A:N") if RUN_PRIMARY else []
    except Exception as e:
        earnings_readable = False
        print(f"::error::기존 DB_실적 읽기 실패({type(e).__name__}: {e}) — "
              "보존이 불가능하므로 DB_실적 본표에 쓰지 않는다")
    target_codes = stale_first(list(target_map.keys()), existing_earnings)
    fs_div_counter = {"CFS": 0, "OFS": 0}
    # 🔴 2026-09-18 P0-2 — `targets - success` 를 결측으로 **역산**하면
    #    `success + skip == targets` 는 정의상 항상 참이다. 시간예산 초과·회로차단·
    #    예외·미처리가 전부 '명시적 결측' 으로 둔갑한다. 실제 경로에서 직접 센다.
    _missing_corp = []
    OUTCOME = {
        "success": 0,
        "allowed_missing_corp_code": 0,   # 비상장·최근상장 — 허용되는 결측
        "allowed_insufficient_data": 0,   # 분기 수 부족 — 허용되는 결측
        "hard_error": 0,                  # 처리 중 예외 — 허용되지 않는다
        "circuit_breaker_unprocessed": 0,  # 회로차단기로 남은 종목
        "time_budget_unprocessed": 0,      # 예산 초과로 남은 종목
    }
    DEBUG_STOCKS = {"005930", "000660", "035420"}  # 🔎 [진단용] 삼성전자/SK하이닉스/NAVER — 분기별 원본 수치를 그대로 로그에 찍어서 확인

    # 🆕 [시간 예산] 워크플로의 하드 타임아웃(30분)에 강제 종료당하면 그때까지 모은 데이터가
    #    단 한 줄도 저장 안 되는 문제가 있었음 — 그 전에 미리 멈추고 지금까지 모은 것만이라도 저장함.
    SCRIPT_TIME_BUDGET_SEC = 55 * 60  # 하드 타임아웃(60분)보다 5분 여유
    script_start = time.time()
    _RUN_STARTED_AT = datetime.datetime.now(KST)   # 영수증 거래일을 여기서 고정한다
    time_budget_hit = False

    for idx, code in enumerate(target_codes if RUN_PRIMARY else []):
        if time.time() - script_start > SCRIPT_TIME_BUDGET_SEC:
            print(f"⏱️ [시간 예산 초과] {SCRIPT_TIME_BUDGET_SEC}초 경과 — 남은 {len(target_codes) - idx}개 종목은 건너뛰고, 지금까지 모은 데이터부터 저장합니다.")
            time_budget_hit = True
            OUTCOME["time_budget_unprocessed"] = len(target_codes) - idx
            break
        corp_code = corp_map.get(code)
        if not corp_code:
            print(f"⚠️ [{code}] DART corp_code 매핑 없음 (비상장·최근상장 등) — 스킵")
            OUTCOME["allowed_missing_corp_code"] += 1
            _missing_corp.append(code)
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
                OUTCOME["allowed_insufficient_data"] += 1
                continue
            fs_div_counter[fs_div] = fs_div_counter.get(fs_div, 0) + 1
            v3_score, streak_label = compute_v3_score(quarters, summary)
            rows_out.append([
                code, target_map.get(code, ""), summary["latest_label"], summary["latest_revenue"], summary["latest_op_profit"],
                summary["rev_growth_yoy_pct"], summary["op_growth_yoy_pct"], summary["rev_growth_pct"], summary["op_growth_pct"],
                summary["is_improving"], v3_score, streak_label,
                "연결" if fs_div == "CFS" else "별도", now_str
            ])
            OUTCOME["success"] += 1

            # 🔴 2026-09-18 GPT 교차검증 §5 — 여기서 종목마다 컨센서스를 같이 불렀다.
            #    보조 원천 하나가 종목당 최대 12초 타임아웃을 일으키면 **주 산출물인
            #    DART 수집이 그만큼 멈춘다.** 127건 실패면 타임아웃만 25분이고,
            #    그게 55분 예산을 먹어 DB_실적이 127/143 에서 잘린 원인이다.
            #    즉 "DART 가 느리다" 가 아니라 "보조가 주의 예산을 먹는다" 였다.
            #    컨센서스는 DB_실적을 저장한 **뒤** PHASE B 에서 따로 돈다.
            consensus_targets.append(code)
        except DartUnreachableError as e:
            print(f"🚨 [회로차단기 발동] {e}")
            print("⏭ 이번 실행은 여기서 조기 종료합니다 — 다음 스케줄 실행에서 다시 시도됩니다.")
            OUTCOME["circuit_breaker_unprocessed"] = len(target_codes) - idx
            break
        except Exception as e:
            print(f"⚠️ [{code}] 실적 처리 실패: {e}")
            OUTCOME["hard_error"] += 1      # 허용되는 결측이 아니다
            continue

        if (idx + 1) % 20 == 0:
            print(f"   ...{idx + 1}/{len(target_codes)} 진행 중")

    partial_note = (" (⏱️ 시간 예산 초과로 일부만 처리됨 — 나머지는 이전 값을 유지하고, "
                    "갱신일시가 오래된 순서라 다음 실행에서 먼저 처리됩니다)") if time_budget_hit else ""

    earnings_blocked = ""
    merged = None
    if not RUN_PRIMARY:
        print("⏭ [DB_실적 생략] phase=aux — 본표는 earnings_collector.yml 이 맡는다.")
    elif len(rows_out) > 1:
        if not earnings_readable:
            earnings_blocked = "기존 시트를 읽지 못해 보존이 불가능하다"
        else:
            try:
                # 통째로 비우지 않는다 — 못 건드린 종목의 이전 행을 살린다(merge 주석 참조).
                merged = merge_earnings_rows(existing_earnings, rows_out)
            except ValueError as e:
                # 🔴 2026-09-18 GPT P0-3 — 앞 커밋에서 나는 여기서 clear() 후 새로 썼다.
                #    "해석할 수 없으니 기존 데이터를 전부 지운다" 는 fail-closed 가 아니다.
                #    특히 예산에 걸린 부분 실행이면 일부 종목만으로 전체 시트를 대체한다.
                #    스키마를 못 믿으면 **본표를 건드리지 않는다.**
                earnings_blocked = f"스키마 불일치({e})"

        if earnings_blocked:
            stage = stage_earnings(rows_out, earnings_blocked)
            print(f"::error::[DB_실적 쓰기 차단] {earnings_blocked} — "
                  f"본표 무수정. 이번 수집분 {len(rows_out) - 1}종목은 {stage} 에 보존했다")
        else:
            out_sheet.update(range_name="A1", values=merged, value_input_option="RAW")
            if len(existing_earnings) > len(merged):
                out_sheet.batch_clear([f"A{len(merged) + 1}:N{len(existing_earnings)}"])
            kept = len(merged) - len(rows_out)
            print(f"✅ [DB_실적] {len(rows_out) - 1}개 종목 기록 완료 "
                  f"(연결기준 {fs_div_counter.get('CFS',0)}개 / 별도기준 {fs_div_counter.get('OFS',0)}개){partial_note}"
                  + (f" · 이번에 못 돈 {kept}개는 이전 값 보존" if kept else ""))
    else:
        print("⚠️ 수집된 실적 데이터가 없습니다.")

    # ══════════════════════════════════════════════════════════════════
    # PHASE B — 컨센서스(보조). 주 산출물을 저장한 뒤에 시작한다.
    # ══════════════════════════════════════════════════════════════════
    if not RUN_PRIMARY:
        # aux 단독 — DART 루프를 안 돌았으니 대상은 목록에서 직접 온다(오래된 순서 유지).
        consensus_targets = list(target_codes)
    if not RUN_AUX:
        print("\n⏭ [PHASE B 생략] phase=primary — 컨센서스는 consensus_aux.yml 이 맡는다.")
        consensus_targets, pre_ok, pre_detail = [], True, []
    else:
        print(f"\n▶️ [PHASE B] 컨센서스 {len(consensus_targets)}종목 — 보조 자료다. "
              "여기서 실패해도 위의 DB_실적은 이미 확정됐다.")
        pre_ok, pre_detail = consensus_preflight(fetch_consensus_estimates)
        for c, pre_state, secs, why in pre_detail:
            print(f"   · preflight {c}: {pre_state} {secs}s {why}")
        if not pre_ok:
            print("::error::[컨센서스 preflight 실패] 대조 종목이 전부 실패 — "
                  "배치를 시작하지 않는다(DART 예산을 지킨다)")
            consensus_targets = []

    for idx, code in enumerate(consensus_targets):
        if time.time() - script_start > SCRIPT_TIME_BUDGET_SEC:
            print(f"⏱️ [PHASE B 예산 초과] 남은 {len(consensus_targets) - idx}종목은 다음 실행에서")
            break
        try:
            consensus = fetch_consensus_estimates(code, debug=(code in DEBUG_STOCKS))
        except Exception as error:
            consensus_failures.append(code)
            print(f"::warning::컨센서스 원천 실패 {code}: {error}")
            consensus = None
        if consensus:
            for q_label, vals in consensus.items():
                consensus_rows_out.append([
                    code, target_map.get(code, ""), q_label,
                    vals.get("매출액", ""), vals.get("영업이익", ""),
                    vals.get("당기순이익", ""), now_str
                ])
        time.sleep(0.2)  # 원천 호출 과다 방지

    consensus_done = len({r[0] for r in consensus_rows_out[1:]})
    state = consensus_health(len(consensus_targets), consensus_done)
    print(f"📊 [컨센서스] {state} — 시도 {len(consensus_targets)} · 성공 {consensus_done} · "
          f"실패 {len(consensus_failures)}")

    # 🔴 GPT §6 — 성공분을 **먼저 정직하게 저장하고** 그 다음에 상태를 판단한다.
    if len(consensus_rows_out) > 1:
        try:
            consensus_sheet = doc.worksheet("DB_컨센서스")
        except Exception:
            consensus_sheet = doc.add_worksheet(title="DB_컨센서스", rows="1000", cols="8")
        from naver_sources import merge_consensus_rows
        old_rows = consensus_sheet.get("A:G")
        merged = merge_consensus_rows(old_rows, consensus_rows_out)
        # Keep unprocessed stocks and their original timestamps; never clear the whole sheet.
        consensus_sheet.update(range_name="A1", values=merged, value_input_option="RAW")
        if len(old_rows) > len(merged):
            consensus_sheet.batch_clear([f"A{len(merged) + 1}:G{len(old_rows)}"])
        print(f"✅ [DB_컨센서스 · 개인 참고용] {len(consensus_rows_out) - 1}행 기록 완료{partial_note}")
    else:
        print("::warning::분기 연결 컨센서스 신규 추정치 없음. 기존 자료/갱신시각 보존; 정상 갱신 아님.")

    # ══════════════════════════════════════════════════════════════════
    # 실행 요약 한 줄 — 안정화 종료선(stability_gate)이 읽는 증거다.
    # 2026-09-18 GPT §1 — "447개 시험보다 실제 생산 실행 한 번이 더 중요한 증거다."
    # 로그를 사람이 눈으로 훑어 판단하지 않도록, 기계가 읽을 수 있게 고정 형식으로 찍는다.
    # ══════════════════════════════════════════════════════════════════
    try:
        import earnings_schema
        final_rows = merged if not earnings_blocked and len(rows_out) > 1 else existing_earnings
        v3_map, v3_stats = earnings_schema.read_v3_map(final_rows)
        stamps = sorted(str(r[earnings_schema.STAMP_COL]).strip()
                        for r in (final_rows or [])[1:]
                        if len(r) > earnings_schema.STAMP_COL and str(r[earnings_schema.STAMP_COL]).strip())
    except Exception as e:
        v3_stats, stamps = {"rows": 0, "used": 0, "blank": 0, "unparsable": 0,
                            "out_of_range": 0, "reason": f"요약 실패 {e}", "col": None}, []
    print("\n[DB_실적]")
    print(f"대상 {len(target_map)}")
    print(f"DART 성공 {len(rows_out) - 1} / 스킵·실패 {len(target_map) - (len(rows_out) - 1)}")
    print(f"DB_실적 최종 보유 {v3_stats['rows']}")
    print(f"V3 정상 {v3_stats['used']} / 빈값 {v3_stats['blank']} / "
          f"해석불가 {v3_stats['unparsable']} / 범위오류 {v3_stats['out_of_range']}")
    print(f"최저 갱신일시 {stamps[0] if stamps else '-'}")
    print(f"최고 갱신일시 {stamps[-1] if stamps else '-'}")
    print(f"schema={EARNINGS_SCHEMA_VERSION}")
    print(f"예산초과={'Y' if time_budget_hit else 'N'} 본표쓰기={'차단' if earnings_blocked else '정상'}")
    print("\n[Consensus]")
    print(f"preflight {'OK' if pre_ok else 'FAIL'}")
    print(f"attempted {len(consensus_targets)}")
    print(f"success {consensus_done}")
    print(f"failed {len(consensus_failures)}")
    print(f"state={state}")

    # ══════════════════════════════════════════════════════════════════
    # 생산 영수증 — Evidence Builder 가 읽는다(사람이 True/False 를 넣지 않는다)
    # ══════════════════════════════════════════════════════════════════
    import production_receipt
    import stability_gate as _sg
    # 🔴 2026-09-19 — 벽시계 날짜를 쓰면 자정을 넘긴 실행이 다음 날(비거래일)로 샌다.
    #    실제로 9/18 23:45 시작 → 00:17 종료 실행이 9/19(토) 로 기록됐다.
    #    거래일로 묶고, **시작 시각** 기준으로 고정한다(긴 실행이 밀리지 않게).
    _cycle = production_receipt.cycle_date_now(_RUN_STARTED_AT)
    _fp = _sg.fingerprint()
    if RUN_PRIMARY:
        # 🔴 P0-2 — 역산하지 않는다. 대상 수와 사유별 합이 맞는지도 같이 싣는다.
        _accounted = sum(OUTCOME.values())
        _unaccounted = len(target_map) - _accounted
        _rok, _rmsg = production_receipt.emit(_cycle, "earnings", {
            "expected_state": "reached" if not earnings_blocked else "blocked",
            "schema_version": EARNINGS_SCHEMA_VERSION,
            "schema_reason": v3_stats.get("reason", ""),
            "features": {
                "v3": {"measured": not v3_stats.get("reason"),
                       "used": v3_stats.get("used", 0),
                       "errors": 0,
                       "unparsable": v3_stats.get("unparsable", 0),
                       "out_of_range": v3_stats.get("out_of_range", 0),
                       "schema_reason": v3_stats.get("reason", "")},
            },
            "v3_used": v3_stats.get("used", 0),
            "v3_blank": v3_stats.get("blank", 0),
            "v3_unparsable": v3_stats.get("unparsable", 0),
            "v3_out_of_range": v3_stats.get("out_of_range", 0),
            "targets": len(target_map),
            "dart_success": OUTCOME["success"],
            "outcomes": dict(OUTCOME),
            "unaccounted": _unaccounted,     # 0 이 아니면 어딘가 세지 않은 경로가 있다
            "time_budget_hit": bool(time_budget_hit),
            # 대상 universe 자체가 줄어든 경우를 따로 본다(입력 시트 읽기 실패)
            "target_source_health": _target_health,
            "sheet_health": dict(_sheet_health),
            # corp_code 누락 종목을 그대로 남긴다 — 지금 새 임계값을 만들지 않고,
            # 며칠 실데이터를 본 뒤 정말 허용 가능한 유형인지 판단한다.
            "missing_corp_codes": list(_missing_corp)[:200],
            "write_blocked": earnings_blocked,
            "rows_final": v3_stats.get("rows", 0),
            "oldest_stamp": stamps[0] if stamps else "",
            "newest_stamp": stamps[-1] if stamps else "",
        }, fingerprint=_fp)
        print(f"🧾 {_rmsg}")
    if RUN_AUX:
        _rok, _rmsg = production_receipt.emit(_cycle, "consensus", {
            "expected_state": "reached" if state in ("OK", "DEGRADED") else "degraded",
            "state": state,
            "preflight_ok": bool(pre_ok),
            "preflight": [{"code": c, "state": st, "secs": sec, "why": w}
                          for c, st, sec, w in pre_detail],
            "attempted": len(consensus_targets),
            "success": consensus_done,
            "failed": len(consensus_failures),
        }, fingerprint=_fp)
        print(f"🧾 {_rmsg}")

    # ══════════════════════════════════════════════════════════════════
    # 종료코드 — 주 산출물과 보조 자료를 가른다 (GPT §7 · Q1)
    # ══════════════════════════════════════════════════════════════════
    # 이전에는 컨센서스 1종목 실패가 워크플로 전체를 적색으로 만들었다. 그래서
    # 9/14~9/17 나흘 연속 빨간불이었는데 1건 실패와 127건 실패가 같은 신호였고,
    # 아무도 보지 않게 됐다. 매일 빨간 경보는 경보가 아니다.
    #
    # 이제: 주 산출물(DB_실적) 실패 → 적색. 보조(컨센서스)는 **완전 붕괴만** 적색.
    # DEGRADED(일부 실패)의 정확한 커버리지 문턱은 원인 조사 후 사전 고정한다 —
    # 지금 90% 같은 숫자를 즉흥적으로 박지 않는다(GPT §6).
    if earnings_blocked:
        print(f"::error::[주 산출물 실패] DB_실적 본표를 쓰지 못했다 — {earnings_blocked}")
        raise SystemExit(1)
    if state == "FAILED":
        print(f"::error::[보조 원천 붕괴] 컨센서스 {len(consensus_targets)}종목 전량 실패. "
              "DB_실적은 위에서 정상 확정됐다.")
        raise SystemExit(1)
    if state == "DEGRADED":
        print(f"::warning::[보조 원천 저하] 컨센서스 {len(consensus_failures)}종목 실패, "
              f"{consensus_done}종목 성공분은 저장됨. 주 산출물은 정상이므로 초록으로 둔다.")
