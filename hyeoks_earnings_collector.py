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
def fetch_year_accounts(corp_code, year):
    """해당 연도의 4개 보고서(1분기/반기/3분기/사업)에서 매출액·영업이익 '누적' 수치를 가져옴."""
    result = {}
    for reprt_code, label in REPRT_CODES:
        try:
            url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
            params = {"crtfc_key": DART_API_KEY, "corp_code": corp_code, "bsns_year": str(year), "reprt_code": reprt_code}
            res = SESSION.get(url, params=params, timeout=10).json()
            if res.get("status") != "000":
                continue  # 013(해당 데이터 없음) 등은 조용히 스킵 — 신규상장/비상장 등 정상적인 경우가 많음

            revenue, op_profit = None, None
            # 연결재무제표(CFS) 우선, 없으면 별도재무제표(OFS)로 대체
            for fs_pref in ["CFS", "OFS"]:
                items = [r for r in res.get("list", []) if r.get("fs_div") == fs_pref and r.get("sj_div") == "IS"]
                if not items:
                    continue
                for r in items:
                    name = str(r.get("account_nm", ""))
                    amt_str = str(r.get("thstrm_amount", "0")).replace(",", "").strip()
                    amt = int(amt_str) if amt_str.lstrip("-").isdigit() else None
                    if revenue is None and ("매출액" in name or name in ("수익(매출액)", "영업수익")):
                        revenue = amt
                    if op_profit is None and "영업이익" in name and "률" not in name and "율" not in name:
                        op_profit = amt
                if revenue is not None or op_profit is not None:
                    break  # 이 fs_div(연결/별도)에서 이미 찾았으면 다른 쪽은 안 봄
            result[label] = {"revenue": revenue, "op_profit": op_profit}
        except Exception as e:
            print(f"⚠️ [DART fetch {corp_code} {year} {label}] {e}")
        time.sleep(0.15)  # DART 호출 과다 방지
    return result


def to_quarterly(year_data):
    """누적 수치를 분기 '단독' 수치로 환산: Q2=H1-Q1, Q3=9M-H1, Q4=FY-9M"""
    def sub(a, b):
        if a is None or b is None:
            return None
        return a - b

    q1, h1, m9, fy = year_data.get("Q1", {}), year_data.get("H1", {}), year_data.get("9M", {}), year_data.get("FY", {})
    quarters = {}
    if q1.get("revenue") is not None:
        quarters["Q1"] = {"revenue": q1["revenue"], "op_profit": q1.get("op_profit")}
    if h1.get("revenue") is not None and q1.get("revenue") is not None:
        quarters["Q2"] = {"revenue": sub(h1["revenue"], q1["revenue"]), "op_profit": sub(h1.get("op_profit"), q1.get("op_profit"))}
    if m9.get("revenue") is not None and h1.get("revenue") is not None:
        quarters["Q3"] = {"revenue": sub(m9["revenue"], h1["revenue"]), "op_profit": sub(m9.get("op_profit"), h1.get("op_profit"))}
    if fy.get("revenue") is not None and m9.get("revenue") is not None:
        quarters["Q4"] = {"revenue": sub(fy["revenue"], m9["revenue"]), "op_profit": sub(fy.get("op_profit"), m9.get("op_profit"))}
    return quarters


def get_recent_quarters(corp_code, num_years=2):
    """최근 num_years년치를 가져와서 시간순으로 정렬된 분기 리스트로 반환."""
    this_year = datetime.datetime.now(KST).year
    all_quarters = []
    for year in range(this_year - num_years, this_year + 1):
        year_data = fetch_year_accounts(corp_code, year)
        if not year_data:
            continue
        q_map = to_quarterly(year_data)
        q_order = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
        for q_name, vals in q_map.items():
            if vals.get("revenue") is not None:
                all_quarters.append({"year": year, "quarter": q_name, "q_sort": q_order[q_name], "revenue": vals["revenue"], "op_profit": vals.get("op_profit")})
    all_quarters.sort(key=lambda x: (x["year"], x["q_sort"]))
    return all_quarters


def summarize(quarters):
    """가장 최근 분기 기준 전분기 대비(QoQ) 증감률과 실적개선여부 계산."""
    if len(quarters) < 2:
        return None
    latest, prev = quarters[-1], quarters[-2]

    rev_growth = None
    if prev["revenue"]:
        rev_growth = round((latest["revenue"] - prev["revenue"]) / abs(prev["revenue"]) * 100, 1)

    op_growth = None
    if latest.get("op_profit") is not None and prev.get("op_profit"):
        op_growth = round((latest["op_profit"] - prev["op_profit"]) / abs(prev["op_profit"]) * 100, 1)

    # 🔎 [수정 여지] "실적개선"의 기준은 일단 "매출 증가 + 이번 분기 영업이익 흑자"로 단순하게 잡음.
    is_improving = (rev_growth is not None and rev_growth > 0) and ((latest.get("op_profit") or 0) > 0)

    return {
        "latest_label": f"{latest['year']}{latest['quarter']}",
        "latest_revenue": latest["revenue"],
        "latest_op_profit": latest.get("op_profit"),
        "rev_growth_pct": rev_growth if rev_growth is not None else "",
        "op_growth_pct": op_growth if op_growth is not None else "",
        "is_improving": "개선" if is_improving else ""
    }


def compute_v3_score(quarters, summary):
    """V1(차트)/V2(수급)와 같은 0~100 스케일의 '실적' 축 점수.
       단일 분기 반짝 성장이 아니라, 여러 분기 이어지는 추세를 더 높게 쳐주도록 설계
       (강의 핵심 — '1~2분기 반짝'과 '구조적 성장'을 구분하려는 목적)."""
    if not summary:
        return 0, ""

    rev_growth = summary["rev_growth_pct"]
    op_growth = summary["op_growth_pct"]
    is_profitable = (summary["latest_op_profit"] or 0) > 0

    # 최근 것부터 거슬러 올라가며 "연속 매출 증가 분기 수" 계산
    consec_rev_growth = 0
    for i in range(len(quarters) - 1, 0, -1):
        cur, pr = quarters[i], quarters[i - 1]
        if cur["revenue"] is not None and pr["revenue"] and cur["revenue"] > pr["revenue"]:
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
    return v3, f"{consec_rev_growth}분기 연속"


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

    header = ["종목코드", "종목명", "최신분기", "매출액", "영업이익", "매출증감률(QoQ,%)", "영업이익증감률(QoQ,%)", "실적개선여부", "V3(실적점수)", "연속성장", "갱신일시"]
    rows_out = [header]
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
    target_codes = list(target_map.keys())

    for idx, code in enumerate(target_codes):
        corp_code = corp_map.get(code)
        if not corp_code:
            print(f"⚠️ [{code}] DART corp_code 매핑 없음 (비상장·최근상장 등) — 스킵")
            continue
        try:
            quarters = get_recent_quarters(corp_code, num_years=2)
            summary = summarize(quarters)
            if not summary:
                print(f"⚠️ [{code}] 실적 데이터 부족 — 스킵")
                continue
            v3_score, streak_label = compute_v3_score(quarters, summary)
            rows_out.append([
                code, target_map.get(code, ""), summary["latest_label"], summary["latest_revenue"], summary["latest_op_profit"],
                summary["rev_growth_pct"], summary["op_growth_pct"], summary["is_improving"], v3_score, streak_label, now_str
            ])
        except Exception as e:
            print(f"⚠️ [{code}] 실적 처리 실패: {e}")
            continue

        if (idx + 1) % 20 == 0:
            print(f"   ...{idx + 1}/{len(target_codes)} 진행 중")

    if len(rows_out) > 1:
        out_sheet.clear()
        out_sheet.update(range_name="A1", values=rows_out, value_input_option="RAW")
        print(f"✅ [DB_실적] {len(rows_out) - 1}개 종목 기록 완료")
    else:
        print("⚠️ 수집된 실적 데이터가 없습니다.")
