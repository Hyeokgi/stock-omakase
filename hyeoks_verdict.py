# -*- coding: utf-8 -*-
# ==========================================================================
# ⚖️ HYEOKS 채널 판정 스냅샷 생성기 (읽기 전용)
# --------------------------------------------------------------------------
# 무엇인가
#   로드맵 §3 이 사전등록한 규칙 그대로 채널별 판정표를 만들어 **박제**한다.
#   9/7 판정일의 산출물이 이것이고, §3-7 이 "그날 값 그대로, 이후 수정 금지"로
#   못박아 둔 바로 그 표다.
#
# 왜 미리 만드는가 (2026-09-04 작성 — 판정일 3거래일 전)
#   결과를 본 뒤에 집계 코드를 짜면, 코드를 짜는 손이 이미 답을 알고 있다.
#   어떤 채널을 넣고 뺄지, 알파를 어떻게 정의할지, 문턱을 어디에 둘지 —
#   전부 결과에 유리한 쪽으로 미세하게 기운다. 그래서 **아직 아무 표도 보기 전에**
#   판정기를 먼저 만든다. Phase 2 때와 같은 순서다.
#   ⚠️ 이 파일을 9/7 이후에 고치고 싶어지면, 그 충동 자체가 신호다. 고치지 말고
#      왜 고치고 싶은지를 로드맵에 적어라.
#
# 규칙 출처 (전부 사전등록분. 여기서 새로 만든 기준은 하나도 없다)
#   §3     호라이즌 — 단기성 T+5 / 리포트중기 T+10 / 리포트장기 T+60
#   §3-1   생존·강화 t≥2.0 / 관찰연장 1.0≤t<2.0 / 폐기 t<1.0 (모두 N≥30 전제)
#          N<30 이면 '판정 불가 → 관찰 연장'
#   §3-3   리포트TOP2_장기는 9/7 에 판정하지 않는다. T+20 중간지표만 본다
#   §3-4-2 순알파 = (종목T+N − 지수T+N) − 0.35%  · 지수벤치는 비용 면제
#          연율 순알파 = 순알파 × (250 / 호라이즌)
#   §3-5   확증(채널 생사)에만 Holm–Bonferroni. **폐기 판정에는 보정을 걸지 않는다**
#   §3-7   N<30 이어도 t<1.0 이고 방향이 음수면 '폐기 후보'로 표시(폐기는 아님)
#   §4-2   Z열(실제캡처거래일)에 '제외' 가 있는 행은 뺀다
#
# 무엇을 하지 않는가
#   · 시트를 **읽기만** 한다. 한 줄도 쓰지 않는다.
#   · 문턱을 조정하지 않는다. 판정을 해석하지 않는다. 표를 만들 뿐이다.
#   · 대조군(랜덤2·랜덤2_배지·지수벤치)은 판정 대상에서 뺀다(§3-1).
# ==========================================================================
import os, sys, math, json, hashlib, argparse, datetime, random

KST = datetime.timezone(datetime.timedelta(hours=9))
# ⚠️ 문서 지정은 **URL 로** 한다. 처음에 open("HYEOKS_주식_자동화") 로 썼다가
#    첫 실행이 그대로 죽었다 — 이름으로 여는 방식은 드라이브 검색에 의존해서
#    이름이 조금만 달라도 실패한다. phase2 가 쓰는 URL 을 그대로 재사용한다.
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SHEET_NAME = "백테스트_로그"
OUT_DIR = "docs"
# 원장 원본 보관 위치.
# ⚠️ [R1 · 2026-09-08] 어제 여기에 "아티팩트로만 남긴다 = 비공개"라고 적었는데 **틀렸다.**
#    이 저장소는 public 이고, `.gitignore` 는 git 커밋만 막을 뿐 아티팩트 접근은 못 막는다.
#    아티팩트 업로드에서 이 디렉터리를 뺐다(verdict.yml 참조).
#    지금 이 파일은 **실행 머신에만** 남고 어디로도 전송되지 않는다.
#    접근 제한 보관소가 생기기 전까지는 공개 쪽에 **SHA256 지문만** 둔다.
LEDGER_DIR = "data/verdict_ledger"


def io_read_self():
    """판정기 자기 자신의 소스. 코드 해시를 남겨 '어느 코드로 판정했나'를 고정한다."""
    with open(os.path.abspath(__file__), "r", encoding="utf-8") as f:
        return f.read()

# ── 시트 열 (BT_HEADER 기준, 0-based) ─────────────────────────────────────
C_TRADE_ID = 0
C_ENTRY_DATE, C_CHANNEL = 1, 2
C_EXCLUDE = 25                       # 실제캡처거래일 = 제외 표식이 남는 칸
STOCK_COL = {1: 17, 3: 18, 5: 19, 10: 20, 20: 26, 60: 27, 120: 28}
INDEX_COL = {1: 21, 3: 22, 5: 23, 10: 24, 20: 29, 60: 30, 120: 31}

# ── 사전등록 상수 ─────────────────────────────────────────────────────────
COST_PCT = 0.35                      # §3-4-2 왕복 수수료·세금 (가정이지 실측 아님)
TRADING_DAYS_YEAR = 250
MIN_N = 30                           # §3-1
T_SURVIVE, T_DISCARD = 2.0, 1.0      # §3-1
CONTROL = "랜덤2"                    # 비교 기준 대조군
CONTROL_LIKE = ("랜덤2", "랜덤2_배지", "지수벤치")   # 판정 대상에서 제외
LONG_CHANNEL = "리포트TOP2_장기"     # §3-3 특칙

# §3-5-1 (2026-09-11 사전등록) — 확증 집합은 **닫혀 있다.**
# 편입 기준은 "9/7 판정 시점에 N≥30 이었는가"(수익률이 아니라 적재량)이고,
# 나중에 30건을 넘긴 채널은 들어오지 않는다. m 도 고정이다 — 채널이 죽어도
# 줄이지 않는다. 줄이면 문턱이 저절로 느슨해지기 때문이다.
CONFIRMATORY = ("차트TOP2", "수급TOP2")
HOLM_M = len(CONFIRMATORY)

HORIZON = {                          # §3 채널 성격별 고정 호라이즌
    "차트TOP2": 5, "수급TOP2": 5, "랜덤2": 5, "랜덤2_배지": 5,
    "리포트TOP2_단기": 5, "리포트TOP2_중기": 10, "리포트TOP2_장기": 60,
}
DEFAULT_HORIZON = 5


# ── 통계 (scipy 없이) ─────────────────────────────────────────────────────
def _betacf(a, b, x):
    """연분수 전개. Numerical Recipes 의 표준 구현."""
    TINY, EPS, MAXIT = 1e-30, 3e-16, 300
    qab, qap, qam = a + b, a + 1.0, a - 1.0
    c, d = 1.0, 1.0 - qab * x / qap
    if abs(d) < TINY:
        d = TINY
    d = 1.0 / d
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < TINY: d = TINY
        c = 1.0 + aa / c
        if abs(c) < TINY: c = TINY
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < TINY: d = TINY
        c = 1.0 + aa / c
        if abs(c) < TINY: c = TINY
        d = 1.0 / d
        de = d * c
        h *= de
        if abs(de - 1.0) < EPS:
            break
    return h


def betai(a, b, x):
    """정규화 불완전 베타 함수 I_x(a,b)."""
    if x <= 0.0: return 0.0
    if x >= 1.0: return 1.0
    lbeta = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
             + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return math.exp(lbeta) * _betacf(a, b, x) / a
    return 1.0 - math.exp(lbeta) * _betacf(b, a, 1.0 - x) / b


def t_two_sided_p(t, df):
    """Student t 양측 p 값. Holm 보정(§3-5)이 p 를 요구하므로 필요하다."""
    if df <= 0 or not math.isfinite(t):
        return 1.0
    return betai(0.5 * df, 0.5, df / (df + t * t))


def welch(a, b):
    """Welch t 와 자유도. 표본이 1개 이하면 (None, None)."""
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        return None, None
    ma, mb = sum(a) / na, sum(b) / nb
    va = sum((x - ma) ** 2 for x in a) / (na - 1)
    vb = sum((x - mb) ** 2 for x in b) / (nb - 1)
    se2 = va / na + vb / nb
    if se2 <= 0:
        return None, None
    t = (ma - mb) / math.sqrt(se2)
    num = se2 ** 2
    den = (va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1)
    return t, (num / den if den > 0 else None)


# ── 시트 → 채널별 순알파 ──────────────────────────────────────────────────
def _num(v):
    """숫자 파싱. **비유한(nan/inf)은 None 으로 돌려준다.**

    🚨 [F07 · 2026-09-07 감사] 원래는 float() 결과를 그대로 냈다. 그런데
       `float("nan")` 은 예외를 안 내고 **None 도 아니다.** 그래서 원장에 'nan' 이
       들어 있으면 성숙값으로 세어지고, 평균·분산이 통째로 nan 이 된다.
       판정표 전체가 조용히 무의미해지는 경로였다."""
    try:
        f = float(str(v).replace(",", "").replace("%", "").strip())
    except Exception:
        return None
    return f if math.isfinite(f) else None


# ── F07 1차 — 원장 입력 검증 (2026-09-07 감사) ────────────────────────────
#
# 감사 지적 그대로다 — 판정기가 원장을 **아무 검사 없이** 집계하고 있었다.
#   · 같은 trade_id 가 두 번 있으면 N=2 로 센다(중복 거부 없음)
#   · float("nan") 은 None 이 아니라 성숙값에 포함된다
#   · 헤더·날짜 형식 검사가 없다
#   · 결과 Markdown 만 저장하고 원장은 남기지 않아, 나중에 "그때 무엇을 봤나"를 복원 못 한다
#   · 같은 날 재실행하면 박제 파일을 덮어쓴다
#
# 방침 — **조용히 고치지 않는다.** 중복은 삭제가 아니라 **충돌 보고 후 판정 중단**이다
# (감사 권고). 어떤 행을 살릴지는 사람이 원장을 보고 정해야 할 문제다.

ABORT, WARN = "ABORT", "WARN"
# 원장 지문 계산 방식. R3(2026-09-08) 이전은 "tab-join-v1" 이었고 값이 다르다.
LEDGER_SHA_ALGO = "canonical-json-v2"
SANE_RETURN_ABS = 500.0      # |수익률| 상한(%). 넘으면 이상치로 보고만 한다

# ── R2 (2026-09-08 재검증) — **읽는 열의 이름을 전부 검증한다** ──────────────
#
# 어제는 헤더 길이와 0번이 'trade_id' 인지만 봤다. 재검증이 재현한 구멍:
#   · 종목T+5 와 지수T+5 의 **열과 값을 맞바꿔도 무경고** (순알파 +1.65% → −2.35%)
#   · 수익률 셀이 `#REF!` 여도 무경고로 집계에서 빠짐
#   · 진입일 `2099-01-01` 인데 수익률이 있으면 무경고로 **성숙 표본에 포함**
#   · 진입일 공란인데 수익률이 있어도 동일
# 열 이름이 자리와 맞는지 확인하면 첫 번째가 잡히고, 나머지는 아래 검사가 맡는다.
EXPECTED_HEADER = {
    C_TRADE_ID: "trade_id", C_ENTRY_DATE: "진입일", C_CHANNEL: "채널",
    C_EXCLUDE: "실제캡처거래일",
    17: "종목T+1", 18: "종목T+3", 19: "종목T+5", 20: "종목T+10",
    21: "지수T+1", 22: "지수T+3", 23: "지수T+5", 24: "지수T+10",
    26: "종목T+20", 27: "종목T+60", 28: "종목T+120",
    29: "지수T+20", 30: "지수T+60", 31: "지수T+120",
}

# 성숙 판정은 **거래일 기준**이어야 하지만 이 스크립트에는 거래일 달력이 없다.
# 달력일 근사로 하되 넉넉히 잡아 **거짓 경보를 줄이고**, 근사임을 리포트에 밝힌다.
#   5거래일 ≈ 7달력일이므로 여유를 둬 1.6배 + 3일.
def calendar_margin(h):
    return int(h * 1.6) + 3


def weekdays_between(d, today):
    """진입일 **다음 날부터** 기준일까지의 평일 수.

    거래일 달력이 없으므로 평일 수를 **상한**으로 쓴다. 휴장일은 평일이어도
    거래가 없으니 실제 거래일 수는 이보다 작거나 같다. 따라서
    **평일 수 < H 이면 T+H 는 확실히 도달할 수 없다.**
    반대로 평일 수가 충분하다고 성숙을 확정할 수는 없다(추석·대체공휴일).
    """
    if today <= d:
        return 0
    n, cur = 0, d + datetime.timedelta(days=1)
    while cur <= today:
        if cur.weekday() < 5:
            n += 1
        cur += datetime.timedelta(days=1)
    return n


def maturity_note(entry_date, h, today):
    """성숙 상태를 세 갈래로 나눈다(재검증 R2 요구).

    NOT_MATURED          — 아직 T+h 가 안 지났다. 값이 없는 게 정상
    MATURED_DATA_MISSING — 지났는데 값이 없다. **데이터 결손**이지 표본 부족이 아니다
    INVALID              — 진입일이 미래이거나 형식이 깨졌다

    ⚠️ 거래일 달력이 없어 **달력일 근사**다. 경계에서는 판정을 유보한다(None).
    """
    if not entry_date:
        return "INVALID"
    try:
        d = datetime.datetime.strptime(entry_date, "%Y-%m-%d").date()
    except ValueError:
        return "INVALID"
    if d > today:
        return "INVALID"
    gap = (today - d).days
    if gap < h:                       # 달력일조차 h 를 못 채웠으면 확실히 미성숙
        return "NOT_MATURED"
    # 🚨 [2-2 · 3차 재검증] 달력일만 보면 **확실히 조기인 값이 경계로 새어 나갔다.**
    #    재현 — 기준일 9/8 · 진입 9/3 · T+5. 달력일 5 라 위 검사를 통과하고
    #    margin 11 미만이라 '경계 유보(None)' 가 되어 순알파 +1.65% 가 N 에 들어갔다.
    #    그런데 9/4·9/7·9/8 로 **평일이 3일뿐**이라 T+5 는 애초에 불가능하다.
    #    평일 수는 거래일 수의 상한이므로 이 검사는 거짓 차단을 만들지 않는다.
    if weekdays_between(d, today) < h:
        return "NOT_MATURED"
    if gap >= calendar_margin(h):     # 넉넉히 지났으면 확실히 성숙 가능
        return "MATURED"
    # 경계 — 판정 유보. ⚠️ 평일 수가 충분해도 휴장일 때문에 성숙을 확정할 수 없다.
    #    거래소 달력이 붙기 전까지 남는 한계다.
    return None


def validate_ledger(rows, today=None):
    """원장을 집계하기 **전에** 검사한다. (심각도, 항목, 상세) 목록을 돌려준다."""
    issues = []
    today = today or datetime.datetime.now(KST).date()
    if not rows or len(rows) < 2:
        issues.append((ABORT, "원장 비어 있음", f"행 {len(rows)}개"))
        return issues

    header = [str(c).strip() for c in rows[0]]
    need = max(EXPECTED_HEADER)
    if len(header) <= need:
        issues.append((ABORT, "헤더 열 부족",
                       f"{len(header)}열 · 최소 {need + 1}열 필요"))
        return issues                 # 열이 모자라면 이름 검사는 의미가 없다

    # R2 ① — 읽는 열의 **이름이 자리와 맞는가**. 열 교환·삽입·삭제를 여기서 잡는다.
    mism = [(i, EXPECTED_HEADER[i], header[i])
            for i in sorted(EXPECTED_HEADER) if header[i] != EXPECTED_HEADER[i]]
    if mism:
        issues.append((ABORT, "헤더 이름 불일치(스키마 변경 의심)",
                       "; ".join(f"{i}번='{got}' (기대 '{want}')"
                                 for i, want, got in mism[:6])
                       + (f" 외 {len(mism) - 6}건" if len(mism) > 6 else "")))

    RET_COLS = set(STOCK_COL.values()) | set(INDEX_COL.values())
    seen, dup, no_id, bad_date, future, nonfinite, extreme = {}, [], 0, [], [], [], []
    nonnum, matured_missing, early_val = [], [], []
    pair_missing = []
    blank_date_val, broken_date_val = 0, []

    for i, row in enumerate(rows[1:], start=2):
        if len(row) <= C_CHANNEL:
            continue
        tid = str(row[C_TRADE_ID]).strip() if len(row) > C_TRADE_ID else ""
        if not tid:
            no_id += 1
        elif tid in seen:
            dup.append((tid, seen[tid], i))
        else:
            seen[tid] = i

        d_raw = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        d_ok = False
        if d_raw:
            try:
                dd = datetime.datetime.strptime(d_raw, "%Y-%m-%d").date()
                d_ok = True
                if dd > today:
                    future.append((i, d_raw))
            except ValueError:
                bad_date.append((i, d_raw))

        ch = str(row[C_CHANNEL]).strip()
        h = HORIZON.get(ch, DEFAULT_HORIZON)
        has_any_val = False

        for col in RET_COLS:
            if len(row) <= col:
                continue
            raw = str(row[col]).strip()
            if not raw:
                continue
            has_any_val = True
            try:
                f = float(raw.replace(",", "").replace("%", ""))
            except Exception:
                # R2 ② — `#REF!` 같은 수식 오류. 조용히 건너뛰지 않는다.
                nonnum.append((i, col, raw[:12]))
                continue
            if not math.isfinite(f):
                nonfinite.append((i, col, raw))
            elif abs(f) > SANE_RETURN_ABS:
                extreme.append((i, col, f))

        # R2 ③④ — 진입일을 믿을 수 없는데 값이 있으면 그대로 성숙 표본에 섞인다.
        #   collect() 는 진입일을 **한 번도 보지 않는다**(위 함수 확인). 그래서
        #   날짜가 공란이든 형식이 깨졌든, 값만 있으면 평균·t 에 그대로 들어간다.
        #   ⚠️ 날짜 형식 이상 자체는 여전히 WARN 이다 — 값이 없는 행은
        #      어차피 집계에 안 들어가므로 판정을 멈출 이유가 없다.
        #      **값이 같이 있을 때만** 중단으로 올린다.
        if has_any_val and not d_ok:
            if d_raw:
                broken_date_val.append((i, d_raw))
            else:
                blank_date_val += 1

        # R2 — 성숙/결손 구분
        #   ⚠️ §4-2 로 **제외된 행은 빼고 본다.** 실측(2026-09-08)에서 이 검사가
        #      행271 랜덤2(2026-07-22)를 "성숙했는데 값이 없다"로 잡았는데,
        #      제외 표식이 붙은 행이 정확히 1행이었다. 거래정지 같은 사유로 제외한
        #      행은 T+5 값이 없는 것이 **정상**이다 — collect() 도 그 행을 안 센다.
        #      집계에 안 들어가는 행을 "데이터 결손"이라 부르면 경고가 무뎌진다.
        #      (무결성 검사 — 중복 id·비유한 수치·미래 날짜 — 는 제외 행에도
        #       그대로 적용한다. 그건 집계 여부와 무관한 시트 손상 신호다.)
        if d_ok and ch and not ch.startswith("지수벤치") and not is_excluded(row):
            # ⚠️ [B · 2차 재검증] 채널 **기본 기간만** 보고 있었다. 랜덤2 는 여러
            #    비교 기간에 쓰이므로 나머지 기간은 검사되지 않았다.
            #    판정에 실제로 쓰이는 모든 채널×기간을 본다.
            if ch == CONTROL:
                hs = set(HORIZON.values()) | {DEFAULT_HORIZON, 20}
            elif ch == LONG_CHANNEL:
                hs = {h, 20}
            else:
                hs = {h}
            for hh in sorted(hs):
                note = maturity_note(d_raw, hh, today)
                si, ii = STOCK_COL.get(hh), INDEX_COL.get(hh)
                if si is None or ii is None:
                    continue
                has_s = len(row) > si and str(row[si]).strip() != ""
                has_i = len(row) > ii and str(row[ii]).strip() != ""
                if note == "MATURED" and not (has_s or has_i):
                    matured_missing.append((i, f"{ch}/T+{hh}", d_raw))
                elif note == "NOT_MATURED" and (has_s or has_i):
                    early_val.append((i, f"{ch}/T+{hh}", d_raw))
                elif has_s != has_i:
                    # 한쪽만 있는 쌍 — 예전에는 조용히 집계에서 빠졌다
                    pair_missing.append((i, f"{ch}/T+{hh}", d_raw))

    if dup:
        issues.append((ABORT, "trade_id 중복",
                       "; ".join(f"{t} (행 {a}, {b})" for t, a, b in dup[:5])
                       + (f" 외 {len(dup) - 5}건" if len(dup) > 5 else "")))
    if nonfinite:
        issues.append((ABORT, "비유한 수치(nan/inf)",
                       "; ".join(f"행{i} 열{c}='{v}'" for i, c, v in nonfinite[:5])))
    if nonnum:
        issues.append((ABORT, "수익률 셀에 숫자가 아닌 값(수식 오류 의심)",
                       "; ".join(f"행{i} 열{c}='{v}'" for i, c, v in nonnum[:5])
                       + (f" 외 {len(nonnum) - 5}건" if len(nonnum) > 5 else "")))
    if future:
        issues.append((ABORT, "진입일이 미래",
                       "; ".join(f"행{i}='{d}'" for i, d in future[:5])))
    if blank_date_val:
        issues.append((ABORT, "진입일 공란인데 수익률이 있는 행",
                       f"{blank_date_val}행 — 집계는 진입일을 보지 않으므로 "
                       "성숙 여부를 확인할 길 없이 표본에 섞인다"))
    if broken_date_val:
        issues.append((ABORT, "진입일 형식이 깨졌는데 수익률이 있는 행",
                       "; ".join(f"행{i}='{d}'" for i, d in broken_date_val[:5])
                       + (f" 외 {len(broken_date_val) - 5}건"
                          if len(broken_date_val) > 5 else "")))
    if no_id:
        issues.append((ABORT, "trade_id 빈 행",
                       f"{no_id}행 — 실거래 행이면 중복 판정이 불가능하므로 중단한다"))
    if bad_date:
        issues.append((WARN, "진입일 형식 이상",
                       "; ".join(f"행{i}='{d}'" for i, d in bad_date[:5])))
    if extreme:
        issues.append((WARN, f"|수익률| > {SANE_RETURN_ABS:.0f}%",
                       "; ".join(f"행{i} 열{c}={v:+.1f}%" for i, c, v in extreme[:5])))
    if matured_missing:
        issues.append((WARN, "성숙했는데 값이 없음(MATURED_DATA_MISSING)",
                       f"{len(matured_missing)}행 — 표본 부족이 아니라 **데이터 결손**이다. "
                       + "; ".join(f"행{i} {c}({d})" for i, c, d in matured_missing[:3])))
    if pair_missing:
        issues.append((WARN, "종목값·지수값 중 한쪽만 있음(쌍결손)",
                       f"{len(pair_missing)}건 — 순알파를 만들 수 없어 집계에서 빠진다. "
                       "'미성숙'과 구분해 세어야 한다. "
                       + "; ".join(f"행{i} {c}({d})" for i, c, d in pair_missing[:3])))
    if early_val:
        issues.append((WARN, "아직 미성숙인데 값이 있음(집계에서 제외됨)",
                       f"{len(early_val)}행 — "
                       + "; ".join(f"행{i} {c}({d})" for i, c, d in early_val[:3])))
    return issues


def sha256_of(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ledger_fingerprint(rows):
    """원장 내용의 지문. 행 순서까지 포함한다 — 같은 데이터면 같은 값이 나와야 한다.

    ⚠️ [R3 정정 2026-09-08] 예전 구현은 셀을 탭으로, 행을 줄바꿈으로 이어 붙였다.
       셀 값 안에 탭이나 줄바꿈이 들어가는 순간 그 구분자가 **경계와 구별되지
       않아서**, 서로 다른 원장이 같은 지문을 냈다:
           [["a\tb", "c"]]  와  [["a", "b\tc"]]  →  같은 해시
       메모 열에 줄바꿈이 든 행은 이 원장에 실제로 있을 수 있다. 지문은 "이 판정이
       무엇을 보고 나왔나"를 고정하는 유일한 근거이므로, 충돌 가능성이 남아 있으면
       근거 구실을 못 한다. JSON 은 셀 안의 탭·줄바꿈·따옴표를 모두 이스케이프하고
       셀 경계를 대괄호·쉼표로 따로 표시하므로 그 모호함이 없다.

    ⚠️ 이 변경으로 **지문 값 자체가 달라진다.** 2026-09-07 판정표에 박혀 있는
       원장 SHA256 은 옛 방식(tab-join-v1)으로 계산된 값이고, 이 코드로는 다시
       나오지 않는다. 그래서 방식 이름을 같이 기록한다 — 지문을 비교할 때는
       **어느 방식으로 계산했는지부터** 맞춰야 한다.
    """
    canon = json.dumps([[str(c) for c in r] for r in rows],
                       ensure_ascii=False, separators=(",", ":"))
    return sha256_of(canon)


def run_bundle_id(ledger_sha, code_sha, now=None):
    """실행 하나를 가리키는 이름. 시각 + 원장·코드 지문."""
    now = now or datetime.datetime.now(KST)
    return f"{now.strftime('%Y%m%dT%H%M%S')}_{ledger_sha[:8]}_{code_sha[:8]}"


def save_run_bundle(base_dir, run_id, meta, rows, report_md):
    """판정 하나의 근거를 **한 디렉터리에 묶어** 배타 생성한다.

    ⚠️ [R3 2026-09-08] 예전에는 `판정_{날짜}_ledger.json` 이라는 날짜 고정
       이름에 `"w"` 로 썼다. 판정표는 배타 생성으로 막아 놓고 근거 파일만
       같은 날 재실행 때 조용히 덮어써진 것이다 — 두 번째 실행이 첫 번째가
       무엇을 봤는지를 지운다.

    ⚠️ 이 함수는 main() 안에 인라인으로 있었고, 그래서 `--self-test` 가
       **한 줄도 지나가지 못했다.** 검증 못 하는 코드에 결함이 숨는다는 것이
       이번 감사가 반복해서 짚은 지점이라 밖으로 뺐다.

    이미 있으면 FileExistsError 를 그대로 올린다 — 근거를 덮어쓰지 않는다.
    """
    run_dir = os.path.join(base_dir, run_id)
    os.makedirs(run_dir)              # exist_ok 없음 = 배타 생성
    writes = (("meta.json", json.dumps(meta, ensure_ascii=False, indent=2)),
              ("ledger.json", json.dumps({"rows": rows}, ensure_ascii=False)),
              ("판정.md", report_md))
    for name, payload in writes:
        with open(os.path.join(run_dir, name), "x", encoding="utf-8") as f:
            f.write(payload)
    return run_dir


def is_excluded(row):
    """§4-2 — Z열에 '제외' 두 글자가 있으면 뺀다.

    ⚠️ 처음엔 여기에 `and "집계복귀" not in memo` 를 덧붙였다. 더 안전해 보였지만
       **틀린 판단이었다.** 같은 규칙이 `omakase.py:857` 과
       `hyeoks_phase2_exit.py:165` 에도 있는데 둘 다 단순 포함 검사다.
       나만 다르게 굴면 같은 행을 어떤 도구는 세고 어떤 도구는 빼게 된다 —
       집계가 도구마다 갈리는 것이 결측보다 훨씬 나쁘다.
       §4-2 가 등록한 규칙도 "'제외' 두 글자가 들어가면 뺀다"이고, 철회는
       '제외' 가 안 들어가는 '집계복귀…' 로 쓰기로 정해 두었다. 규칙을 따른다.
       (남은 위험: '집계복귀 — 이전 제외 철회' 처럼 두 단어가 같이 든 문구를
        쓰면 세 도구 모두 그 행을 뺀다. 그런 문구를 쓰지 않는 것이 규약이다.)"""
    return "제외" in (str(row[C_EXCLUDE]) if len(row) > C_EXCLUDE else "")


def collect(rows, today=None):
    """채널 → {horizon: [순알파...]}. 비용은 §3-4-2 대로 여기서 뺀다.

    ⚠️ 리허설(2026-09-04)에서 두 가지 결함이 드러나 고친 버전이다.

    ① 대조군을 **자기 호라이즌에서만** 모으고 있었다. 그래서 리포트중기(T+10)를
       검정할 때 비교할 랜덤2 T+10 이 없어 t 가 통째로 '—' 로 나왔다.
       → 대조군은 **쓰이는 모든 호라이즌**에서 모은다.

    ② 아직 호라이즌에 도달하지 않은 채널이 **표에서 통째로 사라졌다**.
       → 원시 행수(raw)를 따로 세어 N=0 이어도 표에 남긴다.

    🚨 [B · 2026-09-08 2차 재검증] **성숙 검사가 집계와 끊겨 있었다.**

       `validate_ledger` 가 "아직 미성숙인데 값이 있다"를 경고로 찍어도 이 함수는
       그 값을 **그대로 평균과 t 에 넣었다.** 경고는 사람이 읽는 텍스트일 뿐
       집계를 막지 못했다. 재현 — 9/8 진입 · 기준일 9/8 · T+5 값 존재 →
       WARN 은 떴지만 순알파 +1.65% 로 N 에 포함됐다.
       → 성숙 판정을 **여기서 직접** 하고, 확실히 조기인 값은 **집계에서 뺀다.**

       그리고 종목값·지수값을 **쌍으로** 본다. 예전에는 지수값만 비면 `continue` 로
       조용히 빠졌고 그 행은 '미성숙'으로 세어졌다. 성숙했는데 한쪽이 빈 것은
       미성숙이 아니라 **데이터 결손**이고, 둘은 뜻이 완전히 다르다.

       ⚠️ 성숙 판정은 여전히 **달력일 근사**다(`maturity_note`). 경계에서 `None`
          을 돌려주면 **집계에 넣는다** — 근사 때문에 진짜 값을 버리는 쪽이 더
          나쁘기 때문이다. 확실히 조기인 경우(NOT_MATURED)만 뺀다.
          거래일 달력이 붙기 전까지는 이것이 한계이고, 그 사실을 리포트에 적는다.
    """
    horizons_in_use = set(HORIZON.values()) | {DEFAULT_HORIZON, 20}
    out, raw = {}, {}
    # §3-5-2 — 같은 날 짝짓기용. out 과 **같은 조건에서 같은 값**을 쌓는다.
    # 별도 루프를 만들면 두 경로가 조용히 갈라지므로 여기서 같이 넣는다.
    bydate = {}
    today = today or datetime.datetime.now(KST).date()
    # 🚨 [2-1 · 3차 재검증] 예전에는 새 카운터를 올리고도 **미성숙까지 같이** 올렸다.
    #    한 행이 '쌍결손 1 + 미성숙 1' 로 두 번 세어져 "미성숙과 분리했다"는 말이
    #    사실이 아니었다. 평균은 안 바뀌지만 **누락 사유와 표본 수 설명이 틀려진다.**
    #    → 행마다 **기본 기간의 상태를 하나만** 매긴다(상호배타).
    skipped = {"제외표식": 0, "채널없음": 0, "집계됨": 0,
               "미성숙(호라이즌 미도달)": 0, "조기값(성숙 전·집계 제외)": 0,
               "쌍결손(한쪽만 있음)": 0, "성숙결측(둘 다 없음)": 0}
    # 행×기간 단위 진단. 랜덤2 한 행은 여러 기간을 타므로 **행 수와 다르다.**
    detail = {"조기값(행×기간)": 0, "쌍결손(행×기간)": 0, "성숙결측(행×기간)": 0}
    for row in rows[1:]:
        if len(row) <= C_CHANNEL:
            continue
        if is_excluded(row):
            skipped["제외표식"] += 1
            continue
        ch = str(row[C_CHANNEL]).strip()
        if not ch:
            skipped["채널없음"] += 1
            continue
        raw[ch] = raw.get(ch, 0) + 1
        h = HORIZON.get(ch, DEFAULT_HORIZON)
        d_raw = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        if ch == CONTROL:
            want = horizons_in_use
        elif ch == LONG_CHANNEL:
            want = {h, 20}
        else:
            want = {h}
        primary = "미성숙(호라이즌 미도달)"   # 기본 기간의 상태. 하나만 남는다.
        for hh in sorted(want):
            si, ii = STOCK_COL.get(hh), INDEX_COL.get(hh)
            if si is None or len(row) <= max(si, ii):
                continue
            s_, i_ = _num(row[si]), _num(row[ii])
            note = maturity_note(d_raw, hh, today) if d_raw else "INVALID"
            state = None
            if note == "NOT_MATURED" and (s_ is not None or i_ is not None):
                state = "조기값(성숙 전·집계 제외)"
                detail["조기값(행×기간)"] += 1
            elif (s_ is None) != (i_ is None):
                state = "쌍결손(한쪽만 있음)"
                detail["쌍결손(행×기간)"] += 1
            elif s_ is None:
                if note == "MATURED":
                    state = "성숙결측(둘 다 없음)"
                    detail["성숙결측(행×기간)"] += 1
            else:
                cost = 0.0 if ch.startswith("지수벤치") else COST_PCT
                val = s_ - i_ - cost
                out.setdefault(ch, {}).setdefault(hh, []).append(val)
                if d_raw:
                    bydate.setdefault(ch, {}).setdefault(hh, {}) \
                          .setdefault(d_raw, []).append(val)
                state = "집계됨"
            if hh == h and state is not None:
                primary = state
        skipped[primary] += 1
    skipped.update(detail)
    return out, raw, skipped, bydate


def reconcile(rows, skipped):
    """행 수가 맞는지 센다. 상태가 상호배타라면 합계가 원시 행수와 같아야 한다.

    [2-1] 이 검사가 없어서 '쌍결손 1 + 미성숙 1' 중복이 자기검증을 통과했다.
    행×기간 진단 항목은 행 수가 아니므로 합계에서 뺀다.
    """
    data_rows = sum(1 for r in rows[1:] if len(r) > C_CHANNEL)
    total = sum(v for k, v in skipped.items() if "행×기간" not in k)
    return data_rows, total, data_rows == total


def verdict_for(ch, n, t, mean):
    """§3-1 + §3-7. 문턱은 사전등록값 그대로. 여기서 조정하지 않는다."""
    if ch == LONG_CHANNEL:
        return "판정 안 함(§3-3 장기 특칙)"
    if n < MIN_N:
        base = "판정 불가 → 관찰 연장"
        if t is not None and t < T_DISCARD and mean is not None and mean < 0:
            base += " · ⚠️폐기 후보(§3-7)"
        return base
    if t is None:
        return "판정 불가(t 계산 불가)"
    if t >= T_SURVIVE:
        return "생존·강화(Holm 확인 필요)"
    if t >= T_DISCARD:
        return "관찰 연장"
    return "폐기"


def paired_by_date(bydate, ch, h, ctrl=CONTROL):
    """§3-5-2 — 같은 날 짝지어 재는 1표본 t.

    두 채널은 **같은 날** 매매한다. 그날 시장이 통째로 오르내린 몫은 두 채널에
    똑같이 실리므로 **차이에서 상쇄된다.** Welch 는 그 몫을 양쪽 분산에 그대로
    넣고 재기 때문에 표준오차를 부풀린다 — 같은 효과를 작은 t 로 보고한다.

    절차:
      1. 두 채널 모두 값이 있는 날만 남긴다 (한쪽만 있는 날은 짝이 안 된다)
      2. 그날의 채널 평균끼리 뺀다  → diff_d
      3. diff 에 1표본 t 를 건다. **df = 날짜수 − 1** (행수가 아니다)

    ⚠️ 이 검정은 자유도를 잃는다(77행 → 날짜 수). 분산이 줄어드는 이득과
       자유도가 주는 손해 중 어느 쪽이 큰지는 **실원장을 돌려봐야 안다.**
       더 좋다고 가정하지 않는다.

    돌려주는 값: (t, df, 날짜수, 평균차) — 짝이 2일 미만이면 전부 None.
    """
    a, b = bydate.get(ch, {}).get(h, {}), bydate.get(ctrl, {}).get(h, {})
    days = sorted(set(a) & set(b))
    diffs = [sum(a[d]) / len(a[d]) - sum(b[d]) / len(b[d]) for d in days]
    k = len(diffs)
    if k < 2:
        return None, None, k, None
    mean = sum(diffs) / k
    var = sum((x - mean) ** 2 for x in diffs) / (k - 1)
    if var <= 0:
        return None, None, k, mean
    return mean / math.sqrt(var / k), k - 1, k, mean


def t_one_sided_p(t, df):
    """§3-5-4 — **우월성 단측** p. 방향은 '채널 > 대조군' 으로 고정이다.

    두 질문을 섞지 않는다.
      · 채널이 랜덤보다 **나은가** → 채택 판단. 이 검정이 답한다
      · 채널이 랜덤보다 **나쁜가** → 폐기 판단. §3-1 의 `t<1.0` 이 따로 답하고
        거기에는 애초에 보정을 걸지 않는다

    양측은 우리가 절대 행동하지 않을 쪽(채널이 유의하게 나쁨 → 그래도 채택 안 함)에
    α 의 절반을 쓴다. t 가 음수면 우월성 근거가 아니므로 p 는 1 쪽으로 간다.
    """
    if t is None or not df:
        return None
    two = t_two_sided_p(t, df)
    if two is None:
        return None
    return two / 2.0 if t > 0 else 1.0 - two / 2.0


def holm(pairs, m=None):
    """§3-5 계층1 — Holm–Bonferroni. pairs = [(채널, p)]. 통과 집합을 돌려준다.

    처음 실패하는 지점에서 멈추고 그 뒤는 전부 탈락(문서 절차 4번 그대로).
    `m` 은 §3-5-1 이 고정한 확증 집합 크기다. 생략하면 pairs 길이를 쓴다."""
    if m is None:
        m = len(pairs)
    ok = set()
    for i, (ch, p) in enumerate(sorted(pairs, key=lambda x: x[1]), start=1):
        if p <= 0.05 / (m - i + 1):
            ok.add(ch)
        else:
            break
    return ok


# ── 리포트 ────────────────────────────────────────────────────────────────
# §3-5-5 — 주력(탐색) 갈래의 1차 문턱 감시 대상. §0-1 의 주력 정의를 따른다.
EXPLORATORY = ("리포트TOP2_단기", "리포트TOP2_중기", "리포트TOP2_장기")


def milestone_rows(data, raw, bydate, today):
    """탐색 1차(§3-5 계층2) 도달 여부를 센다. **판정이 아니라 관측이다.**

    1차 = `N ≥ 30` 이고 `t ≥ 2.0` (다중비교 보정 없음). t 는 §3-5-2 의 짝 t 다.
    N 은 §3-1 정의 그대로 **행 수**이고, 짝 t 의 자유도는 날짜 수라 둘이 다르다.
    둘을 섞지 않고 **나란히 적는다** — 같은 'N' 이라는 말이 두 가지를 뜻하면
    언젠가 한쪽 숫자로 다른 쪽 문턱을 넘었다고 말하게 된다.
    """
    ctrl = data.get(CONTROL, {})
    out = []
    for ch in EXPLORATORY:
        h = HORIZON.get(ch, DEFAULT_HORIZON)
        vals = data.get(ch, {}).get(h, [])
        n = len(vals)
        t, df, k, mean = paired_by_date(bydate, ch, h) if bydate else (None, None, 0, None)
        if t is None:                      # 짝이 안 서면 Welch 로 적되 그렇다고 밝힌다
            t, df = welch(vals, ctrl.get(h, []))
            stat = "Welch(짝 미성립)"
        else:
            stat = "짝"
        hit = (n >= MIN_N and t is not None and t >= T_SURVIVE)
        need = n * (T_SURVIVE / t) ** 2 if (t and t > 0 and n) else None

        # ── 왜 Welch 와 짝 t 가 다른가를 분해한다 ──────────────────────────
        # Welch 는 대조군의 **전체** 표본을 쓴다. 대조군이 채널보다 긴 기간에
        # 걸쳐 있으면, 그 바깥 날짜의 성적이 비교에 섞여 들어온다.
        cvals = ctrl.get(h, [])
        a_days = bydate.get(ch, {}).get(h, {}) if bydate else {}
        b_days = bydate.get(CONTROL, {}).get(h, {}) if bydate else {}
        both = sorted(set(a_days) & set(b_days))
        avg = lambda xs: (sum(xs) / len(xs)) if xs else None
        ch_all = avg(vals)
        ct_all = avg(cvals)
        ch_pair = avg([v for d in both for v in a_days[d]])
        ct_pair = avg([v for d in both for v in b_days[d]])
        out.append({
            "ch": ch, "h": h, "n": n, "raw": raw.get(ch, 0), "k": k,
            "t": t, "mean": mean, "stat": stat, "hit": hit, "need": need,
            "ch_all": ch_all, "ct_all": ct_all, "ch_pair": ch_pair, "ct_pair": ct_pair,
            "ct_n": len(cvals), "ct_days": len(b_days),
            "ch_span": (min(a_days), max(a_days)) if a_days else None,
            "ct_span": (min(b_days), max(b_days)) if b_days else None,
            "outside": sorted(set(b_days) - set(a_days)),
            # 채널 **자기 구간 안**에서 며칠을 빠뜨렸나. 구간 밖은 세지 않는다 —
            # 그건 채널이 아직 시작 안 했거나 아직 성숙 안 한 날이다.
            "inside_gap": (sorted(d for d in b_days
                                  if min(a_days) <= d <= max(a_days) and d not in a_days)
                           if a_days and b_days else []),
            "span_days": (len([d for d in b_days if min(a_days) <= d <= max(a_days)])
                          if a_days and b_days else 0),
        })
    return out


def milestone_report(rows, today, scheduled):
    L = [f"# 🎯 주력 1차 문턱 관측 — {today}", ""]
    A = L.append
    A("> **판정이 아니다.** §3-5 계층 2 의 1차 문턱(`N≥30` 이고 `t≥2.0`, **보정 없음**)에")
    A("> 얼마나 왔는지만 센다. 1차를 넘어도 채택이 아니다 — 그다음 4주 새 표본에서")
    A("> **재현**해야 한다(§3-5 계층 2).")
    A("")
    if scheduled:
        A("✅ **정기 관측이다.** §3-5-5 에 따라 이 기록만 1차 달성일을 확정할 수 있다.")
    else:
        A("⚠️ **수동 실행이다. 참고용이며 1차를 확정하지 않는다**(§3-5-5).")
        A("> 아무 때나 들여다보고 넘은 날을 1차로 잡으면 **날짜를 데이터가 고르게 된다.**")
        A("> 1차 달성일은 4주 재현 창의 시작점이라 그 편향이 재현까지 오염시킨다.")
    A("")
    A("| 갈래 | H | 원시행 | N(행) | 짝 날짜 | 평균 차이 | t | 통계량 | 1차 | 1차까지 필요 N |")
    A("|---|--:|--:|--:|--:|--:|--:|---|:--:|--:|")
    for r in rows:
        f = lambda v, s="{:+.2f}%": s.format(v) if v is not None else "—"
        A(f"| {r['ch']} | T+{r['h']} | {r['raw']} | {r['n']} | {r['k'] or '—'} | "
          f"{f(r['mean'])} | {('%.2f' % r['t']) if r['t'] is not None else '—'} | "
          f"{r['stat']} | {'✅' if r['hit'] else '—'} | "
          f"{('%.0f' % r['need']) if r['need'] else '—'} |")
    A("")
    A(f"`N(행)` 은 §3-1 정의(행 수)이고 `짝 날짜` 는 §3-5-2 검정의 자유도 근거다. "
      f"문턱 `N≥{MIN_N}` 은 **행 수** 기준이다.")
    A("")
    A("## 왜 Welch 와 짝 t 가 다른가 — 분해")
    A("")
    A("Welch 는 대조군의 **전체** 표본을 쓴다. 대조군이 채널보다 긴 기간에 걸쳐 있으면")
    A("**그 바깥 날짜의 성적이 비교에 섞인다.** 짝 검정은 겹치는 날만 쓰므로 그게 빠진다.")
    A("")
    A("| 갈래 | 채널 평균 | 대조군 전체 평균 | 대조군 **겹친 날** 평균 | 날짜 불일치 몫 | 겹치지 않는 대조군 날 |")
    A("|---|--:|--:|--:|--:|--:|")
    for r in rows:
        f = lambda v: "{:+.2f}%".format(v) if v is not None else "—"
        gap = (r["ct_pair"] - r["ct_all"]) if (r["ct_pair"] is not None
                                              and r["ct_all"] is not None) else None
        A(f"| {r['ch']} | {f(r['ch_pair'])} | {f(r['ct_all'])} | {f(r['ct_pair'])} | "
          f"{f(gap)} | {len(r['outside'])}일 |")
    A("")
    for r in rows:
        if r["ch_span"] and r["ct_span"]:
            A(f"- **{r['ch']}** 진입일 {r['ch_span'][0]}~{r['ch_span'][1]} · "
              f"대조군 {r['ct_span'][0]}~{r['ct_span'][1]} "
              f"(대조군 {r['ct_n']}행 / {r['ct_days']}일)")
    A("")
    A("## 채널이 자기 구간 안에서 며칠을 빠뜨렸나")
    A("")
    A("대조군은 매 거래일 2건을 낸다. 그래서 **대조군이 있는 날 중 채널이 없는 날**은")
    A("그 채널이 그날 추천을 못 냈다는 뜻이다. ⚠️ **그 이유는 이 표로 알 수 없다** —")
    A("정상 무신호일 수도, 게이트 모순으로 죽어 있었을 수도, 수집 실패일 수도 있다.")
    A("실행 로그와 대조하지 않았으므로 **누락이라고 단정하지 않는다.**")
    A("")
    A("| 갈래 | 진입일 구간 | 구간 안 거래일 | 채널이 낸 날 | 빠뜨린 날 | 가동률 |")
    A("|---|---|--:|--:|--:|--:|")
    for r in rows:
        if not r["ch_span"]:
            A(f"| {r['ch']} | — | — | — | — | — |")
            continue
        ran = r["span_days"] - len(r["inside_gap"])
        rate = (ran / r["span_days"] * 100) if r["span_days"] else None
        A(f"| {r['ch']} | {r['ch_span'][0]}~{r['ch_span'][1]} | {r['span_days']} | "
          f"{ran} | **{len(r['inside_gap'])}** | "
          f"{('%.0f%%' % rate) if rate is not None else '—'} |")
    A("")
    A("가동률이 낮으면 **채널이 간헐적으로만 돌고 있다**는 뜻이다. 성숙 표본이 느리게")
    A("쌓이는 원인이 문턱이 아니라 가동일 수 있으므로, 필요 N 까지의 기간 추정도 그만큼")
    A("길어진다. 빠뜨린 날의 사유 구분은 §6-12 의 거래일 달력·실행 로그 대조가 필요하다.")
    A("")
    A("**날짜 불일치 몫**이 0 에서 멀면, Welch 의 차이 중 그만큼은 실력이 아니라")
    A("**두 표본이 다른 날짜를 보고 있었다는 사실**이다. 항등식은 이렇다.")
    A("")
    A("```")
    A("짝 차이  =  Welch 차이  −  날짜 불일치 몫")
    A("```")
    A("")
    A("- **몫이 양수** → 대조군이 겹친 날에 더 잘 벌었다 → Welch 가 채널을 **과대평가**")
    A("- **몫이 음수** → 대조군이 겹친 날에 더 못 벌었다 → Welch 가 채널을 **과소평가**")
    A("")
    if any(r["hit"] for r in rows):
        A("## 🎯 1차 문턱을 넘은 갈래가 있다")
        A("")
        for r in rows:
            if r["hit"]:
                A(f"- **{r['ch']}** — N={r['n']} · t={r['t']:.2f}")
        A("")
        A("**아직 채택이 아니다.** 이 날짜부터 4주 뒤까지의 **새 표본**에서 같은 방향으로")
        A(f"`t ≥ {T_SURVIVE}` 를 다시 넘어야 한다. 그전에는 §6-9 처럼 **방향만 기록**한다.")
    else:
        A("아직 1차를 넘은 갈래가 없다. **문턱을 낮추지 않는다.**")
    A("")
    A("⚠️ 표본이 작을수록 평균이 크게 튄다. **가장 큰 숫자가 가장 작은 칸에서 나왔다면**")
    A("그것은 우연의 전형적인 모습이고, 재현 요구는 바로 그 경우를 거르라고 있는 것이다.")
    return "\n".join(L) + "\n"


def build_report(data, raw, skipped, today, bydate=None):
    ctrl = data.get(CONTROL, {})
    rows_out, conf = [], []

    # 사전등록된 채널은 표본이 0 이어도 표에 남긴다 — "채널이 없다"와
    # "아직 안 익었다"를 구분하기 위해서다(리허설에서 랜덤2_배지가 사라졌던 문제).
    for ch in sorted(set(data) | set(raw) | set(HORIZON)):
        h = HORIZON.get(ch, DEFAULT_HORIZON)
        vals = data.get(ch, {}).get(h, [])   # 표본 0 인 채널도 표에 남긴다
        n = len(vals)
        mean = sum(vals) / n if n else None
        base = ctrl.get(h, [])
        wt, wdf = welch(vals, base) if ch != CONTROL else (None, None)
        # §3-5-2 — 판정에 쓰는 값은 **짝 t** 다. Welch 는 참고로 같이 싣는다.
        pt, pdf, pk, pm = (paired_by_date(bydate, ch, h) if bydate is not None
                           and ch != CONTROL else (None, None, 0, None))
        t, df = (pt, pdf) if pt is not None else (wt, wdf)
        # §3-5-4 — Holm 에 넣는 p 는 단측이다. 양측 p 도 참고로 남긴다.
        p2 = t_two_sided_p(t, df) if (t is not None and df) else None
        p = t_one_sided_p(t, df)
        ann = mean * (TRADING_DAYS_YEAR / h) if mean is not None else None
        is_ctrl = ch.startswith(CONTROL_LIKE)
        v = "대조군(판정 대상 아님)" if is_ctrl else verdict_for(ch, n, t, mean)
        # §3-5-1 — 확증은 닫힌 집합뿐이다. 나머지는 N 이 아무리 쌓여도 들어오지 않는다.
        if ch in CONFIRMATORY and n >= MIN_N and p is not None:
            conf.append((ch, p))
        if n == 0:
            v = (f"표본 0 — 원시 {raw.get(ch, 0)}행 전부 T+{h} 미도달"
                 if raw.get(ch) else "행 없음")
        rows_out.append({"ch": ch, "h": h, "n": n, "raw": raw.get(ch, 0),
                         "mean": mean, "ann": ann, "t": t, "p": p,
                         "v": v, "ctrl": is_ctrl,
                         "wt": wt, "pt": pt, "pk": pk, "pm": pm, "p2": p2,
                         "paired": pt is not None})

    passed = holm(conf, HOLM_M) if conf else set()
    for r in rows_out:
        if not r["v"].startswith("생존·강화"):
            continue
        if r["ch"] in CONFIRMATORY:
            r["v"] = ("생존·강화 ✅ (Holm 통과)" if r["ch"] in passed
                      else "관찰 연장 — t는 넘었으나 Holm 미통과(§3-5)")
        else:
            # §3-5-1 계층 2 — 문턱을 한 번 넘은 것은 채택이 아니다. 재현이 남았다.
            r["v"] = "탐색 1차 통과 — 독립 재현 필요(§3-5-1)"

    L = []
    A = L.append
    A(f"# ⚖️ 채널 판정 스냅샷 — {today}")
    A("")
    A("> **이 표는 박제다.** §3-7 이 \"그날 값 그대로, 이후 수정 금지\"로 정해 둔 산출물이다.")
    A("> 숫자가 마음에 들지 않아도 고치지 않는다. 다시 계산하고 싶으면 새 날짜로 새 파일을 만든다.")
    A("")
    A("## 판정 기준 (전부 사전등록분)")
    A("")
    A("| 항목 | 값 | 출처 |")
    A("|---|---|---|")
    A(f"| 호라이즌 | 단기성 T+5 · 리포트중기 T+10 · 리포트장기 T+60 | §3 |")
    A(f"| 순알파 | (종목T+N − 지수T+N) − **{COST_PCT}%** · 지수벤치는 비용 면제 | §3-4-2 |")
    A(f"| 연율 환산 | 순알파 × (250 / 호라이즌) | §3-4-2 |")
    A(f"| 생존·강화 | N≥{MIN_N} 이고 t ≥ {T_SURVIVE} **그리고 Holm 통과** | §3-1 · §3-5 |")
    A(f"| 확증 집합 | `{'` · `'.join(CONFIRMATORY)}` — **닫힘**. m={HOLM_M} 고정 | §3-5-1 |")
    A(f"| 탐색 채널 | 확증 밖 채널은 Holm 면제, 대신 **독립 재현** 요구 | §3-5-1 |")
    A(f"| 관찰 연장 | N≥{MIN_N} 이고 {T_DISCARD} ≤ t < {T_SURVIVE} | §3-1 |")
    A(f"| 폐기 | N≥{MIN_N} 이고 t < {T_DISCARD} — **보정 없음** | §3-1 · §3-5 |")
    A(f"| 판정 불가 | N < {MIN_N} → 관찰 연장 | §3-1 |")
    A(f"| 검정 통계량 | **같은 날 짝지은 1표본 t** (df = 날짜수−1) | §3-5-2 |")
    A(f"| p 값 | **우월성 단측** (방향 고정: 채널 > 대조군) | §3-5-4 |")
    A(f"| 대조군 | `{CONTROL}` (같은 호라이즌) | §3-1 |")
    A("")
    A("⚠️ 슬리피지는 **미반영**이다(§3-4-2). §6-4 모의 집행 실측치가 확정되면 그때 더한다.")
    A("")
    A("## 채널별 결과")
    A("")
    A("| 채널 | H | 원시행 | N(성숙) | 평균 순알파 | 연율 환산 | Welch t | p | 판정 |")
    A("|---|--:|--:|--:|--:|--:|--:|--:|---|")
    for r in sorted(rows_out, key=lambda x: (x["ctrl"], -(x["t"] or -9))):
        f = lambda v, s="{:+.2f}%": s.format(v) if v is not None else "—"
        A(f"| {'*' if r['ctrl'] else ''}{r['ch']} | T+{r['h']} | {r['raw']} | {r['n']} | "
          f"{f(r['mean'])} | {f(r['ann'])} | "
          f"{('%.2f' % r['t']) if r['t'] is not None else '—'} | "
          f"{('%.3f' % r['p']) if r['p'] is not None else '—'} | {r['v']} |")
    A("")
    A("`*` = 대조군. 판정 대상이 아니며 §3-5 의 m 에도 포함되지 않는다.")
    A("")

    A("## 같은 날 짝지어 비교 (§3-5-2)")
    A("")
    A("두 채널은 같은 날 매매하므로 **그날 시장이 통째로 움직인 몫은 차이에서 상쇄된다.**")
    A("Welch 는 그 몫을 양쪽 분산에 넣고 재기 때문에 표준오차를 부풀린다. 대신 짝 검정은")
    A("자유도를 잃는다(행 수 → 날짜 수). **어느 쪽이 큰지는 미리 알 수 없어서, 결과를 보기")
    A("전에 짝 검정을 쓰기로 §3-5-2 에 고정했다.** 아래 두 열의 차이가 그 효과다.")
    A("")
    A("| 채널 | 짝 지은 날 | 평균 차이 | 짝 t (판정용) | Welch t (참고) |")
    A("|---|--:|--:|--:|--:|")
    for r in sorted(rows_out, key=lambda x: (x["ctrl"], -(x["t"] or -9))):
        if r["ctrl"]:
            continue
        g = lambda v, s="{:+.2f}": s.format(v) if v is not None else "—"
        A(f"| {r['ch']} | {r['pk'] or '—'} | {g(r['pm'], '{:+.2f}%')} | "
          f"{g(r['pt'])} | {g(r['wt'])} |")
    A("")
    if not any(r["paired"] for r in rows_out):
        A("⚠️ **짝 검정이 하나도 성립하지 않았다.** 두 채널 모두 값이 있는 날이 2일 미만이다.")
        A("이 표의 t 는 Welch 로 되돌아간 값이며, §3-5-2 가 의도한 검정이 아니다.")
        A("")
    A("## 다중비교 보정 (§3-5 계층 1)")
    A("")
    A(f"확증 집합은 §3-5-1 이 `{'` · `'.join(CONFIRMATORY)}` 로 **닫아** 두었다. "
      f"나중에 N≥{MIN_N} 를 채운 채널은 여기 들어오지 않고 탐색으로 간다.")
    A("")
    if conf:
        m = HOLM_M
        A(f"확증 검정 대상 **m = {m}개 고정** (실제 검정 {len(conf)}개).")
        A("")
        A("| 순위 | 채널 | 단측 p | (참고) 양측 p | Holm 문턱 `0.05/(m−i+1)` | 결과 |")
        A("|--:|---|--:|--:|--:|---|")
        stop = False
        for i, (ch, p) in enumerate(sorted(conf, key=lambda x: x[1]), start=1):
            thr = 0.05 / (m - i + 1)
            if stop:
                res = "— (앞에서 멈춤)"
            elif p <= thr:
                res = "통과"
            else:
                res, stop = "**여기서 멈춤**", True
            _p2 = next((r["p2"] for r in rows_out if r["ch"] == ch), None)
            A(f"| {i} | {ch} | {p:.4f} | "
              f"{('%.4f' % _p2) if _p2 is not None else '—'} | {thr:.4f} | {res} |")
    else:
        A(f"**확증 검정 대상이 0개다.** 확증 집합에서 N≥{MIN_N} 를 채운 채널이 없다.")
        A("")
        A("§3-7 이 이 상황을 미리 인정해 뒀다 — *\"그날 대부분의 채널이 N<30 일 가능성이 높다\"*.")
        A("**문턱을 낮추지 않는다.** 관찰 연장이라고 쓰고, 10/5 재판정으로 넘긴다.")
    A("")

    A("## 표본 제외 (§4-2) · 행 상태")
    A("")
    A("행마다 **기본 기간의 상태 하나만** 매긴다(상호배타). 그래서 아래 합계는")
    A("원시 행수와 같아야 한다 — 3차 재검증 2-1 이 지적한 중복 집계를 막는 장치다.")
    A("")
    for k, v in skipped.items():
        if "행×기간" not in k:
            A(f"- {k}: {v}행")
    A("")
    A("행×기간 단위 진단(랜덤2 는 한 행이 여러 기간을 타므로 **행 수와 다르다**):")
    A("")
    for k, v in skipped.items():
        if "행×기간" in k:
            A(f"- {k}: {v}건")
    A("")
    A("> ⚠️ 성숙 판정은 **거래일 달력이 아니라 근사**다. 평일 수가 H 미만이면 확실히")
    A("> 조기로 차단하지만(3차 재검증 2-2), 평일 수가 충분해도 휴장일 때문에 성숙을")
    A("> 확정할 수는 없다. 그 구간은 판정을 유보하고 집계에는 넣는다.")
    A("")
    A("## 이 표를 읽을 때 조심할 것")
    A("")
    A("- **N 은 행 수다.** 같은 날 여러 채널이 같은 종목을 담으면 서로 독립이 아니다.")
    A("- 연율 환산은 *같은 전략을 그 빈도로 계속 돌릴 수 있다*는 가정에 기댄다(§3-4-2).")
    A("- 비용 0.35% 는 **가정이지 실측이 아니다**. 실계좌 수수료가 확정되면 바꾸고 날짜를 남긴다.")
    A("- 슬리피지 미반영이므로 여기 숫자는 **실전보다 낙관적**이다.")
    return "\n".join(L) + "\n", rows_out, conf, passed


# ── 자기검증 ──────────────────────────────────────────────────────────────
def self_test():
    """시트를 건드리기 전에 통계·판정 로직부터 검증한다.
    여기서 깨지면 9/7 판정 자체를 신뢰할 수 없으므로 실행할 이유가 없다."""
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    print("🧪 통계 함수")
    # 알려진 값 대조 — t=2.042, df=30 이면 양측 p ≈ 0.05 (§3-5 표의 m=1 문턱)
    p = t_two_sided_p(2.042, 30)
    chk("t=2.042·df=30 → p≈0.05 (§3-5 표와 일치)", abs(p - 0.05) < 0.002, f"p={p:.4f}")
    p2 = t_two_sided_p(3.030, 30)
    chk("t=3.030·df=30 → p≈0.005 (=0.05/10, m=10 문턱)", abs(p2 - 0.005) < 0.0012, f"p={p2:.4f}")
    chk("t=0 → p=1", abs(t_two_sided_p(0.0, 30) - 1.0) < 1e-9)
    chk("p 는 |t| 에 단조감소", t_two_sided_p(1.0, 20) > t_two_sided_p(2.0, 20))

    print("🧪 Welch t")
    t, df = welch([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
    chk("같은 분포 → t=0", abs(t) < 1e-12, f"t={t:.3g}")
    t, df = welch([10, 11, 12, 13, 14], [1, 2, 3, 4, 5])
    chk("확실히 높은 쪽 → t 큼", t > 5, f"t={t:.2f}")
    chk("표본 1개면 None", welch([1], [1, 2, 3])[0] is None)

    print("🧪 §3-1 판정 문턱")
    chk("N<30 이면 t 가 커도 판정 불가", "판정 불가" in verdict_for("차트TOP2", 29, 9.9, 5.0))
    chk("N<30·t<1·음수 → 폐기 후보 표시", "폐기 후보" in verdict_for("차트TOP2", 20, 0.3, -1.0))
    chk("N<30·t<1·양수 → 폐기 후보 아님", "폐기 후보" not in verdict_for("차트TOP2", 20, 0.3, 1.0))
    chk("N≥30·t≥2 → 생존·강화", verdict_for("차트TOP2", 30, 2.0, 1.0).startswith("생존·강화"))
    chk("N≥30·1≤t<2 → 관찰 연장", verdict_for("차트TOP2", 30, 1.5, 1.0) == "관찰 연장")
    chk("N≥30·t<1 → 폐기", verdict_for("차트TOP2", 30, 0.9, 1.0) == "폐기")
    chk("장기 채널은 판정 안 함(§3-3)", "§3-3" in verdict_for(LONG_CHANNEL, 99, 9.0, 9.0))

    print("🧪 §3-5 Holm")
    # m=3 → 문턱은 순서대로 0.05/3=0.0167, 0.05/2=0.025, 0.05/1=0.05
    got = holm([("a", 0.001), ("b", 0.30), ("c", 0.030)])
    chk("2위가 문턱(0.025)을 넘으면 거기서 멈춘다 — a만 통과",
        got == {"a"}, f"통과={sorted(got)}")
    # ⚠️ 처음엔 c=0.02 로 썼다가 이 검사가 깨졌다. c=0.02 는 2위 문턱 0.025 를
    #    통과하는 값이라 {a,c} 가 **정답**이었다. 구현이 아니라 기대값이 틀렸던 것이고,
    #    자기검증이 그걸 잡았다. 경계 근처 값을 남겨 둔다.
    got = holm([("a", 0.001), ("b", 0.30), ("c", 0.020)])
    chk("2위가 문턱 안(0.020≤0.025)이면 통과하고 3위에서 멈춘다",
        got == {"a", "c"}, f"통과={sorted(got)}")
    chk("전부 매우 유의하면 전부 통과",
        holm([("a", 0.0001), ("b", 0.0002)]) == {"a", "b"})
    chk("전부 유의하지 않으면 아무도 통과 못 함",
        holm([("a", 0.4), ("b", 0.5)]) == set())

    print("🧪 §3-5-4 우월성 단측 p (2026-09-11)")
    chk("양수 t 는 양측의 절반",
        abs(t_one_sided_p(2.0, 60) - t_two_sided_p(2.0, 60) / 2) < 1e-12)
    # 음수 t 는 '우월하다'는 근거가 아니다. 절반으로 깎으면 열등한 채널이 통과한다.
    _neg = t_one_sided_p(-3.0, 60)
    chk("음수 t 는 1 쪽으로 간다 (우월성 근거 아님)", _neg > 0.99, f"p={_neg:.4f}")
    chk("음수 t 를 절반으로 깎지 않는다",
        _neg > t_two_sided_p(-3.0, 60), f"단측={_neg:.4f} 양측={t_two_sided_p(-3.0, 60):.4f}")
    chk("t=0 이면 단측 p=0.5", abs(t_one_sided_p(0.0, 60) - 0.5) < 1e-9)
    chk("t·df 가 없으면 None", t_one_sided_p(None, 60) is None
        and t_one_sided_p(2.0, 0) is None)

    # 구속조건 — 처음에 "단측 임계 t 는 1.96" 으로 적었다가 이 검사가 틀렸다고
    # 알려줬다. 1.96 은 df→∞ 근사고, 실제 자유도(짝 검정이면 날짜수−1 ≈ 43)에서는
    # 2.0 언저리다. 그래서 단측으로 바꿔도 §3-1 의 t≥2.0 과 거의 겹친다 —
    # "단측이면 문턱이 확 낮아진다"는 기대 자체가 틀렸다.
    chk("현실 자유도에서 단측 Holm 임계 t 는 2.0 근처다",
        t_one_sided_p(2.02, 43) < 0.025 < t_one_sided_p(1.98, 43),
        f"t=1.98→{t_one_sided_p(1.98, 43):.4f} · t=2.02→{t_one_sided_p(2.02, 43):.4f}")
    chk("양측이었다면 같은 자유도에서 t=2.2 도 못 넘었다",
        t_two_sided_p(2.2, 43) > 0.025, f"양측p={t_two_sided_p(2.2, 43):.4f}")
    chk("t=2.05 면 Holm 도 §3-1 도 넘는다",
        t_one_sided_p(2.05, 43) < 0.025
        and verdict_for("차트TOP2", 40, 2.05, 1.0).startswith("생존·강화"))
    chk("t=1.9 면 §3-1 에서 막힌다(Holm 과 무관하게)",
        verdict_for("차트TOP2", 40, 1.9, 1.0) == "관찰 연장")

    # 리포트가 단측을 쓰는지 — 양측을 쓰면 여기서 걸린다.
    _sd = {CONTROL: {5: [(-0.1 if i % 2 else 0.1) for i in range(40)]},
           "차트TOP2": {5: [(2.0 if i % 2 else 2.6) for i in range(40)]}}
    _sm, _sro, _scf, _sps = build_report(_sd, {"차트TOP2": 40}, {}, "2026-01-01")
    _cp = dict(_scf).get("차트TOP2")
    _row = next(r for r in _sro if r["ch"] == "차트TOP2")
    chk("Holm 에 들어가는 p 가 단측이다",
        abs(_cp - _row["p2"] / 2) < 1e-12, f"단측={_cp:.6f} 양측={_row['p2']:.6f}")
    chk("양측 p 도 표에 남는다", "(참고) 양측 p" in _sm)

    print("🧪 §3-5-1 확증 집합 닫기 (2026-09-11)")
    # 핵심: 확증 채널이 하나만 남아도 m 은 2 다. 1 로 줄면 문턱이 0.05 로
    # 느슨해져서 "채널이 죽을수록 통과가 쉬워지는" 구멍이 생긴다.
    chk("m 고정이 없으면 p=0.03 이 통과해 버린다",
        holm([("a", 0.03)]) == {"a"})
    chk("m=2 고정이면 같은 p=0.03 은 탈락한다(문턱 0.025)",
        holm([("a", 0.03)], HOLM_M) == set())
    chk("확증 집합은 차트·수급 둘뿐이고 m 도 2다",
        CONFIRMATORY == ("차트TOP2", "수급TOP2") and HOLM_M == 2)

    # 강한 신호를 만들어 둔다 — 문턱을 넘고도 확증에 못 들어가는지 보기 위해서다.
    def strong(n=40, lo=4.9, hi=5.1):
        return [lo if i % 2 else hi for i in range(n)]
    def flat(n=40):
        return [-0.1 if i % 2 else 0.1 for i in range(n)]
    base = {CONTROL: {5: flat(), 10: flat()}}

    d = dict(base); d["리포트TOP2_단기"] = {5: strong()}
    md, ro, cf, ps = build_report(d, {"리포트TOP2_단기": 40}, {}, "2026-01-01")
    chk("나중에 30건을 넘긴 리포트 단기는 확증에 못 들어간다",
        [c for c, _ in cf] == [], f"conf={[c for c, _ in cf]}")
    v = next(r["v"] for r in ro if r["ch"] == "리포트TOP2_단기")
    chk("t를 넘겨도 '생존·강화'가 아니라 '탐색 1차 통과'다",
        v == "탐색 1차 통과 — 독립 재현 필요(§3-5-1)", v)
    chk("탐색 채널의 재현 요구가 리포트에 적힌다", "독립 재현" in md)

    d = dict(base); d["차트TOP2"] = {5: strong()}
    md, ro, cf, ps = build_report(d, {"차트TOP2": 40}, {}, "2026-01-01")
    chk("차트TOP2 는 확증 집합이라 Holm 을 받는다",
        [c for c, _ in cf] == ["차트TOP2"], f"conf={[c for c, _ in cf]}")
    chk("확증 채널 1개여도 리포트는 m=2 고정이라고 쓴다",
        "m = 2개 고정" in md)
    chk("집합이 닫혀 있다는 사실이 리포트에 남는다", "닫아" in md)

    print("🧪 F07 — 원장 입력 검증 (2026-09-07 감사)")
    # ⚠️ today 를 **고정**해서 부른다. 성숙 판정은 오늘 날짜에 의존하므로
    #    이걸 안 박아 두면 같은 검사가 내일은 다른 결과를 낸다.
    T0 = datetime.date(2026, 9, 30)

    def mkheader(**over):
        """실제 BT_HEADER 이름으로 헤더를 만든다.

        ⚠️ 예전에는 `["trade_id"] + [""] * 33` 을 썼다. R2 가 열 이름 검사를
           넣은 뒤로는 그 가짜 헤더가 **정상 원장 검사까지 깨뜨린다** —
           검사가 강해진 만큼 검사용 표본도 진짜와 같아야 한다.
        """
        h = [""] * 34
        for i, name in EXPECTED_HEADER.items():
            h[i] = name
        for i, name in over.items():
            h[int(i)] = name
        return h

    def mkrow(tid, ch, s5=1.0, i5=0.0, date="2026-09-01"):
        r = [""] * 34
        r[C_TRADE_ID], r[C_ENTRY_DATE], r[C_CHANNEL] = tid, date, ch
        if s5 is not None:
            r[STOCK_COL[5]] = str(s5)
        if i5 is not None:
            r[INDEX_COL[5]] = str(i5)
        return r

    def sev(rows):
        return [x[0] for x in validate_ledger(rows, today=T0)]

    hdr34 = mkheader()
    good = [hdr34, mkrow("A_차트TOP2_000660", "차트TOP2")]
    chk("정상 원장 → 이상 없음", sev(good) == [], f"{validate_ledger(good, today=T0)}")
    dup = [hdr34, mkrow("SAME", "차트TOP2"), mkrow("SAME", "수급TOP2")]
    chk("trade_id 중복 → ABORT", ABORT in sev(dup))
    nan = [hdr34, mkrow("N1", "차트TOP2", s5="nan")]
    chk("비유한 수치(nan) → ABORT", ABORT in sev(nan))
    chk("_num('nan') 은 None (성숙값에 안 들어간다)", _num("nan") is None)
    chk("_num('inf') 은 None", _num("inf") is None)
    chk("_num('1.5') 는 살아 있다", _num("1.5") == 1.5)
    bad = [hdr34, mkrow("D1", "차트TOP2", s5=None, i5=None, date="2026-13-99")]
    chk("날짜 형식 이상 + 값 없음 → WARN(중단 아님)", sev(bad) == [WARN], f"{sev(bad)}")
    ext = [hdr34, mkrow("E1", "차트TOP2", s5=9999.0)]
    chk("극단 수익률 → WARN(중단 아님)", sev(ext) == [WARN], f"{sev(ext)}")
    noh = [["엉뚱한열"] + [""] * 33, mkrow("H1", "차트TOP2")]
    chk("헤더 불일치 → ABORT", ABORT in sev(noh))
    chk("빈 원장 → ABORT", validate_ledger([], today=T0)[0][0] == ABORT)

    # ── R2 재검증(2026-09-08)이 지목한 네 구멍 ──────────────────────────
    # ① 종목T+5 와 지수T+5 의 열·값을 맞바꿔도 무경고였다(순알파 +1.65 → −2.35).
    swap = [mkheader(**{"19": "지수T+5", "23": "종목T+5"}),
            mkrow("S1", "차트TOP2")]
    chk("R2① 종목T+5↔지수T+5 열 교환 → ABORT", ABORT in sev(swap), f"{sev(swap)}")
    chk("R2① 교환 사유가 '스키마'로 찍힌다",
        any("스키마" in n for s_, n, _ in validate_ledger(swap, today=T0) if s_ == ABORT))
    ins = [mkheader(**{"20": "끼워넣은열"}), mkrow("S2", "차트TOP2")]
    chk("R2① 열 하나만 밀려도 → ABORT", ABORT in sev(ins))

    # ② 수익률 셀이 `#REF!` 여도 무경고로 집계에서만 빠졌다.
    ref = [hdr34, mkrow("R1", "차트TOP2", s5="#REF!")]
    chk("R2② 수익률 '#REF!' → ABORT", ABORT in sev(ref), f"{sev(ref)}")
    chk("R2② `#REF!` 는 조용히 빠지던 값이었다 — _num 은 여전히 None",
        _num("#REF!") is None)
    chk("R2② 콤마·% 붙은 정상 값은 통과", sev([hdr34, mkrow("R2", "차트TOP2", s5="1,234.5%")])
        == [WARN])   # |1234.5%| > 500 이라 극단 경고만 뜬다(숫자 파싱은 성공)

    # ③ 진입일 2099-01-01 인데 값이 있으면 무경고로 성숙 표본에 들어갔다.
    fut = [hdr34, mkrow("F1", "차트TOP2", date="2099-01-01")]
    chk("R2③ 미래 진입일 → ABORT", ABORT in sev(fut), f"{sev(fut)}")

    # ④ 진입일 공란 + 값 존재도 동일했다.
    nod = [hdr34, mkrow("B1", "차트TOP2", date="")]
    chk("R2④ 진입일 공란 + 값 존재 → ABORT", ABORT in sev(nod), f"{sev(nod)}")
    brk = [hdr34, mkrow("B2", "차트TOP2", date="2026-13-99")]
    chk("R2④ 형식 깨진 진입일 + 값 존재 → ABORT (값 없을 때와 다르다)",
        ABORT in sev(brk), f"{sev(brk)}")

    # ── 성숙/결손 구분 — '표본 부족'과 '데이터 결손'은 다른 이야기다 ──────
    chk("성숙 여유는 거래일이 아니라 달력일 근사", calendar_margin(5) == 11)
    chk("maturity_note — 한참 지났으면 MATURED",
        maturity_note("2026-09-01", 5, T0) == "MATURED")
    chk("maturity_note — 아직 h 달력일도 안 지났으면 NOT_MATURED",
        maturity_note("2026-09-28", 5, T0) == "NOT_MATURED")
    # ⚠️ 2-2(평일 하한) 이후 9/24 는 경계가 아니라 조기다 — 9/25~9/30 평일이 4일뿐이다.
    #    경계는 '평일은 충분한데 달력 여유만 미달'인 구간이어야 한다(진입 9/21, 평일 7일).
    chk("maturity_note — 경계에서는 판정 유보(None)",
        maturity_note("2026-09-21", 5, T0) is None,
        f"평일={weekdays_between(datetime.date(2026, 9, 21), T0)} "
        f"달력={(T0 - datetime.date(2026, 9, 21)).days}")
    chk("maturity_note — 평일 수가 H 미만이면 달력일이 넘어도 조기(2-2)",
        maturity_note("2026-09-24", 5, T0) == "NOT_MATURED")
    chk("maturity_note — 미래 진입일은 INVALID",
        maturity_note("2099-01-01", 5, T0) == "INVALID")
    chk("maturity_note — 공란도 INVALID", maturity_note("", 5, T0) == "INVALID")
    miss = [hdr34, mkrow("M1", "차트TOP2", s5=None, i5=None, date="2026-09-01")]
    chk("성숙했는데 값이 없다 → WARN(데이터 결손, 표본 부족 아님)",
        sev(miss) == [WARN], f"{sev(miss)}")
    early = [hdr34, mkrow("Y1", "차트TOP2", date="2026-09-29")]
    chk("아직 미성숙인데 값이 있다 → WARN", sev(early) == [WARN], f"{sev(early)}")
    # §4-2 로 뺀 행은 값이 없는 게 정상이다 — 실측에서 이걸로 헛경고가 났다
    _ex = mkrow("X1", "차트TOP2", s5=None, i5=None, date="2026-09-01")
    _ex[C_EXCLUDE] = "거래정지 — 측정 제외"
    chk("제외 행은 값이 없어도 결손 경고를 내지 않는다", sev([hdr34, _ex]) == [],
        f"{sev([hdr34, _ex])}")
    _exf = mkrow("X2", "차트TOP2", date="2099-01-01")
    _exf[C_EXCLUDE] = "거래정지 — 측정 제외"
    chk("그래도 제외 행의 무결성 검사(미래 날짜)는 살아 있다",
        ABORT in sev([hdr34, _exf]), f"{sev([hdr34, _exf])}")

    # ── R3 — 지문은 셀 경계를 구분해야 한다 ─────────────────────────────
    _f1 = ledger_fingerprint(good)
    chk("같은 원장 → 같은 지문", _f1 == ledger_fingerprint(good))
    chk("행 하나만 달라도 지문이 바뀐다",
        _f1 != ledger_fingerprint([hdr34, mkrow("A_차트TOP2_000660", "차트TOP2", s5=1.1)]))
    # 예전 구현은 셀을 탭으로 이어 붙였다. 그래서 셀 안에 탭이 있으면
    # 서로 다른 원장이 **같은 지문**을 냈다 — 재현 근거가 무너지는 결함이다.
    chk("R3 셀 안의 탭이 경계를 흉내 내도 지문이 갈린다",
        ledger_fingerprint([["a\tb", "c"]]) != ledger_fingerprint([["a", "b\tc"]]))
    chk("R3 줄바꿈도 마찬가지",
        ledger_fingerprint([["a\nb"], ["c"]]) != ledger_fingerprint([["a"], ["b\nc"]]))
    chk("R3 지문 방식에 버전이 박혀 있다", LEDGER_SHA_ALGO.startswith("canonical-json"))

    # 실행 근거 묶음 — 예전엔 main() 안에 인라인이라 자기검증이 못 지나갔다
    import tempfile, shutil
    _tmp = tempfile.mkdtemp(prefix="verdict_selftest_")
    try:
        _rid = run_bundle_id("a" * 64, "b" * 64,
                             now=datetime.datetime(2026, 9, 8, 17, 0, 0))
        chk("R3 실행 id 에 시각·원장·코드 지문이 다 들어간다",
            _rid == "20260908T170000_aaaaaaaa_bbbbbbbb", _rid)
        _d = save_run_bundle(_tmp, _rid, {"run_id": _rid}, [["h"], ["r"]], "# 판정")
        chk("R3 근거 세 파일이 한 디렉터리에 같이 남는다",
            all(os.path.exists(os.path.join(_d, n))
                for n in ("meta.json", "ledger.json", "판정.md")))
        chk("R3 원장이 그대로 복원된다",
            json.load(open(os.path.join(_d, "ledger.json"),
                           encoding="utf-8"))["rows"] == [["h"], ["r"]])
        _again = None
        try:
            save_run_bundle(_tmp, _rid, {}, [["x"]], "# 다른 판정")
        except FileExistsError:
            _again = "raised"
        chk("R3 같은 실행 id 로 다시 쓰면 덮어쓰지 않고 예외", _again == "raised")
        chk("R3 첫 실행의 근거가 그대로 살아 있다",
            open(os.path.join(_d, "판정.md"), encoding="utf-8").read() == "# 판정")
        # 원장이 한 셀만 달라도 실행 id 가 갈린다 → 같은 초라도 디렉터리가 다르다
        chk("R3 원장이 다르면 실행 id 도 다르다",
            run_bundle_id(ledger_fingerprint([["a"]]), "b" * 64,
                          now=datetime.datetime(2026, 9, 8, 17, 0, 0))
            != run_bundle_id(ledger_fingerprint([["b"]]), "b" * 64,
                             now=datetime.datetime(2026, 9, 8, 17, 0, 0)))
    finally:
        shutil.rmtree(_tmp, ignore_errors=True)
    print()

    print("🧪 §3-4-2 비용·§4-2 제외")
    hdr = [""] * 34
    def mk(ch, s, i, memo=""):
        r = [""] * 34
        r[C_CHANNEL], r[C_EXCLUDE] = ch, memo
        r[STOCK_COL[5]], r[INDEX_COL[5]] = str(s), str(i)
        return r
    d, rw, sk, bd = collect([hdr, mk("차트TOP2", 3.0, 1.0)])
    chk("순알파 = 종목−지수−0.35", abs(d["차트TOP2"][5][0] - (3.0 - 1.0 - 0.35)) < 1e-9,
        f"={d['차트TOP2'][5][0]:.2f}")
    d, rw, sk, bd = collect([hdr, mk("지수벤치_KOSPI", 3.0, 1.0)])
    chk("지수벤치는 비용 면제", abs(d["지수벤치_KOSPI"][5][0] - 2.0) < 1e-9)
    d, rw, sk, bd = collect([hdr, mk("차트TOP2", 3.0, 1.0, "거래정지 — 측정 제외")])
    chk("'제외' 표식 행은 빠진다", sk["제외표식"] == 1 and not d)
    d, rw, sk, bd = collect([hdr, mk("차트TOP2", 3.0, 1.0, "집계복귀 — 철회")])
    chk("'집계복귀'(제외 두 글자 없음) 는 살린다", sk["제외표식"] == 0 and bool(d))
    # 세 구현이 같은 판정을 하는지 — 단순 포함 검사와 일치해야 한다
    for memo, want in [("거래정지 — 측정 제외", True), ("제외:위험종목", True),
                       ("집계복귀 — 철회", False), ("", False),
                       ("집계복귀 — 이전 제외 철회", True)]:
        r = [""] * 34
        r[C_EXCLUDE] = memo
        chk(f"제외판정 '{memo or '(빈칸)'}' → {want}", is_excluded(r) is want)

    print("🧪 B — 성숙 검사가 실제 집계를 막는가 (2차 재검증 · 2026-09-08)")
    _T = datetime.date(2026, 9, 8)

    def mkb(ch, s5=None, i5=None, date="2026-08-01"):
        r = [""] * 34
        r[C_TRADE_ID], r[C_ENTRY_DATE], r[C_CHANNEL] = f"B_{ch}_{date}", date, ch
        if s5 is not None: r[STOCK_COL[5]] = str(s5)
        if i5 is not None: r[INDEX_COL[5]] = str(i5)
        return r

    hdrb = [""] * 34
    # ① 감사자 재현: 당일 진입인데 T+5 값이 있다 → 예전엔 WARN 만 뜨고 N 에 들어갔다
    d, rw, sk, bd = collect([hdrb, mkb("차트TOP2", 3.0, 1.0, "2026-09-08")], today=_T)
    chk("① 조기값은 이제 **집계에서 빠진다**(예전엔 순알파 +1.65% 로 포함)",
        d == {} and sk["조기값(성숙 전·집계 제외)"] == 1, f"data={d} skipped={sk}")
    # [2-1] 상호배타 — 새 카운터를 올렸으면 미성숙은 **0** 이어야 한다.
    #       예전에는 둘 다 1 이라 "미성숙과 분리했다"는 말이 사실이 아니었고,
    #       자기검증이 새 카운터만 보고 통과시켰다.
    chk("①-b 조기값이면 미성숙은 0 (중복 집계 금지)",
        sk["미성숙(호라이즌 미도달)"] == 0, f"{sk}")
    # ② 종목값만 있고 지수값이 없다 → 예전엔 무경고로 '미성숙'에 섞였다
    d, rw, sk, bd = collect([hdrb, mkb("차트TOP2", 3.0, None)], today=_T)
    chk("② 쌍결손을 '미성숙'과 따로 센다",
        sk["쌍결손(한쪽만 있음)"] == 1 and d == {}, f"skipped={sk}")
    chk("②-b 쌍결손이면 미성숙은 0", sk["미성숙(호라이즌 미도달)"] == 0, f"{sk}")
    # ③ 둘 다 없는데 성숙했다 → '성숙결측'
    d, rw, sk, bd = collect([hdrb, mkb("차트TOP2")], today=_T)
    chk("③ 성숙했는데 둘 다 없으면 '성숙결측'으로 센다",
        sk["성숙결측(둘 다 없음)"] == 1, f"skipped={sk}")
    chk("③-b 성숙결측이면 미성숙은 0", sk["미성숙(호라이즌 미도달)"] == 0, f"{sk}")

    # [2-1] 합계 검증 — 상태가 상호배타면 원시 행수와 딱 맞아야 한다
    _mix = [hdrb,
            mkb("차트TOP2", 3.0, 1.0),                       # 집계됨
            mkb("차트TOP2", 3.0, None),                      # 쌍결손
            mkb("차트TOP2"),                                 # 성숙결측
            mkb("차트TOP2", 3.0, 1.0, "2026-09-08"),         # 조기값
            mkb("수급TOP2", None, None, "2026-09-07")]       # 미성숙
    _mix[3][C_EXCLUDE] = ""                                  # 제외표식 없음 확인
    _d, _rw, _sk, _bd = collect(_mix, today=_T)
    _rows, _tot, _ok = reconcile(_mix, _sk)
    chk("합계 검증 — 원시 행수 = 상태 합계", _ok, f"행={_rows} 합계={_tot} {_sk}")
    chk("행×기간 진단은 행 수와 별도 항목으로 표시", "조기값(행×기간)" in _sk)

    # [2-2] 달력일은 통과하지만 평일 수로는 확실히 조기인 사례
    #       기준 9/8 · 진입 9/3 · T+5 → 평일은 9/4·9/7·9/8 로 3일뿐이다.
    chk("2-2 평일 3일뿐이면 T+5 는 NOT_MATURED",
        maturity_note("2026-09-03", 5, _T) == "NOT_MATURED",
        f"평일={weekdays_between(datetime.date(2026, 9, 3), _T)}")
    _late = collect([hdrb, mkb("차트TOP2", 3.0, 1.0, "2026-09-03")], today=_T)
    chk("2-2 그 값은 집계에 안 들어간다(예전엔 +1.65% 로 포함)",
        _late[0] == {}, f"{_late[0]}")
    chk("2-2 평일 수가 충분하면 종전대로 성숙 가능",
        maturity_note("2026-08-01", 5, _T) == "MATURED")
    # 정상값은 종전대로 들어간다
    d, rw, sk, bd = collect([hdrb, mkb("차트TOP2", 3.0, 1.0)], today=_T)
    chk("정상 성숙값은 그대로 집계된다",
        abs(d["차트TOP2"][5][0] - (3.0 - 1.0 - 0.35)) < 1e-9, f"{d}")
    # 경계(달력일 근사가 유보)는 **버리지 않는다** — 근사로 진짜 값을 버리는 쪽이 더 나쁘다.
    # ⚠️ 처음엔 진입 9/2 를 경계 사례로 썼다가 2-2 수정 뒤 실패했다. 9/3~9/8 평일이
    #    4일뿐이라 T+5 가 **확실히 불가능**하고, 경계가 아니라 조기가 맞다 — 시험이 틀렸다.
    #    진짜 경계는 평일은 충분하지만 달력 여유(margin)를 못 채운 구간이다.
    #    진입 8/31 → 평일 6일(9/1~9/8) ≥ 5, 달력 8일 < margin 11 → 유보.
    chk("경계 사례 고르기 — 평일은 충분하고 달력 여유는 미달",
        weekdays_between(datetime.date(2026, 8, 31), _T) >= 5
        and (_T - datetime.date(2026, 8, 31)).days < calendar_margin(5))
    d, rw, sk, bd = collect([hdrb, mkb("차트TOP2", 3.0, 1.0, "2026-08-31")], today=_T)
    chk("경계 유보(None)는 집계에 넣는다", d.get("차트TOP2", {}).get(5) is not None,
        f"note={maturity_note('2026-08-31', 5, _T)} data={d}")
    # 검증기도 모든 채널×기간을 본다 — 랜덤2 는 여러 기간에 쓰인다.
    # ⚠️ 처음엔 T+5 값만 넣고 "T+10 이 검사되는가"를 물었다가 실패했다.
    #    T+10 값이 없으면 T+10 에 경고할 것이 없는 게 **맞다** — 내 시험이 틀렸다.
    #    실제로 다기간을 타는지 보려면 **T+10 열에 값을 넣어야** 한다.
    _r10 = [""] * 34
    _r10[C_TRADE_ID], _r10[C_ENTRY_DATE], _r10[C_CHANNEL] = "R10", "2026-09-01", CONTROL
    _r10[STOCK_COL[10]], _r10[INDEX_COL[10]] = "3.0", "1.0"   # T+10 은 아직 미성숙
    _iss = validate_ledger([mkheader(), _r10], today=_T)
    chk("랜덤2 의 T+10 조기값이 잡힌다(기본 기간 T+5 만 보던 것 수정)",
        any("T+10" in d_ for _s, _n, d_ in _iss), f"{_iss}")
    chk("그 조기값은 집계에서도 빠진다",
        collect([hdrb, _r10], today=_T)[0] == {}, f"{collect([hdrb, _r10], today=_T)[0]}")
    print()

    print("🧪 리허설에서 드러난 두 결함 (2026-09-04)")
    # ① 대조군은 모든 호라이즌에서 모여야 한다 — 안 그러면 중기(T+10) 검정의 t 가 통째로 없다
    def mk10(ch, s10, i10):
        r = [""] * 34
        r[C_CHANNEL] = ch
        r[STOCK_COL[10]], r[INDEX_COL[10]] = str(s10), str(i10)
        return r
    d, rw, sk, bd = collect([hdr, mk10(CONTROL, 1.0, 0.0), mk10("리포트TOP2_중기", 2.0, 0.0)])
    chk("대조군이 T+10 에서도 모인다(중기 검정용)",
        10 in d.get(CONTROL, {}), f"랜덤2 호라이즌={sorted(d.get(CONTROL, {}))}")
    # ② 호라이즌 미도달 채널이 표에서 사라지면 안 된다
    d, rw, sk, bd = collect([hdr, mk10("랜덤2_배지", 1.0, 0.0)])   # T+5 는 비어 있음
    chk("미성숙 채널도 원시행수로 남는다",
        rw.get("랜덤2_배지") == 1 and sk["미성숙(호라이즌 미도달)"] == 1,
        f"raw={rw.get('랜덤2_배지')} 미성숙={sk['미성숙(호라이즌 미도달)']}")
    md, ro, cf, ps = build_report(d, rw, sk, "2026-01-01")
    chk("N=0 이어도 표에 남고 사유가 적힌다",
        any(r["ch"] == "랜덤2_배지" and r["n"] == 0 for r in ro) and "T+5 미도달" in md)

    print("🧪 §3-5-2 같은 날 짝지어 비교 (2026-09-11)")
    # ① 드리프트 방지 — bydate 를 다 합치면 out 과 **같은 값 집합**이어야 한다.
    #    두 구조를 다른 루프로 쌓았다면 이 검사가 언젠가 깨진다.
    _rows = [hdrb]
    for i, dd in enumerate(["2026-08-03", "2026-08-04", "2026-08-05"]):
        _rows += [mkb("차트TOP2", 3.0 + i, 1.0, dd), mkb(CONTROL, 1.0, 1.0, dd)]
    _d, _rw, _sk, _bd = collect(_rows, today=_T)
    for _ch in ("차트TOP2", CONTROL):
        flat = sorted(v for day in _bd[_ch][5].values() for v in day)
        chk(f"{_ch}: 날짜별 합계가 집계 목록과 같다",
            flat == sorted(_d[_ch][5]), f"{flat} vs {sorted(_d[_ch][5])}")

    # ② 짝짓기 산수 — 차트 순알파는 3.0/4.0/5.0 에서 지수1.0·비용0.35 를 뺀 값,
    #    대조군은 매일 1.0-1.0-0.35. 차이는 +2.0/+3.0/+4.0 이어야 한다.
    t, df, k, m = paired_by_date(_bd, "차트TOP2", 5)
    chk("짝지은 날은 3일, df=2", k == 3 and df == 2, f"k={k} df={df}")
    chk("평균 차이는 +3.00%p", abs(m - 3.0) < 1e-9, f"{m}")

    # ③ 한쪽만 있는 날은 짝이 안 된다 — 조용히 포함시키면 안 된다.
    _solo = _rows + [mkb("차트TOP2", 9.0, 1.0, "2026-08-06")]
    _bd2 = collect(_solo, today=_T)[3]
    chk("대조군이 없는 날은 짝에서 빠진다",
        paired_by_date(_bd2, "차트TOP2", 5)[2] == 3,
        f"k={paired_by_date(_bd2, '차트TOP2', 5)[2]}")

    # ④ 시장이 통째로 움직인 날 — 짝 t 가 Welch 보다 커야 한다.
    #    ⚠️ 이건 **상한을 보여주는 인공 자료다.** 모든 행의 지수를 0 으로 둬서 시장
    #       충격이 알파에 그대로 남아 있다. 실제 원장은 행마다 그 종목의 벤치로
    #       지수를 이미 빼므로(omakase.py:3064) 지울 공통 몫이 거의 없다.
    #       실제 이득은 ④-c 가 보여준다 — 사실상 0 이다. 처음에 이 검사만 보고
    #       "짝 t 가 24까지 오른다"고 보고했던 것이 과장이었다.
    _mkt = [hdrb]
    for i in range(12):
        dd = "2026-08-%02d" % (3 + i)
        shock = 20.0 if i % 2 else -20.0        # 그날 시장 전체가 크게 움직임
        edge = 2.0 + 0.4 * (i % 3)              # 우위는 작고 날마다 조금씩 다르다
        _mkt += [mkb("차트TOP2", shock + edge, 0.0, dd), mkb(CONTROL, shock, 0.0, dd)]
    _md, _mrw, _msk, _mbd = collect(_mkt, today=_T)
    _wt, _wdf = welch(_md["차트TOP2"][5], _md[CONTROL][5])
    _pt = paired_by_date(_mbd, "차트TOP2", 5)[0]
    chk("공통 충격이 크면 Welch 는 효과를 못 본다", abs(_wt) < 1.0, f"welch t={_wt:.3f}")
    chk("짝 검정은 같은 효과를 잡아낸다", _pt > 5.0, f"paired t={_pt:.2f}")

    # ④-c 실제 조건 — 지수를 행별로 빼고 나면 짝짓기가 가져오는 이득이 사라진다.
    #      개별 종목 변동이 시장 변동보다 훨씬 커서 지울 공통 몫 자체가 작다.
    #      이 검사가 §3-5-2 를 "빨라지는 변경"으로 오해하지 않게 막는다.
    _rng = random.Random(11)
    _real = [hdrb]
    for i in range(30):
        dd = "2026-07-%02d" % (1 + i)
        M = _rng.gauss(0, 2.75)                 # 그날 시장
        for ch, edge in (("차트TOP2", 2.2), (CONTROL, 0.0)):
            for _ in range(2):
                r = M + _rng.gauss(0, 9.6) + edge
                # 지수 칸을 M 으로 채운다 = 실제 원장이 하는 일
                _real.append(mkb(ch, round(r, 2), round(M, 2), dd))
    _rd, _rrw, _rsk, _rbd = collect(_real, today=_T)
    _rw_t, _ = welch(_rd["차트TOP2"][5], _rd[CONTROL][5])
    _rp_t = paired_by_date(_rbd, "차트TOP2", 5)[0]
    chk("지수를 행별로 빼면 짝 t 와 Welch t 가 사실상 같다",
        abs(_rp_t - _rw_t) / abs(_rw_t) < 0.25,
        f"welch={_rw_t:.2f} paired={_rp_t:.2f} 비율={_rp_t/_rw_t:.2f}배")
    chk("그래도 짝 검정은 N 을 날짜로 센다(행으로 부풀리지 않는다)",
        paired_by_date(_rbd, "차트TOP2", 5)[2] == 30 and len(_rd["차트TOP2"][5]) == 60,
        f"날짜={paired_by_date(_rbd, '차트TOP2', 5)[2]} 행={len(_rd['차트TOP2'][5])}")

    # ④-b 날마다 차이가 똑같으면 분산이 0 이라 t 가 정의되지 않는다.
    #     그때는 Welch 로 돌아간다 — 실데이터에서 날 일은 없지만 동작을 적어 둔다.
    _flatdiff = [hdrb]
    for i in range(6):
        dd = "2026-08-%02d" % (3 + i)
        _flatdiff += [mkb("차트TOP2", 3.0, 0.0, dd), mkb(CONTROL, 1.0, 0.0, dd)]
    _fbd = collect(_flatdiff, today=_T)[3]
    chk("날짜별 차이가 완전히 같으면 짝 t 는 None (분산 0)",
        paired_by_date(_fbd, "차트TOP2", 5)[0] is None)

    # ⑤ 리포트가 판정에 짝 t 를 쓰는지 — Welch 를 쓰면 여기서 걸린다.
    _rep = build_report(_md, _mrw, _msk, "2026-01-01", _mbd)
    _r = next(r for r in _rep[1] if r["ch"] == "차트TOP2")
    chk("판정 t 는 짝 t 다", abs(_r["t"] - _pt) < 1e-9, f"{_r['t']} vs {_pt}")
    chk("Welch 도 참고로 같이 실린다", "Welch t (참고)" in _rep[0])

    # ⑥ 짝이 성립 안 하면 경고를 남긴다 — 조용히 Welch 로 돌아가면 안 된다.
    _one = collect([hdrb, mkb("차트TOP2", 3.0, 1.0), mkb(CONTROL, 1.0, 1.0)], today=_T)
    _md2 = build_report(_one[0], _one[1], _one[2], "2026-01-01", _one[3])[0]
    chk("짝이 2일 미만이면 리포트가 그 사실을 알린다",
        "짝 검정이 하나도 성립하지 않았다" in _md2)


    print("🧪 §3-5-5 주력 1차 문턱 관측 (2026-09-11)")
    _ms_rows = [hdrb]
    for i in range(16):
        dd = "2026-07-%02d" % (1 + i)
        _ms_rows += [mkb("리포트TOP2_단기", 4.0 + (i % 3), 0.0, dd),
                     mkb("리포트TOP2_단기", 4.2 + (i % 3), 0.0, dd),
                     mkb(CONTROL, 0.1 if i % 2 else -0.1, 0.0, dd)]
    _md_, _mraw, _msk_, _mbd_ = collect(_ms_rows, today=_T)
    _ms = milestone_rows(_md_, _mraw, _mbd_, "2026-01-01")
    _sh = next(r for r in _ms if r["ch"] == "리포트TOP2_단기")
    chk("N 은 행 수, 짝 날짜는 따로 센다 — 섞지 않는다",
        _sh["n"] == 32 and _sh["k"] == 16, f"N={_sh['n']} 날짜={_sh['k']}")
    chk("N≥30 이고 t≥2.0 이면 1차 표시", _sh["hit"], f"t={_sh['t']:.2f}")
    chk("문턱 N 은 행 수 기준이라고 적는다",
        "문턱 `N≥30` 은 **행 수** 기준" in milestone_report(_ms, "2026-01-01", True))

    # 표본이 모자라면 t 가 아무리 커도 1차가 아니다.
    _few = [hdrb]
    for i in range(6):
        dd = "2026-07-%02d" % (1 + i)
        _few += [mkb("리포트TOP2_단기", 9.0 + i, 0.0, dd), mkb(CONTROL, 0.1 if i % 2 else -0.1, 0.0, dd)]
    _fd, _fr, _fs, _fb = collect(_few, today=_T)
    _f1 = next(r for r in milestone_rows(_fd, _fr, _fb, "x") if r["ch"] == "리포트TOP2_단기")
    chk("N<30 이면 t 가 커도 1차가 아니다", (not _f1["hit"]) and _f1["n"] == 6,
        f"N={_f1['n']} t={_f1['t']}")

    # 날짜 불일치 분해 — 대조군이 채널보다 긴 기간에 걸쳐 있을 때가 핵심이다.
    # 겹치는 3일은 대조군이 평범하고, 바깥 3일은 대조군이 크게 벌었다고 만든다.
    # 그러면 Welch 는 대조군 전체 평균이 높아져 채널을 과소평가한다.
    _gap = [hdrb]
    for dd in ("2026-07-01", "2026-07-02", "2026-07-03"):        # 겹치는 날
        _gap += [mkb("리포트TOP2_단기", 3.0, 0.0, dd), mkb(CONTROL, 1.0, 0.0, dd)]
    for dd in ("2026-06-01", "2026-06-02", "2026-06-03"):        # 대조군만 있는 날
        _gap += [mkb(CONTROL, 21.0, 0.0, dd)]
    _gd, _gr, _gs, _gb = collect(_gap, today=_T)
    _g = next(r for r in milestone_rows(_gd, _gr, _gb, "x") if r["ch"] == "리포트TOP2_단기")
    chk("겹치지 않는 대조군 날을 센다", len(_g["outside"]) == 3, f"{_g['outside']}")
    chk("대조군 전체 평균은 바깥 날에 끌려 올라간다",
        _g["ct_all"] > _g["ct_pair"] + 5, f"전체={_g['ct_all']:.2f} 겹친={_g['ct_pair']:.2f}")
    chk("겹친 날 대조군 평균은 그 영향을 안 받는다",
        abs(_g["ct_pair"] - 0.65) < 1e-9, f"{_g['ct_pair']}")
    # 구간 안 결손 — 구간 **밖**을 세면 안 된다. 밖은 아직 시작 안 했거나 미성숙이다.
    _cov = [hdrb]
    for dd in ("2026-07-01", "2026-07-03"):                  # 채널이 낸 날 (2일)
        _cov += [mkb("리포트TOP2_단기", 3.0, 0.0, dd)]
    for dd in ("2026-07-01", "2026-07-02", "2026-07-03"):    # 구간 안 대조군 (3일)
        _cov += [mkb(CONTROL, 1.0, 0.0, dd)]
    for dd in ("2026-06-20", "2026-08-01"):                  # 구간 **밖** 대조군
        _cov += [mkb(CONTROL, 1.0, 0.0, dd)]
    _cd, _cr, _cs, _cb = collect(_cov, today=_T)
    _c = next(r for r in milestone_rows(_cd, _cr, _cb, "x") if r["ch"] == "리포트TOP2_단기")
    chk("구간 안 거래일만 센다(밖 2일은 제외)", _c["span_days"] == 3, f"{_c['span_days']}")
    chk("빠뜨린 날은 07-02 하나뿐", _c["inside_gap"] == ["2026-07-02"], f"{_c['inside_gap']}")
    chk("가동률이 리포트에 찍힌다", "가동률" in milestone_report([_c], "x", False))
    chk("사유를 단정하지 않는다고 밝힌다",
        "누락이라고 단정하지 않는다" in milestone_report([_c], "x", False))

    # 부호 방향 — 처음에 리포트에 거꾸로 적었다. 항등식으로 못 박는다.
    #   짝 차이 = Welch 차이 − 불일치 몫
    # 여기서는 대조군이 바깥 날에 크게 벌어 전체 평균이 높으므로 몫이 **음수**이고,
    # 따라서 Welch 는 채널을 과소평가한다.
    _w, _ = welch(_gd["리포트TOP2_단기"][5], _gd[CONTROL][5])
    _gapv = _g["ct_pair"] - _g["ct_all"]
    chk("불일치 몫 부호 — 대조군 전체가 더 높으면 몫은 음수", _gapv < 0, f"{_gapv:.2f}")
    chk("항등식: 짝 차이 = Welch 차이 − 불일치 몫",
        abs((_g["ch_pair"] - _g["ct_pair"])
            - ((_g["ch_pair"] - _g["ct_all"]) - _gapv)) < 1e-9)
    chk("양수 몫은 과대평가라고 적는다",
        "몫이 양수** → 대조군이 겹친 날에 더 잘 벌었다 → Welch 가 채널을 **과대평가"
        in milestone_report([_g], "x", False))
    chk("불일치 몫이 리포트에 찍힌다",
        "날짜 불일치 몫" in milestone_report([_g], "x", False))
    chk("표본 기간도 같이 찍는다",
        "2026-06-01" in milestone_report([_g], "x", False))

    # 🔒 반복 관찰 방어 — 수동 실행은 1차를 확정하지 못한다고 본문에 적혀야 한다.
    _manual = milestone_report(_ms, "2026-01-01", False)
    chk("수동 실행이면 1차를 확정하지 않는다고 밝힌다",
        "1차를 확정하지 않는다" in _manual and "날짜를 데이터가 고르게" in _manual)
    chk("정기 관측이면 확정 가능하다고 밝힌다",
        "이 기록만 1차 달성일을 확정" in milestone_report(_ms, "2026-01-01", True))
    chk("1차를 넘어도 재현이 남았다고 적는다", "아직 채택이 아니다" in _manual)
    chk("아무도 못 넘으면 문턱을 낮추지 않는다고 적는다",
        "문턱을 낮추지 않는다" in milestone_report(
            [dict(r, hit=False) for r in _ms], "2026-01-01", True))


    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음 — 판정을 돌리지 말 것"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="통계·판정 로직만 검증(시트 접근 없음)")
    ap.add_argument("--out", default="", help="출력 파일 경로. 비우면 docs/판정_<오늘>.md")
    ap.add_argument("--stdout-only", action="store_true", help="파일로 쓰지 않고 화면에만")
    ap.add_argument("--milestone", action="store_true",
                    help="주력(탐색) 1차 문턱 관측만 한다. 판정 파일을 만들지 않는다")
    ap.add_argument("--scheduled", action="store_true",
                    help="정기 관측이다(§3-5-5). 이 표시가 있는 기록만 1차 달성일을 확정한다")
    ap.add_argument("--force", action="store_true",
                    help="같은 날짜 판정 파일이 있어도 덮어쓴다. 박제를 깨는 행위이므로 "
                         "정말 다시 만들어야 할 때만 쓴다")
    a = ap.parse_args()

    if a.self_test:
        return self_test()

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(SHEET_URL)
    rows = doc.worksheet(SHEET_NAME).get_all_values()   # 읽기 전용. 여기 한 줄뿐이다.

    today = datetime.datetime.now(KST).strftime("%Y-%m-%d")

    # ── F07 ① 집계하기 **전에** 원장을 검사한다 ──────────────────────────
    issues = validate_ledger(rows)
    if issues:
        print("\n🔎 [원장 입력 검증]")
        for sev, name, detail in issues:
            print(f"  {'❌' if sev == ABORT else '⚠️'} [{sev}] {name} — {detail}")
    if any(sev == ABORT for sev, _, _ in issues):
        print("\n❌ 치명적 결함이 있어 판정을 중단한다.")
        print("   중복 trade_id 는 **조용히 지우지 않는다** — 어떤 행을 살릴지는")
        print("   원장을 보고 사람이 정할 문제다(감사 권고). 원장을 고친 뒤 다시 돌려라.")
        return 2

    ledger_sha = ledger_fingerprint(rows)
    code_sha = sha256_of(io_read_self())
    data, raw, skipped, bydate = collect(rows)
    _rows_n, _total_n, _bal = reconcile(rows, skipped)
    if not _bal:
        print(f"\n❌ 행 상태 합계가 원시 행수와 다르다 — {_total_n} vs {_rows_n}")
        print("   상태가 상호배타가 아니거나 세는 곳이 빠졌다는 뜻이다(3차 재검증 2-1).")
        return 2
    print(f"🔢 행 상태 합계 검증 — 원시 {_rows_n}행 = 상태 합계 {_total_n} ✅")
    if a.milestone:
        ms = milestone_rows(data, raw, bydate, today)
        text = milestone_report(ms, today, a.scheduled)
        print(text)
        if not a.stdout_only:
            os.makedirs("data/milestone", exist_ok=True)
            path = a.out or f"data/milestone/{today}.md"
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"저장: {path}")
        return 0

    md, rows_out, conf, passed = build_report(data, raw, skipped, today, bydate)

    # ── F07 ② 근거를 판정표에 박아 넣는다 ────────────────────────────────
    md += ("\n## 재현 정보 (F07)\n\n"
           f"- 원장 행수 — **{len(rows) - 1}행**(헤더 제외)\n"
           f"- 원장 SHA256 — `{ledger_sha}` (계산 방식 `{LEDGER_SHA_ALGO}`)\n"
           f"- 판정기 SHA256 — `{code_sha}`\n"
           f"- 입력 검증 — {'경고 ' + str(len(issues)) + '건' if issues else '이상 없음'}\n"
           + "".join(f"  - [{sev}] {n} — {d}\n" for sev, n, d in issues)
           + "\n> 같은 원장·같은 판정기로 다시 돌리면 같은 표가 나와야 한다.\n"
             "> 두 해시가 다르면 그건 **다른 판정**이다.\n")

    if a.stdout_only:
        print(md)
    else:
        path = a.out or f"{OUT_DIR}/판정_{today}.md"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # ── F07 ③ 박제 파일은 **배타 생성**한다 ──────────────────────────
        #    §3-7 이 "그날 값 그대로, 이후 수정 금지"라고 정해 놓고
        #    같은 날 재실행하면 조용히 덮어써지고 있었다.
        try:
            with open(path, "x", encoding="utf-8") as f:
                f.write(md)
        except FileExistsError:
            if not a.force:
                print(f"\n❌ 이미 존재한다 — {path}")
                print("   §3-7 이 정한 박제 파일이다. 덮어쓰지 않는다.")
                print("   정말 다시 만들어야 하면 --force, 아니면 --out 으로 다른 이름을 주라.")
                return 2
            with open(path, "w", encoding="utf-8") as f:
                f.write(md)
            print(f"⚠️ --force 로 덮어썼다 — {path} (박제를 깬 것이므로 이유를 기록할 것)")
        print(f"💾 저장: {path}")

        # ── F07 ④ 원장 원본을 근거로 남긴다 ─────────────────────────────
        #    결과 Markdown 만 남기면 "그때 무엇을 봤나"를 복원할 수 없다(감사 지적).
        #    ⚠️ [R1 정정 2026-09-08] 이 파일은 **실행 머신에만** 남는다.
        #       저장소 커밋 대상이 아니고(.gitignore), 아티팩트에도 싣지 않는다.
        #       즉 액션 러너에서 돌리면 잡이 끝날 때 **사라진다.**
        #       접근 제한 보관소를 붙이기 전까지는 그게 의도된 동작이다 —
        #       공개 경로로 새는 것보다 낫다. 재현 근거는 판정표의 SHA256 지문이 맡는다.
        #    ⚠️ [R3 정정 2026-09-08] 예전에는 `판정_{날짜}_ledger.json` 이라는
        #       **날짜 고정 이름**에 `"w"` 로 썼다. 판정표는 배타 생성으로
        #       막아 놓고 근거 파일만 같은 날 재실행 때 조용히 덮어써진 것이다 —
        #       두 번째 실행이 첫 번째가 무엇을 봤는지를 지워 버린다.
        #       실행마다 **고유 디렉터리**를 배타 생성하고, 판정표·원장·설정을
        #       한 묶음으로 같이 넣는다. 흩어져 있으면 나중에 어느 원장이 어느
        #       판정표에 대응하는지 맞출 수가 없다.
        run_id = run_bundle_id(ledger_sha, code_sha)
        meta = {"run_id": run_id,
                "captured_at": datetime.datetime.now(KST).isoformat(),
                "sheet": SHEET_NAME, "sheet_url": SHEET_URL,
                "ledger_sha256": ledger_sha,
                "ledger_sha_algo": LEDGER_SHA_ALGO,
                "code_sha256": code_sha,
                "verdict_path": path,
                "row_count": len(rows) - 1,
                "config": {"COST_PCT": COST_PCT, "MIN_N": MIN_N,
                           "T_SURVIVE": T_SURVIVE, "T_DISCARD": T_DISCARD,
                           "CONTROL": CONTROL, "HORIZON": HORIZON,
                           "DEFAULT_HORIZON": DEFAULT_HORIZON},
                "issues": [{"severity": s_, "name": n, "detail": d}
                           for s_, n, d in issues]}
        try:
            run_dir = save_run_bundle(LEDGER_DIR, run_id, meta, rows, md)
        except FileExistsError:
            print(f"\n❌ 실행 근거가 이미 있다 — {LEDGER_DIR}/{run_id}")
            print("   같은 초에 같은 원장·같은 코드로 두 번 돌았다는 뜻이다.")
            print("   근거를 덮어쓰지 않고 멈춘다.")
            return 2
        print(f"🗄️ 실행 근거 한 묶음: {run_dir}/ (meta.json · ledger.json · 판정.md)")
        print("   ⚠️ 이 디렉터리는 실행 머신에만 남는다(커밋·아티팩트 모두 제외, R1).")
        print("      액션에서 돌렸다면 잡 종료와 함께 사라진다. 보존이 필요하면")
        print("      접근 제한 보관소를 먼저 붙여라. 공개 경로로 내보내지 말 것.")

    # 로그 tail 에서 바로 보이도록 핵심만 다시 찍는다
    print("\n════════ 판정 요약 ════════")
    print(f"전체 {len(rows) - 1}행 · 채널 {len(rows_out)}개 · 확증 검정 대상 m={len(conf)}")
    for r in sorted(rows_out, key=lambda x: (x["ctrl"], -(x["t"] or -9))):
        print(f"  {'*' if r['ctrl'] else ' '}{r['ch']:<18} T+{r['h']:<3}"
              f" raw={r['raw']:<4} N={r['n']:<4}"
              f" 순알파={('%+.2f%%' % r['mean']) if r['mean'] is not None else '—':>9}"
              f" t={('%.2f' % r['t']) if r['t'] is not None else '—':>6}  {r['v']}")
    if not conf:
        print("  → 확증 검정 대상 0개. §3-7 대로 문턱을 낮추지 않고 관찰 연장으로 남긴다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
