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
import os, sys, math, json, hashlib, argparse, datetime

KST = datetime.timezone(datetime.timedelta(hours=9))
# ⚠️ 문서 지정은 **URL 로** 한다. 처음에 open("HYEOKS_주식_자동화") 로 썼다가
#    첫 실행이 그대로 죽었다 — 이름으로 여는 방식은 드라이브 검색에 의존해서
#    이름이 조금만 달라도 실패한다. phase2 가 쓰는 URL 을 그대로 재사용한다.
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SHEET_NAME = "백테스트_로그"
OUT_DIR = "docs"
# 원장 원본 보관 위치. **공개 저장소에 커밋하지 않는다**(.gitignore) — 아티팩트로만 남긴다.
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
SANE_RETURN_ABS = 500.0      # |수익률| 상한(%). 넘으면 이상치로 보고만 한다


def validate_ledger(rows):
    """원장을 집계하기 **전에** 검사한다. (심각도, 항목, 상세) 목록을 돌려준다."""
    issues = []
    if not rows or len(rows) < 2:
        issues.append((ABORT, "원장 비어 있음", f"행 {len(rows)}개"))
        return issues

    header = [str(c).strip() for c in rows[0]]
    need = max(C_TRADE_ID, C_ENTRY_DATE, C_CHANNEL, C_EXCLUDE,
               max(STOCK_COL.values()), max(INDEX_COL.values()))
    if len(header) <= need:
        issues.append((ABORT, "헤더 열 부족",
                       f"{len(header)}열 · 최소 {need + 1}열 필요"))
    if header and header[C_TRADE_ID] != "trade_id":
        issues.append((ABORT, "헤더 불일치",
                       f"0번 열이 'trade_id' 가 아님 — '{header[C_TRADE_ID] if header else ''}'"))

    seen, dup, no_id, bad_date, nonfinite, extreme = {}, [], 0, [], [], []
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

        d = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        if d:
            try:
                datetime.datetime.strptime(d, "%Y-%m-%d")
            except ValueError:
                bad_date.append((i, d))

        for col in set(STOCK_COL.values()) | set(INDEX_COL.values()):
            if len(row) <= col:
                continue
            raw = str(row[col]).strip()
            if not raw:
                continue
            try:
                f = float(raw.replace(",", "").replace("%", ""))
            except Exception:
                continue
            if not math.isfinite(f):
                nonfinite.append((i, col, raw))
            elif abs(f) > SANE_RETURN_ABS:
                extreme.append((i, col, f))

    if dup:
        issues.append((ABORT, "trade_id 중복",
                       "; ".join(f"{t} (행 {a}, {b})" for t, a, b in dup[:5])
                       + (f" 외 {len(dup) - 5}건" if len(dup) > 5 else "")))
    if nonfinite:
        issues.append((ABORT, "비유한 수치(nan/inf)",
                       "; ".join(f"행{i} 열{c}='{v}'" for i, c, v in nonfinite[:5])))
    if no_id:
        issues.append((WARN, "trade_id 빈 행", f"{no_id}행"))
    if bad_date:
        issues.append((WARN, "진입일 형식 이상",
                       "; ".join(f"행{i}='{d}'" for i, d in bad_date[:5])))
    if extreme:
        issues.append((WARN, f"|수익률| > {SANE_RETURN_ABS:.0f}%",
                       "; ".join(f"행{i} 열{c}={v:+.1f}%" for i, c, v in extreme[:5])))
    return issues


def sha256_of(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ledger_fingerprint(rows):
    """원장 내용의 지문. 행 순서까지 포함한다 — 같은 데이터면 같은 값이 나와야 한다."""
    return sha256_of("\n".join("\t".join(str(c) for c in r) for r in rows))


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


def collect(rows):
    """채널 → {horizon: [순알파...]}. 비용은 §3-4-2 대로 여기서 뺀다.

    ⚠️ 리허설(2026-09-04)에서 두 가지 결함이 드러나 고친 버전이다.

    ① 대조군을 **자기 호라이즌에서만** 모으고 있었다. 그래서 리포트중기(T+10)를
       검정할 때 비교할 랜덤2 T+10 이 없어 t 가 통째로 '—' 로 나왔다.
       9/7 에 그대로 돌았다면 '표본 부족'이 아니라 '대조군 부재' 때문에
       판정 불가가 뜨는데, 로그만 봐서는 그 둘을 구분할 수 없었다.
       → 대조군은 **쓰이는 모든 호라이즌**에서 모은다.

    ② 아직 호라이즌에 도달하지 않은 채널이 **표에서 통째로 사라졌다**.
       랜덤2_배지(8/31 신설)는 T+5 가 아직 안 채워져 한 줄도 안 나왔다.
       "채널이 없다"와 "표본이 아직 안 익었다"는 완전히 다른 이야기다.
       → 원시 행수(raw)를 따로 세어 N=0 이어도 표에 남긴다.
    """
    horizons_in_use = set(HORIZON.values()) | {DEFAULT_HORIZON, 20}
    out, raw = {}, {}
    skipped = {"제외표식": 0, "채널없음": 0, "미성숙(호라이즌 미도달)": 0}
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
        # 대조군은 모든 호라이즌에서 모은다(①). 장기 채널은 §3-3 대로 T+20 도 같이(②는 raw 로).
        if ch == CONTROL:
            want = horizons_in_use
        elif ch == LONG_CHANNEL:
            want = {h, 20}
        else:
            want = {h}
        matured = False
        for hh in want:
            si, ii = STOCK_COL.get(hh), INDEX_COL.get(hh)
            if si is None or len(row) <= max(si, ii):
                continue
            s, i = _num(row[si]), _num(row[ii])
            if s is None or i is None:
                continue
            cost = 0.0 if ch.startswith("지수벤치") else COST_PCT
            out.setdefault(ch, {}).setdefault(hh, []).append(s - i - cost)
            if hh == h:
                matured = True
        if not matured:
            skipped["미성숙(호라이즌 미도달)"] += 1
    return out, raw, skipped


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


def holm(pairs):
    """§3-5 계층1 — Holm–Bonferroni. pairs = [(채널, p)]. 통과 집합을 돌려준다.

    처음 실패하는 지점에서 멈추고 그 뒤는 전부 탈락(문서 절차 4번 그대로)."""
    ok, m = set(), len(pairs)
    for i, (ch, p) in enumerate(sorted(pairs, key=lambda x: x[1]), start=1):
        if p <= 0.05 / (m - i + 1):
            ok.add(ch)
        else:
            break
    return ok


# ── 리포트 ────────────────────────────────────────────────────────────────
def build_report(data, raw, skipped, today):
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
        t, df = welch(vals, base) if ch != CONTROL else (None, None)
        p = t_two_sided_p(t, df) if (t is not None and df) else None
        ann = mean * (TRADING_DAYS_YEAR / h) if mean is not None else None
        is_ctrl = ch.startswith(CONTROL_LIKE)
        v = "대조군(판정 대상 아님)" if is_ctrl else verdict_for(ch, n, t, mean)
        if (not is_ctrl) and ch != LONG_CHANNEL and n >= MIN_N and p is not None:
            conf.append((ch, p))
        if n == 0:
            v = (f"표본 0 — 원시 {raw.get(ch, 0)}행 전부 T+{h} 미도달"
                 if raw.get(ch) else "행 없음")
        rows_out.append({"ch": ch, "h": h, "n": n, "raw": raw.get(ch, 0),
                         "mean": mean, "ann": ann, "t": t, "p": p,
                         "v": v, "ctrl": is_ctrl})

    passed = holm(conf) if conf else set()
    for r in rows_out:
        if r["v"].startswith("생존·강화"):
            r["v"] = ("생존·강화 ✅ (Holm 통과)" if r["ch"] in passed
                      else "관찰 연장 — t는 넘었으나 Holm 미통과(§3-5)")

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
    A(f"| 관찰 연장 | N≥{MIN_N} 이고 {T_DISCARD} ≤ t < {T_SURVIVE} | §3-1 |")
    A(f"| 폐기 | N≥{MIN_N} 이고 t < {T_DISCARD} — **보정 없음** | §3-1 · §3-5 |")
    A(f"| 판정 불가 | N < {MIN_N} → 관찰 연장 | §3-1 |")
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

    A("## 다중비교 보정 (§3-5 계층 1)")
    A("")
    if conf:
        m = len(conf)
        A(f"확증 검정 대상 **m = {m}개** (N≥{MIN_N} 인 판정 대상 채널만).")
        A("")
        A("| 순위 | 채널 | p | Holm 문턱 `0.05/(m−i+1)` | 결과 |")
        A("|--:|---|--:|--:|---|")
        stop = False
        for i, (ch, p) in enumerate(sorted(conf, key=lambda x: x[1]), start=1):
            thr = 0.05 / (m - i + 1)
            if stop:
                res = "— (앞에서 멈춤)"
            elif p <= thr:
                res = "통과"
            else:
                res, stop = "**여기서 멈춤**", True
            A(f"| {i} | {ch} | {p:.4f} | {thr:.4f} | {res} |")
    else:
        A(f"**확증 검정 대상이 0개다.** N≥{MIN_N} 를 채운 판정 대상 채널이 없다.")
        A("")
        A("§3-7 이 이 상황을 미리 인정해 뒀다 — *\"그날 대부분의 채널이 N<30 일 가능성이 높다\"*.")
        A("**문턱을 낮추지 않는다.** 관찰 연장이라고 쓰고, 10/5 재판정으로 넘긴다.")
    A("")

    A("## 표본 제외 (§4-2)")
    A("")
    for k, v in skipped.items():
        A(f"- {k}: {v}행")
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

    print("🧪 F07 — 원장 입력 검증 (2026-09-07 감사)")
    def mkrow(tid, ch, s5=1.0, i5=0.0, date="2026-09-01"):
        r = [""] * 34
        r[C_TRADE_ID], r[C_ENTRY_DATE], r[C_CHANNEL] = tid, date, ch
        r[STOCK_COL[5]], r[INDEX_COL[5]] = str(s5), str(i5)
        return r
    hdr34 = ["trade_id"] + [""] * 33
    good = [hdr34, mkrow("A_차트TOP2_000660", "차트TOP2")]
    chk("정상 원장 → 이상 없음", validate_ledger(good) == [], f"{validate_ledger(good)}")
    dup = [hdr34, mkrow("SAME", "차트TOP2"), mkrow("SAME", "수급TOP2")]
    chk("trade_id 중복 → ABORT", ABORT in [x[0] for x in validate_ledger(dup)])
    nan = [hdr34, mkrow("N1", "차트TOP2", s5="nan")]
    chk("비유한 수치(nan) → ABORT", ABORT in [x[0] for x in validate_ledger(nan)])
    chk("_num('nan') 은 None (성숙값에 안 들어간다)", _num("nan") is None)
    chk("_num('inf') 은 None", _num("inf") is None)
    chk("_num('1.5') 는 살아 있다", _num("1.5") == 1.5)
    bad = [hdr34, mkrow("D1", "차트TOP2", date="2026-13-99")]
    chk("날짜 형식 이상 → WARN(중단 아님)", [x[0] for x in validate_ledger(bad)] == [WARN])
    ext = [hdr34, mkrow("E1", "차트TOP2", s5=9999.0)]
    chk("극단 수익률 → WARN(중단 아님)", [x[0] for x in validate_ledger(ext)] == [WARN])
    noh = [["엉뚱한열"] + [""] * 33, mkrow("H1", "차트TOP2")]
    chk("헤더 불일치 → ABORT", ABORT in [x[0] for x in validate_ledger(noh)])
    chk("빈 원장 → ABORT", validate_ledger([])[0][0] == ABORT)
    _f1 = ledger_fingerprint(good)
    chk("같은 원장 → 같은 지문", _f1 == ledger_fingerprint(good))
    chk("행 하나만 달라도 지문이 바뀐다",
        _f1 != ledger_fingerprint([hdr34, mkrow("A_차트TOP2_000660", "차트TOP2", s5=1.1)]))
    print()

    print("🧪 §3-4-2 비용·§4-2 제외")
    hdr = [""] * 34
    def mk(ch, s, i, memo=""):
        r = [""] * 34
        r[C_CHANNEL], r[C_EXCLUDE] = ch, memo
        r[STOCK_COL[5]], r[INDEX_COL[5]] = str(s), str(i)
        return r
    d, rw, sk = collect([hdr, mk("차트TOP2", 3.0, 1.0)])
    chk("순알파 = 종목−지수−0.35", abs(d["차트TOP2"][5][0] - (3.0 - 1.0 - 0.35)) < 1e-9,
        f"={d['차트TOP2'][5][0]:.2f}")
    d, rw, sk = collect([hdr, mk("지수벤치_KOSPI", 3.0, 1.0)])
    chk("지수벤치는 비용 면제", abs(d["지수벤치_KOSPI"][5][0] - 2.0) < 1e-9)
    d, rw, sk = collect([hdr, mk("차트TOP2", 3.0, 1.0, "거래정지 — 측정 제외")])
    chk("'제외' 표식 행은 빠진다", sk["제외표식"] == 1 and not d)
    d, rw, sk = collect([hdr, mk("차트TOP2", 3.0, 1.0, "집계복귀 — 철회")])
    chk("'집계복귀'(제외 두 글자 없음) 는 살린다", sk["제외표식"] == 0 and bool(d))
    # 세 구현이 같은 판정을 하는지 — 단순 포함 검사와 일치해야 한다
    for memo, want in [("거래정지 — 측정 제외", True), ("제외:위험종목", True),
                       ("집계복귀 — 철회", False), ("", False),
                       ("집계복귀 — 이전 제외 철회", True)]:
        r = [""] * 34
        r[C_EXCLUDE] = memo
        chk(f"제외판정 '{memo or '(빈칸)'}' → {want}", is_excluded(r) is want)

    print("🧪 리허설에서 드러난 두 결함 (2026-09-04)")
    # ① 대조군은 모든 호라이즌에서 모여야 한다 — 안 그러면 중기(T+10) 검정의 t 가 통째로 없다
    def mk10(ch, s10, i10):
        r = [""] * 34
        r[C_CHANNEL] = ch
        r[STOCK_COL[10]], r[INDEX_COL[10]] = str(s10), str(i10)
        return r
    d, rw, sk = collect([hdr, mk10(CONTROL, 1.0, 0.0), mk10("리포트TOP2_중기", 2.0, 0.0)])
    chk("대조군이 T+10 에서도 모인다(중기 검정용)",
        10 in d.get(CONTROL, {}), f"랜덤2 호라이즌={sorted(d.get(CONTROL, {}))}")
    # ② 호라이즌 미도달 채널이 표에서 사라지면 안 된다
    d, rw, sk = collect([hdr, mk10("랜덤2_배지", 1.0, 0.0)])   # T+5 는 비어 있음
    chk("미성숙 채널도 원시행수로 남는다",
        rw.get("랜덤2_배지") == 1 and sk["미성숙(호라이즌 미도달)"] == 1,
        f"raw={rw.get('랜덤2_배지')} 미성숙={sk['미성숙(호라이즌 미도달)']}")
    md, ro, cf, ps = build_report(d, rw, sk, "2026-01-01")
    chk("N=0 이어도 표에 남고 사유가 적힌다",
        any(r["ch"] == "랜덤2_배지" and r["n"] == 0 for r in ro) and "T+5 미도달" in md)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음 — 판정을 돌리지 말 것"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="통계·판정 로직만 검증(시트 접근 없음)")
    ap.add_argument("--out", default="", help="출력 파일 경로. 비우면 docs/판정_<오늘>.md")
    ap.add_argument("--stdout-only", action="store_true", help="파일로 쓰지 않고 화면에만")
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
    data, raw, skipped = collect(rows)
    md, rows_out, conf, passed = build_report(data, raw, skipped, today)

    # ── F07 ② 근거를 판정표에 박아 넣는다 ────────────────────────────────
    md += ("\n## 재현 정보 (F07)\n\n"
           f"- 원장 행수 — **{len(rows) - 1}행**(헤더 제외)\n"
           f"- 원장 SHA256 — `{ledger_sha}`\n"
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
        #    ⚠️ 공개 저장소에 원장을 커밋하지 않는다 — .gitignore 대상이고
        #       워크플로 아티팩트로만 보존한다.
        lp = f"{LEDGER_DIR}/판정_{today}_ledger.json"
        os.makedirs(LEDGER_DIR, exist_ok=True)
        with open(lp, "w", encoding="utf-8") as f:
            json.dump({"captured_at": datetime.datetime.now(KST).isoformat(),
                       "sheet": SHEET_NAME, "ledger_sha256": ledger_sha,
                       "code_sha256": code_sha,
                       "issues": [{"severity": s_, "name": n, "detail": d}
                                  for s_, n, d in issues],
                       "rows": rows}, f, ensure_ascii=False)
        print(f"🗄️ 원장 근거: {lp} (저장소에 커밋하지 않음 · 아티팩트로 보존)")

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
