# -*- coding: utf-8 -*-
# ==========================================================================
# 🌇 HYEOKS 종가베팅 분석기 — §6-12-3(확정 명세) + §6-12-5(익일 시가 청산)
# --------------------------------------------------------------------------
# 무엇인가
#   15:05 슬롯에 사서 **익일 시가에 전량 청산**했을 때, '오후수급 비율' 최상위
#   5분위(Q5)가 같은 날 유니버스 전체보다 나았는가를 재는 도구다.
#   사전등록은 전부 §6-12·§6-12-2·§6-12-3·§6-12-5·§6-12-6 에 이미 얼려 두었고,
#   이 파일은 그 명세를 **그대로 옮긴 것**이다. 여기서 정의를 새로 만들지 않는다.
#
# 무엇을 하지 않나 (이게 더 중요하다)
#   · 시트에 쓰지 않는다. 네트워크를 쓰지 않는다. `data/market_snapshot` 만 읽는다.
#   · **파일럿이 끝나기 전에는 수익률을 아예 계산하지 않는다.**
#     §6-12-3 "실성과 계산만 파일럿 종료일까지 막는다" 의 구현이다.
#     '계산은 하되 안 보여주기' 로는 막을 수 없다 — 계산하면 결국 본다.
#   · 파일럿이 끝나도 σ·자기상관까지만 낸다. **알파를 판정하지 않는다.**
#   · 본 구간이 끝나야 ①·② 와 HAC 판정을 낸다. 그 전엔 함수가 거부한다.
#
# 단계 (데이터가 정하며, 플래그로 건너뛸 수 없다)
#   ┌ 0. 파일럿 미완  (성숙일 < 20)  → 수집·품질·탈락만. 수익률 계산 금지
#   ├ 1. 파일럿 완료  (성숙일 ≥ 20)  → σ·자기상관·필요 N 만. 평균/t/p 출력 금지
#   └ 2. 본 구간 완료 (성숙일 ≥ 20+본구간) → ①·②·HAC 판정 1회
#
# 판정은 HAC(지연 5거래일) 하나가 한다(§6-12-3). 일반 t·블록 부트스트랩은
# 참고이며, HAC 와 크게 엇갈리면 유리한 쪽을 고르지 않고 '불안정'으로 보고한다.
# ==========================================================================
import os, sys, csv, gzip, math, argparse, datetime, statistics, json, tempfile
from closing_study import split_windows, VERSION as STUDY_VERSION
from hyeoks_trading_calendar import load_nontrading, next_trading_day

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hyeoks_verdict import t_two_sided_p, block_bootstrap_p   # 이미 자기검증된 통계만 빌린다

SNAP_DIR   = "data/market_snapshot"
KST        = datetime.timezone(datetime.timedelta(hours=9))

# ── §6-12 정의 1 · §6-12-3 · §6-12-6 에서 온 상수. 여기서 조정하지 않는다 ──
MIN_TURNOVER   = 5_000_000_000    # 정의 1 — 누적 거래대금 50억
PILOT_DAYS     = 20               # §6-12-3 파일럿 길이(거래일)
COST           = 0.0035           # §3-4-2 — ②에만 쓴다. ①에는 절대 쓰지 않는다
TARGET_EFFECT  = 0.0010           # +0.10%p/일. σ 추정 전에 고정한 값
Z_ALPHA, Z_BETA = 1.96, 0.84
HAC_LAG        = 5                # 고정. 데이터를 보고 바꾸지 않는다
BLOCK          = 5
RESAMPLES      = 10000
MAIN_CAP       = 120              # 자원 한도
MAIN_FLOOR     = 40               # §6-12-6 — 본 구간이 파일럿보다 짧으면 의미가 없다
MIN_PER_Q      = 10               # §6-12-6 — 분위당 최소 종목 수
QUINTILES      = 5
PRICE_LIMIT    = 0.30             # 가격제한폭
LIMIT_TOL      = 0.005            # 제한폭 판정 여유(체결가 호가단위·반올림)
ADJ_TOL        = 0.35             # §6-12-6 — 기업행사 조정 의심 문턱
REVERSAL_TOL   = 0.999            # 누적 거래대금은 줄어들 수 없다
BACKUP_FROM    = (15, 6)          # 이 시각 이후 캡처는 백업 cron 발사분(§6-12 주의)

DROP_REASONS = ["거래대금미달", "거래정지", "관리종목류", "진입가없음",
                "13시행없음", "분모0", "거래대금역전",
                "익일소멸", "익일시가없음", "익일제한폭이탈", "기업행사조정의심"]


# ══════════════════════════════════════════════════════════════════════
# 1. 읽기 — 스냅샷 원본만 본다
# ══════════════════════════════════════════════════════════════════════
def _f(v, default=0.0):
    try:
        result = float(str(v).replace(",", "").strip() or default)
        return result if math.isfinite(result) else default
    except Exception:
        return default


def read_snapshot(path):
    """스냅샷 1개 → (meta, {코드: 행}, 중복건수). 중복은 **먼저 나온 행**을 남긴다.

    `_read` (hyeoks_market_snapshot.py) 는 dict 로 덮어써서 나중 행이 이긴다.
    여기서 먼저 나온 행을 남기는 이유는 '어느 쪽이 맞는가' 를 모르기 때문이다.
    모르면 규칙을 고정하고 건수를 보고한다 — 그게 재현 가능한 쪽이다.
    """
    with gzip.open(path, "rt", encoding="utf-8") as fp:
        rows = list(csv.reader(fp))
    meta = {}
    for kv in rows[0][1:]:
        if "=" in kv:
            k, v = kv.split("=", 1)
            meta[k] = v
    hdr, out, dup = rows[1], {}, 0
    for r in rows[2:]:
        d = dict(zip(hdr, r))
        code = (d.get("itemcode") or "").strip()
        if not code:
            continue
        if code in out:
            dup += 1
            continue
        out[code] = d
    return meta, out, dup


def scan_dates(snap_dir=SNAP_DIR):
    """15:05 슬롯이 있는 날짜를 오름차순으로. 이것이 우리가 관측한 거래일 달력이다."""
    days = []
    for fn in sorted(os.listdir(snap_dir)) if os.path.isdir(snap_dir) else []:
        if fn.endswith("_1505.csv.gz"):
            days.append(fn[:10])
    return sorted(set(days))


# ══════════════════════════════════════════════════════════════════════
# 2. 유니버스와 분위 — §6-12 정의 1·7, §6-12-6 동률·최소수 규칙
# ══════════════════════════════════════════════════════════════════════
def build_universe(rows1505, rows1300):
    """정의 1 유니버스 ∩ {오후수급 비율 계산 가능}.

    ⚠️ 비율을 못 내는 종목은 **유니버스에서도 뺀다.** Q5 가 뽑힐 수 없는 종목이
    대조군에만 들어가면, 대조군이 'Q5 를 뽑은 모집단' 이 아니게 된다.
    빠진 건수는 전부 세어서 보고한다 — 조용히 줄이지 않는다.
    """
    items, drop = [], {k: 0 for k in DROP_REASONS}
    for code, r in rows1505.items():
        if _f(r.get("tradeAmount")) < MIN_TURNOVER:
            drop["거래대금미달"] += 1
            continue
        if (r.get("tradeStopYn") or "").strip().upper() == "Y":
            drop["거래정지"] += 1
            continue
        if str(r.get("manageStatusGb") or "0").strip() != "0":
            drop["관리종목류"] += 1
            continue
        p = _f(r.get("nowPrice"))
        if p <= 0:
            drop["진입가없음"] += 1
            continue
        prev = rows1300.get(code)
        if prev is None:
            drop["13시행없음"] += 1
            continue
        a13 = _f(prev.get("tradeAmount"))
        if a13 <= 0:
            drop["분모0"] += 1
            continue
        ratio = _f(r.get("tradeAmount")) / a13
        if ratio < REVERSAL_TOL:          # 누적값이 줄었다 = 둘 중 하나가 잘못된 관측
            drop["거래대금역전"] += 1
            continue
        items.append({"code": code, "name": r.get("itemname", ""),
                      "P": p, "ratio": ratio})
    return items, drop


def assign_quintiles(items):
    """§6-12-6 — 순위 기반 5분위. 동률은 **종목코드 오름차순**으로 깬다.

    왜 순위 기반인가: 비율값으로 경계를 그으면 동률이 한쪽에 몰린 날 Q5 가
    텅 비거나 절반이 된다. 순위로 자르면 분위 크기가 항상 고르다.
    왜 종목코드로 깨는가: 결과와 아무 상관이 없는 값이라서다. 수익률·거래대금
    으로 깨면 **그 순간 결과가 배정에 들어온다.**
    돌려주는 값: (items, 동률로 경계가 갈린 건수)
    """
    ordered = sorted(items, key=lambda x: (x["ratio"], x["code"]))
    n = len(ordered)
    for i, it in enumerate(ordered):
        it["q"] = min(QUINTILES - 1, (i * QUINTILES) // n)
    ties = 0
    for i in range(1, n):
        if ordered[i]["q"] != ordered[i - 1]["q"] and \
           abs(ordered[i]["ratio"] - ordered[i - 1]["ratio"]) < 1e-12:
            ties += 1
    return ordered, ties


# ══════════════════════════════════════════════════════════════════════
# 3. 수익률 — §6-12-5. **여기서 종가 이동분을 빼지 않는다**
# ══════════════════════════════════════════════════════════════════════
def exit_open(code, nxt_slots):
    """익일 시가와, 그 시가를 검사할 전일종가. (시가, 전일종가, 사유) 중 하나."""
    chosen = None
    seen = []
    for slot in ("1300", "1505"):          # 13시 것을 우선한다 — 장 초반 값이라 정정이 덜 섞인다
        rows = nxt_slots.get(slot)
        if not rows:
            continue
        r = rows.get(code)
        if not r:
            continue
        o = _f(r.get("openPrice"))
        if o > 0:
            seen.append(o)
            if chosen is None:
                chosen = (o, r)
    if chosen is None:
        # 다음 날 표에 아예 없으면 상장폐지·이전상장, 있는데 시가가 0이면 거래정지다
        present = any((nxt_slots.get(s) or {}).get(code) for s in ("1300", "1505"))
        return None, None, ("익일시가없음" if present else "익일소멸"), seen
    o, r = chosen
    rate = _f(r.get("prevChangeRate"), default=None) if r.get("prevChangeRate") not in (None, "") else None
    prev_close = None
    if rate is not None and rate > -100:
        now = _f(r.get("nowPrice"))
        if now > 0:
            prev_close = now / (1.0 + rate / 100.0)
    return o, prev_close, None, seen


def day_returns(items, nxt_slots, drop, with_returns=True):
    """분위 배정이 끝난 종목들의 익일 시가 수익률. 제외 사유는 drop 에 누적한다."""
    kept, open_mismatch = [], 0
    for it in items:
        o, prev_close, why, seen = exit_open(it["code"], nxt_slots)
        if len(seen) == 2 and seen[0] != seen[1]:
            open_mismatch += 1
        if why:
            drop[why] += 1
            continue
        if prev_close is None:
            drop["익일시가없음"] += 1      # 검사할 수 없으면 쓰지 않는다
            continue
        # ① 거래소 규칙 — 시가는 전일종가 ±30% 안에 있어야 한다
        if abs(o / prev_close - 1.0) > PRICE_LIMIT + LIMIT_TOL:
            drop["익일제한폭이탈"] += 1
            continue
        # ② 기업행사 — 익일의 기준가 축과 우리 진입가 축이 다르면 수익률이 허수다
        if abs(prev_close / it["P"] - 1.0) > ADJ_TOL:
            drop["기업행사조정의심"] += 1
            continue
        it = dict(it)
        if with_returns:
            it["ret"] = o / it["P"] - 1.0  # no return calculation in quality-only mode
        kept.append(it)
    return kept, open_mismatch


def day_stats(kept):
    """하루치 값 하나씩. ①은 비용 전, ②는 비용 후 — 둘을 섞지 않는다."""
    if not kept:
        return None
    uni = statistics.fmean(x["ret"] for x in kept)
    qm = {}
    for q in range(QUINTILES):
        g = [x["ret"] for x in kept if x["q"] == q]
        qm[q] = statistics.fmean(g) if g else None
    med = statistics.median(x["ret"] for x in kept)
    q5 = qm[QUINTILES - 1]
    if q5 is None:
        return None
    return {
        "n": len(kept),
        "uni_mean": uni,
        "uni_median": med,
        "q_mean": qm,
        "rel": q5 - uni,                        # ① 주 분석. 비용 미적용(상쇄)
        "abs_net": q5 - COST,                   # ② 비용 후 절대. 참고가 아니라 별도 질문
        "rel_median": q5 - med,                 # 기술 참고. 판단에 쓰지 않는다
        "spread": (q5 - qm[0]) if qm[0] is not None else None,
    }


# ══════════════════════════════════════════════════════════════════════
# 4. 하루를 통째로 — 단계 0 에서는 여기까지만 돌고 수익률에 손대지 않는다
# ══════════════════════════════════════════════════════════════════════
def build_day(date, snap_dir, nontrading, dates, with_returns):
    """하루치 결과 dict. `with_returns=False` 면 **수익률을 계산하지 않는다.**"""
    out = {"date": date, "ok": False, "why": "", "drop": {k: 0 for k in DROP_REASONS}}
    if date in nontrading or datetime.date.fromisoformat(date).weekday() >= 5:
        out['why'] = '휴장일파일'
        return out
    p1505 = os.path.join(snap_dir, f"{date}_1505.csv.gz")
    p1300 = os.path.join(snap_dir, f"{date}_1300.csv.gz")
    if not (os.path.exists(p1505) and os.path.exists(p1300)):
        out["why"] = "슬롯결측"
        return out
    m5, r5, dup5 = read_snapshot(p1505)
    m3, r3, dup3 = read_snapshot(p1300)
    out["captured"] = m5.get("capturedAt", "")
    out["dup"] = dup5 + dup3
    items, drop = build_universe(r5, r3)
    out["drop"] = drop
    out["universe"] = len(items)
    if len(items) < QUINTILES * MIN_PER_Q:
        out["why"] = f"유니버스 {len(items)}종목 < {QUINTILES * MIN_PER_Q}(분위당 {MIN_PER_Q})"
        return out
    items, ties = assign_quintiles(items)
    out["ties"] = ties

    nd = next_trading_day(date, nontrading)
    out["exit_date"] = nd
    if nd is None or nd not in dates:
        out["why"] = "익일미성숙" if (nd and nd > (dates[-1] if dates else "")) else "익일달력결측"
        return out
    nxt = {}
    for slot in ("1300", "1505"):
        pp = os.path.join(snap_dir, f"{nd}_{slot}.csv.gz")
        if os.path.exists(pp):
            nxt[slot] = read_snapshot(pp)[1]
    kept, mismatch = day_returns(items, nxt, out["drop"], with_returns=with_returns)
    out["open_mismatch"] = mismatch
    out["kept"] = len(kept)
    if not kept:
        out["why"] = "익일 가격을 붙일 수 있는 종목이 없다"
        return out
    if min(sum(1 for x in kept if x["q"] == q) for q in range(QUINTILES)) < MIN_PER_Q:
        out["why"] = f"익일 결측 후 어떤 분위가 {MIN_PER_Q}종목 미만"
        return out
    if with_returns:
        out.update(day_stats(kept))
    out["ok"] = True
    out["matured"] = True
    return out


def collect(snap_dir=SNAP_DIR, with_returns=False):
    dates = scan_dates(snap_dir)
    nont = load_nontrading(snap_dir)
    return [build_day(d, snap_dir, nont, dates, with_returns) for d in dates], dates


def matured_count(days):
    return sum(1 for d in days if d.get("matured"))


# ══════════════════════════════════════════════════════════════════════
# 5. 통계 — HAC 하나가 판정한다
# ══════════════════════════════════════════════════════════════════════
def autocov(x, k):
    n = len(x)
    m = statistics.fmean(x)
    return sum((x[t] - m) * (x[t - k] - m) for t in range(k, n)) / n


def hac(x, lag=HAC_LAG):
    """Newey–West(Bartlett) 장기분산으로 평균의 t 를 낸다.

    `S = γ0 + 2Σ(1 − k/(L+1))γk` 는 Bartlett 커널이라 항상 ≥ 0 이다.
    자기상관이 없으면 S → γ0 이고 HAC 는 일반 t 로 수렴한다 — 그래서
    이걸 주 기준으로 써도 **잃는 것이 없다**(§6-12-3).
    돌려주는 값: (t, 평균, 장기표준편차 √S, 실제로 쓴 lag, df)
    """
    n = len(x)
    if n < 3:
        return None, None, None, lag, 0
    L = min(int(lag), n - 1)
    m = statistics.fmean(x)
    S = autocov(x, 0)
    for k in range(1, L + 1):
        S += 2.0 * (1.0 - k / (L + 1.0)) * autocov(x, k)
    if S <= 0:
        return None, m, 0.0, L, n - 1
    se = math.sqrt(S / n)
    return (m / se if se > 0 else None), m, math.sqrt(S), L, n - 1


def plain_t(x):
    n = len(x)
    if n < 2:
        return None, None
    m = statistics.fmean(x)
    sd = statistics.stdev(x)
    if sd <= 0:
        return None, n - 1
    return m / (sd / math.sqrt(n)), n - 1


def bootstrap_two_sided(x, block=BLOCK, resamples=RESAMPLES):
    """양측 p. §6-12-3 이 '1표본 **양측** t' 로 얼렸으므로 부트스트랩도 양측으로 맞춘다."""
    pg = block_bootstrap_p(x, block, resamples=resamples, alt="greater")[0]
    pl = block_bootstrap_p(x, block, resamples=resamples, alt="less")[0]
    if pg is None or pl is None:
        return None
    return min(1.0, 2.0 * min(pg, pl))


def required_n(sigma):
    """§6-12-3 의 공식. σ 는 §6-12-6 대로 **HAC 장기표준편차**를 넣는다."""
    if sigma is None or sigma <= 0:
        return None
    return math.ceil(((Z_ALPHA + Z_BETA) / (TARGET_EFFECT / sigma)) ** 2)


def main_window(sigma):
    """(필요N, 본구간, 한도초과여부). 한도를 넘으면 본 구간을 정하지 않는다."""
    need = required_n(sigma)
    if need is None:
        return None, None, False
    if need > MAIN_CAP:
        return need, None, True
    return need, min(max(need, MAIN_FLOOR), MAIN_CAP), False


# ══════════════════════════════════════════════════════════════════════
# 6. 보고서
# ══════════════════════════════════════════════════════════════════════
def pct(v, nd=3):
    return "—" if v is None else f"{v * 100:+.{nd}f}%p"


def _backup_fired(cap):
    try:
        hh, mm = int(cap[11:13]), int(cap[14:16])
    except Exception:
        return False
    return (hh, mm) >= BACKUP_FROM


def collection_block(days, dates):
    L = []
    ok = [d for d in days if d.get("matured")]
    L.append(f"- 스냅샷 보유 거래일 **{len(dates)}일** (첫날 {dates[0] if dates else '—'} · 마지막 {dates[-1] if dates else '—'})")
    L.append(f"- 그중 **성숙(익일 시가까지 붙는) 거래일 {len(ok)}일** / 파일럿 목표 {PILOT_DAYS}일")
    bad = [d for d in days if not d.get("matured")]
    if bad:
        L.append("")
        L.append("| 날짜 | 표본이 안 되는 사유 | 유니버스 |")
        L.append("|---|---|---:|")
        for d in bad:
            L.append(f"| {d['date']} | {d.get('why') or '—'} | {d.get('universe', 0)} |")
    caps = [d.get("captured", "") for d in days if d.get("captured")]
    late = [c for c in caps if _backup_fired(c)]
    L.append("")
    L.append(f"- `capturedAt` 수집 시작 시각 — 15:06 이전 {len(caps) - len(late)}일 / **15:06 이후 {len(late)}일**"
             + (f" ({', '.join(c[11:16] for c in late)})" if late else ""))
    L.append("  > 수집 시작 시각은 시세 관측/체결 시각이나 cron 발사 원인의 증거가 아니다.")
    tot = {k: sum(d["drop"].get(k, 0) for d in days) for k in DROP_REASONS}
    L.append("")
    L.append("| 유니버스 탈락 사유 | 합계 |")
    L.append("|---|---:|")
    for k in DROP_REASONS:
        if tot[k]:
            L.append(f"| {k} | {tot[k]:,} |")
    dups = sum(d.get("dup", 0) for d in days)
    mism = sum(d.get("open_mismatch", 0) for d in days)
    tie = sum(d.get("ties", 0) for d in days)
    L.append("")
    L.append(f"- 중복 종목행 {dups}건(먼저 나온 행 채택) · 분위 경계 동률 {tie}건(종목코드순) "
             f"· 13시/15:05 시가 불일치 {mism}건(13시 값 채택)")
    return L


def report(snap_dir=SNAP_DIR, today=None, state=None):
    state = {} if state is None else state
    today = today or datetime.datetime.now(KST).strftime("%Y-%m-%d")
    days, dates = collect(snap_dir, with_returns=False)
    n_mat = matured_count(days)
    L = [f"# 🌇 종가베팅 관측 — {today}", "",
         "사전등록: `docs/전략로드맵.md` §6-12 · §6-12-2 · §6-12-3 · §6-12-5 · §6-12-6.",
         "진입 15:05 슬롯 `nowPrice` → **익일 시가 전량 청산**. 표본 단위는 거래일이다.", ""]

    if n_mat < PILOT_DAYS:
        if 'pilot' in state:
            raise ValueError('frozen pilot inputs disappeared')
        L += ["## 단계 0 — 파일럿 미완", "",
              f"성숙 거래일 **{n_mat}일 / {PILOT_DAYS}일**. "
              "§6-12-3 이 실성과 계산을 파일럿 종료일까지 막았다.", "",
              "> 🚫 **이 보고서는 수익률을 계산하지 않았다.** 숨긴 것이 아니라 돌리지 않았다.",
              "> 계산해 두고 안 보여주는 방식은 막는 장치가 아니다 — 계산하면 결국 본다.", "",
              "### 수집 현황", ""]
        L += collection_block(days, dates)
        L += ["", "### 남은 일", "",
              f"- 성숙일 {PILOT_DAYS - n_mat}일 더 쌓이면 단계 1(σ·자기상관)로 자동 전환된다.",
              "- 성과를 보고 기준을 맞추지 않는다. 오류 수정은 근거·버전을 남기며, 동결 후 변경은 별도 검토한다."]
        return "\n".join(L), 0

    # ── 여기서부터만 수익률을 만든다 ────────────────────────────────
    days, dates = collect(snap_dir, with_returns=True)
    series = [d for d in days if d.get("ok") and "rel" in d]
    def calibrate(values):
        sd = hac(values)[2]
        need, window, over = main_window(sd)
        return {'sigma': sd, 'need': need, 'window': window, 'over': over}
    calibration, pilot, main_rows = split_windows(
        series, state, PILOT_DAYS, calibrate,
        {'pilot_days': PILOT_DAYS, 'effect': TARGET_EFFECT, 'lag': HAC_LAG,
         'floor': MAIN_FLOOR, 'cap': MAIN_CAP, 'cost': COST,
         'block': BLOCK, 'resamples': RESAMPLES, 'turnover': MIN_TURNOVER,
         'min_per_q': MIN_PER_Q, 'quintiles': QUINTILES, 'price_limit': PRICE_LIMIT,
         'limit_tol': LIMIT_TOL, 'adj_tol': ADJ_TOL, 'reversal_tol': REVERSAL_TOL,
         'z_alpha': Z_ALPHA, 'z_beta': Z_BETA})
    if calibration is None:
        return '\n'.join(L + ['가격 검증 후 유효 파일럿 20일 미달. 판정 보류.']), 0
    sd_h, need, win, over = (calibration[k] for k in ('sigma', 'need', 'window', 'over'))
    rel = [d['rel'] for d in pilot]
    lag_used = HAC_LAG
    n_main = len(main_rows)

    if win is None or n_main < win:
        L += ["## 단계 1 — 파일럿 완료, 본 구간 진행 중", "",
              f"성숙 거래일 **{len(series)}일** (파일럿 {PILOT_DAYS} + 본 구간 {max(0, n_main)}).", "",
              "> 🚫 **평균·t·p 를 출력하지 않는다.** §6-12-3 은 파일럿의 용도를",
              "> 'σ·자기상관 추정만' 으로 못박았다. 알파를 여기서 보면 본 구간이",
              "> 이미 결과를 아는 구간이 된다.", "",
              "### σ 추정 (§6-12-6 — HAC 장기표준편차를 쓴다)", "",
              f"- 일별 표준편차(단순) — {pct(statistics.stdev(rel) if len(rel) > 1 else None)}",
              f"- **HAC 장기표준편차 σ̂ (지연 {lag_used}) — {pct(sd_h)}**  ← 최초 파일럿 20일로 고정",
              ""]
        L.append("| 자기상관 | " + " | ".join(f"ρ{k}" for k in range(1, HAC_LAG + 1)) + " |")
        L.append("|---|" + "---:|" * HAC_LAG)
        g0 = autocov(rel, 0)
        L.append("| 값 | " + " | ".join(
            (f"{autocov(rel, k) / g0:+.3f}" if g0 > 0 and len(rel) > k else "—")
            for k in range(1, HAC_LAG + 1)) + " |")
        L += ["", "### 본 구간 길이", ""]
        if over:
            L += [f"필요 N = **{need}거래일** > 자원 한도 {MAIN_CAP}일.", "",
                  "> 🚨 §6-12-3 대로 **확증 검정을 돌리지 않는다.** 검정력이 없는 줄 알면서",
                  "> 돌리면 '유의하지 않음' 이 '효과 없음' 으로 오독된다. 그건 검정이 아니라 알리바이다.", "",
                  "택할 것 — ① 현재 설계 보류 ② 관찰 연장(다음 심사일 지정) ③ 종료.",
                  "**사람이 사유와 함께 고른다. 이 스크립트가 고르지 않는다.**"]
        elif win is None:
            L += ['파일럿 분산 추정 불가/0: 본 구간 길이를 정하지 않고 보류한다.',
                  '새 자료로 자동 재추정하지 않는다. 원인 확인과 별도 설계 검토가 필요하다.']
        else:
            L += [f"- 필요 N = ceil(((1.96+0.84)/(0.10%p/σ̂))²) = **{need}거래일**",
                  f"- 본 구간 = min(max({need}, {MAIN_FLOOR}), {MAIN_CAP}) = **{win}거래일**",
                  f"- 진행 {max(0, n_main)}/{win}일 — {max(0, win - n_main)}일 남았다", "",
                  "민감도(§6-12-6) — " + " · ".join(
                      f"σ̂×{m}: {required_n(sd_h * m)}일" for m in (0.8, 1.0, 1.2))]
        L += ["", "### 수집 현황", ""] + collection_block(days, dates)
        return "\n".join(L), 1

    # ── 단계 2 — 본 구간 완료. 판정 1회 ────────────────────────────
    # Validate frozen inputs before replaying; never incorporate subsequent dates.
    if 'final_report' in state:
        return state['final_report'], 2
    series = main_rows
    rel = [d['rel'] for d in series]
    t_h, mean_h, sd_h, lag_used, df_h = hac(rel)
    L[0] = f"# 🌇 종가베팅 고정 본 구간 판정 — {series[-1]['date']}"
    t_p, df_p = plain_t(rel)
    p_h = t_two_sided_p(t_h, df_h) if t_h is not None else None
    p_p = t_two_sided_p(t_p, df_p) if t_p is not None else None
    p_b = bootstrap_two_sided(rel)
    absn = [d["abs_net"] for d in series]
    t_a, df_a = plain_t(absn)

    unstable = (p_h is not None and p_b is not None and
                ((p_h < 0.05) != (p_b < 0.05) or
                 (max(p_h, p_b) > 3 * min(p_h, p_b) and min(p_h, p_b) < 0.2)))

    L += ["## 단계 2 — 본 구간 완료 · 판정", "",
          f"검정 표본 **본 구간 {len(series)}일**. 파일럿 {PILOT_DAYS}일 제외. 이후 자료도 제외.", "",
          "### ① 상대 선정 능력 — Q5 평균 − 유니버스 평균 (비용 미적용)", "",
          f"- 일평균 **{pct(mean_h)}/일**",
          f"- **HAC(지연 {lag_used}) t = {t_h:.3f} · p = {p_h:.4f}** ← 채택 여부는 이것 하나가 정한다"
          if t_h is not None else "- HAC 계산 불가",
          f"- (참고) 일반 t = {t_p:.3f} · p = {p_p:.4f}" if t_p is not None else "- (참고) 일반 t 계산 불가",
          f"- (참고) 블록({BLOCK}일) 부트스트랩 양측 p = {p_b:.4f}" if p_b is not None else "- (참고) 부트스트랩 불가",
          ""]
    if unstable:
        L += ["> 🚨 **불안정.** HAC 와 부트스트랩이 크게 엇갈린다. §6-12-3 대로",
              "> 유리한 쪽을 고르지 않고 이 사실 자체를 결론으로 보고한다.", ""]
    L += ["### ② 비용 후 절대 수익성 — Q5 평균 − 0.35% (§3-4-2)", "",
          f"- 일평균 **{pct(statistics.fmean(absn))}/일** · t = " +
          (f"{t_a:.3f}" if t_a is not None else "—"), "",
          "> ①과 ②는 **별개의 질문**이다(§6-12-3 4차 재검증 정정). 시장보다 덜 떨어져",
          "> 이겨도 돈은 잃을 수 있고, 져도 상승장이면 번다. 한쪽으로 다른 쪽을 대신하지 않는다.", "",
          "### 기술 참고 — 판단에 쓰지 않는다", "",
          "| 분위 | 일평균 수익률 |", "|---|---:|"]
    for q in range(QUINTILES):
        v = [d["q_mean"][q] for d in series if d["q_mean"].get(q) is not None]
        L.append(f"| Q{q + 1} | {pct(statistics.fmean(v)) if v else '—'} |")
    L += [f"| 유니버스 평균 | {pct(statistics.fmean(d['uni_mean'] for d in series))} |",
          f"| 유니버스 중앙값 | {pct(statistics.fmean(d['uni_median'] for d in series))} |", "",
          "Q5−Q1 스프레드 " + pct(statistics.fmean(
              d["spread"] for d in series if d["spread"] is not None)) +
          " · 중앙값 대비 " + pct(statistics.fmean(d["rel_median"] for d in series)),
          "",
          "> 단조성·Q5−Q1·중앙값 대비가 더 좋아 보여도 결론을 바꾸지 않는다.",
          "> 바꾸고 싶어지면 그것이 선택 편의의 신호다(§6-12-3).", "",
          "### 채택 여부", "",
          "§6-12 정의 8 — 이 항목은 **탐색적**이다. ①이 유의해도 **독립 재현(별도 60거래일)**",
          "전에는 채택하지 않는다. 이 보고서는 재현 구간의 시작을 알리는 문서이지 채택 통보가 아니다.",
          "", f"고정 구간: {series[0]['date']} ~ {series[-1]['date']} · {STUDY_VERSION}",
          "후속 실행은 이 판정을 재표시한다. 새 누적 검정이나 독립 재현 판정이 아니다."]
    state['final_report'] = "\n".join(L)
    return state['final_report'], 2


# ══════════════════════════════════════════════════════════════════════
# 7. 자기검증 — 합성 데이터로 §6-12-3 이 나열한 함정을 전부 밟는다
# ══════════════════════════════════════════════════════════════════════
HDR = ["itemcode", "itemname", "sosok", "nowPrice", "openPrice", "highPrice", "lowPrice",
       "prevChangeRate", "tradeVolume", "tradeAmount", "marketSum", "listedStockCnt",
       "frgnHoldRate", "manageStatusGb", "tradeStopYn", "marketAlertType", "marketStatus"]


def _row(code, now, amt, op=0, rate=0.0, stop="N", manage="0", name="x"):
    d = dict.fromkeys(HDR, "")
    d.update(itemcode=code, itemname=name, sosok="0", nowPrice=now, openPrice=op,
             prevChangeRate=rate, tradeAmount=amt, manageStatusGb=manage,
             tradeStopYn=stop, marketAlertType="00", marketStatus="OPEN")
    return [str(d[k]) for k in HDR]


def _write(dirpath, date, slot, rows):
    p = os.path.join(dirpath, f"{date}_{slot}.csv.gz")
    with gzip.open(p, "wt", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(["#meta", f"capturedAt={date}T15:02:10+09:00", f"slot={slot}"])
        w.writerow(HDR)
        for r in rows:
            w.writerow(r)


def _synth(dirpath, dates, n=60, signal=0.0):
    """날짜별로 n종목. ratio 는 코드 순서대로 커지고, Q5(상위 1/5)에만 signal 을 준다.

    ⚠️ **진입가(`nowPrice`)를 1000 으로 고정한다.** 처음엔 다음날 시가를 그날의
    `nowPrice` 에도 넣었는데, 그러면 신호가 분자와 분모에 동시에 들어가 서로
    지워졌다(기대 +1.6%p 가 +0.065%p 로 나왔다). 자기검증이 그걸 잡았다.
    `prevChangeRate=0` 이라 유도 전일종가도 1000 이 되어 제한폭·기업행사 검사를
    통과한다 — 이 합성은 **분위 배정과 집계**를 재는 것이고, 이중 차감은 위의
    전용 검사가 따로 잡는다.
    """
    prev = None
    for di, d in enumerate(dates):
        r13, r15 = [], []
        for i in range(n):
            code = f"{i:06d}"
            amt13 = 10_000_000_000
            amt15 = amt13 * (1.0 + i / n)              # 코드가 클수록 오후수급이 크다
            r13.append(_row(code, 1000, amt13))
            r15.append(_row(code, 1000, amt15))
        if prev is not None:
            # 오늘 시가 = 어제 종베의 청산가. Q5(상위 20%)만 signal 만큼 더 준다
            for i in range(n):
                base = 1000.0 * (1.0 + 0.001 * ((di * 7 + i) % 5 - 2))
                if i >= n - n // QUINTILES:
                    base *= (1.0 + signal)
                op = f"{base:.4f}"
                r13[i][HDR.index("openPrice")] = op
                r15[i][HDR.index("openPrice")] = op
        _write(dirpath, d, "1300", r13)
        _write(dirpath, d, "1505", r15)
        prev = d


def _bizdays(start, k):
    out, d = [], datetime.date.fromisoformat(start)
    while len(out) < k:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += datetime.timedelta(days=1)
    return out


def self_test():
    import tempfile, shutil
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    print("🧪 분위 배정 (§6-12-6)")
    its = [{"code": f"{i:06d}", "ratio": 1.0 + i * 0.01, "P": 1} for i in range(50)]
    a, ties = assign_quintiles(its)
    sizes = [sum(1 for x in a if x["q"] == q) for q in range(QUINTILES)]
    chk("50종목이면 10씩 고르게", sizes == [10] * 5, f"{sizes}")
    chk("동률 없음", ties == 0)
    flat = [{"code": f"{i:06d}", "ratio": 1.0, "P": 1} for i in range(50)]
    a2, ties2 = assign_quintiles(flat)
    s2 = [sum(1 for x in a2 if x["q"] == q) for q in range(QUINTILES)]
    chk("전부 동률이어도 분위가 고르다 (값 경계였다면 Q5 가 비었다)", s2 == [10] * 5, f"{s2}")
    chk("전부 동률이면 경계 동률이 잡힌다", ties2 == QUINTILES - 1, f"ties={ties2}")
    chk("동률은 종목코드순으로 깬다 — 최상위가 가장 큰 코드",
        a2[-1]["code"] == "000049")
    odd = [{"code": f"{i:06d}", "ratio": i, "P": 1} for i in range(53)]
    a3, _ = assign_quintiles(odd)
    chk("53종목이어도 모든 분위가 비지 않는다",
        all(any(x["q"] == q for x in a3) for q in range(QUINTILES)))

    print("🧪 유니버스 (§6-12 정의 1)")
    r15 = {"A": dict(zip(HDR, _row("A", 1000, MIN_TURNOVER))),
           "B": dict(zip(HDR, _row("B", 1000, MIN_TURNOVER - 1))),
           "C": dict(zip(HDR, _row("C", 1000, MIN_TURNOVER, stop="Y"))),
           "D": dict(zip(HDR, _row("D", 1000, MIN_TURNOVER, manage="1"))),
           "E": dict(zip(HDR, _row("E", 1000, MIN_TURNOVER))),
           "F": dict(zip(HDR, _row("F", 1000, MIN_TURNOVER))),
           "G": dict(zip(HDR, _row("G", 0, MIN_TURNOVER)))}
    r13 = {k: dict(zip(HDR, _row(k, 1000, MIN_TURNOVER // 2))) for k in r15}
    r13["E"]["tradeAmount"] = "0"                       # 분모 0
    del r13["F"]                                        # 13시 행 누락
    items, drop = build_universe(r15, r13)
    chk("50억 미달 제외", drop["거래대금미달"] == 1)
    chk("거래정지 제외", drop["거래정지"] == 1)
    chk("관리종목류 제외", drop["관리종목류"] == 1)
    chk("진입가 0 제외", drop["진입가없음"] == 1)
    chk("분모 0 제외", drop["분모0"] == 1)
    chk("13시 행 누락 제외", drop["13시행없음"] == 1)
    chk("남는 건 A 하나", [x["code"] for x in items] == ["A"], f"{[x['code'] for x in items]}")
    r13["A"]["tradeAmount"] = str(MIN_TURNOVER * 2)     # 누적 거래대금 역전
    items2, drop2 = build_universe(r15, r13)
    chk("거래대금 역전 제외", drop2["거래대금역전"] == 1 and not items2)

    print("🧪 중복 종목")
    tmp = tempfile.mkdtemp(prefix="cb_dup_")
    _write(tmp, "2026-01-05", "1505", [_row("X", 1000, 1, name="first"),
                                       _row("X", 2000, 2, name="second")])
    _, rows, dup = read_snapshot(os.path.join(tmp, "2026-01-05_1505.csv.gz"))
    chk("중복은 먼저 나온 행을 남기고 센다",
        dup == 1 and rows["X"]["itemname"] == "first")
    shutil.rmtree(tmp)

    print("🧪 익일 가격 예외 (§6-12-6)")
    d = {k: 0 for k in DROP_REASONS}
    base = [{"code": "A", "P": 1000.0, "q": 4}]
    nxt = {"1505": {"A": dict(zip(HDR, _row("A", 1100, 1, op=1100, rate=10.0)))}}
    kept, _ = day_returns(base, nxt, d)
    chk("정상 — 수익률은 익일시가/진입가 − 1", abs(kept[0]["ret"] - 0.10) < 1e-9,
        f"ret={kept[0]['ret']:.4f}")
    kept, _ = day_returns(base, {"1505": {}}, d)
    chk("익일 표에 없으면 '익일소멸'", d["익일소멸"] == 1 and not kept)
    d = {k: 0 for k in DROP_REASONS}
    kept, _ = day_returns(base, {"1505": {"A": dict(zip(HDR, _row("A", 1000, 1, op=0, stop="Y")))}}, d)
    chk("익일 거래정지(시가 0)면 '익일시가없음'", d["익일시가없음"] == 1 and not kept)
    d = {k: 0 for k in DROP_REASONS}
    # 전일종가 1000 인데 시가 1400 → 제한폭 이탈
    kept, _ = day_returns(base, {"1505": {"A": dict(zip(HDR, _row("A", 1400, 1, op=1400, rate=40.0)))}}, d)
    chk("시가가 전일종가 ±30% 밖이면 제외", d["익일제한폭이탈"] == 1 and not kept)
    d = {k: 0 for k in DROP_REASONS}
    # 액면분할 5:1 — 익일 기준가 200, 우리 진입가 1000. 제한폭 검사는 통과한다
    kept, _ = day_returns(base, {"1505": {"A": dict(zip(HDR, _row("A", 200, 1, op=200, rate=0.0)))}}, d)
    chk("기업행사(액면분할) 조정은 제한폭 검사를 통과하므로 별도 검사가 잡는다",
        d["기업행사조정의심"] == 1 and d["익일제한폭이탈"] == 0 and not kept)
    d = {k: 0 for k in DROP_REASONS}
    kept, mm = day_returns(base, {"1300": {"A": dict(zip(HDR, _row("A", 1100, 1, op=1100, rate=10.0)))},
                                  "1505": {"A": dict(zip(HDR, _row("A", 1100, 1, op=1105, rate=10.0)))}}, d)
    chk("13시/15:05 시가가 다르면 13시 값을 쓰고 센다",
        mm == 1 and abs(kept[0]["ret"] - 0.10) < 1e-9)

    print("🧪 🚫 이중 차감 (§6-12-5)")
    ks = [{"q": 4, "ret": 0.03}, {"q": 4, "ret": 0.01}, {"q": 0, "ret": -0.01}, {"q": 1, "ret": 0.0},
          {"q": 2, "ret": 0.005}, {"q": 3, "ret": 0.002}]
    s1 = day_stats(ks)
    s2 = day_stats([dict(x, ret=x["ret"] - 0.0123) for x in ks])   # 모두에게 같은 상수를 물린다
    chk("①은 공통 비용 상수에 불변이다 (상쇄되므로)", abs(s1["rel"] - s2["rel"]) < 1e-12,
        f"{s1['rel']:.8f} vs {s2['rel']:.8f}")
    chk("②만 비용을 뺀다", abs(s1["abs_net"] - (statistics.fmean([0.03, 0.01]) - COST)) < 1e-12)
    chk("①은 정의 그대로 Q5평균 − 유니버스평균이다",
        abs(s1["rel"] - (statistics.fmean([0.03, 0.01])
                         - statistics.fmean(x["ret"] for x in ks))) < 1e-12)
    # 진입가와 그날 종가가 다른 상황을 만든다. 종가 이동분을 또 빼는 구현이라면
    # (익일시가 − 그날종가)/P = 0 이 나온다. 올바른 구현은 익일시가/P − 1 = +10% 다.
    d = {k: 0 for k in DROP_REASONS}
    nxt = {"1505": {"A": dict(zip(HDR, _row("A", 1100, 1, op=1100, rate=0.0)))}}   # 유도 전일종가 1100
    kept, _ = day_returns([{"code": "A", "P": 1000.0, "q": 4}], nxt, d)
    chk("진입가(1000)와 그날 종가(1100)가 다를 때도 익일시가/진입가 − 1 이다",
        kept and abs(kept[0]["ret"] - 0.10) < 1e-9, f"ret={kept[0]['ret'] if kept else None}")

    print("🧪 HAC")
    xs = [0.01, -0.01] * 12
    t_h, m, sd, lag, df = hac(xs)
    chk("평균 0 이면 t≈0", abs(m) < 1e-12)
    # ⚠️ 처음엔 ((i*37)%11-5) 같은 주기 수열을 iid 대용으로 썼다가 두 검사가 깨졌다.
    #    주기 수열은 자기상관이 강해서 iid 가 아니다 — 구현이 아니라 기대가 틀렸던 것이다.
    import random as _r
    rng = _r.Random(20260913)
    iid = [rng.gauss(0, 0.003) for _ in range(400)]
    t_i, _, sd_i, _, _ = hac(iid)
    chk("자기상관이 없으면 HAC σ 는 단순 σ 에 가깝다",
        abs(sd_i - statistics.stdev(iid)) / statistics.stdev(iid) < 0.15,
        f"{sd_i:.5f} vs {statistics.stdev(iid):.5f}")
    pos, v = [], 0.0
    for _ in range(400):
        v = 0.8 * v + rng.gauss(0, 0.003)             # AR(1) 양의 자기상관
        pos.append(v + 0.001)
    t_pos, _, sd_pos, _, _ = hac(pos)
    t_pl, _ = plain_t(pos)
    chk("양의 자기상관이면 HAC t < 일반 t (부풀림을 깎는다)", abs(t_pos) < abs(t_pl),
        f"HAC {t_pos:.2f} vs 일반 {t_pl:.2f}")
    chk("HAC σ 는 그때 단순 σ 보다 크다", sd_pos > statistics.stdev(pos))
    chk("표본 3개 미만이면 None", hac([0.1, 0.2])[0] is None)
    chk("lag 는 n−1 로 잘린다", hac([0.01, -0.02, 0.03, 0.0])[3] == 3)

    print("🧪 본 구간 길이 (§6-12-3 규칙 · §6-12-6 확정)")
    # 로드맵 표의 '약 50일 / 약 197일' 은 반올림한 예시다(§6-12-3 이 그렇게 밝혔다).
    # 공식을 정확히 풀면 (2.8/0.4)²=49, (2.8/0.2)²=196 이다. 표가 아니라 공식을 따른다.
    chk("σ=0.25%p → 필요 N=49 (표의 '약 50')", required_n(0.0025) == 49, f"{required_n(0.0025)}")
    chk("σ=0.50%p → 필요 N=196 (표의 '약 197')", required_n(0.0050) == 196, f"{required_n(0.0050)}")
    need, win, over = main_window(0.0050)
    chk("필요 N 이 한도를 넘으면 본 구간을 정하지 않는다", over and win is None)
    need, win, over = main_window(0.0025)
    chk("필요 N 49 면 본 구간 49", (need, win, over) == (49, 49, False), f"{need},{win}")
    need, win, over = main_window(0.0010)
    chk("필요 N 이 하한(40)보다 작으면 하한을 쓴다", win == MAIN_FLOOR, f"need={need} win={win}")
    chk("σ 가 없으면 None", main_window(None) == (None, None, False))

    print("🧪 단계 전환 — 파일럿이 수익률 계산을 막는가")
    tmp = tempfile.mkdtemp(prefix="cb_stage_")
    ds = _bizdays("2026-01-05", 8)
    _synth(tmp, ds, n=60, signal=0.0)
    txt, stage = report(tmp, today="2026-01-20")
    chk("성숙일이 파일럿 미만이면 단계 0", "단계 0" in txt and stage == 0)
    chk("단계 0 은 수익률을 계산하지 않았다고 밝힌다", "수익률을 계산하지 않았다" in txt)
    chk("단계 0 에 평균·t 가 없다", "일평균" not in txt and " t = " not in txt)
    days, _ = collect(tmp, with_returns=False)
    chk("with_returns=False 면 수익률 키가 아예 없다",
        all("rel" not in d for d in days))
    chk("성숙일은 관측일보다 적다 (마지막 날은 익일이 없다)",
        matured_count(days) == len(ds) - 1, f"{matured_count(days)}/{len(ds)}")
    shutil.rmtree(tmp)

    print("🧪 무신호 / 알려진 신호")
    tmp = tempfile.mkdtemp(prefix="cb_sig_")
    ds = _bizdays("2026-01-05", 26)
    _synth(tmp, ds, n=60, signal=0.0)
    days, _ = collect(tmp, with_returns=True)
    ser = [d for d in days if d.get("ok") and "rel" in d]
    chk("무신호면 ① 일평균이 0 에 붙는다",
        abs(statistics.fmean(d["rel"] for d in ser)) < 5e-4,
        f"{statistics.fmean(d['rel'] for d in ser) * 100:+.4f}%p")
    shutil.rmtree(tmp)
    tmp = tempfile.mkdtemp(prefix="cb_sig2_")
    _synth(tmp, ds, n=60, signal=0.02)
    days, _ = collect(tmp, with_returns=True)
    ser = [d for d in days if d.get("ok") and "rel" in d]
    got = statistics.fmean(d["rel"] for d in ser)
    # Q5 에 +2%, Q5 는 전체의 1/5 → 기대 ① = 0.02 × (1 − 1/5) = 1.6%p
    chk("알려진 신호 +2% 를 Q5 에 넣으면 ① ≈ +1.6%p", abs(got - 0.016) < 6e-4,
        f"{got * 100:+.4f}%p")
    chk("그 값이 Q5 평균 − 유니버스 평균과 정확히 같다",
        abs(got - statistics.fmean(d["q_mean"][4] - d["uni_mean"] for d in ser)) < 1e-12)
    shutil.rmtree(tmp)

    print("🧪 달력 — 빠진 거래일을 이웃으로 붙이지 않는다 (§6-12-3)")
    tmp = tempfile.mkdtemp(prefix="cb_cal_")
    ds = _bizdays("2026-01-05", 6)
    keep = [x for x in ds if x != ds[2]]          # 가운데 하루를 통째로 지운다
    _synth(tmp, keep, n=60, signal=0.0)
    days, _ = collect(tmp, with_returns=True)
    by = {d["date"]: d for d in days}
    chk("하루 건너뛴 날은 표본이 되지 않는다",
        by[ds[1]].get("matured") is not True and by[ds[1]]["why"] == "익일달력결측",
        f"{by[ds[1]].get('why')}")
    chk("그 다음 날은 정상으로 살아 있다", by[ds[3]].get("matured") is True)
    open(os.path.join(tmp, "nontrading.txt"), "w", encoding="utf-8").write(ds[2] + "  # 임시공휴일\n")
    days, _ = collect(tmp, with_returns=True)
    by = {d["date"]: d for d in days}
    chk("휴장일로 등록하면 그날은 이어 붙는다", by[ds[1]].get("matured") is True)
    shutil.rmtree(tmp)

    print("🧪 분위당 최소 종목 수")
    tmp = tempfile.mkdtemp(prefix="cb_min_")
    ds = _bizdays("2026-01-05", 4)
    _synth(tmp, ds, n=QUINTILES * MIN_PER_Q - 1)
    days, _ = collect(tmp, with_returns=True)
    chk("유니버스가 분위당 최소수에 못 미치면 그날은 버린다",
        all(not d.get("matured") for d in days) and "분위당" in days[0]["why"], days[0]["why"])
    shutil.rmtree(tmp)

    print("🧪 단계 1 — σ 는 내지만 알파는 안 낸다")
    tmp = tempfile.mkdtemp(prefix="cb_s1_")
    ds = _bizdays("2026-01-05", 24)
    _synth(tmp, ds, n=60, signal=0.0)
    txt, stage = report(tmp, today="2026-02-10")
    chk("단계 1 로 넘어간다", "단계 1" in txt and stage == 1)
    chk("σ 를 낸다", "HAC 장기표준편차" in txt)
    chk("평균·t·p 를 내지 않는다고 밝힌다", "평균·t·p 를 출력하지 않는다" in txt)
    chk("실제로 ① 값이 본문에 없다", "① 상대 선정 능력" not in txt)
    shutil.rmtree(tmp)

    print("🧪 단계 2 — 판정은 HAC 하나가 한다")
    tmp = tempfile.mkdtemp(prefix="cb_s2_")
    ds = _bizdays("2026-01-05", 120)
    _synth(tmp, ds, n=60, signal=0.0)
    txt, stage = report(tmp, today="2026-07-01")
    chk("본 구간까지 차면 단계 2", "단계 2" in txt and stage == 2, txt.split("\n")[4][:40])
    chk("HAC 가 판정한다고 명시", "채택 여부는 이것 하나가 정한다" in txt)
    chk("일반 t·부트스트랩은 참고로 표시", "(참고) 일반 t" in txt and "부트스트랩" in txt)
    chk("①과 ②를 나란히 낸다", "① 상대 선정 능력" in txt and "② 비용 후 절대 수익성" in txt)
    chk("탐색적이라 재현 전 채택 없음을 적는다", "독립 재현" in txt and "채택 통보가 아니다" in txt)
    chk("기술 참고를 판단에 쓰지 않는다고 적는다", "결론을 바꾸지 않는다" in txt)
    shutil.rmtree(tmp)

    print("🧪 부트스트랩 양측")
    chk("분산 0 이면 None", bootstrap_two_sided([0.0] * 10, 5, 200) is None)
    p = bootstrap_two_sided([0.05] * 10 + [0.04] * 10, 5, 2000)
    chk("전부 같은 부호로 크게 치우치면 p 가 작다", p is not None and p < 0.10, f"p={p}")

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음 — 종베 분석을 돌리지 말 것"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true",
                    help="합성 데이터로 정의·통계·단계 잠금을 검증한다(파일 접근 없음)")
    ap.add_argument("--dir", default=SNAP_DIR, help="스냅샷 폴더")
    ap.add_argument("--out", default="", help="출력 경로. 비우면 docs/종베관측_<오늘>.md")
    ap.add_argument("--stdout-only", action="store_true", help="파일로 쓰지 않고 화면에만")
    ap.add_argument("--force", action="store_true", help="호환용 인수. 고정 판정을 덮어쓰지는 않는다")
    ap.add_argument('--state', default='data/closing_bet/study_v2.json',
                    help='고정 파일럿/본 구간 상태. 삭제하여 재검정하지 말 것')
    a = ap.parse_args()

    if a.self_test:
        return self_test()

    today = datetime.datetime.now(KST).strftime("%Y-%m-%d")
    state = {}
    if os.path.exists(a.state):
        with open(a.state, encoding='utf-8') as fp:
            state = json.load(fp)
    txt, stage = report(a.dir, today, state)
    print(txt)
    if a.stdout_only:
        print('\n미저장 미리보기: 상태 동결/공식 판정 기록을 완료한 실행이 아닙니다.')
        return 0
    if state:
        os.makedirs(os.path.dirname(a.state) or '.', exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(a.state) or '.', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fp:
                json.dump(state, fp, ensure_ascii=False, indent=2, allow_nan=False)
            os.replace(tmp, a.state)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
    # 단계 0·1 은 매번 갱신되는 현황이라 한 파일을 덮어쓴다.
    # 단계 2 는 **판정**이라 날짜가 박힌 파일로 남기고 덮어쓰지 않는다 — 박제다.
    if stage < 2:
        path = a.out or os.path.join("docs", "종베관측_현황.md")
    else:
        path = a.out or os.path.join("docs", f"종베판정_{STUDY_VERSION}.md")
        if os.path.exists(path):
            with open(path, encoding='utf-8') as fp:
                if fp.read() != txt + '\n':
                    raise ValueError('기존 고정 판정과 불일치: 덮어쓰기 금지')
            print(f"\n기존 고정 판정과 동일 — {path}")
            return 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write(txt + "\n")
    print(f"\n📝 {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
