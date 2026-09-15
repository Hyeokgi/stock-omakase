# -*- coding: utf-8 -*-
# ==========================================================================
# 🏴 테마 대장 A/B/C 분해 — 대장 선정 효과와 추가 조건 효과를 가른다
# --------------------------------------------------------------------------
# 왜 만드나 — 운영대전제 §4 가 요구한 분해
#   "대장 선정 효과와 추가 조건 효과를 분리한다: 당시 투자 가능 후보군 →
#    당시 테마 대장 후보 → 추가 조건 통과 후보. 오른 종목뿐 아니라 실패한
#    후보와 무신호일도 보존한다."
#   §6-6 이 '대장·당일단타 13건 −1.44%' 로 종베 자동매매를 기각했을 때,
#   로드맵 결론 2 가 이렇게 좁혔다 — **그건 '조건 없는 대장' 의 성적이다.**
#   B−A(대장을 고른 효과)와 C−B(조건을 더한 효과)가 섞여 있으면
#   무엇이 안 통했는지 알 수 없다. 그래서 나눈다.
#
# 🔒 이 파일이 지키는 것 — 순서가 결과를 정한다
#   1. **정의를 먼저 못 박는다.** A·B 의 정의는 아래 상수와 함수에 고정돼 있고
#      데이터를 보고 고르지 않는다. B 는 사전 선언된 **두 변형**을 둘 다 내고
#      어느 쪽도 고르지 않는다(§6-8 항목 8 이 '일치율' 을 등록해 뒀다).
#   2. **C 의 문턱을 오늘 정하지 않는다.** C 후보 변수는 계산해서 **기록만** 한다.
#      문턱으로 거르지 않는다. 지금 문턱을 정하면 그것이 사후 맞춤이다.
#      문턱은 별도 사전등록 문서로 정한다.
#   3. **성숙 전에는 수익률을 0줄도 계산하지 않는다**(단계 잠금).
#      `hyeoks_closing_bet.py` 와 같은 논리다. 숫자를 보고 정의를 고치는 경로를
#      물리적으로 막는다. 지금 스냅샷은 13일이라 무조건 Stage 0 이다.
#   4. **무신호일과 실패 후보를 남긴다.** 대장이 안 뽑힌 날도 행으로 남는다.
#      오른 종목만 남기면 그 표는 이미 거짓말이다.
#   5. **다음 날 결과로 대장·테마 소속을 소급 정의하지 않는다**(운영대전제 §4).
#      A·B·C 는 전부 **진입일 15:05 스냅샷만** 보고 정한다. 익일 데이터는
#      수익률 계산에만 쓰고, 선정에는 절대 쓰지 않는다.
#
# 무엇을 하지 않나
#   · 판정하지 않는다. 문턱·채택·폐기를 말하지 않는다.
#   · 스캐너를 바꾸지 않는다. 읽기만 한다.
#   · 시트를 쓰지 않는다.
#   · 익일 시가는 **연구 대용값**이다(운영대전제 §3). 체결 가능성 인증이 아니다.
# ==========================================================================
import argparse
import datetime
import os
import sys

from hyeoks_closing_bet import (SNAP_DIR, KST, MIN_TURNOVER, PILOT_DAYS, COST,
                                PRICE_LIMIT, LIMIT_TOL, ADJ_TOL,
                                read_snapshot, scan_dates, exit_open, _f)

# ── 사전등록 정의 — 데이터를 보고 바꾸지 않는다 ─────────────────────────
LEADER_POOL     = 5      # B: 테마 내 거래대금 상위 N — omakase.py:604 `[:5]`
MARKET_TOP_N    = 5      # B_시장: 시장 전체 거래대금 상위 N (§6-8 조건 1)
# 🔴 2026-09-15 정정 — 이전 값 3 은 **내가 만든 문턱**이었다(ChatGPT 지적).
#    생산 코드는 `omakase.py:556·584` 에서 `len(stocks_val) >= 2` 다. 거기 맞춘다.
#    "내가 고른 문턱이 하나도 없다" 는 주장과 충돌했던 유일한 값이다.
MIN_THEME_SIZE  = 2      # omakase.py:556·584 `if len(stocks_val) >= 2`
DOMINANCE_MULT  = 5.0    # omakase.py:607 — 1등이 2등의 5배 넘으면 그 테마를 통째로 배제

# ── C 의 문턱 — **내가 고른 값이 하나도 없다** ──────────────────────────
# 전부 `omakase.py` 에 이미 있는 상수를 그대로 가져온다. 오늘 정한 값이 아니다.
#   · MIN_BREAKOUT_TV = omakase.py:1709 `min_breakout_tv = 10_000_000_000`
#   · DANTA_LO/HI     = omakase.py:1710 `min_danta_rate = 0.03` · :2013 `< 0.295`
# 이것이 중요한 이유 — C 의 문턱을 오늘 고르면 그것이 사후 맞춤이다.
# 시스템이 이미 쓰고 있는 값을 쓰면 "무엇을 재는가"가 명확하다:
#   **현행 시스템이 실제로 거르는 그 조건의 효과**를 잰다.
MIN_BREAKOUT_TV = 10_000_000_000
DANTA_LO, DANTA_HI = 0.03, 0.295

# C 후보 변수 — 계산해서 **기록만** 한다. 이쪽에는 문턱이 없다(별도 사전등록 대상).
C_VARS = ("오후수급비", "고가유지율", "등락률", "상한가여부")

STAGE0, STAGE1 = 0, 1


def theme_of(row):
    """종목의 소속 테마 — `topThemeNo` 하나만 쓴다.

    `themeNos` 전부를 쓰면 한 종목이 여러 테마의 대장이 되어 **중복 계산**된다.
    어느 테마가 '그 종목의 테마' 인가는 네이버가 이미 `topThemeNo` 로 답해 뒀다.
    우리가 다시 고르면 그것이 또 하나의 손잡이다.
    """
    return (row.get("topThemeNo") or "").strip()


def build_A(rows1505):
    """A — 당시 투자 가능 후보군. **테마 소속은 A 의 조건이 아니다.**

    테마 소속을 A 에 넣으면 B−A 에 '테마 소속 효과' 가 섞인다. 그래서 A 는
    순수 투자가능성만 보고, 테마 소속 여부는 `has_theme` 로 표시만 해 둔다.
    """
    items, drop = [], {"거래대금미달": 0, "거래정지": 0, "관리종목류": 0, "진입가없음": 0}
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
        items.append({"code": code, "name": r.get("itemname", ""), "P": p,
                      "amt": _f(r.get("tradeAmount")),
                      "rate": _f(r.get("prevChangeRate")),
                      "high": _f(r.get("highPrice")),
                      "theme": theme_of(r),
                      "has_theme": bool(theme_of(r)),
                      "alert": (r.get("marketAlertType") or "").strip()})
    return items, drop


def build_B_current(items):
    """B_현행 — 테마 내 거래대금 상위 `LEADER_POOL` 중 등락률 1위. 테마당 최대 1종목.

    로드맵 §6-7 이 적어 둔 **현행 시스템의 정의**다. 여기서 새로 만들지 않는다.
    로드맵은 이 정의를 의심하고 있다(상대순위이고 거래량 급증 조건이 없다).
    의심은 의심대로 두고, 현행이 무엇을 뽑는지부터 정확히 잰다.
    """
    by_theme = {}
    for it in items:
        if it["theme"]:
            by_theme.setdefault(it["theme"], []).append(it)
    out, excluded = [], 0
    for theme, members in sorted(by_theme.items()):
        if len(members) < MIN_THEME_SIZE:
            continue
        pool = sorted(members, key=lambda x: (-x["amt"], x["code"]))[:LEADER_POOL]
        # omakase.py:607 — 1등이 2등의 5배를 넘으면 '개별주' 로 보고 테마를 통째로 뺀다.
        # 이 줄을 빼면 시스템이 안 뽑는 대장을 우리가 뽑게 된다.
        if len(pool) >= 2 and pool[0]["amt"] >= pool[1]["amt"] * DOMINANCE_MULT:
            excluded += 1
            continue
        best = sorted(pool, key=lambda x: (-x["rate"], x["code"]))[0]
        out.append(dict(best, theme=theme))
    # 같은 종목이 여러 테마의 대장이 될 수는 없다(topThemeNo 가 하나뿐이므로)
    return sorted(out, key=lambda x: (-x["amt"], x["code"])), excluded


def build_C(leaders):
    """C — B 중 **현행 시스템의 추가 조건**을 통과한 것.

    `omakase.py:2014-2015` 가 `is_leader_raw` 에 더 얹는 두 가지를 그대로 옮긴다:
      · `is_true_theme_leader` = 대장 **그리고** 거래대금 ≥ 100억
      · `is_theme_daejang`     = 그 위에 **단타 레인지**(3% ≤ 등락률 < 29.5%)
    (`is_junk`/`is_financial_risk` 는 스냅샷에 입력이 없어 제외했고, 그만큼
     여기 C 는 시스템보다 **약간 느슨하다**. 이 차이를 보고서에 적는다.)

    단계별로 몇 개가 남는지 세어 돌려준다 — 어디서 깎이는지 하나로 뭉치면
    9/14 에 겪은 '복합 조건 0' 오독을 되풀이하게 된다.
    """
    tv_ok = [x for x in leaders if x["amt"] >= MIN_BREAKOUT_TV]
    both = [x for x in tv_ok if DANTA_LO <= x["rate"] / 100.0 < DANTA_HI]
    return both, {"B": len(leaders), "+거래대금100억": len(tv_ok), "+단타레인지": len(both)}


def market_top_proxy(rows1505):
    """시장 거래대금 상위 `MARKET_TOP_N` **proxy**. ⚠️ `A` 이전 raw 에서 뽑는다.

    🔴 **2026-09-15 정정 — 여기가 순환논리였다.**
    이전 구현은 `A` 에서 뽑아 놓고 다시 "이 종목들이 `A` 안에 있는가" 를 셌다.
    `A` 에서 뽑았으니 **항상 100%** 다. 그 100% 를 근거로 "유니버스 문제가 아니다"
    라고 결론지었는데, **항등식을 증거로 쓴 것**이었다(ChatGPT 9/15 지적).

    유니버스 포착력을 재려면 **A 를 통과하기 전 모집단**에서 뽑아야 한다.
    그래서 `rows1505`(15:05 스냅샷 원본)를 받는다.

    ⚠️ 이것은 **proxy 이지 '진짜 주도주' 가 아니다.**
    로드맵 §6-8 은 거래대금 **그리고** 등락률을 함께 요구하는데
    이 구현은 **거래대금만** 본다. 이름에 proxy 를 박아 두는 이유다.
    """
    items = []
    for code, r in rows1505.items():
        amt = _f(r.get("tradeAmount"))
        if amt <= 0:                      # 거래가 없으면 '거래대금 상위' 자체가 성립 안 한다
            continue
        #  `c_values` 가 쓰는 키를 전부 채운다. A 를 안 거치므로 여기서 직접 담아야 한다.
        items.append({"code": code, "name": r.get("itemname", ""), "amt": amt,
                      "rate": _f(r.get("prevChangeRate")),
                      "P": _f(r.get("nowPrice")), "high": _f(r.get("highPrice")),
                      "theme": theme_of(r), "has_theme": bool(theme_of(r)),
                      "alert": (r.get("marketAlertType") or "").strip()})
    return sorted(items, key=lambda x: (-x["amt"], x["code"]))[:MARKET_TOP_N]


def agreement(b1, b2):
    """§6-8 항목 8 — 시장 전체 대장과 테마 내 대장의 일치율(자카드)."""
    s1 = {x["code"] for x in b1}
    s2 = {x["code"] for x in b2}
    if not s1 and not s2:
        return None, 0, 0, 0
    inter, union = len(s1 & s2), len(s1 | s2)
    return (inter / union if union else None), inter, len(s1), len(s2)


def market_capture(Bm, A, Bc, C):
    """§6-7 질문 1 — 그날의 **진짜 주도주를 우리가 후보에 넣었는가.**

    자카드(일치율)만 보면 오해한다. B 가 30여 개, proxy 가 5개면 자카드 최댓값이
    5/30 ≈ 17% 라서, 낮게 나오는 것이 **정의상 당연**하다.
    묻고 싶은 것은 그게 아니라 **'시장 상위 5가 우리 단계마다 몇 개 살아남는가'** 다.
    그래서 분모를 proxy 로 두고 각 단계의 **포함률**을 센다.

    §6-7 이 적어 둔 해석까지 그대로 옮긴다 — A 에서 이미 빠지면
    선정 기준이 아니라 **유니버스**의 문제다.

    🔴 **이 함수가 의미를 가지려면 `Bm` 이 `A` 밖에서 와야 한다.**
    `A` 에서 뽑은 것을 넣으면 `inA` 가 항상 100% 인 항등식이 된다(2026-09-15 정정).
    """
    m = {x["code"] for x in Bm}
    if not m:
        return {"n": 0, "inA": None, "inB": None, "inC": None}
    return {"n": len(m),
            "inA": len(m & {x["code"] for x in A}),
            "inB": len(m & {x["code"] for x in Bc}),
            "inC": len(m & {x["code"] for x in C})}


def c_values(item, rows1300):
    """C 후보 변수를 **계산만** 한다. 거르지 않는다. 문턱이 여기 없는 것이 요점이다."""
    prev = rows1300.get(item["code"])
    a13 = _f(prev.get("tradeAmount")) if prev else 0.0
    return {
        "오후수급비": (item["amt"] / a13) if a13 > 0 else None,   # §6-8 #3
        "고가유지율": (item["P"] / item["high"]) if item["high"] > 0 else None,  # §6-8 #7
        "등락률": item["rate"],                                   # §6-8 조건 2
        "상한가여부": item["rate"] >= 29.0,                        # §6-8 #10
    }


# ══════════════════════════════════════════════════════════════════════
# Stage 1 수익률 — **사전등록** (2026-09-15, 수익률을 보기 전에 못 박는다)
# ══════════════════════════════════════════════════════════════════════
# 🔴 왜 지금 쓰나 — ChatGPT 9/15 지적: "Stage 1 에 도달해도 수익률을 계산하는
#    코드가 없다." 사실이었다. `build_day` 에 `with_returns=True` 분기 자체가 없었고
#    `exit_open` 을 import 만 하고 호출하지 않았다.
#    **아직 수익률을 하나도 보지 않은 지금이 사양을 정할 마지막 깨끗한 시점**이다.
#
# 무엇을 계산하나 — 운영대전제 §4
#   · 진입 = 진입일 15:05 현재가(`P`)  ·  청산 = **익일 시가**(`exit_open`)
#   · 기본 수익률 = `익일시가 / 15:05가 − 1`. 비용 차감 전/후를 **둘 다** 낸다
#   · 계층별 **동일가중 평균**을 내고, 그 차이를 분리한다:
#       **B − A** = 대장을 고른 효과      (테마 소속 + 대장 판정)
#       **C − B** = 추가 조건의 효과      (거래대금 100억 + 단타 레인지)
#   · 날짜별로 먼저 평균을 내고 **날짜 단위로 집계**한다. 종목 단위로 뭉치면
#     종목 수가 많은 날이 결과를 지배한다.
#
# 제외 규칙 — `hyeoks_closing_bet.day_returns` 와 **같은 가드를 그대로 쓴다**
#   · 익일 시가 없음 / 익일 소멸
#   · 익일 시가가 전일종가 ±30%(+여유) 밖 → 거래소 규칙 위반, 관측 오류
#   · 전일종가와 우리 진입가 축이 35% 넘게 다름 → 기업행사 조정 의심
#   여기서 새 가드를 만들지 않는다. 다른 가드를 쓰면 종베 결과와 비교가 안 된다.
#
# 이 사양이 하지 않는 것
#   · 판정하지 않는다. 문턱·채택·폐기를 말하지 않는다. 20일은 파일럿이다
#   · 익일 시가는 **연구 대용값**이다(운영대전제 §3). 체결 가능성 인증이 아니다
#   · 계층 간 차이에 유의성 검정을 붙이지 않는다 — 검정은 별도 사전등록이 필요하다
RET_LAYERS = ("A", "B", "C")


def layer_returns(items, nxt_slots):
    """한 계층의 익일 시가 수익률. (수익률 목록, 제외사유). 가드는 종베와 동일."""
    rets, drop = [], {"익일시가없음": 0, "익일소멸": 0,
                      "익일제한폭이탈": 0, "기업행사조정의심": 0}
    for it in items:
        o, prev_close, why, _seen = exit_open(it["code"], nxt_slots)
        if why:
            drop[why] = drop.get(why, 0) + 1
            continue
        if prev_close is None:
            drop["익일시가없음"] += 1          # 검사할 수 없으면 쓰지 않는다
            continue
        if abs(o / prev_close - 1.0) > PRICE_LIMIT + LIMIT_TOL:
            drop["익일제한폭이탈"] += 1
            continue
        if abs(prev_close / it["P"] - 1.0) > ADJ_TOL:
            drop["기업행사조정의심"] += 1
            continue
        rets.append(o / it["P"] - 1.0)
    return rets, drop


def mean(xs):
    return (sum(xs) / len(xs)) if xs else None


def layer_diffs(daily):
    """날짜별 계층 평균 목록 → B−A, C−B 의 **날짜 평균**.

    같은 날짜 안에서 뺀 뒤 날짜로 평균낸다. 계층별 전체 평균을 먼저 내고 빼면
    종목 수가 많은 날이 결과를 지배한다.
    """
    out = {}
    for hi, lo in (("B", "A"), ("C", "B")):
        d = [row[hi] - row[lo] for row in daily
             if row.get(hi) is not None and row.get(lo) is not None]
        out[f"{hi}-{lo}"] = {"n일": len(d), "평균": mean(d)}
    return out


def build_day(date, snap_dir, with_returns):
    """하루치 A/B/C 분해. `with_returns=False` 면 **수익률을 계산하지 않는다.**"""
    p15 = os.path.join(snap_dir, f"{date}_1505.csv.gz")
    p13 = os.path.join(snap_dir, f"{date}_1300.csv.gz")
    if not (os.path.exists(p15) and os.path.exists(p13)):
        return {"date": date, "skip": "스냅샷없음"}
    meta, rows1505, dup15 = read_snapshot(p15)
    _, rows1300, dup13 = read_snapshot(p13)

    A, drop = build_A(rows1505)
    Bc, dom_excl = build_B_current(A)
    Bm = market_top_proxy(rows1505)   # ⚠️ A 이전 raw 에서. 순환 제거
    C, funnel = build_C(Bc)
    jac, inter, n1, n2 = agreement(Bc, Bm)
    cap = market_capture(Bm, A, Bc, C)

    day = {"date": date, "capturedAt": meta.get("capturedAt", ""),
           "dup": dup15 + dup13,
           "A": len(A), "A_theme": sum(1 for x in A if x["has_theme"]),
           "themes": len({x["theme"] for x in A if x["theme"]}),
           "B_current": len(Bc), "B_market": len(Bm), "C": len(C),
           "funnel": funnel, "dom_excl": dom_excl,
           "agreement": jac, "agree_n": inter, "capture": cap,
           "drop": drop, "signal": bool(C)}

    # C 후보 변수는 항상 기록한다 — 수익률과 무관하다
    day["C_vals"] = {"B_current": [c_values(x, rows1300) for x in Bc],
                     "B_market":  [c_values(x, rows1300) for x in Bm],
                     "C":         [c_values(x, rows1300) for x in C]}
    if not with_returns:
        day["returns"] = None      # 🔒 단계 잠금 — 여기서 끝난다
        return day

    # ── Stage 1 — 익일 시가 수익률 (위 사전등록 사양 그대로) ──────────
    nxt = next_trading_snapshots(date, snap_dir)
    if not nxt:
        day["returns"] = None
        day["returns_note"] = "익일 스냅샷 없음"
        return day
    res, drops = {}, {}
    for name, items in (("A", A), ("B", Bc), ("C", C)):
        r, d = layer_returns(items, nxt)
        res[name] = {"n": len(r), "평균": mean(r),
                     "평균_비용후": (mean(r) - COST) if r else None}
        drops[name] = d
    day["returns"] = res
    day["ret_drop"] = drops
    return day


def next_trading_snapshots(date, snap_dir):
    """`date` **다음** 관측 거래일의 슬롯들. 없으면 빈 dict.

    관측된 스냅샷 달력을 그대로 쓴다 — 휴장 달력을 여기서 새로 해석하지 않는다.
    `scan_dates` 가 곧 '우리가 실제로 관측한 거래일' 이다.
    """
    dates = scan_dates(snap_dir)
    if date not in dates:
        return {}
    i = dates.index(date)
    if i + 1 >= len(dates):
        return {}
    nxt = dates[i + 1]
    out = {}
    for slot in ("1300", "1505"):
        path = os.path.join(snap_dir, f"{nxt}_{slot}.csv.gz")
        if os.path.exists(path):
            out[slot] = read_snapshot(path)[1]
    return out


def collect(snap_dir=SNAP_DIR, with_returns=False):
    """관측된 거래일 전부. 기본값은 **수익률 없음**이다. 기본값이 안전한 쪽이어야 한다."""
    dates = scan_dates(snap_dir)
    return [build_day(d, snap_dir, with_returns) for d in dates], dates


def stage_of(dates):
    """성숙 거래일 수로 단계를 정한다. 진입일은 **다음 거래일이 있어야** 성숙한다."""
    matured = max(0, len(dates) - 1)
    return (STAGE1 if matured >= PILOT_DAYS else STAGE0), matured


def pct(v, nd=3):
    return "—" if v is None else f"{v * 100:+.{nd}f}%"


def stage1_eta(dates, nontrading=None):
    """Stage 1 이 되는 **예정 거래일**. 검증된 휴장 달력으로만 계산한다.

    왜 도구 안에 넣나 — 2026-09-15 에 나는 이것을 손으로 세어 '9/25 경' 이라고 적었다.
    **추석(9/24·9/25) 이틀을 빼먹었다.** 진행계획이 *"9/23 진입의 다음 예정 거래일은
    9/28"* 이라고 못박아 둔 바로 그 지점이다. 손계산을 없애면 그 실수가 사라진다.

    돌려주는 값: (예정일, 남은 거래일 수, 사유). 달력 범위 밖이면 (None, n, 사유) —
    **평일로 추정하지 않는다.** 그 fail-closed 가 `hyeoks_trading_calendar` 의 기본 동작이다.
    """
    import hyeoks_trading_calendar as cal
    stage, matured = stage_of(dates)
    if stage == STAGE1:
        return None, 0, "이미 Stage 1"
    need = PILOT_DAYS - matured
    if not dates:
        return None, need, "관측 거래일이 없어 기산점이 없다"
    nt = cal.load_nontrading() if nontrading is None else nontrading
    d = dates[-1]
    try:
        for _ in range(need):
            d = cal.next_trading_day(d, nt)
    except ValueError as e:                        # 달력 범위 밖
        return None, need, f"거래일 달력 미검증 — {e}"
    return d, need, ""


def report(snap_dir=SNAP_DIR, today=None):
    stage, matured = stage_of(scan_dates(snap_dir))
    #  🔒 Stage 가 수익률 계산 여부를 정한다. 하드코딩하지 않는다.
    #     이전 판은 `with_returns=False` 가 박혀 있어 Stage 1 이 돼도 구조만 냈다.
    days, dates = collect(snap_dir, with_returns=(stage == STAGE1))
    today = today or datetime.datetime.now(KST).strftime("%Y-%m-%d")
    L = [f"# 🏴 테마 대장 A/B/C 분해 — {today}", ""]
    L.append(f"관측 거래일 **{len(dates)}일** · 성숙(익일 존재) **{matured}일** / "
             f"파일럿 문턱 {PILOT_DAYS}일 → **Stage {stage}**")
    L.append("")
    if stage == STAGE0:
        eta, need, why = stage1_eta(dates)
        L += ["> 🔒 **Stage 0 — 수익률을 계산하지 않았다.**",
              f"> 성숙 거래일이 {matured}일로 파일럿 문턱 {PILOT_DAYS}일에 못 미친다.",
              "> 여기서 수익률을 내면 그 숫자를 보고 A·B·C 정의를 고치게 된다.",
              "> 이 파일은 그 경로를 **코드로 막는다** — 계산 자체를 하지 않는다.",
              "> 아래는 **구조 집계**뿐이다. 성과가 아니다."]
        if eta:
            L.append(f"> **Stage 1 예정일: {eta}** (거래일 {need}일 더 · 검증된 휴장 달력 기준). "
                     "손으로 세지 않는다 — 추석·대체공휴일이 여기서 빠진다.")
        else:
            L.append(f"> **Stage 1 예정일 산출 불가** — {why}")
        L.append("> ⚠️ 예정일은 **수집이 매일 성공했을 때**의 날짜다. 결측일이 생기면 뒤로 밀린다.")
        L.append("")

    if stage == STAGE1:
        daily = [{"date": d["date"],
                  **{k: (d["returns"][k]["평균"] if d.get("returns") else None)
                     for k in RET_LAYERS}}
                 for d in days if not d.get("skip")]
        diffs = layer_diffs(daily)
        L += ["## 📈 Stage 1 — 익일 시가 수익률 (사전등록 사양)", "",
              "| 계층 | 날짜 수 | 평균(비용 전) | 평균(비용 후) |", "|---|--:|--:|--:|"]
        for k in RET_LAYERS:
            vals = [r[k] for r in daily if r[k] is not None]
            m = mean(vals)
            L.append(f"| {k} | {len(vals)} | {pct(m)} | {pct(m - COST) if m is not None else '—'} |")
        L += ["", "| 분리 | 날짜 수 | 평균 차 |", "|---|--:|--:|"]
        for key in ("B-A", "C-B"):
            d = diffs[key]
            label = "**B−A** 대장을 고른 효과" if key == "B-A" else "**C−B** 추가 조건의 효과"
            L.append(f"| {label} | {d['n일']} | {pct(d['평균'])} |")
        L += ["",
              "> 익일 시가는 **연구 대용값**이다(운영대전제 §3). 체결 가능성 인증이 아니다.",
              "> 비용 0.35%는 §3-4-2 값이며 계층 평균에서 일괄 차감한 참고값이다.",
              "> **판정이 아니다.** 20일은 파일럿이고 유의성 검정은 별도 사전등록이 필요하다.",
              ""]

    L += ["## A → B → C 퍼널 (단계별로 센다)", "",
          "| 날짜 | A | 테마소속 | 테마 | **B** 대장 | 5배배제 | +거래대금100억 | **C** +단타 | B_시장 | 일치 |",
          "|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|"]
    for d in days:
        if d.get("skip"):
            L.append(f"| {d['date']} | — | — | — | — | — | — | — | — | *{d['skip']}* |")
            continue
        f = d["funnel"]
        j = "—" if d["agreement"] is None else f"{d['agree_n']} ({d['agreement']*100:.0f}%)"
        L.append(f"| {d['date']} | {d['A']} | {d['A_theme']} | {d['themes']} | "
                 f"**{f['B']}** | {d['dom_excl']} | {f['+거래대금100억']} | "
                 f"**{f['+단타레인지']}** | {d['B_market']} | {j} |")
    L.append("")
    L.append("> 단계별로 세는 이유 — 9/14 에 복합 조건 통과 0 을 '이격 이탈 0' 으로 읽는 "
             "오독을 했다. 한 숫자로 뭉치면 어디서 깎였는지 못 가린다.")
    L.append("")

    # §6-7 질문 1 — 자카드보다 이쪽이 묻는 것에 가깝다
    L += [f"## §6-7 질문 1 — 시장 거래대금 상위 {MARKET_TOP_N} **proxy** 가 단계마다 몇 개 남나",
          "",
          "> 🔴 **2026-09-15 정정.** 이전 판에서는 이 proxy 를 `A` 에서 뽑아 놓고 "
          "`A` 포함률을 셌다 — **항상 100% 인 항등식**이었고, 그걸 근거로 "
          "\"유니버스 문제가 아니다\" 라고 결론지었다. 지금은 **`A` 이전 15:05 원본**에서 뽑는다.",
          "> ⚠️ 이것은 **proxy 다.** §6-8 은 거래대금 **그리고** 등락률을 요구하는데 "
          "이 구현은 거래대금만 본다. '진짜 주도주' 와 동일시하지 않는다.",
          "",
          f"| 날짜 | proxy | A 안 | **B**(대장) 안 | **C**(조건통과) 안 |",
          "|---|--:|--:|--:|--:|"]
    tot = {"n": 0, "inA": 0, "inB": 0, "inC": 0}
    for d in days:
        if d.get("skip"):
            continue
        c = d["capture"]
        for k in tot:
            tot[k] += c[k] or 0
        L.append(f"| {d['date']} | {c['n']} | {c['inA']} | **{c['inB']}** | **{c['inC']}** |")
    if tot["n"]:
        L.append(f"| **합계** | **{tot['n']}** | **{tot['inA']}** "
                 f"({tot['inA']/tot['n']*100:.0f}%) | **{tot['inB']}** "
                 f"({tot['inB']/tot['n']*100:.0f}%) | **{tot['inC']}** "
                 f"({tot['inC']/tot['n']*100:.0f}%) |")
    L += ["",
          "> §6-7 의 해석을 그대로 옮긴다 — **A 에서 이미 빠지면 선정 기준이 아니라 "
          "유니버스의 문제**다. A 는 통과하는데 B 에서 빠지면 대장 판정의 문제다.",
          "> ⚠️ '시장 상위 5 = 진짜 주도주' 는 §6-8 자막에서 온 **미검증 가정**이다.",
          ""]

    nosig = [d["date"] for d in days if not d.get("skip") and not d["signal"]]
    L.append(f"**무신호일** {len(nosig)}일" + (f" — {', '.join(nosig)}" if nosig else " (없음)"))
    L.append("> 무신호일을 지우지 않는다. 지우면 '대장이 있던 날' 만 남아 표가 거짓말을 한다.")
    L.append("")
    L += ["## 사전등록 상태", "",
          f"- **A** 투자가능(거래대금 ≥ {MIN_TURNOVER//100_000_000}억 · 거래정지·관리종목 제외). "
          "테마 소속은 A 의 조건이 **아니다**",
          f"- **B** 테마 내 거래대금 상위 {LEADER_POOL} 중 등락률 1위 "
          f"+ 1등이 2등의 {DOMINANCE_MULT:g}배 넘는 테마 배제 (`omakase.py:604·607·611`)",
          f"- **C** B 중 거래대금 ≥ {MIN_BREAKOUT_TV//100_000_000}억 **그리고** "
          f"등락률 {DANTA_LO*100:g}~{DANTA_HI*100:g}% (`omakase.py:1709·1710·2013~2015`) — "
          "**내가 고른 문턱이 하나도 없다. 전부 시스템에 이미 있던 값이다**",
          f"- **시장 상위 proxy** 거래대금 상위 {MARKET_TOP_N} — **`A` 이전 15:05 원본**에서 뽑는다. "
          "§6-8 조건 1 은 등락률도 요구하지만 이 구현은 거래대금만 본다(그래서 proxy)",
          "- ⚠️ **B 는 현행의 근사(proxy)다.** 생산은 같은 대장 코드를 공유하는 테마를 "
          "합친 뒤 상위 5 를 다시 만드는데(`omakase.py:597~612`), 여기서는 `topThemeNo` 로 묶는다",
          "- ⚠️ C 는 시스템보다 **약간 느슨하다** — `is_junk`·`is_financial_risk` 는 "
          "스냅샷에 입력이 없어 뺐다",
          f"- **C** 후보 변수 {' · '.join(C_VARS)} 는 **계산해서 기록만** 한다. "
          "**문턱이 코드에 없다** — 문턱은 별도 사전등록으로 정한다",
          "- A·B·C 는 전부 **진입일 15:05 스냅샷만** 본다. 익일 데이터로 선정하지 않는다",
          ""]
    return "\n".join(L)


# ══════════════════════════════════════════════════════════════════════
# 합성 시험 — 진행계획 9/14~9/27 '테마 대장 A/B/C 분석기·합성 시험'
# ══════════════════════════════════════════════════════════════════════
def _row(code, name, amt, price=1000, rate=0.0, high=None, theme="", stop="N",
         manage="0", alert="00"):
    return {"itemcode": code, "itemname": name, "tradeAmount": str(amt),
            "nowPrice": str(price), "prevChangeRate": str(rate),
            "highPrice": str(high if high is not None else price),
            "topThemeNo": theme, "themeNos": theme, "tradeStopYn": stop,
            "manageStatusGb": manage, "marketAlertType": alert}


def self_test():
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    B = MIN_TURNOVER
    mk = lambda rows: {r["itemcode"]: r for r in rows}

    print("🧪 A — 투자 가능 후보군")
    rows = mk([_row("1", "통과", B), _row("2", "미달", B - 1),
               _row("3", "정지", B, stop="Y"), _row("4", "관리", B, manage="1"),
               _row("5", "가격0", B, price=0)])
    A, drop = build_A(rows)
    chk("조건 맞는 것만 남는다", [x["code"] for x in A] == ["1"])
    chk("탈락 사유를 전부 센다",
        drop == {"거래대금미달": 1, "거래정지": 1, "관리종목류": 1, "진입가없음": 1}, str(drop))
    chk("테마 없어도 A 에는 들어간다 — 테마 소속은 A 의 조건이 아니다",
        A[0]["has_theme"] is False)

    print("🧪 B_현행 — 테마 내 거래대금 상위 5 중 등락률 1위")
    #  거래대금 순: a>b>c>d>e>f  /  등락률은 f 가 1위지만 f 는 상위 5 밖이다
    rows = mk([_row("a", "a", B*10, rate=1.0, theme="T1"),
               _row("b", "b", B*9,  rate=2.0, theme="T1"),
               _row("c", "c", B*8,  rate=3.0, theme="T1"),
               _row("d", "d", B*7,  rate=4.0, theme="T1"),
               _row("e", "e", B*6,  rate=5.0, theme="T1"),
               _row("f", "f", B*5,  rate=99.0, theme="T1")])
    Bc, _ = build_B_current(build_A(rows)[0])
    chk("상위 5 안에서만 등락률 1위를 고른다 (f 는 6위라 제외)",
        [x["code"] for x in Bc] == ["e"], str([x["code"] for x in Bc]))
    chk("테마당 최대 1종목", len(Bc) == 1)

    print("🧪 B_현행 — 최소 테마 크기는 **생산 코드와 같아야** 한다")
    chk("MIN_THEME_SIZE 가 omakase.py:556·584 의 2 와 같다 — 내가 고른 값이 아니다",
        MIN_THEME_SIZE == 2)
    one = mk([_row("x", "x", B*3, rate=9.0, theme="T9")])
    chk("1종목 테마는 대장을 안 뽑는다", build_B_current(build_A(one)[0])[0] == [])
    two = mk([_row("x", "x", B*3, rate=1.0, theme="T9"),
              _row("y", "y", B*2, rate=9.0, theme="T9")])
    chk("2종목 테마는 뽑는다 (생산과 동일)",
        [z["code"] for z in build_B_current(build_A(two)[0])[0]] == ["y"])

    print("🧪 B_현행 — 테마가 여럿이면 테마마다 하나씩")
    two = mk([_row(f"p{i}", f"p{i}", B*(10-i), rate=float(i), theme="T1") for i in range(3)] +
             [_row(f"q{i}", f"q{i}", B*(10-i), rate=float(i), theme="T2") for i in range(3)])
    chk("테마 2개면 대장 2종목", len(build_B_current(build_A(two)[0])[0]) == 2)

    print("🧪 시장 상위 proxy — A **이전** raw 에서 뽑는다 (순환 제거)")
    many = mk([_row(f"m{i:02d}", f"m{i}", B*(50-i), rate=0.0) for i in range(12)])
    Bm = market_top_proxy(many)
    chk(f"상위 {MARKET_TOP_N}개", len(Bm) == MARKET_TOP_N)
    chk("거래대금 내림차순 상위가 맞다",
        [x["code"] for x in Bm] == [f"m{i:02d}" for i in range(MARKET_TOP_N)])

    # 🔴 순환논리 회귀 테스트 — 이것이 이번 수정의 핵심이다.
    #    A 가 거르는 종목이 시장 거래대금 1위면, proxy 는 그것을 잡고 inA 는 100% 가 아니어야 한다.
    trap = mk([_row("halt", "거래정지 1위", B * 999, stop="Y"),      # A 탈락, 거래대금 1위
               _row("mng",  "관리종목 2위", B * 998, manage="1"),    # A 탈락
               _row("ok1",  "정상", B * 10, rate=5.0, theme="T1"),
               _row("ok2",  "정상", B * 9,  rate=4.0, theme="T1")])
    A_trap, _ = build_A(trap)
    Bm_trap = market_top_proxy(trap)
    cap = market_capture(Bm_trap, A_trap, [], [])
    chk("proxy 가 A 탈락 종목도 잡는다", {"halt", "mng"} <= {x["code"] for x in Bm_trap})
    chk("🔑 inA 가 100% 가 아니다 — 항등식이 깨졌다", cap["inA"] < cap["n"],
        f"inA={cap['inA']} / n={cap['n']}")
    chk("A 를 통과한 것만 inA 로 센다", cap["inA"] == 2)
    #    반대로 A 에서 뽑아 넣으면 항등식이 된다는 것도 같이 박아 둔다(무엇이 틀렸었는지 남긴다).
    from_A = sorted(A_trap, key=lambda x: -x["amt"])[:MARKET_TOP_N]
    chk("(참고) A 에서 뽑으면 inA 는 항상 100% — 이전 판의 오류",
        market_capture(from_A, A_trap, [], [])["inA"] == len(from_A))
    chk("거래대금 0 은 proxy 에서 뺀다",
        market_top_proxy(mk([_row("z", "z", 0)])) == [])
    #  proxy 항목이 c_values 가 요구하는 키를 전부 갖는가 — 첫 실행에서 KeyError 로 터졌다.
    #  자체검증은 통과하는데 실제 경로가 죽는 종류라 명시적으로 건다.
    _pr = market_top_proxy(mk([_row("k", "k", B*5, price=900, high=1000, rate=3.0)]))[0]
    chk("proxy 항목이 c_values 키를 전부 갖는다 (P·high·rate·amt)",
        all(k in _pr for k in ("P", "high", "rate", "amt", "code")), str(sorted(_pr)))
    chk("proxy 로도 c_values 가 돈다",
        c_values(_pr, {"k": {"tradeAmount": "1"}})["고가유지율"] == 0.9)

    print("🧪 동률은 종목코드로 깬다 — 실행할 때마다 달라지면 재현이 안 된다")
    tie = mk([_row("zz", "zz", B*5, rate=1.0), _row("aa", "aa", B*5, rate=1.0)])
    chk("거래대금 동률이면 코드 오름차순",
        market_top_proxy(tie)[0]["code"] == "aa")
    tie2 = mk([_row(f"t{i}", f"t{i}", B*(9-i), rate=7.0, theme="T1") for i in range(3)])
    chk("등락률 동률이면 코드 오름차순",
        build_B_current(build_A(tie2)[0])[0][0]["code"] == "t0")

    print("🧪 5배 룰 — 1등이 2등을 압도하면 그 테마를 통째로 뺀다 (omakase.py:607)")
    dom = mk([_row("big", "big", B*100, rate=1.0, theme="T1"),
              _row("s1", "s1", B*2, rate=9.0, theme="T1"),
              _row("s2", "s2", B*1, rate=8.0, theme="T1")])
    got, excl = build_B_current(build_A(dom)[0])
    chk("5배 넘으면 대장이 안 나온다", got == [], str([x["code"] for x in got]))
    chk("배제된 테마 수를 센다", excl == 1)
    nodom = mk([_row("big", "big", B*4, rate=1.0, theme="T1"),
                _row("s1", "s1", B*2, rate=9.0, theme="T1"),
                _row("s2", "s2", B*1, rate=8.0, theme="T1")])
    got2, excl2 = build_B_current(build_A(nodom)[0])
    chk("5배 미만이면 정상 선정", [x["code"] for x in got2] == ["s1"] and excl2 == 0)

    print("🧪 C — 시스템의 추가 조건. 문턱은 omakase.py 에서 가져온 값이다")
    lead = [{"code": "a", "amt": MIN_BREAKOUT_TV, "rate": 5.0},      # 둘 다 통과
            {"code": "b", "amt": MIN_BREAKOUT_TV - 1, "rate": 5.0},  # 거래대금 미달
            {"code": "c", "amt": MIN_BREAKOUT_TV, "rate": 2.9},      # 단타 하한 미달
            {"code": "d", "amt": MIN_BREAKOUT_TV, "rate": 29.5}]     # 단타 상한 초과
    C, fn = build_C(lead)
    chk("둘 다 통과한 것만 C", [x["code"] for x in C] == ["a"], str([x["code"] for x in C]))
    chk("퍼널을 단계별로 센다 — 한 숫자로 뭉치지 않는다",
        fn == {"B": 4, "+거래대금100억": 3, "+단타레인지": 1}, str(fn))
    chk(f"단타 하한 {DANTA_LO*100:g}% 는 포함(이상)",
        build_C([{"code": "x", "amt": MIN_BREAKOUT_TV, "rate": 3.0}])[1]["+단타레인지"] == 1)
    chk(f"단타 상한 {DANTA_HI*100:g}% 는 제외(미만)",
        build_C([{"code": "x", "amt": MIN_BREAKOUT_TV, "rate": 29.5}])[1]["+단타레인지"] == 0)
    chk("거래대금 문턱은 이상(=100억 포함)",
        build_C([{"code": "x", "amt": MIN_BREAKOUT_TV, "rate": 5.0}])[1]["+거래대금100억"] == 1)
    chk("C 문턱이 omakase.py 상수와 같다 — 내가 고른 값이 아니다",
        MIN_BREAKOUT_TV == 10_000_000_000 and (DANTA_LO, DANTA_HI) == (0.03, 0.295))

    print("🧪 일치율 (§6-8 항목 8)")
    j, inter, n1, n2 = agreement([{"code": "1"}, {"code": "2"}], [{"code": "2"}, {"code": "3"}])
    chk("자카드 = 1/3", abs(j - 1/3) < 1e-12, f"{j}")
    chk("교집합 수", inter == 1)
    chk("둘 다 비면 None", agreement([], [])[0] is None)
    chk("완전 일치면 1.0", agreement([{"code": "1"}], [{"code": "1"}])[0] == 1.0)
    chk("완전 불일치면 0.0", agreement([{"code": "1"}], [{"code": "2"}])[0] == 0.0)

    print("🧪 C 후보 변수 — 계산만 하고 거르지 않는다")
    it = {"code": "1", "amt": 300.0, "P": 90.0, "high": 100.0, "rate": 5.0}
    cv = c_values(it, {"1": {"tradeAmount": "100"}})
    chk("오후수급비 = 15:05 / 13:00", cv["오후수급비"] == 3.0)
    chk("고가유지율 = 현재가 / 당일고가", cv["고가유지율"] == 0.9)
    chk("등락률 그대로", cv["등락률"] == 5.0)
    chk("상한가 판정", cv["상한가여부"] is False)
    chk("상한가면 True", c_values(dict(it, rate=29.9), {"1": {"tradeAmount": "100"}})["상한가여부"])
    chk("13시 행이 없으면 None — 0 으로 채우지 않는다",
        c_values(it, {})["오후수급비"] is None)
    chk("고가 0 이면 None", c_values(dict(it, high=0.0), {})["고가유지율"] is None)
    src = open(__file__, encoding="utf-8").read().split("def self_test")[0]
    chk("C 에 문턱 상수가 없다 — 이것이 이 파일의 핵심 약속이다",
        "C_THRESHOLD" not in src and "C_MIN" not in src and "C_CUT" not in src)

    print("🧪 🔒 단계 잠금 — 성숙 전에는 수익률을 계산하지 않는다")
    chk(f"{PILOT_DAYS}일 미만이면 Stage 0",
        stage_of(["d"] * PILOT_DAYS)[0] == STAGE0, f"성숙 {PILOT_DAYS-1}일")
    chk(f"{PILOT_DAYS}일 이상이면 Stage 1",
        stage_of(["d"] * (PILOT_DAYS + 1))[0] == STAGE1)
    chk("진입일은 다음 거래일이 있어야 성숙한다", stage_of(["d"] * 13)[1] == 12)
    chk("거래일 0이면 성숙 0 (음수가 되지 않는다)", stage_of([])[1] == 0)
    chk("collect 의 기본값이 수익률 없음 — 기본값이 안전한 쪽이어야 한다",
        collect.__defaults__[1] is False)

    print("🧪 Stage 1 예정일 — 손계산을 없앤다 (2026-09-15 추석 누락 재발 방지)")
    import hyeoks_trading_calendar as cal
    _nt = cal.load_nontrading()
    # 9/15 까지 13일 관측(성숙 12) → 8거래일 뒤. 추석 9/24·25 를 건너뛰어야 한다.
    _obs = ["2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03",
            "2026-09-04", "2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10",
            "2026-09-11", "2026-09-14", "2026-09-15"]
    _eta, _need, _why = stage1_eta(_obs, _nt)
    chk("남은 거래일 수", _need == 8, str(_need))
    chk("추석(9/24·25)을 건너뛴 2026-09-29 이어야 한다", _eta == "2026-09-29", str(_eta))
    chk("9/25 는 답이 아니다 — 추석이라 거래일이 아니다", _eta != "2026-09-25")
    chk("이미 Stage 1 이면 예정일 없음",
        stage1_eta(["d"] * (PILOT_DAYS + 1), _nt) == (None, 0, "이미 Stage 1"))
    chk("관측이 없으면 기산점이 없다고 말한다", "기산점" in stage1_eta([], _nt)[2])
    _far, _, _w = stage1_eta(["2026-12-30"], _nt)   # 2027 달력 미검증 구간으로 넘어간다
    chk("달력 범위를 넘으면 None 이고 사유를 말한다 (평일 추정 금지)",
        _far is None and "미검증" in _w, _w[:40])

    print("🧪 🔴 Stage 1 이 되면 **정말로** 수익률이 계산되는가 (9/15 결함 회귀)")
    #  ChatGPT 지적 — Stage 1 에 도달해도 계산 경로가 없었다. 합성 스냅샷으로 직접 건다.
    import gzip, shutil, tempfile
    tmp = tempfile.mkdtemp()
    try:
        hdr = ("itemcode,itemname,tradeAmount,nowPrice,openPrice,highPrice,"
               "prevChangeRate,topThemeNo,themeNos,tradeStopYn,manageStatusGb,marketAlertType")
        def write(day, slot, rows):
            with gzip.open(os.path.join(tmp, f"{day}_{slot}.csv.gz"), "wt", encoding="utf-8") as f:
                f.write("#meta,slot=%s\n%s\n" % (slot, hdr))
                for r in rows:
                    f.write(",".join(str(x) for x in r) + "\n")
        #  21 거래일 → 성숙 20일 → Stage 1. 매일 같은 3종목(한 테마), 익일 시가 +10%.
        days = [f"2026-1{m}-{d:02d}" for m in (0, 1) for d in range(1, 12)][:21]
        for i, day in enumerate(days):
            #  1505: 진입가 1000 / 1300: 절반 거래대금 / 익일 시가는 1100(= +10%)
            rows15 = [[f"s{j}", f"s{j}", MIN_TURNOVER * (30 - j), 1000, 1100, 1000,
                       5.0, "T1", "T1", "N", "0", "00"] for j in range(3)]
            write(day, "1505", rows15)
            write(day, "1300", [[r[0], r[1], MIN_TURNOVER * 10, 1000, 1100, 1000,
                                 5.0, "T1", "T1", "N", "0", "00"] for r in rows15])
        st, matured = stage_of(scan_dates(tmp))
        chk("합성 21일이면 Stage 1", st == STAGE1, f"성숙 {matured}일")
        got, _ = collect(tmp, with_returns=True)
        rets = [d for d in got if d.get("returns")]
        chk("🔑 Stage 1 에서 수익률이 실제로 계산된다", len(rets) > 0, f"{len(rets)}일")
        a = rets[0]["returns"]["A"]
        chk("익일 시가 +10% 를 그대로 잡는다", abs(a["평균"] - 0.10) < 1e-9, str(a["평균"]))
        chk("비용 후는 COST 만큼 낮다",
            abs(a["평균_비용후"] - (0.10 - COST)) < 1e-9)
        chk("계층별로 따로 낸다", set(rets[0]["returns"]) == set(RET_LAYERS))
        txt = report(tmp, today="2026-11-30")
        chk("리포트가 Stage 1 구간을 찍는다", "Stage 1 — 익일 시가 수익률" in txt)
        chk("B−A 와 C−B 를 분리해 찍는다",
            "대장을 고른 효과" in txt and "추가 조건의 효과" in txt)
        chk("연구 대용값임을 밝힌다", "연구 대용값" in txt)
        chk("판정이 아니라고 밝힌다", "판정이 아니다" in txt)
        #  Stage 0 이면 여전히 계산하지 않는다(잠금이 살아 있는가)
        for day in days[5:]:
            for slot in ("1300", "1505"):
                os.remove(os.path.join(tmp, f"{day}_{slot}.csv.gz"))
        chk("거래일이 줄면 다시 Stage 0", stage_of(scan_dates(tmp))[0] == STAGE0)
        chk("🔒 Stage 0 이면 리포트가 수익률을 안 낸다",
            "Stage 1 — 익일 시가 수익률" not in report(tmp, today="2026-11-30"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("🧪 계층 차이 — 날짜 안에서 빼고 날짜로 평균낸다")
    dd = [{"A": 0.01, "B": 0.03, "C": 0.06}, {"A": 0.02, "B": 0.02, "C": 0.05}]
    df = layer_diffs(dd)
    chk("B−A 는 날짜별 차의 평균", abs(df["B-A"]["평균"] - 0.01) < 1e-12, str(df["B-A"]))
    chk("C−B 도 같은 방식", abs(df["C-B"]["평균"] - 0.03) < 1e-12)
    chk("한쪽이 None 인 날은 뺀다",
        layer_diffs([{"A": 0.01, "B": None, "C": 0.05}])["B-A"]["n일"] == 0)

    print("🧪 선정에 익일 데이터를 쓰지 않는다 (운영대전제 §4)")
    body = src.split("def build_day")[0]
    for fn in ("build_A", "build_B_current", "build_B_market", "c_values"):
        seg = body.split(f"def {fn}")[1].split("\ndef ")[0] if f"def {fn}" in body else ""
        chk(f"{fn} 이 익일 인자를 받지 않는다",
            "nxt" not in seg.split(")")[0] and "next" not in seg.split(")")[0])

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="스냅샷 없이 분해 로직만 검증")
    ap.add_argument("--snap-dir", default=SNAP_DIR)
    a = ap.parse_args()
    if a.self_test:
        return self_test()
    print(report(a.snap_dir))
    return 0


if __name__ == "__main__":
    sys.exit(main())
