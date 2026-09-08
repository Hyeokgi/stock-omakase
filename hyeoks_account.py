"""계좌 수준 성과 — 종목별 평균 수익률과 계좌 수익률은 다르다.

왜 만들었나 (2026-09-08, 외부 4차 검토)
---------------------------------------
§3 은 **행 단위 평균 순알파**를 잰다. 그건 계좌 수익률이 아니다.

> "같은 날 여러 종목을 보유할 때의 자금 배분, 보유 기간 중첩, 업종 집중,
>  실제 체결 비용을 반영해야 한다. **평균 수익률이 양수여도 큰 손실 몇 번으로
>  계좌가 크게 훼손될 수 있다.**"

채널 10개가 하루 최대 20건을 쌓는데, 그게 계좌에 어떤 부담인지 지금까지
아무도 재지 않았다. 이 모듈은 그걸 잰다.

⚠️ **관측이지 집행이 아니다.** 이 숫자로 매매하지 않는다(§0).

무엇을 가정하는가 — 전부 명시한다
----------------------------------
1. **포지션당 명목금액 1단위 균등.** §3-4-3 의 1단계(균등 비중)와 같다.
2. **보유기간은 채널 호라이즌.** T+H 에 청산한다고 본다.
3. **거래일은 평일 근사.** 거래소 달력이 없다(§3-2-0-b3 과 같은 한계).
   휴장일이 있으면 실제보다 짧게 잡히므로 **동시 보유가 과소평가**된다.
4. **비용은 §3-4-2 의 0.35%** 를 포지션마다 한 번 뺀다. 슬리피지 미반영.
5. **중간 평가는 관측된 마크(T+1·3·5·10·20)만** 쓴다. 그 사이는 직전 마크를
   유지하는 계단으로 본다 — 없는 값을 지어내지 않는다.

⚠️ 그래서 **MDD 는 하한이다.** 마크 사이의 골은 보이지 않는다. 실제 MDD 는
   이 값보다 **크면 컸지 작지 않다.** 이 사실을 리포트에 항상 같이 적는다.
"""
import datetime

KST = datetime.timezone(datetime.timedelta(hours=9))

C_TRADE_ID, C_ENTRY, C_CHANNEL, C_NAME, C_CODE, C_THEME = 0, 1, 2, 3, 4, 5
C_EXCLUDE = 25

HORIZON = {"리포트TOP2_중기": 10, "리포트TOP2_장기": 60}
DEFAULT_HORIZON = 5
COST_PCT = 0.35
# 관측된 중간 마크. (일수, 종목열, 지수열)
MARKS = ((1, 17, 21), (3, 18, 22), (5, 19, 23), (10, 20, 24), (20, 26, 29),
         (60, 27, 30), (120, 28, 31))
# 대조군·벤치마크는 '전략 계좌'가 아니다. 따로 센다.
CONTROL_PREFIXES = ("랜덤2", "지수벤치")


def _num(v):
    try:
        f = float(str(v).strip().replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None
    return f if f == f and abs(f) != float("inf") else None


def is_excluded(row):
    return "제외" in (str(row[C_EXCLUDE]) if len(row) > C_EXCLUDE else "")


def is_control(channel):
    return any(channel.startswith(p) for p in CONTROL_PREFIXES)


def add_trading_days(d, n):
    """평일 기준 n거래일 뒤. ⚠️ 휴장일 미반영 — 실제보다 이르게 잡힌다."""
    cur, left = d, n
    while left > 0:
        cur += datetime.timedelta(days=1)
        if cur.weekday() < 5:
            left -= 1
    return cur


def build_positions(rows, today=None):
    """원장 행 → 포지션. 비용은 여기서 한 번 뺀다(§3-4-2).

    반환 포지션의 `marks` 는 {경과거래일: 순수익률%} 이고 **관측된 것만** 담는다.
    """
    today = today or datetime.datetime.now(KST).date()
    out, skipped = [], {"제외표식": 0, "날짜없음": 0, "채널없음": 0, "마크없음": 0}
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
        try:
            entry = datetime.datetime.strptime(
                str(row[C_ENTRY]).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            skipped["날짜없음"] += 1
            continue
        h = HORIZON.get(ch, DEFAULT_HORIZON)
        cost = 0.0 if ch.startswith("지수벤치") else COST_PCT
        marks = {}
        for d, si, ii in MARKS:
            if d > h or len(row) <= max(si, ii):
                continue
            s, i = _num(row[si]), _num(row[ii])
            if s is None or i is None:
                continue
            marks[d] = s - i - cost          # 순알파 = 종목 − 지수 − 비용
        if not marks:
            skipped["마크없음"] += 1
            continue
        out.append({
            "trade_id": str(row[C_TRADE_ID]).strip(),
            "channel": ch, "horizon": h, "entry": entry,
            "name": str(row[C_NAME]).strip() if len(row) > C_NAME else "",
            "code": str(row[C_CODE]).strip() if len(row) > C_CODE else "",
            "theme": str(row[C_THEME]).strip() if len(row) > C_THEME else "",
            "marks": marks,
            "closed": max(marks) >= h,       # 호라이즌 마크가 있으면 청산 완료
            "close_date": add_trading_days(entry, min(h, max(marks))),
            "final": marks[min(h, max(marks))],
        })
    return out, skipped


def mark_at(pos, day):
    """경과 `day` 거래일 시점의 평가값. **관측된 직전 마크를 유지**한다."""
    seen = [d for d in pos["marks"] if d <= day]
    return pos["marks"][max(seen)] if seen else 0.0


def daily_series(positions):
    """거래일마다 (열린 포지션 수, 미실현 포함 평가손익 합, 실현손익 누적)."""
    if not positions:
        return []
    start = min(p["entry"] for p in positions)
    end = max(p["close_date"] for p in positions)
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += datetime.timedelta(days=1)

    rows = []
    for d in days:
        open_n, mtm, realized = 0, 0.0, 0.0
        for p in positions:
            if d < p["entry"]:
                continue
            elapsed = sum(1 for x in days if p["entry"] < x <= d)
            if d >= p["close_date"]:
                realized += p["final"]
            else:
                open_n += 1
                mtm += mark_at(p, elapsed)
        rows.append({"date": d, "open": open_n,
                     "equity": realized + mtm, "realized": realized})
    return rows


def metrics(positions, series=None):
    """계좌 지표. 단위는 '포지션 1단위' 기준 %."""
    series = series if series is not None else daily_series(positions)
    if not series:
        return None
    peak_open = max(r["open"] for r in series)
    eq = [r["equity"] for r in series]
    peak, mdd, mdd_at = eq[0], 0.0, None
    for r in series:
        peak = max(peak, r["equity"])
        dd = peak - r["equity"]
        if dd > mdd:
            mdd, mdd_at = dd, r["date"]
    # 계좌 % 로 환산 — 최대 동시 보유만큼 슬롯을 두고 균등 배분했다고 본다
    denom = peak_open or 1
    return {
        "positions": len(positions),
        "trading_days": len(series),
        "max_concurrent": peak_open,
        "avg_concurrent": sum(r["open"] for r in series) / len(series),
        "sum_pnl_units": eq[-1],
        "account_return_pct": eq[-1] / denom,
        "mdd_units": mdd,
        "mdd_pct": mdd / denom,
        "mdd_at": mdd_at,
        "win_rate": (sum(1 for p in positions if p["final"] > 0) / len(positions) * 100
                     if positions else 0.0),
        "worst": min((p["final"] for p in positions), default=0.0),
        "best": max((p["final"] for p in positions), default=0.0),
    }


def concentration(positions):
    """같은 날 같은 테마·채널에 몇 개가 겹쳤나. §3-4-3 의 20% 상한 대비 참고."""
    by_day = {}
    for p in positions:
        by_day.setdefault(p["entry"], []).append(p)
    worst_theme, worst_day, worst_n = "", None, 0
    same_stock = 0
    for d, ps in by_day.items():
        themes = {}
        codes = {}
        for p in ps:
            if p["theme"]:
                themes[p["theme"]] = themes.get(p["theme"], 0) + 1
            if p["code"]:
                codes[p["code"]] = codes.get(p["code"], 0) + 1
        for t, n in themes.items():
            if n > worst_n:
                worst_theme, worst_day, worst_n = t, d, n
        same_stock += sum(n - 1 for n in codes.values() if n > 1)
    return {"max_same_theme": worst_n, "theme": worst_theme, "theme_day": worst_day,
            "same_stock_dup": same_stock,
            "max_entries_in_a_day": max((len(v) for v in by_day.values()), default=0)}


def self_test():
    ok = True

    def chk(label, cond, note=""):
        nonlocal ok
        ok = ok and bool(cond)
        print(f"  {'✅' if cond else '❌'} {label}" + (f"   {note}" if note else ""))

    print("🧪 계좌 수준 성과")
    hdr = [""] * 38

    def mk(tid, ch, entry, s5=None, i5=0.0, s1=None, i1=0.0, name="A", code="000001",
           theme="", memo=""):
        r = [""] * 38
        r[C_TRADE_ID], r[C_ENTRY], r[C_CHANNEL] = tid, entry, ch
        r[C_NAME], r[C_CODE], r[C_THEME] = name, code, theme
        r[C_EXCLUDE] = memo
        if s1 is not None:
            r[17], r[21] = str(s1), str(i1)
        if s5 is not None:
            r[19], r[23] = str(s5), str(i5)
        return r

    T = datetime.date(2026, 9, 30)
    ps, sk = build_positions([hdr, mk("A1", "차트TOP2", "2026-09-01", s5=3.0, i5=1.0)], T)
    chk("순알파 = 종목 − 지수 − 0.35", abs(ps[0]["final"] - 1.65) < 1e-9, f"{ps[0]['final']}")
    chk("청산일이 평일 5거래일 뒤",
        ps[0]["close_date"] == datetime.date(2026, 9, 8), f"{ps[0]['close_date']}")
    chk("제외 행은 빠진다",
        build_positions([hdr, mk("X", "차트TOP2", "2026-09-01", s5=3.0,
                                 memo="거래정지 — 측정 제외")], T)[0] == [])
    chk("마크가 하나도 없으면 포지션이 아니다",
        build_positions([hdr, mk("N", "차트TOP2", "2026-09-01")], T)[1]["마크없음"] == 1)

    # 중간 마크를 실제로 쓰는가 — 이게 MDD 의 핵심이다
    p = build_positions([hdr, mk("M", "차트TOP2", "2026-09-01", s1=-8.0, s5=3.0)], T)[0][0]
    chk("T+1 마크가 보존된다", abs(p["marks"][1] - (-8.35)) < 1e-9, f"{p['marks']}")
    chk("경과 0일이면 아직 마크 없음 → 0", mark_at(p, 0) == 0.0)
    chk("경과 2일이면 **직전 마크(T+1)를 유지**", abs(mark_at(p, 2) - (-8.35)) < 1e-9)
    chk("경과 5일이면 T+5", abs(mark_at(p, 5) - 2.65) < 1e-9)

    ser = daily_series([p])
    lowest = min(r["equity"] for r in ser)
    chk("중간에 −8.35 까지 빠지는 것이 곡선에 보인다",
        abs(lowest - (-8.35)) < 1e-9, f"최저={lowest:.2f}")
    m = metrics([p], ser)
    # ⚠️ 8.35 다. 처음엔 11.0(= −8.35 에서 +2.65 까지)으로 적었다가 이 검사가 잡았다.
    #    그건 낙폭이 아니라 **진폭**이다. 낙폭은 **앞선 고점**에서의 하락이고,
    #    골(−8.35) 앞의 고점은 진입 시점 0 이므로 8.35 가 맞다. 내 기대값이 틀렸다.
    chk("MDD 가 그 골을 잡는다(청산가만 봤다면 0 이었다)",
        abs(m["mdd_units"] - 8.35) < 1e-9, f"MDD={m['mdd_units']:.2f}단위")
    # 청산가만 보는 곡선이었다면 낙폭이 0 이었다는 것도 고정해 둔다
    _close_only = [{"date": p["close_date"], "open": 0,
                    "equity": p["final"], "realized": p["final"]}]
    chk("같은 포지션도 청산가만 보면 MDD 0 — 중간 마크가 있어야 보인다",
        metrics([p], _close_only)["mdd_units"] == 0.0)
    chk("최종 손익은 청산값", abs(m["sum_pnl_units"] - 2.65) < 1e-9)

    # 동시 보유와 자금
    rows = [hdr,
            mk("C1", "차트TOP2", "2026-09-01", s5=1.0, code="000001"),
            mk("C2", "차트TOP2", "2026-09-01", s5=1.0, code="000002"),
            mk("C3", "차트TOP2", "2026-09-02", s5=1.0, code="000003")]
    ps3, _ = build_positions(rows, T)
    m3 = metrics(ps3)
    chk("최대 동시 보유 3", m3["max_concurrent"] == 3, f"{m3['max_concurrent']}")
    chk("계좌 % 는 최대 동시 보유로 나눈다",
        abs(m3["account_return_pct"] - m3["sum_pnl_units"] / 3) < 1e-9)
    chk("하루 최대 진입 2건", concentration(ps3)["max_entries_in_a_day"] == 2)

    conc = concentration(build_positions([hdr,
        mk("T1", "차트TOP2", "2026-09-01", s5=1.0, code="1", theme="반도체"),
        mk("T2", "수급TOP2", "2026-09-01", s5=1.0, code="2", theme="반도체"),
        mk("T3", "리포트TOP2_단기", "2026-09-01", s5=1.0, code="1", theme="반도체")], T)[0])
    chk("같은 날 같은 테마 3건을 잡는다", conc["max_same_theme"] == 3, f"{conc}")
    chk("같은 날 같은 종목 중복도 센다", conc["same_stock_dup"] == 1)

    chk("대조군 판별", is_control("랜덤2") and is_control("지수벤치_KOSPI")
        and not is_control("차트TOP2"))
    chk("지수벤치는 비용 면제",
        abs(build_positions([hdr, mk("B", "지수벤치_KOSPI", "2026-09-01",
                                     s5=2.0, i5=1.0)], T)[0][0]["final"] - 1.0) < 1e-9)
    chk("중기 채널은 T+10 호라이즌", HORIZON["리포트TOP2_중기"] == 10)
    chk("빈 원장도 죽지 않는다", metrics([]) is None)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def build_report(strat, ctrl, sk, today):
    """계좌 리포트. ⚠️ 가정을 숫자보다 먼저 적는다."""
    L = []
    A = L.append
    A(f"# 💰 계좌 수준 성과 — {today}")
    A("")
    A("> §3 이 재는 **행 단위 평균 순알파**는 계좌 수익률이 아니다. 이 표는 같은 원장을")
    A("> **계좌 관점**으로 다시 본 것이다. 자금 배분·보유 중첩·집중도가 들어간다.")
    A("> ⚠️ **관측이지 집행이 아니다.** 이 숫자로 매매하지 않는다(§0).")
    A("")
    A("## 가정 (숫자보다 먼저 읽을 것)")
    A("")
    A("| # | 가정 | 영향 |")
    A("|---|---|---|")
    A("| 1 | 포지션당 명목금액 **1단위 균등** (§3-4-3 1단계) | 비중을 바꾸면 결과가 바뀐다 |")
    A("| 2 | 보유기간 = 채널 호라이즌, T+H 청산 | 실제 청산 규칙(손절·트레일링) 미반영 |")
    A("| 3 | 거래일은 **평일 근사** (거래소 달력 없음) | 휴장일만큼 **동시 보유가 과소평가** |")
    A("| 4 | 비용 **0.35%** 포지션당 1회, 슬리피지 미반영 | 실전은 이보다 나쁘다 |")
    A("| 5 | 중간 평가는 **관측 마크(T+1·3·5·10·20)만**, 사이는 직전 마크 유지 | 마크 사이의 골은 안 보인다 |")
    A("")
    A("> 🚨 **그래서 MDD 는 하한이다.** 실제 최대낙폭은 이 값보다 **크면 컸지 작지 않다.**")
    A("")

    def block(title, m, c, note=""):
        A(f"## {title}")
        A("")
        if not m:
            A("_포지션 없음_")
            A("")
            return
        A("| 지표 | 값 |")
        A("|---|--:|")
        A(f"| 포지션 수 | {m['positions']}건 |")
        A(f"| 관측 거래일 | {m['trading_days']}일 |")
        A(f"| **최대 동시 보유** | **{m['max_concurrent']}건** |")
        A(f"| 평균 동시 보유 | {m['avg_concurrent']:.1f}건 |")
        A(f"| 누적 손익(1단위 기준) | {m['sum_pnl_units']:+.2f} |")
        A(f"| **계좌 수익률**(최대 동시 보유 슬롯 균등) | **{m['account_return_pct']:+.2f}%** |")
        A(f"| **최대낙폭(MDD)** | **{m['mdd_pct']:.2f}%** ({m['mdd_units']:.2f}단위) |")
        A(f"| MDD 시점 | {m['mdd_at']} |")
        A(f"| 승률 | {m['win_rate']:.0f}% |")
        A(f"| 최악 / 최고 1건 | {m['worst']:+.2f}% / {m['best']:+.2f}% |")
        A("")
        A(f"- 하루 최대 진입 **{c['max_entries_in_a_day']}건**")
        A(f"- 같은 날 같은 테마 최대 **{c['max_same_theme']}건**"
          + (f" ({c['theme']}, {c['theme_day']})" if c['max_same_theme'] > 1 else ""))
        A(f"- 같은 날 같은 종목 중복 **{c['same_stock_dup']}건**"
          + " — 서로 다른 채널이 같은 종목을 담으면 분산이 아니라 **배수 베팅**이다"
          if c['same_stock_dup'] else "")
        if note:
            A("")
            A(note)
        A("")

    ms, cs = metrics(strat), concentration(strat)
    mc, cc = metrics(ctrl), concentration(ctrl)
    block("전략 채널 합산", ms, cs)
    block("대조군(랜덤2·지수벤치) 합산", mc, cc,
          "> 대조군은 '전략 계좌'가 아니다. **같은 계좌 규칙을 대조군에 적용하면 어떤가**를")
    if ms and mc:
        A("## 전략 − 대조군")
        A("")
        A("| 지표 | 전략 | 대조군 | 차이 |")
        A("|---|--:|--:|--:|")
        A(f"| 계좌 수익률 | {ms['account_return_pct']:+.2f}% | "
          f"{mc['account_return_pct']:+.2f}% | "
          f"{ms['account_return_pct'] - mc['account_return_pct']:+.2f}%p |")
        A(f"| MDD | {ms['mdd_pct']:.2f}% | {mc['mdd_pct']:.2f}% | "
          f"{ms['mdd_pct'] - mc['mdd_pct']:+.2f}%p |")
        A(f"| 최대 동시 보유 | {ms['max_concurrent']}건 | {mc['max_concurrent']}건 | |")
        A("")
        A("> ⚠️ 이 차이에 **유의성 검정을 붙이지 않았다.** 계좌 곡선은 일별 값이 서로")
        A("> 독립이 아니라 §3 의 t 검정을 그대로 쓸 수 없다. 여기서는 **크기만 본다.**")
        A("")

    A("## 채널별")
    A("")
    A("| 채널 | 건수 | 누적손익 | 최대동시 | MDD |")
    A("|---|--:|--:|--:|--:|")
    for ch in sorted({p["channel"] for p in strat + ctrl}):
        sub = [p for p in strat + ctrl if p["channel"] == ch]
        mm = metrics(sub)
        if mm:
            A(f"| {'*' if is_control(ch) else ''}{ch} | {mm['positions']} | "
              f"{mm['sum_pnl_units']:+.2f} | {mm['max_concurrent']} | {mm['mdd_pct']:.2f}% |")
    A("")
    A("`*` = 대조군")
    A("")
    A("## 제외된 행")
    A("")
    for k, v in sk.items():
        A(f"- {k}: {v}행")
    A("")
    return "\n".join(L)


def main():
    import argparse, os
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--stdout-only", action="store_true")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(
        "https://docs.google.com/spreadsheets/d/"
        "1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit")
    rows = doc.worksheet("백테스트_로그").get_all_values()   # 읽기 전용
    today = datetime.datetime.now(KST).strftime("%Y-%m-%d")

    pos, sk = build_positions(rows)
    strat = [p for p in pos if not is_control(p["channel"])]
    ctrl = [p for p in pos if is_control(p["channel"])]
    print(f"📒 원장 {len(rows) - 1}행 → 포지션 {len(pos)}건 "
          f"(전략 {len(strat)} · 대조군 {len(ctrl)})")

    md = build_report(strat, ctrl, sk, today)
    if a.stdout_only:
        print(md)
    else:
        path = a.out or f"data/account/{today}_account.md"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)
        print(f"💾 저장: {path}")
        print(md)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
