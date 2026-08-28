# -*- coding: utf-8 -*-
# ==========================================================================
# 🚪 HYEOKS Phase 2 — 청산 규칙 비교 (연구 전용 · 읽기만 한다)
# --------------------------------------------------------------------------
# 왜 만들었나
#   전략로드맵 §0 의 자기진단이 이렇다 —
#     "edge는 이미 나쁘지 않고 sizing·exit는 검증이 0이다.
#      다음 수익 개선의 무게중심은 선정이 아니라 보유·청산·비중이다."
#   그런데 그 뒤로 만들어진 것은 전부 선정(A축) 쪽이었고, Phase 2 는 착수조차 안 됐다.
#   Phase 2 는 **새 수집이 전혀 필요 없다**. 로드맵이 직접 그렇게 적어두었다 —
#   "추가 채널 없이 기존 로그로 계산 가능 — 새 인프라 불필요."
#   이 스크립트가 그것이다.
#
# 무엇을 하나
#   백테스트_로그의 **같은 진입 표본** 위에서 청산 규칙 3종을 나란히 돌린다.
#     ① 고정      — 목표가/손절가 도달 시 청산 (현행 연구 기준값)
#     ② 트레일링  — 목표가 닿으면 손절선을 본전으로, 이후 고가×92% 로 따라 올림 (현행 실전 로직)
#     ③ T+N 청산 — 조건 없이 T+5 / T+10 / T+20 종가에 판다
#   그리고 로드맵이 요구한 산출물을 낸다 — **"일찍 팔았으면 얼마를 놓쳤나"의 실측치.**
#
# 무엇을 하지 않나 (중요)
#   · 구글시트에 **쓰지 않는다.** 읽기 전용이다 → 시트 락 불필요, 9/7 기준선 무영향.
#   · 선정 로직·점수·채널에 어떤 영향도 주지 않는다. 순수 사후 분석이다.
#   · 결론을 내지 않는다. N<30 이면 §3 규칙대로 "방향만 기록"하고 판정을 보류한다.
#
# ⚠️ 일봉 근사의 한계 — 반드시 결과와 함께 읽을 것
#   일봉은 그날 고가·저가가 **어느 순서로** 나왔는지 알려주지 않는다. 목표가와 손절가를
#   같은 날 둘 다 건드린 경우 실제 결과는 순서에 달렸는데 우리는 그걸 모른다.
#   omakase 의 기존 터치 판정과 같은 규약으로 **보수적으로 손절을 먼저** 적용하고,
#   그런 행이 몇 건인지 결과에 따로 보고한다. 그 비율이 높으면 이 비교 자체의 해상도가 낮은 것이다.
#
# 사용법
#   python hyeoks_phase2_exit.py            → 시트 읽고 분석, data/phase2_exit/ 에 저장
#   python hyeoks_phase2_exit.py --self-test → 시트 없이 합성 데이터로 규칙 엔진만 검증
#   python hyeoks_phase2_exit.py --horizon 20 → 비교 보유창(거래일) 지정 (기본 20)
# ==========================================================================
import os
import io
import sys
import csv
import time
import argparse
import datetime
import statistics
import xml.etree.ElementTree as ET

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

KST = datetime.timezone(datetime.timedelta(hours=9))
OUT_DIR = "data/phase2_exit"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

# 백테스트_로그 열 인덱스 (omakase.BT_HEADER 와 동일 — 여기서 재정의하지 말고 이름으로 참조할 것)
C_ENTRY_DATE, C_CHANNEL, C_NAME, C_CODE = 1, 2, 3, 4
C_ENTRY_PRICE, C_CAPTURE_MEMO = 16, 25
C_T5, C_TARGET, C_STOP = 19, 32, 33

TRAILING_PCT = 0.92      # omakase.check_target_alerts_and_trailing_stop 과 같은 값
MIN_N_FOR_VERDICT = 30   # §3-1 판정 문턱. 이보다 적으면 방향만 기록한다.
CALIB_TOLERANCE = 0.05   # T+5 재계산 허용 오차(%p)
CALIB_MIN_MATCH = 0.95   # 이 비율 미만이면 앵커 해석이 틀린 것으로 보고 중단

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
})


# ── 데이터 취득 ────────────────────────────────────────────────────────────

_BAR_CACHE = {}


def get_daily_bars(symbol, count=200):
    """fchart 일봉. omakase.get_daily_bars 와 같은 포맷(date 는 'YYYY-MM-DD')."""
    if symbol in _BAR_CACHE:
        return _BAR_CACHE[symbol]
    bars = []
    try:
        r = SESSION.get(f"https://fchart.stock.naver.com/sise.nhn"
                        f"?symbol={symbol}&timeframe=day&count={count}&requestType=0",
                        verify=False, timeout=8)
        for item in ET.fromstring(r.text).findall(".//item"):
            d = (item.get("data") or "").split("|")
            if len(d) < 5 or not d[0]:
                continue
            rd = d[0]
            try:
                bars.append({'date': f"{rd[:4]}-{rd[4:6]}-{rd[6:8]}",
                             'open': float(d[1]), 'high': float(d[2]),
                             'low': float(d[3]), 'close': float(d[4])})
            except Exception:
                continue
    except Exception as e:
        print(f"⚠️ [일봉 실패 {symbol}] {e}")
    _BAR_CACHE[symbol] = bars
    time.sleep(0.05)
    return bars


def read_backtest_log():
    """백테스트_로그를 읽어 온다. **읽기 전용** — 이 스크립트는 시트에 절대 쓰지 않는다."""
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(SHEET_URL)
    return doc.worksheet("백테스트_로그").get_all_values()


# ── 공통 헬퍼 ──────────────────────────────────────────────────────────────

def _num(v):
    try:
        return float(str(v).replace(',', '').replace('%', '').strip())
    except Exception:
        return 0.0


def is_excluded_row(row):
    """omakase.is_excluded_row 와 동일 규약 — Z열(idx 25)에 '제외' 표식이 있으면 뺀다."""
    return "제외" in (str(row[C_CAPTURE_MEMO]) if len(row) > C_CAPTURE_MEMO else "")


# ── 청산 규칙 3종 ──────────────────────────────────────────────────────────

def simulate_exits(bars, entry_idx, base, target, stop, horizon):
    """진입 다음날부터 horizon 거래일까지 보유하며 세 규칙을 각각 돌린다.

    반환 dict — 각 규칙의 (수익률%, 종료사유, 종료일차).
    ⚠️ 일봉은 고가·저가의 **순서**를 주지 않는다. 같은 날 목표가·손절가를 둘 다 건드리면
       omakase 의 기존 터치 판정과 같은 규약으로 **손절을 먼저** 적용한다(보수적).
       그런 날이 있었는지는 'ambiguous' 로 따로 돌려준다.
    """
    window = bars[entry_idx + 1: entry_idx + 1 + horizon]
    if not window or base <= 0:
        return None

    def pct(p):
        return (p - base) / base * 100.0

    ambiguous = False

    # ① 고정 — 목표가/손절가 도달 시 청산
    fixed = None
    for d, b in enumerate(window, start=1):
        hit_stop = stop > 0 and b['low'] <= stop
        hit_tgt = target > 0 and b['high'] >= target
        if hit_stop and hit_tgt:
            ambiguous = True
        if hit_stop:
            fixed = (pct(stop), "손절", d)
            break
        if hit_tgt:
            fixed = (pct(target), "익절", d)
            break
    if fixed is None:
        fixed = (pct(window[-1]['close']), "만기", len(window))

    # ② 트레일링 — 목표가 닿으면 손절선을 본전으로, 이후 고가×92% 로 따라 올림
    #    (omakase.check_target_alerts_and_trailing_stop 의 일봉 근사)
    trail_line, hit_target, trailing = stop, False, None
    for d, b in enumerate(window, start=1):
        if trail_line > 0 and b['low'] <= trail_line:
            trailing = (pct(trail_line), "트레일링손절" if hit_target else "손절", d)
            break
        if not hit_target and target > 0 and b['high'] >= target:
            hit_target = True
            trail_line = max(trail_line, base)          # 본전 확보
        if hit_target:
            trail_line = max(trail_line, b['high'] * TRAILING_PCT)
    if trailing is None:
        trailing = (pct(window[-1]['close']), "만기", len(window))

    # ③ 무조건 T+N 청산
    tn = {}
    for h in (5, 10, 20):
        if h <= len(window):
            tn[h] = (pct(window[h - 1]['close']), f"T+{h}", h)

    return {"fixed": fixed, "trailing": trailing, "tn": tn,
            "ambiguous": ambiguous, "final": pct(window[-1]['close']),
            "held": len(window)}


# ── 집계 ──────────────────────────────────────────────────────────────────

def describe(vals):
    if not vals:
        return {"n": 0}
    return {"n": len(vals),
            "mean": statistics.mean(vals),
            "median": statistics.median(vals),
            "win": sum(1 for v in vals if v > 0) / len(vals) * 100.0,
            "worst": min(vals),
            "best": max(vals)}


def fmt(d, key):
    return f"{d[key]:+.2f}" if d.get("n") else "—"


# ── 리포트 ────────────────────────────────────────────────────────────────

def build_report(recs, horizon, calib, skipped):
    now = datetime.datetime.now(KST)
    L = ["# 🚪 Phase 2 — 청산 규칙 비교", "",
         f"_생성 {now.strftime('%Y-%m-%d %H:%M:%S')} KST · 보유창 T+{horizon} 거래일_", "",
         "> 전략로드맵 Phase 2 의 산출물이다. **같은 진입 표본** 위에서 청산 규칙만 바꿔 비교한다.",
         "> 새 수집 없이 백테스트_로그와 일봉만으로 계산했다. 시트에는 쓰지 않는다(읽기 전용).", ""]

    # 자기검증 결과를 맨 앞에 — 이게 통과 안 되면 아래 숫자는 볼 필요가 없다
    L += ["## 자기검증 (앵커 해석이 맞는가)", "",
          "로그의 `종목T+5` 를 일봉으로 다시 계산해 원본과 대조한다. "
          "진입가·기준일 해석이 틀리면 여기서 어긋난다.", "",
          f"- 대조 가능 행 **{calib['n']}건** · 일치(±{CALIB_TOLERANCE}%p) **{calib['match']}건** "
          f"= **{calib['rate'] * 100:.1f}%**",
          f"- 판정 — {'✅ 통과' if calib['ok'] else '❌ 실패. 아래 숫자를 신뢰하지 말 것'}", ""]
    if calib['worst']:
        L += ["가장 크게 어긋난 3건:", "",
              "| 종목 | 진입일 | 로그값 | 재계산 | 차이 |", "|---|---|---:|---:|---:|"]
        for w in calib['worst'][:3]:
            L.append(f"| {w[0]} | {w[1]} | {w[2]:+.2f}% | {w[3]:+.2f}% | {w[4]:+.2f}%p |")
        L.append("")

    if not recs:
        L += ["## 결과 없음", "", "시뮬레이션 가능한 행이 없다. "
              "목표가·손절가·진입가가 채워지고 보유창이 지난 행이 필요하다.", ""]
        L += [f"- 건너뛴 행: {skipped}", ""]
        return "\n".join(L) + "\n"

    fixed = [r['sim']['fixed'][0] for r in recs]
    trail = [r['sim']['trailing'][0] for r in recs]
    n_amb = sum(1 for r in recs if r['sim']['ambiguous'])

    L += ["## 규칙별 성적 — 전체", "",
          f"표본 **N={len(recs)}**"
          + (f" · ⚠️ §3-1 판정 문턱(N≥{MIN_N_FOR_VERDICT}) 미달 → **방향만 기록, 판정 보류**"
             if len(recs) < MIN_N_FOR_VERDICT else ""), "",
          "| 규칙 | N | 평균 | 중앙값 | 승률 | 최악 | 최고 |",
          "|---|---:|---:|---:|---:|---:|---:|"]
    rows = [("① 고정 목표/손절", describe(fixed)), ("② 트레일링 92%", describe(trail))]
    for h in (5, 10, 20):
        v = [r['sim']['tn'][h][0] for r in recs if h in r['sim']['tn']]
        if v:
            rows.append((f"③ T+{h} 무조건 청산", describe(v)))
    for label, d in rows:
        L.append(f"| {label} | {d['n']} | {fmt(d, 'mean')}% | {fmt(d, 'median')}% | "
                 f"{d['win']:.0f}% | {d['worst']:+.2f}% | {d['best']:+.2f}% |")
    L += ["",
          f"> ⚠️ 같은 날 목표가·손절가를 **둘 다** 건드린 행 **{n_amb}건** "
          f"({n_amb / len(recs) * 100:.0f}%). 일봉은 순서를 모르므로 보수적으로 손절 처리했다. "
          + ("**이 비율이 높아 비교의 해상도가 낮다.**" if n_amb / len(recs) > 0.2
             else "비율이 낮아 결론에 큰 영향은 없다."), ""]

    # 로드맵이 요구한 바로 그 숫자
    early = [(r, r['sim']['final'] - r['sim']['fixed'][0])
             for r in recs if r['sim']['fixed'][1] == "익절"]
    L += ["## 🎯 \"일찍 팔았으면 얼마를 놓쳤나\"", "",
          "로드맵 Phase 2 가 요구한 산출물이다. **고정 규칙으로 익절된 건**만 모아, "
          f"그대로 T+{horizon} 까지 들고 갔을 때와 비교한다.", ""]
    if early:
        diffs = [d for _, d in early]
        dd = describe(diffs)
        pos = sum(1 for d in diffs if d > 0)
        L += [f"- 익절로 종료된 건 **{len(early)}건**",
              f"- 계속 들고 갔을 때의 차이 — 평균 **{dd['mean']:+.2f}%p** · "
              f"중앙값 **{dd['median']:+.2f}%p** · 최대 **{dd['best']:+.2f}%p** · 최소 **{dd['worst']:+.2f}%p**",
              f"- 더 벌었을 건 **{pos}/{len(early)}건** ({pos / len(early) * 100:.0f}%)", "",
              "| 종목 | 채널 | 익절수익 | 만기까지 | 차이 |", "|---|---|---:|---:|---:|"]
        for r, d in sorted(early, key=lambda x: -x[1])[:8]:
            L.append(f"| {r['name']} | {r['channel']} | {r['sim']['fixed'][0]:+.2f}% | "
                     f"{r['sim']['final']:+.2f}% | **{d:+.2f}%p** |")
        L += ["", "> 양수면 일찍 판 것이 손해였다는 뜻이다. "
                  "다만 **익절된 건만 본 것**이라 그 자체로 편향이 있다 — "
                  "손절된 건은 계속 들고 갔으면 더 나빠졌을 수 있다. 위 전체 표와 함께 읽을 것.", ""]
    else:
        L += ["익절로 종료된 건이 없다. 표본이 더 쌓여야 한다.", ""]

    # 채널별
    chans = sorted({r['channel'] for r in recs})
    if len(chans) > 1:
        L += ["## 채널별", "", "| 채널 | N | ① 고정 | ② 트레일링 | ③ T+20 |",
              "|---|---:|---:|---:|---:|"]
        for c in chans:
            sub = [r for r in recs if r['channel'] == c]
            f_ = describe([r['sim']['fixed'][0] for r in sub])
            t_ = describe([r['sim']['trailing'][0] for r in sub])
            n_ = describe([r['sim']['tn'][20][0] for r in sub if 20 in r['sim']['tn']])
            L.append(f"| {c} | {len(sub)} | {fmt(f_, 'mean')}% | {fmt(t_, 'mean')}% | {fmt(n_, 'mean')}% |")
        L += ["", f"> 채널별 N 이 작다. §3-5 다중비교 보정에 따라 **개별 채널 비교로 결론내지 않는다.**", ""]

    L += ["## 읽는 법", "",
          "- 이 표는 **선정 능력이 아니라 청산 규칙**만 비교한다. 진입 표본은 세 규칙이 완전히 같다.",
          "- 일봉 근사다. 장중 순서·슬리피지·체결 가능성은 반영되지 않는다.",
          f"- N<{MIN_N_FOR_VERDICT} 인 구간은 §3 규칙대로 **판정하지 않는다.** 방향만 본다.",
          f"- 건너뛴 행: {skipped}", ""]
    return "\n".join(L) + "\n"


# ── 메인 ──────────────────────────────────────────────────────────────────

def analyze(rows, horizon):
    recs, skipped = [], {"제외표식": 0, "값없음": 0, "일봉없음": 0, "미성숙": 0}
    calib = {"n": 0, "match": 0, "worst": []}

    for row in rows[1:]:
        if len(row) < 34:
            skipped["값없음"] += 1
            continue
        if is_excluded_row(row):
            skipped["제외표식"] += 1
            continue
        channel = str(row[C_CHANNEL]).strip()
        if channel.startswith("지수벤치"):
            skipped["제외표식"] += 1
            continue

        base = _num(row[C_ENTRY_PRICE])
        target, stop = _num(row[C_TARGET]), _num(row[C_STOP])
        entry_date = str(row[C_ENTRY_DATE]).strip()
        code = str(row[C_CODE]).replace("'", "").strip().zfill(6)
        if base <= 0 or target <= 0 or stop <= 0 or not entry_date or not code.strip('0'):
            skipped["값없음"] += 1
            continue

        bars = get_daily_bars(code)
        if not bars:
            skipped["일봉없음"] += 1
            continue
        entry_idx = next((k for k, b in enumerate(bars) if b['date'] == entry_date), None)
        if entry_idx is None or entry_idx + 1 + horizon > len(bars):
            skipped["미성숙"] += 1
            continue

        # 자기검증 — 로그의 T+5 를 다시 계산해 본다
        logged_t5 = str(row[C_T5]).strip()
        if logged_t5 and entry_idx + 5 < len(bars):
            recomputed = (bars[entry_idx + 5]['close'] - base) / base * 100.0
            diff = recomputed - _num(logged_t5)
            calib["n"] += 1
            if abs(diff) <= CALIB_TOLERANCE:
                calib["match"] += 1
            else:
                calib["worst"].append((str(row[C_NAME]).strip(), entry_date,
                                       _num(logged_t5), recomputed, diff))

        sim = simulate_exits(bars, entry_idx, base, target, stop, horizon)
        if sim:
            recs.append({"name": str(row[C_NAME]).strip(), "channel": channel,
                         "code": code, "entry": entry_date, "sim": sim})

    calib["worst"].sort(key=lambda w: -abs(w[4]))
    calib["rate"] = calib["match"] / calib["n"] if calib["n"] else 0.0
    calib["ok"] = calib["n"] == 0 or calib["rate"] >= CALIB_MIN_MATCH
    return recs, calib, skipped


def write_outputs(recs, horizon, calib, skipped):
    os.makedirs(OUT_DIR, exist_ok=True)
    today = datetime.datetime.now(KST).strftime('%Y-%m-%d')
    md = f"{OUT_DIR}/{today}_exit_comparison.md"
    io.open(md, "w", encoding="utf-8", newline="\n").write(
        build_report(recs, horizon, calib, skipped))
    print(f"📝 리포트 {md}")

    if recs:
        cp = f"{OUT_DIR}/{today}_exit_rows.csv"
        with io.open(cp, "w", encoding="utf-8", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(["종목명", "종목코드", "채널", "진입일", "고정수익%", "고정사유", "고정일차",
                        "트레일링수익%", "트레일링사유", "트레일링일차",
                        "T+5%", "T+10%", "T+20%", "만기수익%", "동일봉양방터치"])
            for r in recs:
                s = r['sim']
                w.writerow([r['name'], r['code'], r['channel'], r['entry'],
                            f"{s['fixed'][0]:.2f}", s['fixed'][1], s['fixed'][2],
                            f"{s['trailing'][0]:.2f}", s['trailing'][1], s['trailing'][2]]
                           + [f"{s['tn'][h][0]:.2f}" if h in s['tn'] else "" for h in (5, 10, 20)]
                           + [f"{s['final']:.2f}", "Y" if s['ambiguous'] else ""])
        print(f"📊 행별 원본 {cp}")


def self_test():
    """시트 없이 규칙 엔진만 검증한다. 답을 아는 합성 일봉을 만들어 넣는다."""
    def bar(o, h, l, c):
        return {'date': 'x', 'open': o, 'high': h, 'low': l, 'close': c}

    base, target, stop = 100.0, 110.0, 92.0
    ok = True

    def check(label, bars, want_fixed, want_trail):
        nonlocal ok
        # bars[0] 은 신호일, 진입은 bars[1] 부터
        s = simulate_exits([bar(100, 100, 100, 100)] + bars, 0, base, target, stop, len(bars))
        got = (round(s['fixed'][0], 2), s['fixed'][1]), (round(s['trailing'][0], 2), s['trailing'][1])
        hit = got == (want_fixed, want_trail)
        ok = ok and hit
        print(f"  {'✅' if hit else '❌'} {label}\n      고정={got[0]} 트레일링={got[1]}"
              + ("" if hit else f"\n      기대: 고정={want_fixed} 트레일링={want_trail}"))

    print("🧪 청산 규칙 엔진 자기검증")
    # 1) 목표가 도달 후 계속 상승 → 고정은 +10%, 트레일링은 고점 150×0.92=138 까지 따라감
    check("목표가 도달 후 급등(트레일링이 더 벌어야 함)",
          [bar(100, 112, 99, 111), bar(111, 150, 110, 149), bar(149, 150, 130, 132)],
          (10.0, "익절"), (38.0, "트레일링손절"))
    # 2) 손절만 도달
    check("손절 도달", [bar(100, 101, 91, 93)], (-8.0, "손절"), (-8.0, "손절"))
    # 3) 아무것도 안 닿음 → 만기 종가
    check("미도달 만기청산", [bar(100, 105, 96, 104), bar(104, 106, 98, 105)],
          (5.0, "만기"), (5.0, "만기"))
    # 4) 같은 날 양방 터치 → 보수적으로 손절
    s = simulate_exits([bar(100, 100, 100, 100), bar(100, 115, 90, 100)], 0, base, target, stop, 1)
    amb = s['ambiguous'] and s['fixed'][1] == "손절"
    ok = ok and amb
    print(f"  {'✅' if amb else '❌'} 같은 날 양방 터치 → 손절 우선 + ambiguous 표시")

    print("\n✅ 전부 통과" if ok else "\n❌ 실패한 항목이 있다")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="시트 없이 규칙 엔진만 검증")
    ap.add_argument("--horizon", type=int, default=20, help="비교 보유창(거래일). 기본 20")
    a = ap.parse_args()

    if a.self_test:
        return self_test()

    now = datetime.datetime.now(KST)
    print(f"🚪 [Phase 2 청산 규칙 비교] {now.strftime('%Y-%m-%d %H:%M:%S')} KST · 보유창 T+{a.horizon}")
    try:
        rows = read_backtest_log()
    except Exception as e:
        print(f"❌ 백테스트_로그 읽기 실패: {e}")
        return 1
    print(f"📖 {len(rows) - 1}행 로드 (읽기 전용 — 시트에 쓰지 않는다)")

    recs, calib, skipped = analyze(rows, a.horizon)
    print(f"🧮 시뮬레이션 {len(recs)}행 · 건너뜀 {skipped}")
    print(f"🔍 자기검증 T+5 일치율 {calib['rate'] * 100:.1f}% ({calib['match']}/{calib['n']})")
    if not calib['ok']:
        print("❌ 자기검증 실패 — 진입가·기준일 해석이 로그와 어긋난다. "
              "리포트는 남기되 숫자를 신뢰하지 말 것.")

    write_outputs(recs, a.horizon, calib, skipped)
    if recs:
        f = statistics.mean([r['sim']['fixed'][0] for r in recs])
        t = statistics.mean([r['sim']['trailing'][0] for r in recs])
        print(f"   ① 고정 {f:+.2f}%  ·  ② 트레일링 {t:+.2f}%  ·  차이 {t - f:+.2f}%p")
        if len(recs) < MIN_N_FOR_VERDICT:
            print(f"   ⚠️ N={len(recs)} < {MIN_N_FOR_VERDICT} — §3 규칙대로 판정하지 않는다(방향만 기록)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
