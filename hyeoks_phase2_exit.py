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

# ── 🔻 사후 배정 (--assign) — 반드시 읽고 쓸 것 ────────────────────────────
# 백테스트_로그 310행 중 **121행**이 목표가·손절가가 비어 있다(차트TOP2 54 · 랜덤2 47 ·
# 수급TOP2 20 · 리포트 채널은 0). omakase.py 의 적재 코드 주석이 원인을 말한다 —
# "관망류는 빈 값". 즉 그 값들은 **애초에 존재하지 않았다.** 지금 채우는 것은 복원이
# 아니라 **없던 값을 만드는 것**이다.
#
# ⚠️ 그리고 우리는 이미 원본 17행의 Phase 2 결과를 봤다. 그 뒤에 배정 규칙을 고르면
#    무의식적으로라도 결과에 유리한 쪽을 고를 수 있다(§0 이 금지하는 바로 그 행동).
#    그래서 세 가지 방어를 건다.
#      ① 백테스트_로그에 **쓰지 않는다.** 별도 파일에만 남긴다.
#      ② **규칙을 두 개** 돌려 결론이 규칙 선택에 흔들리는지 본다.
#         순위가 뒤집히면 그건 '배정 방식에 의존하는 결론'이라는 뜻이고, 그 자체가 답이다.
#      ③ 원본 표본과 **합치지 않는다.** 늘 따로 보고한다.
#    §3-5 의 **탐색적 항목**이므로, 독립 표본에서 재현되기 전까지 채택하지 않는다.
#
# 규칙 A (band) — 로드맵 §3-4 가 사전등록한 단기 손익비 밴드의 중앙값.
#   진입가만 쓰므로 자의성이 가장 적다. 차트를 보지 않는다.
# 규칙 B (chart) — omakase.analyze_single_stock 의 일반 분기(2191~2192행)를 재현.
#   target = max(60일 고가, 진입가×1.05) · stop = min(20일선, 진입가×0.95)
#   ⚠️ 원본은 타점유형별로 ATR·기준봉 등 다른 분기를 타지만, 그 플래그를 사후에
#      복원할 수 없다. 따라서 이것은 원본의 재현이 아니라 **근사**다.
ASSIGN_BAND = (0.095, 0.07)   # §3-4 단기: 목표 +7~12% / 손절 -6~8% → 각 중앙값


def assign_levels(mode, bars, entry_idx, base):
    """비어 있는 목표가·손절가를 사후 배정한다. **진입일 이전 정보만** 쓴다(미래참조 없음)."""
    if mode == "band":
        return base * (1 + ASSIGN_BAND[0]), base * (1 - ASSIGN_BAND[1])
    if mode == "chart":
        past = bars[max(0, entry_idx - 59): entry_idx + 1]     # 진입일까지만
        if len(past) < 20:
            return 0.0, 0.0
        high60 = max(b['high'] for b in past)
        ma20 = sum(b['close'] for b in past[-20:]) / 20.0
        target = high60 if high60 > base else base * 1.05
        stop = min(ma20, base * 0.95)
        return target, stop
    return 0.0, 0.0
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

def analyze(rows, horizon, diag=None, assign="none"):
    """diag 가 주어지면 스킵 사유를 원인별·채널별로 쌓는다(왜 표본이 줄었는지 추적용)."""
    recs, skipped = [], {"제외표식": 0, "값없음": 0, "일봉없음": 0, "미성숙": 0}
    calib = {"n": 0, "match": 0, "worst": []}

    def note(reason, ch, row=None):
        # ⚠️ '값없음' 으로 뭉뚱그리면 목표가가 없는 건지 진입가가 없는 건지 구분이 안 된다.
        #    표본이 왜 줄었는지는 Phase 2 해석에 직결되므로 원인을 쪼개서 남긴다.
        if diag is None:
            return
        diag.setdefault(reason, {}).setdefault(ch or "(채널없음)", []).append(
            (str(row[C_NAME]).strip() if row else "", str(row[C_ENTRY_DATE]).strip() if row else ""))

    for row in rows[1:]:
        ch = str(row[C_CHANNEL]).strip() if len(row) > C_CHANNEL else ""
        if len(row) < 34:
            skipped["값없음"] += 1
            note("행이짧음(34열미만)", ch, row if len(row) > C_ENTRY_DATE else None)
            continue
        if is_excluded_row(row):
            skipped["제외표식"] += 1
            note("Z열 제외표식", ch, row)
            continue
        channel = ch
        if channel.startswith("지수벤치"):
            skipped["제외표식"] += 1
            note("지수벤치(대조군)", ch, row)
            continue

        base = _num(row[C_ENTRY_PRICE])
        target, stop = _num(row[C_TARGET]), _num(row[C_STOP])
        entry_date = str(row[C_ENTRY_DATE]).strip()
        code = str(row[C_CODE]).replace("'", "").strip().zfill(6)
        # 사후 배정 모드 — 목표가·손절가만 비고 나머지가 온전한 행을 되살린다.
        assigned = False
        if assign != "none" and base > 0 and entry_date and code.strip('0') \
                and (target <= 0 or stop <= 0):
            _b = get_daily_bars(code)
            _i = next((k for k, b in enumerate(_b) if b['date'] == entry_date), None)
            if _b and _i is not None:
                target, stop = assign_levels(assign, _b, _i, base)
                assigned = target > 0 and stop > 0

        if base <= 0 or target <= 0 or stop <= 0 or not entry_date or not code.strip('0'):
            skipped["값없음"] += 1
            miss = []
            if target <= 0: miss.append("목표가")
            if stop <= 0: miss.append("손절가")
            if base <= 0: miss.append("진입가")
            if not entry_date: miss.append("진입일")
            if not code.strip('0'): miss.append("종목코드")
            note("없음: " + "·".join(miss), ch, row)
            continue

        bars = get_daily_bars(code)
        if not bars:
            skipped["일봉없음"] += 1
            note("일봉 조회 실패", ch, row)
            continue
        entry_idx = next((k for k, b in enumerate(bars) if b['date'] == entry_date), None)
        if entry_idx is None:
            skipped["미성숙"] += 1
            note("진입일이 일봉에 없음", ch, row)
            continue
        if entry_idx + 1 + horizon > len(bars):
            skipped["미성숙"] += 1
            note(f"보유창 미성숙(T+{horizon} 미도달)", ch, row)
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
                         "code": code, "entry": entry_date, "sim": sim,
                         "assigned": assigned})

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
    ap.add_argument("--diagnose", action="store_true",
                    help="분석 대신 '왜 표본에서 빠졌는가'를 원인별·채널별로 출력")
    ap.add_argument("--assign", choices=["none", "band", "chart", "both"], default="none",
                    help="비어 있는 목표가·손절가를 사후 배정한다(원본과 분리 보고). "
                         "both 는 두 규칙을 다 돌려 결론이 규칙에 흔들리는지 본다")
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

    # ── 사후 배정 비교 모드 — 두 규칙의 결론이 같은지 본다 ──────────────────
    if a.assign == "both":
        print("\n🔻 [사후 배정 비교] 목표가·손절가가 비어 있던 행을 두 규칙으로 각각 채워 돌린다.")
        print("   결론이 규칙에 따라 뒤집히면 그건 '배정 방식에 의존하는 결론'이라는 뜻이다.\n")
        table = {}
        for mode in ("none", "band", "chart"):
            r, _c, _s = analyze(rows, a.horizon, None, mode)
            if not r:
                continue
            asg = sum(1 for x in r if x.get("assigned"))
            table[mode] = {
                "n": len(r), "배정": asg,
                "① 고정": statistics.mean([x['sim']['fixed'][0] for x in r]),
                "② 트레일링": statistics.mean([x['sim']['trailing'][0] for x in r]),
                "③ T+20": statistics.mean([x['sim']['tn'][20][0] for x in r if 20 in x['sim']['tn']]),
            }
            # ⚠️ 배정분만 따로 — 전체를 비교하면 '배정 규칙의 차이'와 '모집단의 차이'가 섞인다.
            #    원본 17행은 리포트 채널이고 배정분 98행은 차트/수급/랜덤 채널이라 애초에 다른 집단이다.
            only = [x for x in r if x.get("assigned")]
            if only:
                table[mode + "_배정분"] = {
                    "n": len(only), "배정": len(only),
                    "① 고정": statistics.mean([x['sim']['fixed'][0] for x in only]),
                    "② 트레일링": statistics.mean([x['sim']['trailing'][0] for x in only]),
                    "③ T+20": statistics.mean([x['sim']['tn'][20][0] for x in only if 20 in x['sim']['tn']]),
                }
        label = {"none": "원본만(리포트 채널)", "band": "원본+밴드 배정", "chart": "원본+차트 배정",
                 "band_배정분": "  └ 배정분만(밴드)", "chart_배정분": "  └ 배정분만(차트)"}
        order_keys = ("none", "band", "band_배정분", "chart", "chart_배정분")
        print(f"{'':<26}{'N':>5}{'배정':>5}{'① 고정':>10}{'② 트레일링':>12}{'③ T+20':>10}")
        print("-" * 70)
        for m in order_keys:
            if m not in table:
                continue
            t = table[m]
            print(f"{label[m]:<26}{t['n']:>5}{t['배정']:>5}"
                  f"{t['① 고정']:>+10.2f}{t['② 트레일링']:>+12.2f}{t['③ T+20']:>+10.2f}")

        rank = lambda m: sorted(("① 고정", "② 트레일링", "③ T+20"), key=lambda k: -table[m][k])
        print()
        for m in order_keys:
            if m in table:
                print(f"   {label[m]:<26} 순위: {' > '.join(rank(m))}")

        # 🔑 핵심 판정을 두 개로 나눈다 — 배정 '규칙'의 영향과 '모집단'의 영향은 다른 문제다.
        print()
        rule_same = ("band_배정분" in table and "chart_배정분" in table
                     and rank("band_배정분") == rank("chart_배정분"))
        pop_same = ("band_배정분" in table and rank("none") == rank("band_배정분"))
        if rule_same:
            print("   ✅ [배정 규칙] 밴드와 차트가 **같은 순위**다. 결론은 배정 방식에 의존하지 않는다.")
        else:
            print("   ⚠️ [배정 규칙] 밴드와 차트의 순위가 다르다. 배정 방식에 의존하는 결론이다.")
        if pop_same:
            print("   ✅ [모집단] 원본(리포트)과 배정분(차트/수급/랜덤)의 순위도 같다.")
        else:
            print("   🔻 [모집단] 원본(리포트)과 배정분(차트/수급/랜덤)의 순위가 **다르다.**")
            print("      → 이건 배정 탓이 아니라 **채널마다 최적 청산이 다르다**는 신호일 수 있다.")
            print("        원본과 배정분을 합쳐서 하나의 결론을 내면 안 된다.")
        print("\n   ⚠️ 배정분은 원본에 없던 값을 만든 것이다. §3-5 탐색적 항목이므로")
        print("      독립 표본에서 재현되기 전까지 채택하지 않는다. 시트에는 쓰지 않았다.")
        return 0

    diag = {} if a.diagnose else None
    recs, calib, skipped = analyze(rows, a.horizon, diag, a.assign)

    if a.diagnose:
        total = sum(len(v) for r in diag.values() for v in r.values())
        print(f"\n🔎 [표본 진단] 전체 {len(rows) - 1}행 · 시뮬 대상 {len(recs)}행 · 제외 {total}행\n")
        for reason in sorted(diag, key=lambda r: -sum(len(v) for v in diag[r].values())):
            per = diag[reason]
            n = sum(len(v) for v in per.values())
            print(f"── {reason} — {n}행")
            for chan in sorted(per, key=lambda c: -len(per[c])):
                items = per[chan]
                sample = ", ".join(f"{nm}({dt})" for nm, dt in items[:3] if nm)
                print(f"     {chan:<22} {len(items):>4}행   {sample}")
            print()
        return 0

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
