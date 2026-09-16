# -*- coding: utf-8 -*-
"""설계별 σ 실측 — 검증에 며칠이 필요한지는 σ 가 정한다.

왜 만드나 (2026-09-16)
----------------------
`docs/검증가속계획_2026-09-16.md` §0 이 설계를 바꾸면 60거래일 → 44거래일이라고
적었는데, **σ 가 ① 하나만 실측이고 나머지는 내 가정이었다.** 가정이 틀리면 표가
통째로 틀린다. 그리고 `hyeoks_verdict.paired_by_date` 의 docstring 이 이미
같은 경고를 하고 있다 —

    "분산이 줄어드는 이득과 자유도가 주는 손해 중 어느 쪽이 큰지는
     **실원장을 돌려봐야 안다.** 더 좋다고 가정하지 않는다."

이 도구가 그걸 돌린다.

⚠️ 설계 쇼핑 금지 — 이 도구가 지키는 규율
------------------------------------------
σ 를 재는 것과 δ(효과)를 재는 것은 **완전히 다른 일이다.**
여러 설계의 t·p 를 같이 보고 그중 가장 유의한 설계를 고르면 그건 p-해킹이다.

그래서 이 도구는:
  · **σ 만 보고한다.** 설계별 t·p·유의성을 계산하지 않는다.
  · 필요 표본은 δ 를 **가로축으로 둔 곡선**으로 낸다. 내가 δ 를 고르지 않는다.
  · 판정은 `hyeoks_verdict` 가 한다. 이 도구는 판정하지 않는다.

σ 는 잡음의 크기이지 성적이 아니다. 성적을 안 보고 고를 수 있는 것이 σ 뿐이라
설계 선택의 근거로 쓸 수 있다.

읽기 전용: 시트를 읽기만 한다.
"""
import argparse
import datetime
import math
import os
import statistics as st
import sys

from hyeoks_verdict import (C_ENTRY_DATE, C_CHANNEL, STOCK_COL, INDEX_COL,
                            HORIZON, CONTROL, is_excluded, t_one_sided_p)

# 원장 헤더(omakase.py:1029) 기준 — "V1"=9, "V2"=10
C_V1, C_V2 = 9, 10

# 채널이 무엇으로 순위를 매기는가 (omakase.py:3132·3163)
RANK_KEY = {"차트TOP2": "v1", "수급TOP2": "v2"}

KST = datetime.timezone(datetime.timedelta(hours=9))
SIGMA_VERSION = "sigma-v2"

# 🔴 2026-09-16 정정 — v1 은 전부 α=0.05 로 계산해 놓고 "확증에 필요한 기간"처럼 읽혔다.
#    Track C(차트TOP2·수급TOP2)의 실제 문턱은 결정일 family α=0.0125 에
#    Holm m=2 를 적용한 **첫 관문 0.00625** 다. 두 값을 같이 낸다.
#    ⚠️ 그래도 이건 **IID t 검정 참고치**다. 실제 확증은 H 길이 이동블록 부트스트랩이고
#       T+5·T+10 은 인접일 보유기간이 겹쳐 독립이 아니다. 아래 수치를
#       "현재 시스템의 검증 소요기간"으로 쓰면 안 된다.
ALPHA_REF = 0.05        # 참고용(느슨한 쪽)
ALPHA_TRACK_C = 0.00625  # §3-5-7 Track C 첫 관문
TRIM_K = 3          # 절사평균에서 위아래로 덜어낼 개수


# ── 원장 → 픽 ────────────────────────────────────────────────────────
def _num(v):
    try:
        f = float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def picks(rows, horizon_of=None):
    """원장에서 (채널, 날짜, 원수익률, 알파) 를 뽑는다. 채널 고유 호라이즌 기준.

    알파 = 종목T+H − 지수T+H. 둘 다 있어야 한다 — 한쪽만 있으면 버린다.
    """
    horizon_of = horizon_of or (lambda ch: HORIZON.get(ch))
    out = []
    for row in rows[1:]:
        if not any(str(c).strip() for c in row) or is_excluded(row):
            continue
        if len(row) <= C_CHANNEL:
            continue
        ch = str(row[C_CHANNEL]).strip()
        day = str(row[C_ENTRY_DATE]).strip()[:10]
        if not ch or not day or ch.startswith("지수벤치"):
            continue
        h = horizon_of(ch)
        si, ii = STOCK_COL.get(h), INDEX_COL.get(h)
        if si is None or ii is None or len(row) <= max(si, ii):
            continue
        s, i = _num(row[si]), _num(row[ii])
        if s is None or i is None:
            continue
        out.append({"channel": ch, "date": day, "h": h, "raw": s, "alpha": s - i,
                    "v1": _num(row[C_V1]) if len(row) > C_V1 else None,
                    "v2": _num(row[C_V2]) if len(row) > C_V2 else None})
    return out


def by_date(picks_, key="alpha"):
    """{채널: {날짜: [값…]}}"""
    out = {}
    for p in picks_:
        out.setdefault(p["channel"], {}).setdefault(p["date"], []).append(p[key])
    return out


# ── 설계별 관측열 ────────────────────────────────────────────────────
def series_pick(picks_, ch, key):
    """① / ② — 픽 하나가 관측 하나."""
    return [p[key] for p in picks_ if p["channel"] == ch]


def series_daily(picks_, ch, key="alpha"):
    """③ — 그날 픽들의 평균이 관측 하나. 계좌가 실제로 겪는 단위다."""
    d = by_date(picks_, key).get(ch, {})
    return [st.fmean(v) for _, v in sorted(d.items())]


def series_paired(picks_, ch, ctrl=CONTROL, key="alpha"):
    """④ — 같은 날 대조군과의 차이. `verdict.paired_by_date` 와 같은 정의.

    두 채널 모두 값이 있는 날만 남긴다.
    """
    d = by_date(picks_, key)
    a, b = d.get(ch, {}), d.get(ctrl, {})
    return [st.fmean(a[k]) - st.fmean(b[k]) for k in sorted(set(a) & set(b))]


DESIGNS = (
    ("① 픽·원수익률", lambda p, ch: series_pick(p, ch, "raw"), "픽"),
    ("② 픽·알파", lambda p, ch: series_pick(p, ch, "alpha"), "픽"),
    ("③ 날짜·알파평균", lambda p, ch: series_daily(p, ch), "거래일"),
    ("④ 날짜짝짓기(vs 랜덤2)", lambda p, ch: series_paired(p, ch), "거래일"),
)


# ── σ 와 필요 표본 ───────────────────────────────────────────────────
def sigma_of(xs):
    """표본표준편차. 2개 미만이면 None — 만들어내지 않는다."""
    return st.stdev(xs) if len(xs) >= 2 else None


def t_crit(df, alpha=0.05):
    """단측 t 임계값. scipy 없이 `t_one_sided_p` 를 이분법으로 뒤집는다."""
    if df is None or df < 1:
        return None
    lo, hi = 0.0, 50.0
    for _ in range(200):
        mid = (lo + hi) / 2
        if t_one_sided_p(mid, df) > alpha:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def need_n(sigma, delta, alpha=0.05, power=0.80, cap=100000):
    """검정력 `power` 로 δ 를 잡는 데 필요한 관측 수.

    ⚠️ 정규근사(z)가 아니라 **t 로 푼다.** 날짜짝짓기는 자유도를 잃으므로
       z 로 계산하면 그 손해가 통째로 빠진다 — 그게 이 측정의 요점이다.
       n 이 df 를 정하고 df 가 임계값을 정하므로 수렴할 때까지 반복한다.
    """
    if not sigma or sigma <= 0 or not delta or delta <= 0:
        return None
    z_beta = 0.8416                      # Φ⁻¹(0.80)
    # 🔴 2026-09-16 — n=2 에서 출발하면 t_crit(1) 이 매우 커서 첫 걸음이 수십만으로
    #    튄다. 그 중간값이 cap 에 걸려 **답이 있는데 None 을 냈다**(σ=10.01·δ=1.5·
    #    α=0.00625 는 500 으로 수렴하는데 115113 을 지나다 잘렸다).
    #    정규근사로 씨앗을 잡고 시작한다. cap 은 **수렴한 값에만** 적용한다.
    n = max(2, math.ceil(((1.6449 + z_beta) * sigma / delta) ** 2))
    for _ in range(200):
        tc = t_crit(n - 1, alpha)
        if tc is None:
            return None
        nxt = max(2, math.ceil(((tc + z_beta) * sigma / delta) ** 2))
        if nxt == n:
            return n if n <= cap else None
        n = nxt
    return n if n <= cap else None


def shape(xs, trim=TRIM_K):
    """분포 모양 — 평균만 보면 복권형 편향을 못 본다(§1 실측).

    절사평균은 위아래를 **같이** 덜어낸다. 위만 덜면 아래로 치우친 값이 나온다.
    """
    n = len(xs)
    if not n:
        return {}
    s = sorted(xs)
    out = {"n": n, "mean": st.fmean(s), "median": st.median(s),
           "win": sum(1 for x in s if x > 0) / n}
    if n > 2 * trim:
        out["trimmed"] = st.fmean(s[trim:-trim])
    return out



def rank_gap(picks_, ch, key=None):
    """같은 날 **1순위와 2순위**의 알파 차이. 순위가 알파를 담고 있는가.

    왜 이 설계인가
    --------------
    "픽을 3·5개로 늘리면 δ 가 얼마나 떨어지나"를 과거 데이터로 재려면
    **뽑히지 않은 후보의 수익률**이 필요한데 그건 어디에도 없다
    (`candidate_pool` 은 메모리에서 계산되고 TOP2 만 원장에 남는다).

    대신 **뽑힌 둘 사이의 순위 효과**는 잴 수 있다. 같은 날 1순위가 2순위보다
    꾸준히 좋다면 순위에 정보가 있는 것이고, 픽을 늘리면 δ 가 떨어진다.
    차이가 0 근처면 순위가 정보를 안 담는다는 뜻이고, **픽을 늘려도 δ 손실이 작다.**

    ⚠️ 한계 — 이건 **상위 2개 안에서의** 기울기다. 3~5위로의 외삽이 아니다.
       점수 분포의 잘린 꼬리만 보는 것이므로 외삽하면 틀릴 수 있다.

    돌려주는 값: (차이 목록, 쓴 점수열) — 점수가 없으면 ([], None)
    """
    k = key or RANK_KEY.get(ch)
    if not k:
        return [], None
    byd = {}
    for p in picks_:
        if p["channel"] == ch and p.get(k) is not None:
            byd.setdefault(p["date"], []).append(p)
    gaps = []
    for _, group in sorted(byd.items()):
        if len(group) < 2:
            continue
        g = sorted(group, key=lambda x: x[k], reverse=True)
        if g[0][k] == g[1][k]:
            continue                      # 동점이면 순위가 없다 — 세지 않는다
        gaps.append(g[0]["alpha"] - g[1]["alpha"])
    return gaps, k


def score_bands(picks_, ch, key=None, edges=(40, 60, 80)):
    """점수 구간별 알파. omakase.py:3137 이 기록한 '역U자'가 지금도 보이는가."""
    k = key or RANK_KEY.get(ch)
    if not k:
        return []
    lo = [-math.inf] + list(edges)
    hi = list(edges) + [math.inf]
    out = []
    for a, b in zip(lo, hi):
        vals = [p["alpha"] for p in picks_
                if p["channel"] == ch and p.get(k) is not None and a <= p[k] < b]
        label = (f"<{b:g}" if a == -math.inf else
                 f"{a:g}+" if b == math.inf else f"{a:g}~{b:g}")
        out.append((label, shape(vals)))
    return out


# ── 리포트 ───────────────────────────────────────────────────────────
DELTA_GRID = (0.5, 1.0, 1.5, 2.0, 3.0)     # %p — **내가 고르지 않는다.** 곡선으로 낸다


def report(picks_, channels=None, as_of=None):
    chans = channels or sorted({p["channel"] for p in picks_} - {CONTROL})
    # 🔴 as_of 는 **문자열**로 들어온다(main 이 그렇게 준다).
    #    f"{as_of:%Y-%m-%d}" 로 쓰면 str 에 날짜 포맷을 걸어 ValueError 가 난다.
    #    자기검증이 as_of=None 으로만 돌려서 실행 때 처음 터졌다.
    stamp = as_of or datetime.datetime.now(KST).strftime("%Y-%m-%d")
    L = [f"# 설계별 σ 실측 — {stamp}", "",
         f"`{SIGMA_VERSION}` · 원장 픽 {len(picks_)}건 · 대조군 `{CONTROL}`", "",
         "> **이 문서는 판정이 아니다.** σ(잡음)만 잰다. 설계별 t·p·유의성은 계산하지 않는다 —",
         "> 여러 설계의 유의성을 보고 고르면 그게 p-해킹이다. 판정은 `hyeoks_verdict` 가 한다.", "",
         "> 필요 표본은 δ 를 가로축에 둔 **곡선**이다. δ 를 내가 고르지 않는다.", ""]

    for ch in chans:
        L += [f"## {ch}", "",
              f"필요 표본은 **α={ALPHA_REF} / α={ALPHA_TRACK_C}** 두 값을 나란히 낸다. "
              "뒤가 Track C 의 실제 첫 관문(Holm m=2)이다.", "",
              "| 설계 | 관측 수 | 단위 | σ(%p) | "
              + " | ".join(f"δ={d}" for d in DELTA_GRID) + " |",
              "|---|---:|---|---:|" + "---:|" * len(DELTA_GRID)]
        best = None
        for name, fn, unit in DESIGNS:
            xs = fn(picks_, ch)
            s = sigma_of(xs)
            cells = []
            for d in DELTA_GRID:
                n1 = need_n(s, d, alpha=ALPHA_REF)
                n2 = need_n(s, d, alpha=ALPHA_TRACK_C)
                cells.append(f"{n1 or '—'} / {n2 or '—'}")
            L.append(f"| {name} | {len(xs)} | {unit} | "
                     + (f"{s:.2f}" if s else "—") + " | " + " | ".join(cells) + " |")
            # '거래일' 단위끼리만 비교한다 — 픽과 거래일은 같은 자가 아니다
            if s and unit == "거래일":
                n2 = need_n(s, 1.5)
                if n2 and (best is None or n2 < best[1]):
                    best = (name, n2)
        L.append("")
        if best:
            L.append(f"> σ 만 보면 δ=1.5%p 기준 **{best[0]}** 가 {best[1]}거래일로 가장 적다.")
            L += ["> ⚠️ **σ 가 작다고 그 설계를 고르면 안 된다.** ③과 ④는 같은 질문이 아니다 —",
                  "> ③은 `전략 − 지수`(시장 대비 알파), ④는 `전략 알파 − 같은 날 대조군 알파`",
                  "> (랭킹 자체의 부가가치)를 묻는다. **먼저 무엇을 승인할지 정하고 σ 는 그다음이다.**",
                  "> 같은 데이터에서 분산이 가장 작은 변환을 고르는 것도 설계 선택이므로,",
                  "> 고른 뒤에는 **새 미사용 표본에서 검증해야 한다.**"]
        L.append("")

        # 🔴 1.5단계 — 순위가 알파를 담고 있는가 (픽 확대의 δ 손실)
        gaps, k = rank_gap(picks_, ch)
        if k:
            L += ["", f"**순위 효과** (같은 날 1순위 − 2순위 알파, 점수열 `{k}`):", ""]
            if len(gaps) >= 2:
                m, sd = st.fmean(gaps), st.stdev(gaps)
                se = sd / math.sqrt(len(gaps))
                srt = sorted(gaps)
                worst = srt[:max(1, len(srt) // 10)]
                sh_g = shape(gaps, trim=1)
                L += [f"- 짝 {len(gaps)}일 · 평균 차 **{m:+.2f}%p** · σ {sd:.2f} · 표준오차 {se:.2f}",
                      f"- **중앙값 {sh_g['median']:+.2f}%p**"
                      + (f" · 절사평균(±1) {sh_g['trimmed']:+.2f}%p"
                         if 'trimmed' in sh_g else ""),
                      f"- 1순위가 이긴 날 {sum(1 for g in gaps if g > 0)}/{len(gaps)}"
                      f" · 최악 10% 평균 {st.fmean(worst):+.2f}%p", ""]
                if len(gaps) >= 5 and (m < 0) != (sh_g['median'] < 0):
                    L += ["> ⚠️ **평균과 중앙값의 부호가 다르다.** 몇 번의 큰 손실이 평균을 끌고 있다 —",
                          "> '순위가 역전됐다'가 아니라 **'상위 구간에 큰 하방 꼬리가 있다'** 로 읽어야 한다.", ""]
                L += ["> 차이가 0 근처면 순위가 정보를 안 담는다 — **픽을 늘려도 δ 손실이 작다.**",
                      "> ⚠️ 이건 **상위 2개 안에서의** 기울기다. 3~5위로의 외삽이 아니다.", ""]
            else:
                L += ["- 짝지을 날이 부족하다(2일 미만). 측정 불가.", ""]
            bands = score_bands(picks_, ch)
            if any(b[1] for b in bands):
                L += [f"점수 구간별 알파 (`{k}`):", "",
                      "| 구간 | N | 평균 | 중앙값 | 승률 |", "|---|---:|---:|---:|---:|"]
                for lab, sh2 in bands:
                    if sh2:
                        L.append(f"| {lab} | {sh2['n']} | {sh2['mean']:+.2f}%p | "
                                 f"{sh2['median']:+.2f}%p | {100*sh2['win']:.0f}% |")
                L += ["", "> 2026-08-07 기록(N=189)은 V2 가 **역U자**라고 했다 — "
                      "상위 구간이 가장 나빴다(omakase.py:3137). 지금도 그런지 본다.", ""]

        # 분포 모양 — 평균만 보면 복권형 편향을 못 본다
        L += ["분포 모양 (픽·알파):", "",
              "| 항목 | 값 |", "|---|---:|"]
        sh = shape(series_pick(picks_, ch, "alpha"))
        for k, lab in (("n", "N"), ("mean", "평균"), ("median", "중앙값"),
                       ("trimmed", f"절사평균(±{TRIM_K})"), ("win", "승률")):
            if k in sh:
                v = sh[k]
                L.append(f"| {lab} | " + (f"{v}" if k == "n" else
                         f"{100*v:.0f}%" if k == "win" else f"{v:+.2f}%p") + " |")
        L.append("")

    # 대조군을 따로 보여준다 — 9/08 부분집합에서 랜덤2 가 중앙값·승률 1위였다
    L += [f"## 대조군 {CONTROL}", "",
          "9/08 부분집합(48행)에서 **대조군이 중앙값·승률 1위**였다. 전체 원장에서도 "
          "그런지 본다. 그렇다면 선정 로직을 의심할 근거다.", "",
          "| 항목 | 값 |", "|---|---:|"]
    sh = shape(series_pick(picks_, CONTROL, "alpha"))
    for k, lab in (("n", "N"), ("mean", "평균"), ("median", "중앙값"),
                   ("trimmed", f"절사평균(±{TRIM_K})"), ("win", "승률")):
        if k in sh:
            v = sh[k]
            L.append(f"| {lab} | " + (f"{v}" if k == "n" else
                     f"{100*v:.0f}%" if k == "win" else f"{v:+.2f}%p") + " |")
    L += ["",
          "## 읽는 법", "",
          "- σ 가 작을수록 같은 확신에 필요한 관측이 적다. **그게 유일한 단축 수단이다.**",
          "- 필요 표본은 t 로 풀었다 — 날짜짝짓기가 자유도를 잃는 손해가 반영돼 있다.",
          "  정규근사(z)로 계산하면 그 손해가 통째로 빠진다.",
          "- 픽 단위와 거래일 단위를 직접 비교하지 않는다. 하루에 픽이 몇 개인지가 다르다.",
          "- 평균이 양수인데 중앙값이 음수면 **소수 종목이 평균을 끌고 있다.**",
          "  그 모양의 엣지를 4~5종목 계좌로 담을 수 있는지는 별도 문제다(계획 §2).", ""]
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
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
    rows = doc.worksheet("백테스트_로그").get_all_values()      # 읽기 전용
    p = picks(rows)
    print(f"📒 원장 {len(rows)-1}행 → 알파 계산 가능한 픽 {len(p)}건")
    today = datetime.datetime.now(KST).strftime("%Y-%m-%d")
    md = report(p, as_of=today)
    path = a.out or f"docs/시그마실측_{today}.md"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w", encoding="utf-8").write(md)
    print(md)
    print(f"💾 저장: {path}")
    return 0



# ── 자기검증 ─────────────────────────────────────────────────────────
def _chk(label, ok, extra=""):
    print(f"  {'✅' if ok else '❌'} {label}" + (f"   {extra}" if extra else ""))
    return bool(ok)


def self_test():
    print("🧪 σ 실측")
    ok = True
    H = {1: 17, 3: 18, 5: 19, 10: 20}

    def row(ch, day, stock, index, n=32, excl=""):
        r = [""] * n
        r[C_ENTRY_DATE], r[C_CHANNEL] = day, ch
        si, ii = STOCK_COL[5], INDEX_COL[5]
        r[si], r[ii] = str(stock), str(index)
        if excl:
            r[25] = excl
        return r

    hdr = [""] * 32
    rows = [hdr,
            row("차트TOP2", "2026-09-01", 5.0, 1.0),
            row("차트TOP2", "2026-09-01", 3.0, 1.0),
            row("랜덤2", "2026-09-01", 2.0, 1.0),
            row("차트TOP2", "2026-09-02", -1.0, -2.0),
            row("랜덤2", "2026-09-02", 0.0, -2.0)]
    p = picks(rows, horizon_of=lambda ch: 5)
    ok &= _chk("알파 = 종목 − 지수", [x["alpha"] for x in p][:2] == [4.0, 2.0],
               f"{[x['alpha'] for x in p]}")
    ok &= _chk("원수익률도 같이 남긴다", p[0]["raw"] == 5.0)
    ok &= _chk("제외행은 빼고 센다",
               len(picks([hdr, row("차트TOP2", "2026-09-01", 5, 1, excl="제외")],
                         horizon_of=lambda ch: 5)) == 0)
    ok &= _chk("지수값이 없으면 버린다(한쪽만으로 알파를 만들지 않는다)",
               len(picks([hdr, row("차트TOP2", "2026-09-01", 5, "")],
                         horizon_of=lambda ch: 5)) == 0)
    ok &= _chk("지수벤치는 픽이 아니다",
               len(picks([hdr, row("지수벤치_KOSPI", "2026-09-01", 5, 1)],
                         horizon_of=lambda ch: 5)) == 0)

    ok &= _chk("③ 날짜평균 — 그날 픽들을 먼저 평균낸다",
               series_daily(p, "차트TOP2") == [3.0, 1.0],
               f"{series_daily(p, '차트TOP2')}")
    #   09-01: 차트 (4+2)/2=3.0 − 랜덤2 1.0 = +2.0
    #   09-02: 차트 1.0        − 랜덤2 2.0 = −1.0  ← 대조군이 이긴 날도 그대로 남는다
    ok &= _chk("④ 짝짓기 — 같은 날 대조군을 뺀다",
               series_paired(p, "차트TOP2") == [2.0, -1.0],
               f"{series_paired(p, '차트TOP2')}")
    ok &= _chk("대조군이 이긴 날을 버리지 않는다",
               any(x < 0 for x in series_paired(p, "차트TOP2")))
    only_one = [hdr, row("차트TOP2", "2026-09-03", 9, 1)]
    ok &= _chk("짝이 없는 날은 ④에서 빠진다",
               series_paired(picks(only_one, horizon_of=lambda ch: 5), "차트TOP2") == [])

    ok &= _chk("관측 1개면 σ 를 만들어내지 않는다", sigma_of([1.0]) is None)
    ok &= _chk("σ 는 표본표준편차", abs(sigma_of([1.0, 3.0]) - math.sqrt(2)) < 1e-12)

    # 🔴 이 검사가 이 도구의 핵심이다 — 자유도 손해가 실제로 반영되는가
    n_small = need_n(4.5, 1.5)
    ok &= _chk("필요 표본이 나온다", isinstance(n_small, int), f"σ=4.5 δ=1.5 → {n_small}")
    z_only = math.ceil(((1.6449 + 0.8416) * 4.5 / 1.5) ** 2)
    ok &= _chk("t 로 풀면 정규근사보다 **크다**(자유도 손해가 빠지지 않았다)",
               n_small > z_only, f"t={n_small} vs z={z_only}")
    ok &= _chk("σ 가 절반이면 필요 표본은 대략 1/4",
               2.5 < need_n(9.0, 1.5) / need_n(4.5, 1.5) < 5.5,
               f"{need_n(9.0,1.5)} / {need_n(4.5,1.5)}")
    ok &= _chk("δ 가 커지면 필요 표본이 준다", need_n(4.5, 3.0) < need_n(4.5, 1.0))
    ok &= _chk("δ 가 0 이면 None(무한대를 숫자로 안 만든다)", need_n(4.5, 0) is None)
    ok &= _chk("σ 가 없으면 None", need_n(None, 1.5) is None)
    ok &= _chk("δ 가 아주 작으면 None(캡)", need_n(4.5, 0.001) is None)
    # 🔴 회귀 — 중간 반복값이 캡을 넘었다고 답을 버리면 안 된다
    ok &= _chk("엄한 α 에서도 수렴한 답을 낸다(중간값이 커도 버리지 않는다)",
               need_n(10.01, 1.5, alpha=ALPHA_TRACK_C) == 500,
               f"{need_n(10.01, 1.5, alpha=ALPHA_TRACK_C)}")
    ok &= _chk("씨앗을 바꿔도 같은 값으로 수렴한다",
               need_n(7.76, 1.5, alpha=ALPHA_TRACK_C) == 302,
               f"{need_n(7.76, 1.5, alpha=ALPHA_TRACK_C)}")

    ok &= _chk("t 임계값이 자유도에 따라 준다", t_crit(5) > t_crit(100) > 1.6)

    sh = shape([-5.0, -1.0, 0.5, 2.0, 40.0])
    ok &= _chk("평균이 중앙값보다 크면 꼬리가 끈다", sh["mean"] > sh["median"])
    ok &= _chk("승률은 양수 비율", abs(sh["win"] - 0.6) < 1e-12, f"{sh['win']}")
    ok &= _chk("절사평균은 표본이 충분할 때만",
               "trimmed" not in shape([1.0, 2.0]) and
               "trimmed" in shape([float(i) for i in range(10)]))
    sym = shape([float(i) for i in range(10)])
    ok &= _chk("절사평균은 위아래를 **같이** 덜어낸다(한쪽만 덜면 치우친다)",
               abs(sym["trimmed"] - 4.5) < 1e-12, f"{sym['trimmed']}")

    # 절사평균은 n > 2·TRIM_K 일 때만 나온다 — 리포트 검사는 충분한 표본으로 돌린다
    big = [hdr] + [row("차트TOP2", f"2026-09-{d:02d}", v, 1.0)
                   for d, v in enumerate([5, 3, -1, 2, 8, -4, 1, 0, 6, 30], start=1)]
    big += [row("랜덤2", f"2026-09-{d:02d}", 1.0, 1.0) for d in range(1, 11)]
    pbig = picks(big, horizon_of=lambda ch: 5)
    # 🔴 main 이 주는 것과 **같은 타입**으로 부른다 — 문자열 as_of
    # ── 1.5단계 순위 효과 ────────────────────────────────────────
    def rrow(ch, day, stock, index, v1=None, v2=None, n=32):
        r = [""] * n
        r[C_ENTRY_DATE], r[C_CHANNEL] = day, ch
        r[STOCK_COL[5]], r[INDEX_COL[5]] = str(stock), str(index)
        if v1 is not None:
            r[C_V1] = str(v1)
        if v2 is not None:
            r[C_V2] = str(v2)
        return r

    # 1순위(점수 90)가 2순위(70)보다 매일 +2%p 좋은 경우
    good = [hdr]
    for d in range(1, 6):
        day = f"2026-09-{d:02d}"
        good += [rrow("차트TOP2", day, 3.0, 0.0, v1=90),
                 rrow("차트TOP2", day, 1.0, 0.0, v1=70)]
    g, k = rank_gap(picks(good, horizon_of=lambda ch: 5), "차트TOP2")
    ok &= _chk("순위 효과 — 1순위가 좋으면 양수", g == [2.0] * 5 and k == "v1", f"{g}")

    # 순위가 정보를 안 담는 경우(1순위가 오히려 나쁨)
    bad = [hdr]
    for d in range(1, 6):
        day = f"2026-09-{d:02d}"
        bad += [rrow("수급TOP2", day, 0.0, 0.0, v2=90),
                rrow("수급TOP2", day, 2.0, 0.0, v2=70)]
    g2, k2 = rank_gap(picks(bad, horizon_of=lambda ch: 5), "수급TOP2")
    ok &= _chk("역전된 경우도 그대로 음수로 낸다(0 으로 뭉개지 않는다)",
               g2 == [-2.0] * 5 and k2 == "v2", f"{g2}")
    ok &= _chk("채널마다 다른 점수열을 쓴다(차트=v1, 수급=v2)",
               RANK_KEY["차트TOP2"] == "v1" and RANK_KEY["수급TOP2"] == "v2")
    tie = [hdr, rrow("차트TOP2", "2026-09-01", 3.0, 0.0, v1=80),
           rrow("차트TOP2", "2026-09-01", 1.0, 0.0, v1=80)]
    ok &= _chk("동점이면 순위가 없다 — 세지 않는다",
               rank_gap(picks(tie, horizon_of=lambda ch: 5), "차트TOP2")[0] == [])
    one = [hdr, rrow("차트TOP2", "2026-09-01", 3.0, 0.0, v1=80)]
    ok &= _chk("픽이 하나면 짝이 없다",
               rank_gap(picks(one, horizon_of=lambda ch: 5), "차트TOP2")[0] == [])
    ok &= _chk("점수가 없는 채널은 측정하지 않는다",
               rank_gap(picks(good, horizon_of=lambda ch: 5), "리포트TOP2_단기") == ([], None))

    bands = score_bands(picks(bad, horizon_of=lambda ch: 5), "수급TOP2")
    labs = [b[0] for b in bands]
    ok &= _chk("점수 구간이 경계 없이 이어진다", labs == ["<40", "40~60", "60~80", "80+"],
               f"{labs}")
    filled = {lab: sh for lab, sh in bands if sh}
    ok &= _chk("구간별로 나눠 담는다",
               filled["60~80"]["mean"] == 2.0 and filled["80+"]["mean"] == 0.0,
               f"{ {k: v['mean'] for k, v in filled.items()} }")

    md = report(pbig, channels=["차트TOP2"], as_of="2026-09-16")
    ok &= _chk("as_of 가 문자열이어도 죽지 않는다(main 이 쓰는 경로)",
               "# 설계별 σ 실측 — 2026-09-16" in md)
    ok &= _chk("as_of 가 없으면 오늘 날짜로 채운다",
               "# 설계별 σ 실측 — " in report(pbig, channels=["차트TOP2"]))
    ok &= _chk("리포트에 설계별 σ 가 나온다", "① 픽·원수익률" in md and "④ 날짜짝짓기" in md)
    ok &= _chk("리포트가 **판정이 아니라고** 밝힌다", "판정이 아니다" in md)
    ok &= _chk("Track C 의 실제 α 를 같이 낸다(α=0.05 만 내면 낙관적으로 읽힌다)",
               f"α={ALPHA_TRACK_C}" in md)
    ok &= _chk("α 가 엄해지면 필요 표본이 는다",
               need_n(7.76, 1.5, alpha=ALPHA_TRACK_C) > need_n(7.76, 1.5, alpha=ALPHA_REF),
               f"{need_n(7.76,1.5,alpha=ALPHA_TRACK_C)} vs {need_n(7.76,1.5,alpha=ALPHA_REF)}")
    ok &= _chk("③·④ 가 다른 질문이라고 리포트에 박혀 있다",
               "같은 질문이 아니다" in md and "먼저 무엇을 승인할지" in md)
    ok &= _chk("설계를 고른 뒤 새 표본 검증이 필요하다고 적는다", "미사용 표본" in md)
    ok &= _chk("설계별 유의성을 계산하지 않는다 — p-해킹 방지",
               "p-해킹" in md and " p=" not in md and "t=" not in md)
    ok &= _chk("δ 는 곡선으로 낸다(내가 고르지 않는다)",
               all(f"δ={d}" in md for d in DELTA_GRID))
    ok &= _chk("대조군을 따로 보여준다", f"## 대조군 {CONTROL}" in md)
    ok &= _chk("분포 모양을 병기한다", "중앙값" in md and "승률" in md and "절사평균" in md)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1

if __name__ == "__main__":
    sys.exit(main())
