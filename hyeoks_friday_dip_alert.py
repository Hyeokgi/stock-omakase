# -*- coding: utf-8 -*-
# ==========================================================================
# 🌙 금요일 밤 하락 알림 `friday-dip-v1` — 금요일 급등주 1~2위가 19:40 에 KRX 종가보다 3% 이상 빠졌으면 텔레그램으로 알린다
# --------------------------------------------------------------------------
# 사용자 지시 2026-10-23: "금요일 1~2위종목 중 3%이상 하락한 종목을 추적해줘. 그종목이 있으면 텔레그램으로
#   7시 40분이 지나고 알려줘. KRX 장후시간외 거래(18시 또는 20시까지 거래)에서도 적용될 수 있는지 확인하고 … 포함해서."
#   같은 날 "다른 리포트나 알림처럼 깃허브에 넣어주는건 어때?" → "응, 추천대로 테마 빼고 진행해줘". PC 판(D:\AI 허브)의 깃허브 이식.
# 대상: 금요일 KRX 등락률 10% 이상 29.5% 미만 · 거래대금(KRX 종가 × 거래량) 500억 이상 · 보통주 중 거래대금 1~2위.
#   관측 '금요일 밤 하락 20:00 매수'(허브 work\관측_금요일밤하락_2026-10-23)와 같은 정의다. 이 알림은 참고용이며 관측 채점은 허브 규칙이 따로 한다.
# 후보 고르기: 전 종목 일봉 대신 한투 등락률 순위(시장별 30개)를 등락률 구간으로 나눠 훑는다.
#   🔴 저녁 순위의 등락률은 애프터마켓 현재가 기준이다(10/23 실측: 15:30 종가 1,944 → 21시 순위 1,946). 종가 +10% 종목이 밤에 빠지면
#   순위에서는 낮게 보이므로 −5% 까지 넓게 훑고, 전일 종가 · 거래량은 일봉(J), 오늘 종가는 15:30 분봉으로 다시 확인한다
#   (당일 저녁 일봉의 종가 자리도 애프터마켓 현재가다).
# 19:40 가격: NXT 일봉(NX)의 현재 종가, KRX 애프터마켓(16:00~20:00 연속거래, 2026-09-14~)의 최근 1분봉.
# 공개 저장소에는 data/research_runs/friday_dip_runs.csv 의 건수만 남긴다. 로그에 종목명 · 코드를 찍지 않는다.
# 운영 선정 · 주문 · 시트와 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import datetime
import os
import sys
import time

import hyeoks_research_daily as R

VERSION = "friday-dip-v1"
BASE = "https://openapi.koreainvestment.com:9443"
KST = R.KST
DROP = -0.03
RET_LO, RET_HI = 0.10, 0.295
AMOUNT_MIN = 5e10
TOP = 2
SCAN_LO, SCAN_HI = -5.0, 30.0     # 등락률 순위를 훑는 구간(%)
PROXY_MIN = 3e10                  # 순위의 현재가 × 누적 거래량이 이 이상인 종목만 일봉으로 확인한다
PAGE = 30                         # 등락률 순위 한 번에 오는 최대 종목 수
CHECK_AT = "19:40"
LATEST = "19:58"                  # 이 시각이 지나 시작하면 20:00 매수에 쓸 수 없어 아무것도 하지 않는다
SEND_BY = "19:59"                 # 조회가 늦어져 이 시각을 넘기면 보내지 않는다(2026-10-25 Codex 검토)
PAUSE = 0.07                      # 한투 호출 사이 쉼(초) — 초당 20건 제한
DONE = {"대상없음", "해당없음", "발송성공", "응답불명"}
RUN_COLS = ["day", "runId", "version", "startedAt", "checkedAt", "scanCalls", "scanned", "confirmed", "events", "hits",
            "incomplete", "status", "sendStatus", "note"]


def token(post, key, secret):
    r = post(f"{BASE}/oauth2/tokenP", json={"grant_type": "client_credentials", "appkey": key, "appsecret": secret},
             headers={"content-type": "application/json"}, timeout=10)
    tok = r.json().get("access_token") if getattr(r, "status_code", 0) == 200 else None
    if not tok:
        raise RuntimeError(f"토큰 발급 실패 HTTP {getattr(r, 'status_code', '?')}")
    return tok


def kis_getter(get, key, secret, tok, sleep):
    """(경로, tr_id, 입력) → 응답 JSON. rt_cd ≠ 0 이면 1초 쉬고 한 번 더, 그래도 안 되면 예외."""
    h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}", "appkey": key,
         "appsecret": secret, "custtype": "P"}

    def call(path, tr_id, params):
        for k in range(2):
            sleep(PAUSE if k == 0 else 1.0)
            r = get(BASE + path, headers=dict(h, tr_id=tr_id), params=params, timeout=15)
            js = r.json() if getattr(r, "status_code", 0) == 200 else {}
            if str(js.get("rt_cd")) == "0":
                return js
        raise RuntimeError(f"{tr_id} 응답 오류 HTTP {getattr(r, 'status_code', '?')} {js.get('msg_cd', '')}")
    return call


def rank_band(kis, iscd, lo, hi, plo=None, phi=None):
    js = kis("/uapi/domestic-stock/v1/ranking/fluctuation", "FHPST01700000",
             {"fid_cond_mrkt_div_code": "J", "fid_cond_scr_div_code": "20170", "fid_input_iscd": iscd,
              "fid_rank_sort_cls_code": "0", "fid_input_cnt_1": "0", "fid_prc_cls_code": "0",
              "fid_input_price_1": "" if plo is None else str(plo), "fid_input_price_2": "" if phi is None else str(phi),
              "fid_vol_cnt": "", "fid_trgt_cls_code": "0", "fid_trgt_exls_cls_code": "0",
              "fid_div_cls_code": "0", "fid_rsfl_rate1": f"{lo:.2f}", "fid_rsfl_rate2": f"{hi:.2f}"})
    return js.get("output") or []


PRICE_MAX = 10_000_000


def scan(band, lo=SCAN_LO, hi=SCAN_HI):
    """band(시장, 등락률 하한, 상한, 가격 하한, 상한) → 순위 행들. 30개가 꽉 차면 등락률 구간을 반으로,
    더 나눌 수 없으면(보합 0.00% 처럼 같은 등락률이 30개 넘음) **가격 구간**으로 나눠 다시 받는다.
    🔴 2026-10-25 Codex 검토 — 처음에는 0.00% 근처를 잘린 채 넘겼는데, 종가 +10% 종목이 저녁에 9% 넘게 빠져
    그 구간에 있으면 바로 그 종목이 알림 대상이다. 10/24 실측: 가격 필터(fid_input_price_1/2, 양끝 포함)가 동작한다.
    (코드 → 행, 호출 수, 가격으로도 더 나눌 수 없는데 꽉 찬 구간 목록)."""
    got, calls, cut = {}, 0, []
    for iscd in ("0001", "1001"):                 # 코스피 · 코스닥
        todo = [(lo, hi, None, None)]
        while todo:
            a, b, pa, pb = todo.pop()
            rows = band(iscd, a, b, pa, pb)
            calls += 1
            for x in rows:
                got[x.get("stck_shrn_iscd", "")] = x
            if len(rows) < PAGE:
                continue
            mid = round((a + b) / 2, 2)
            if pa is None and b - a >= 0.02 and mid not in (a, b):
                todo += [(a, mid, None, None), (mid, b, None, None)]
                continue
            pa, pb = (0, PRICE_MAX) if pa is None else (pa, pb)
            if pb - pa < 1:
                cut.append((iscd, a, b, pa, pb))
            else:
                pm = (pa + pb) // 2
                todo += [(a, b, pa, pm), (a, b, pm + 1, pb)]
    got.pop("", None)
    return got, calls, cut


def prelist(rows):
    """보통주(코드 끝자리 0)이면서 순위의 현재가 × 누적 거래량이 PROXY_MIN 이상."""
    out = []
    for code, x in rows.items():
        try:
            proxy = float(x.get("stck_prpr") or 0) * float(x.get("acml_vol") or 0)
        except ValueError:
            continue
        if code.endswith("0") and proxy >= PROXY_MIN:
            out.append(code)
    return sorted(out)


def minute_bars(kis, code, day8, hhmmss):
    js = kis("/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice", "FHKST03010230",
             {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_HOUR_1": hhmmss, "FID_INPUT_DATE_1": day8,
              "FID_PW_DATA_INCU_YN": "N", "FID_FAKE_TICK_INCU_YN": ""})
    return [x for x in js.get("output2") or [] if x.get("stck_bsop_date") == day8]


def day_bar(kis, code, day8):
    """전 거래일 종가는 일봉, 오늘 종가는 15:30 분봉.
    🔴 2026-10-23 실측: 당일 저녁 일봉의 '종가' 자리는 애프터마켓 현재가다(그날 1위 종목 19:15 1,950 · 21:00 1,946,
    15:30 종가 1,944). 지난 날짜 일봉 종가는 15:30 분봉과 같았다(10/15 · 10/22). 거래량은 일봉 누적(애프터 포함)."""
    first = (datetime.datetime.strptime(day8, "%Y%m%d") - datetime.timedelta(days=10)).strftime("%Y%m%d")
    js = kis("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
             {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": first, "FID_INPUT_DATE_2": day8,
              "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
    rows = sorted((x["stck_bsop_date"], float(x.get("stck_clpr") or 0), float(x.get("acml_vol") or 0))
                  for x in js.get("output2") or [] if x.get("stck_bsop_date"))
    if len(rows) < 2 or rows[-1][0] != day8 or rows[-1][2] <= 0 or rows[-2][1] <= 0:
        return None
    (_, _, v), (_, pc, _) = rows[-1], rows[-2]
    c = next((float(x["stck_prpr"]) for x in minute_bars(kis, code, day8, "153000")
              if x.get("stck_cntg_hour") == "153000" and float(x.get("stck_prpr") or 0) > 0), None)
    if c is None:
        return None
    return {"code": code, "close": c, "ret": c / pc - 1, "amount": c * v}


def pick(bars):
    ev = [b for b in bars if b and RET_LO - 1e-9 <= b["ret"] < RET_HI and b["amount"] >= AMOUNT_MIN]
    return sorted(ev, key=lambda b: -b["amount"])[:TOP]


def nx_now(kis, code, day8):
    js = kis("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
             {"FID_COND_MRKT_DIV_CODE": "NX", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": day8, "FID_INPUT_DATE_2": day8,
              "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
    for x in js.get("output2") or []:
        if x.get("stck_bsop_date") == day8 and float(x.get("acml_vol") or 0) > 0:
            return float(x["stck_clpr"])
    return None


def krx_after_now(kis, code, day8, hhmmss):
    bars = [(x["stck_cntg_hour"], float(x["stck_prpr"])) for x in minute_bars(kis, code, day8, hhmmss)
            if "160000" <= x.get("stck_cntg_hour", "") <= hhmmss and int(x.get("cntg_vol") or 0) > 0]
    return max(bars)[1] if bars else None


def chg(p, close):
    return None if p is None else p / close - 1


def message(day, hhmm, picks, names):
    """picks — day_bar 결과에 nx · kr(19:40 가격)를 더한 것. (본문, 조건 해당 수)."""
    lines, hits = [], 0
    for k, r in enumerate(picks, 1):
        dn, dk = chg(r["nx"], r["close"]), chg(r["kr"], r["close"])
        flag = any(d is not None and d <= DROP + 1e-12 for d in (dn, dk))
        hits += flag
        f = lambda p, d: "거래 없음" if p is None else f"{p:,.0f}원 ({d * 100:+.1f}%)"
        lines.append(f"{k}위 {names.get(r['code'], r['code'])}({r['code']}) — KRX 종가 {r['close']:,.0f}원"
                     f"(당일 {r['ret'] * 100:+.1f}%, 거래대금 {r['amount'] / 1e8:,.0f}억)\n"
                     f"   NXT {f(r['nx'], dn)} · KRX 애프터 {f(r['kr'], dk)}{'  ← 3% 이상 하락' if flag else ''}")
    tail = ("\n\n조건(종가 대비 −3% 이하) 해당 종목이 있습니다. 관측 규칙은 20:00 NXT 종가 매수 → 다음 거래일 09:00 KRX 시가 매도."
            if hits else "\n\n조건 해당 없음.")
    text = (f"[금요일 밤 하락 확인] {day} {hhmm}\n" + "\n".join(lines) + tail +
            "\n참고 알림 · 자동주문 없음 · 매수 추천 아님")
    return text, hits


def already_done(day, runs_dir=R.RUNS_DIR):
    path = os.path.join(runs_dir, "friday_dip_runs.csv")
    if not os.path.exists(path):
        return False
    with open(path, encoding="utf-8") as fh:
        return any(r.get("day") == day and r.get("status") in DONE for r in csv.DictReader(fh))


def log_run(st, runs_dir=R.RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "friday_dip_runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def at(now, hhmm):
    hh, mm = (int(x) for x in hhmm.split(":"))
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)


def main(argv=None, env=None, get=None, post=None, clock=None, sleep=time.sleep, runs_dir=R.RUNS_DIR):
    ap = argparse.ArgumentParser()
    ap.add_argument("--check-at", default=CHECK_AT)
    ap.add_argument("--day", help="시험용 — 금요일 · 기록 확인 없이 그날로 본다(YYYY-MM-DD)")
    ap.add_argument("--dry-run", action="store_true", help="보내지도 기록하지도 않는다")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    clock = clock or (lambda: datetime.datetime.now(KST))
    now = clock()
    day = a.day or now.strftime("%Y-%m-%d")
    st = {"day": day, "runId": env.get("GITHUB_RUN_ID", ""), "version": VERSION,
          "startedAt": now.isoformat(timespec="seconds"), "incomplete": 0}
    record = (lambda: None) if a.dry_run else (lambda: log_run(st, runs_dir))
    if not a.day:
        if now.weekday() != 4:
            print(f"ℹ️ {day} 은 금요일이 아니다 — 생략")
            return 0
        ok, why = R.trading_day(day)
        if not ok:
            print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''} — 생략")
            return 0
        if already_done(day, runs_dir):
            print(f"ℹ️ {day} 은 앞 실행이 이미 확인했다 — 생략")
            return 0
        if now >= at(now, LATEST):
            print(f"ℹ️ {LATEST} 이 지나 시작했다 — 20:00 매수에 쓸 수 없어 생략")
            st.update(status="늦은시작")
            record()
            return 0
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    if get is None or post is None:
        import requests
        get, post = get or requests.get, post or requests.post
    day8 = day.replace("-", "")
    # 🔴 2026-10-25 Codex 검토 — 후보도 19:40 에 고른다. 거래량은 조회 시점까지의 누적(애프터 포함)이라
    #    18:00 백업이 고르면 19:40 실행과 1~2위가 달라질 수 있다. 확정 일봉 거래대금도 애프터를 포함한다
    #    (10/22 실측: 일봉 거래대금 = 19:59 봉 누적). 그래서 확인 시각에 고르는 쪽이 관측 정의에 가장 가깝다.
    wait = (at(clock(), a.check_at) - clock()).total_seconds()
    if wait > 0 and not a.day:
        print(f"⏳ {a.check_at} 까지 {wait / 60:.0f}분 기다린다")
        sleep(wait)
    try:
        kis = kis_getter(get, key, secret, token(post, key, secret), sleep)
        rows, calls, cut = scan(lambda iscd, lo, hi, plo=None, phi=None: rank_band(kis, iscd, lo, hi, plo, phi))
        cands = prelist(rows)
        picks = pick([day_bar(kis, c, day8) for c in cands])
    except Exception as e:
        print(f"❌ 후보 조회 실패 {str(e)[:80]}")
        st.update(status="오류", note="후보 조회 실패")
        record()
        return 1
    st.update(scanCalls=calls, scanned=len(rows), confirmed=len(cands), events=len(picks), incomplete=len(cut))
    print(f"🔎 순위 호출 {calls}회 · 순위 종목 {len(rows)} · 일봉 확인 {len(cands)} · 조건 1~{TOP}위 {len(picks)}"
          + (f" · ⚠️ 가격으로도 못 나눈 구간 {len(cut)}" if cut else ""))
    if not picks:
        # 조회가 잘렸으면 '대상 없음' 으로 확정하지 않는다 — 뒤 실행이 다시 본다(DONE 에 없는 상태).
        st.update(status="조회불완전" if cut else "대상없음", checkedAt=clock().isoformat(timespec="seconds"), hits=0)
        record()
        return 0
    t = clock()
    hhmmss = min(t.strftime("%H%M%S"), "200000")
    try:
        for r in picks:
            r["nx"], r["kr"] = nx_now(kis, r["code"], day8), krx_after_now(kis, r["code"], day8, hhmmss)
    except Exception as e:
        print(f"❌ 19:40 가격 조회 실패 {str(e)[:80]}")
        st.update(status="오류", note="가격 조회 실패", checkedAt=t.isoformat(timespec="seconds"))
        record()
        return 1
    names = {c: (rows[c].get("hts_kor_isnm") or c) for c in rows}
    text, hits = message(day, t.strftime("%H:%M"), picks, names)
    st.update(checkedAt=t.isoformat(timespec="seconds"), hits=hits)
    print(f"🌙 조건 해당 {hits}/{len(picks)}")        # 종목명 · 가격은 공개 로그에 찍지 않는다
    if not hits:
        st.update(status="조회불완전" if cut else "해당없음")
        record()
        return 0
    if a.dry_run:
        print("ℹ️ 시험 실행 — 보내지 않는다")
        return 0
    # 🔴 2026-10-25 Codex 검토 — 시작할 때만 19:58 을 보면, 조회가 늦어져 20:00 매수 뒤에 보낼 수 있다.
    if not a.day and clock() >= at(clock(), SEND_BY):
        print(f"ℹ️ {SEND_BY} 이 지나 20:00 매수에 쓸 수 없어 보내지 않는다")
        st.update(status="늦은발송생략")
        record()
        return 0
    sent, err = R.send(text, post, env.get("TELEGRAM_BOT_TOKEN", ""))
    st.update(status=R.send_status(sent, err), sendStatus=R.send_status(sent, err), note=err)
    record()
    print(f"📨 {st['status']}{(' — ' + err) if err else ''}")
    return 0 if sent else 1


if __name__ == "__main__":
    sys.exit(main())
