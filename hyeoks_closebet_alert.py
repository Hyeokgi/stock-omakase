# -*- coding: utf-8 -*-
# ==========================================================================
# ⏰ 15:05 종베 후보 알림 `closebet-alert-v1` — 15:05 KRX 기준 급등주 후보를 텔레그램으로 한 번 보낸다(잠정)
# --------------------------------------------------------------------------
# 사용자 답 2026-10-31(웹앱 E단계): "15:05 종베 후보 알림", "금요일 1,2등주 … 메시지를 전송해줘".
# 평일 — KRX 등락률 15% 이상 29.5% 미만 · 거래대금(현재가 × 누적 거래량) 500억 이상 보통주, 거래대금 순 상위 5.
#   거래량 배수(오늘 15:05 까지 ÷ 직전 20거래일 평균)와 허브 관측 1(거래량 배수 ≤ 3.35)의 조건 해당 여부를 함께 적는다.
#   관측 1 의 테마 소속 · 시총 조건은 비공개 스냅샷이라 여기서는 보지 않는다(참고 표시일 뿐 채점은 허브 규칙).
# 금요일 — 위에 더해 10% 이상 29.5% 미만 · 500억 이상 거래대금 1~2위(허브 관측 5 · 6 의 대상, 15:05 잠정).
# 관측 기간 결과(수익)는 판정일(2027-04 · 10) 전에는 어디에도 싣지 않는다 — 후보만 알린다.
# 15:05 값은 잠정이다(종가 · 거래량은 15:30 까지 바뀐다). 15:18 이 지나면 종가 매수에 쓸 수 없어 보내지 않는다.
# 발사는 GAS 15:05 스냅샷 트리거가 함께 한다(깃허브 예약은 몇 시간씩 늦음). 공개 로그에 종목명 · 코드를 찍지 않는다.
# 운영 선정 · 주문과 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import datetime
import os
import sys
import time

import hyeoks_friday_dip_alert as F
import hyeoks_research_daily as R
from kis_token_share import token_for_run
from telegram_target import footer

VERSION = "closebet-alert-v1"
KST = R.KST
START_AT, SEND_BY = "15:05", "15:18"
HOT_LO, FRI_LO, RET_HI = 0.15, 0.10, 0.295
AMOUNT_MIN = 5e10
VOL_MULT_MAX = 3.35
TOP_HOT, TOP_FRI = 5, 2


def rows_to_cands(rows):
    """순위 행 → 보통주 후보(등락률 · 15:05 거래대금 근사)."""
    out = []
    for code, x in rows.items():
        try:
            ret, price, vol = float(x.get("prdy_ctrt") or 0) / 100, float(x.get("stck_prpr") or 0), float(x.get("acml_vol") or 0)
        except ValueError:
            continue
        if code.endswith("0") and price > 0 and vol > 0:
            out.append({"code": code, "name": x.get("hts_kor_isnm") or code, "ret": ret, "price": price, "vol": vol,
                        "amount": price * vol})
    return out


def choose(cands, lo, top):
    ev = [c for c in cands if lo - 1e-9 <= c["ret"] < RET_HI and c["amount"] >= AMOUNT_MIN]
    return sorted(ev, key=lambda c: -c["amount"])[:top]


def vol_avg20(kis, code, day8):
    """직전 20거래일 평균 거래량(오늘 제외). 20개가 안 되면 None."""
    first = (datetime.datetime.strptime(day8, "%Y%m%d") - datetime.timedelta(days=45)).strftime("%Y%m%d")
    js = kis("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
             {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": first, "FID_INPUT_DATE_2": day8,
              "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
    vols = [float(x.get("acml_vol") or 0) for x in sorted(js.get("output2") or [], key=lambda x: x.get("stck_bsop_date", ""))
            if x.get("stck_bsop_date") and x["stck_bsop_date"] < day8]
    return sum(vols[-20:]) / 20 if len(vols) >= 20 and sum(vols[-20:]) > 0 else None


def line(k, c):
    m = c.get("mult")
    mult = "거래량 배수 –" if m is None else f"거래량 {m:.1f}배{' ✓' if m <= VOL_MULT_MAX + 1e-9 else ''}"
    return f"{k}. {c['name']}({c['code']}) {c['ret'] * 100:+.1f}% · {c['amount'] / 1e8:,.0f}억 · {mult}"


def message(day, hhmm, hot, fri):
    wd = "월화수목금토일"[datetime.date.fromisoformat(day).weekday()]
    out = [f"[15:05 종베 후보 · 잠정] {day} ({wd}) {hhmm}",
           "급등 15% 이상 · 거래대금 500억 이상 · 거래대금 순 (KRX 지금 기준)"]
    out += [line(k, c) for k, c in enumerate(hot, 1)] or ["해당 종목 없음"]
    if hot:
        out.append("✓ = 거래량이 직전 20일 평균의 3.35배 이하(관측 1 조건 · 테마 · 시총 확인은 안 함)")
    if fri is not None:
        out += ["", "금요일 1·2위 — 10% 이상 · 500억 이상 거래대금 순(관측 5 · 6 대상)"]
        out += [f"{k}위 {c['name']}({c['code']}) {c['ret'] * 100:+.1f}% · {c['amount'] / 1e8:,.0f}억" for k, c in enumerate(fri, 1)] \
            or ["해당 종목 없음"]
    out.append("\n종가 · 거래량은 15:30 까지 바뀝니다. 관측 결과(수익)는 판정일 전에는 싣지 않습니다.")
    return "\n".join(out) + footer("home")


def main(argv=None, env=None, get=None, post=None, clock=None, sleep=time.sleep):
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", help="시험용 — 거래일 · 시각 확인 없이 그날로 본다(YYYY-MM-DD)")
    ap.add_argument("--dry-run", action="store_true", help="보내지 않고 본문만 출력")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    clock = clock or (lambda: datetime.datetime.now(KST))
    now = clock()
    day = a.day or now.strftime("%Y-%m-%d")
    if not a.day:
        ok, why = R.trading_day(day)
        if not ok:
            print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''} — 생략")
            return 0
        if now >= F.at(now, SEND_BY):
            print(f"ℹ️ {SEND_BY} 이 지나 시작했다 — 종가 매수에 쓸 수 없어 생략")
            return 0
        wait = (F.at(now, START_AT) - now).total_seconds()
        if wait > 0:
            print(f"⏳ {START_AT} 까지 {wait / 60:.0f}분 기다린다")
            sleep(wait)
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    if get is None or post is None:
        import requests
        get, post = get or requests.get, post or requests.post
    day8 = day.replace("-", "")
    is_fri = datetime.date.fromisoformat(day).weekday() == 4
    try:
        kis = F.kis_getter(get, key, secret, token_for_run(get, post, key, secret, F.BASE, F.token)[0], sleep)
        rows, calls, cut = F.scan(lambda iscd, lo, hi, plo=None, phi=None: F.rank_band(kis, iscd, lo, hi, plo, phi),
                                  lo=FRI_LO * 100, hi=30.0)
        cands = rows_to_cands(rows)
        hot = choose(cands, HOT_LO, TOP_HOT)
        fri = choose(cands, FRI_LO, TOP_FRI) if is_fri else None
        for c in hot:
            try:
                avg = vol_avg20(kis, c["code"], day8)
                c["mult"] = c["vol"] / avg if avg else None
            except Exception:
                c["mult"] = None
    except Exception as e:
        print(f"❌ 후보 조회 실패 {str(e)[:80]}")
        return 1
    t = clock()
    print(f"🔎 순위 호출 {calls}회 · 순위 종목 {len(rows)} · 15% 후보 {len(hot)}"
          + (f" · 금요일 1~2위 {len(fri)}" if fri is not None else "") + (f" · ⚠️ 잘린 구간 {len(cut)}" if cut else ""))
    text = message(day, t.strftime("%H:%M"), hot, fri)
    if a.dry_run:
        print("ℹ️ 시험 실행 — 보내지 않는다(본문은 종목 정보라 로그에 찍지 않는다)")
        return 0
    if not a.day and t >= F.at(t, SEND_BY):
        print(f"ℹ️ 조회가 {SEND_BY} 을 넘겨 보내지 않는다")
        return 0
    sent, err = R.send(text, post, env.get("TELEGRAM_BOT_TOKEN", ""))
    print(f"📨 {R.send_status(sent, err)}{(' — ' + err) if err else ''}")
    return 0 if sent else 1


if __name__ == "__main__":
    sys.exit(main())
