# -*- coding: utf-8 -*-
# ==========================================================================
# 🧮 15:05 장중 잠정 수급 기록 `flow-estimate-v1` — 한투 종목별 외인기관 추정가집계 (연구 전용)
# --------------------------------------------------------------------------
# 설계: docs/collaboration/2026-10-14_Claude_잠정수급_기록설계.md (판단 기준 값은 사용자 결정 대기)
# 왜: K12(등락률 10%↑ + 당일 기관·외국인 동반 순매수, 하루 한 종목)는 장 마감 뒤 확정 수급으로 찾은 조건이다.
#     15:05 에 보이는 것은 한투 가집계(tr HHPTJ04160200)의 14:30 입력분뿐이고, 과거 잠정치는 어디에도 없어
#     앞으로 쌓는 수밖에 없다. 놓친 날은 복원할 수 없다.
# 세 가지 모드
#   (기본) 기록 — 거래일 14:50~15:25 KST 에 시작한 실행만. 네이버 전 종목 시세 1회 → 15:05 등락률 10%↑·거래대금 50억↑
#          (거래정지 제외, 거래대금 큰 순 최대 400) 종목마다 추정가집계 1회. 원본(응답 전부 + 그 시점 시세)은
#          드라이브 비공개, 공개 `data/research_runs/flow_estimate_runs.csv` 는 건수만.
#   --probe — 토큰 1회 + 시험 종목 2개 호출. 응답 **구조만** 출력(값·부호 없음). 저장·커밋 없음.
#   --reconcile DIR — 판정 때 PC 에서 한 번. 비공개 원본 + 한투 일별 확정 순매수 → 일치율 요약(로컬, 공개 안 함).
#     드라이브 창구는 쓰기 전용이라 러너가 지난 잠정치를 읽을 수 없다 — 그래서 대조는 매일이 아니라 판정 때 한다.
# 운영 KIS 토큰(구글 시트 칸)은 쓰지 않고 읽기만 한다 — 기록 실행은 오늘 것이 통하면 함께 쓰고, 아니면 실행당 1회 발급
# (2026-10-30 사용자 결정 '알림 공유', kis_token_share). 시험 호출 · 대조는 그대로 따로 발급한다.
# 하지 않는 것: 주문·시트·선정 변경. 공개 저장소·로그에 종목코드·종목명·수급 값을 남기지 않는다. 지문 대상이 아니다.
# ==========================================================================
import argparse
import collections
import csv
import datetime
import glob
import gzip
import json
import os
import random
import sys
import time

import hyeoks_research_daily as R
from kis_token_share import token_for_run
from hyeoks_kis_news import BASE, token

VERSION = "flow-estimate-v1"
PATH = "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
TR_ID = "HHPTJ04160200"
# hyeoks_market_snapshot.MARKET_URL 과 같은 전 종목 시세다(그 모듈은 import 하면 표준출력을 바꿔서 가져오지 않는다).
MARKET_URL = ("https://stock.naver.com/api/domestic/market/stock/default"
              "?tradeType=KRX&marketType=ALL&orderType=priceTop&startIdx=0&pageSize=3000")
NAVER_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*",
                 "Referer": "https://stock.naver.com/"}
# 🔒 2026-10-25 덧붙임(판정 전 · 일치율 보기 전, 설계 문서 §10) — 확정치와 거래량 배수의 직전 20거래일 거래량을
#    네이버(m.stock trend · fchart, robots 가 막는 경로)에서 한투로 바꾼다. 지표 · 묶음 · 판정 기준은 그대로다.
INVESTOR_PATH = "/uapi/domestic-stock/v1/quotations/inquire-investor"
INVESTOR_TR = "FHKST01010900"     # 종목 투자자 — 최근 30거래일 일별 외국인 · 기관 순매수 수량(2026-10-25 구조 확인)
DAILY_PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
DAILY_TR = "FHKST03010100"        # 기간별 일봉 — 한 번에 최대 100봉
DAILY_LOOKBACK = 45               # 직전 20거래일을 덮는 달력 일수(설 · 추석 연휴 포함)
WINDOW = ((14, 50), (15, 25))     # 시작 시각 창 — 15:05 스냅샷 슬롯과 같다
CALL_DEADLINE = (15, 29)          # 장 마감(15:30) 뒤 응답은 15:05 에 보이던 값이라고 볼 수 없다 → 그 전에 멈춘다
MIN_ROWS = 1500                   # 전 종목 시세가 이보다 적으면 잘린 응답
MIN_CHANGE = 10.0                 # K12 의 등락률 조건(%) — 15:05 값
MIN_AMOUNT = 50e8                 # K12 대상의 거래대금 하한(원) — 15:05 값
LIMIT_CHANGE = 29.5               # 이 이상은 그날 살 수 없다 — 부르기는 하되(일치율 표본) 하루 한 종목 고르기에서 뺀다
U2_AMOUNT, U2_CAP = 500e8, 1000e8 # 사용자 필터(거래대금 500억·시총 1,000억) — 대조 때 묶음으로만 쓴다
CAP = 400                         # 하루 호출 상한. 2019-01~2026-09 하루 최대 329건(중앙 16, 99% 70)
PAUSE = 0.25                      # 호출 간격(초) — 초당 4건. 운영 omakase 와 같은 앱키라 한도(초당 20건)를 넉넉히 남긴다
RATE_LIMIT = "EGW00201"           # 초당 거래건수 초과
STOP_AFTER = 3                    # 속도 제한이 아닌 응답 오류(rt_cd ≠ 0)가 연속 3회면 그날 중단
PROBE_CODES = ("005930", "086520")
KEEP = ("sosok", "nowPrice", "prevChangeRate", "tradeVolume", "tradeAmount", "marketSum", "tradeStopYn",
        "manageStatusGb", "marketAlertType")
FIELDS = {"frg": "frgn_fake_ntby_qty", "org": "orgn_fake_ntby_qty", "sum": "sum_fake_ntby_qty"}
RUNS_FILE = "flow_estimate_runs.csv"
RUN_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "marketRows", "candidates", "capped", "called",
            "ok", "failed", "rateLimitRetries", "withRows", "latestFilled", "inputCodes", "status", "stopReason",
            "privateFile", "privateStatus"]
BOOT_N, BOOT_SEED = 2000, 20261014
KST = R.KST


def num(v):
    """'1,234'·'+5'·'-7' → 실수. 빈칸·형식 아님 → None (0 과 구분한다)."""
    try:
        s = str(v).replace(",", "").replace("+", "").strip()
        return float(s) if s else None
    except (TypeError, ValueError):
        return None


def as_rows(v):
    return [v] if isinstance(v, dict) else [x for x in (v or []) if isinstance(x, dict)]


# ── 기록 (15:05) ──────────────────────────────────────────────────────
def candidates(rows, cap=CAP):
    """15:05 시세에서 부를 종목 — (부를 행, 상한에 잘린 수). 거래대금 큰 순, 같으면 종목코드 순."""
    out = []
    for x in rows:
        c = str(x.get("itemcode", "")).strip()
        if len(c) != 6 or not c.isalnum() or x.get("tradeStopYn") == "Y":
            continue
        if (num(x.get("prevChangeRate")) or 0) >= MIN_CHANGE and (num(x.get("tradeAmount")) or 0) >= MIN_AMOUNT:
            out.append(x)
    out.sort(key=lambda x: (-(num(x.get("tradeAmount")) or 0), str(x.get("itemcode"))))
    return out[:cap], max(0, len(out) - cap)


def headers(key, secret, tok):
    return {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}", "appkey": key,
            "appsecret": secret, "tr_id": TR_ID, "custtype": "P"}


def call_one(get, h, code, clock, sleep):
    """종목 하나 — (기록, 속도 제한 재시도 수). 속도 제한은 1초 쉬고 두 번까지, 통신·형식 오류는 한 번 다시."""
    rec, retries = {"code": code}, 0
    for attempt in range(3):
        rec["calledAt"] = clock().isoformat(timespec="seconds")
        try:
            r = get(BASE + PATH, headers=h, params={"MKSC_SHRN_ISCD": code}, timeout=10)
            js = r.json()
            if not isinstance(js, dict) or "rt_cd" not in js:
                raise ValueError(f"HTTP {getattr(r, 'status_code', '?')} 형식 아님")
        except Exception as e:
            rec.update(rt_cd="", msg_cd="", error=f"{type(e).__name__}: {str(e)[:60]}")
            if attempt == 0:
                sleep(1.0)
                continue
            break
        rec.update(receivedAt=clock().isoformat(timespec="seconds"), rt_cd=str(js.get("rt_cd")),
                   msg_cd=str(js.get("msg_cd", "")), msg1=str(js.get("msg1", ""))[:80], error="",
                   output1=js.get("output1"), output2=as_rows(js.get("output2")))
        if rec["rt_cd"] != "0" and rec["msg_cd"] == RATE_LIMIT and attempt < 2:
            retries += 1
            sleep(1.0)
            continue
        break
    return rec, retries


def hour_gb(x):
    try:
        return int(str(x.get("bsop_hour_gb", "")).strip())
    except ValueError:
        return None


def latest(rows, field):
    """15:05 에 보이는 가장 늦은 입력분의 값 — (값, 입력구분). 입력구분 숫자가 클수록 늦다고 본다(시험 호출로 확인할 것).
    그 칸이 빈 행은 건너뛴다. 없으면 (None, None)."""
    best = None
    for x in rows:
        g, v = hour_gb(x), num(x.get(field))
        if g is not None and v is not None and (best is None or g > best[1]):
            best = (v, g)
    return best or (None, None)


def record(day, market, get, h, clock, sleep, run_id="", cap=CAP, pause=PAUSE):
    """(종목 기록들, 공개 상태). market = 네이버 전 종목 시세 행들. 공개 상태에는 건수만 담는다."""
    cands, capped = candidates(market, cap)
    st = {"day": day, "runId": run_id, "version": VERSION, "startedAt": clock().isoformat(timespec="seconds"),
          "marketRows": len(market), "candidates": len(cands) + capped, "capped": capped, "status": "완료",
          "stopReason": ""}
    deadline = clock().replace(hour=CALL_DEADLINE[0], minute=CALL_DEADLINE[1], second=0, microsecond=0)
    recs, retries, bad_run = [], 0, 0
    for i, x in enumerate(cands):
        if i:
            sleep(pause)
        if clock() >= deadline:                  # 쉰 뒤에 본다 — 쉬는 사이 마감을 넘길 수 있다
            st.update(status="중단", stopReason=f"{CALL_DEADLINE[0]}:{CALL_DEADLINE[1]:02d} 이후 호출 안 함")
            break
        rec, n = call_one(get, h, str(x.get("itemcode", "")).strip(), clock, sleep)
        rec["quote"] = {k: x.get(k, "") for k in KEEP}
        recs.append(rec)
        retries += n
        bad_run = bad_run + 1 if rec["rt_cd"] not in ("0", "") and rec["msg_cd"] != RATE_LIMIT else 0
        if bad_run >= STOP_AFTER:
            st.update(status="중단", stopReason=f"응답 오류 연속 {STOP_AFTER}회 ({rec['msg_cd']})")
            break
    ok = [r for r in recs if r["rt_cd"] == "0"]
    codes = collections.Counter(hour_gb(x) for r in ok for x in r["output2"])
    st.update(finishedAt=clock().isoformat(timespec="seconds"), called=len(recs), ok=len(ok),
              failed=len(recs) - len(ok), rateLimitRetries=retries, withRows=sum(1 for r in ok if r["output2"]),
              latestFilled=sum(1 for r in ok if latest(r["output2"], FIELDS["org"])[0] is not None
                               and latest(r["output2"], FIELDS["frg"])[0] is not None),
              inputCodes="|".join(f"{'?' if k is None else k}:{v}"
                                  for k, v in sorted(codes.items(), key=lambda kv: -1 if kv[0] is None else kv[0])))
    return recs, st


def store(recs, st, meta, uploader, runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    """원본 → 드라이브 비공개(러너 사본은 .gitignore 폴더). 공개 영수증 장부에는 이름·크기·해시만."""
    name = f"research_flowest_{st['day']}_{st['runId'] or 'local'}.json.gz"
    body = dict(meta, version=VERSION, day=st["day"], runId=st["runId"], records=recs)
    data = gzip.compress(json.dumps(body, ensure_ascii=False).encode("utf-8"), mtime=0)
    rc = R.upload_receipt("flowest", st["runId"], name, data, uploader, private_dir)
    R.append_receipts([rc], runs_dir)
    st.update(privateFile=name, privateStatus=rc["status"])
    return rc


def log_run(st, runs_dir=R.RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, RUNS_FILE)
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def already_done(day, runs_dir=R.RUNS_DIR):
    path = os.path.join(runs_dir, RUNS_FILE)
    if not os.path.exists(path):
        return False
    with open(path, encoding="utf-8") as fh:
        return any(r.get("day") == day and r.get("status") == "완료" and r.get("privateStatus") == "접수"
                   for r in csv.DictReader(fh))


def in_window(now, window=WINDOW):
    (sh, sm), (eh, em) = window
    return sh * 60 + sm <= now.hour * 60 + now.minute <= eh * 60 + em


def main_record(env, get, post, clock, sleep, uploader=None, runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    now = clock()
    day = now.strftime("%Y-%m-%d")
    ok, why = R.trading_day(day)
    if not ok:
        print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''} — 기록 생략")
        return 0
    if not in_window(now):
        print(f"ℹ️ 시작 {now:%H:%M} — 창 14:50~15:25 밖이라 기록하지 않는다(13:00 회차·늦게 도착한 실행)")
        return 0
    if already_done(day, runs_dir):
        print(f"ℹ️ {day} 은 이미 기록했다 — 중복 실행으로 보고 끝낸다")
        return 0
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    base = {"day": day, "runId": env.get("GITHUB_RUN_ID", ""), "version": VERSION,
            "startedAt": now.isoformat(timespec="seconds")}
    requested = clock()
    try:
        r = get(MARKET_URL, headers=NAVER_HEADERS, timeout=30)
        market = r.json() if getattr(r, "status_code", 200) == 200 else []
    except Exception as e:
        print(f"⚠️ 시세 조회 실패 {type(e).__name__}")
        market = []
    received = clock()
    if not isinstance(market, list) or len(market) < MIN_ROWS:
        n = len(market) if isinstance(market, list) else "?"
        print(f"❌ 시세 {n}건 — 임계 {MIN_ROWS} 미달, 기록 중단")
        log_run(dict(base, status="중단", stopReason=f"시세 {n}건(임계 {MIN_ROWS})"), runs_dir)
        return 1
    tok = None
    for attempt in range(2):                     # 토큰 발급은 1분에 1회 — 다른 수집과 겹치면 한 번 기다렸다 다시
        try:
            tok = token_for_run(get, post, key, secret, BASE, token)[0]
            break
        except Exception as e:
            print(f"⚠️ {str(e)[:80]}")
            if not attempt:
                sleep(65)
    if not tok:
        log_run(dict(base, status="중단", stopReason="토큰 발급 실패", marketRows=len(market)), runs_dir)
        return 1
    recs, st = record(day, market, get, headers(key, secret, tok), clock, sleep, run_id=base["runId"])
    st["startedAt"] = base["startedAt"]
    meta = {"marketRequestedAt": requested.isoformat(timespec="seconds"),
            "marketReceivedAt": received.isoformat(timespec="seconds"),
            "filter": {"minChange": MIN_CHANGE, "minAmount": MIN_AMOUNT, "cap": CAP, "window": WINDOW,
                       "callDeadline": CALL_DEADLINE}}
    store(recs, st, meta, uploader or R.drive_uploader(), runs_dir, private_dir)
    log_run(st, runs_dir)
    print("🧮 " + json.dumps({k: st.get(k) for k in RUN_COLS}, ensure_ascii=False))
    return 0 if st["status"] == "완료" and st.get("privateStatus") == "접수" else 1


# ── 시험 호출 ─────────────────────────────────────────────────────────
def probe(env, get, post, codes, clock, sleep):
    """응답 구조만 출력한다 — 값·부호는 찍지 않는다(칸마다 '값'·'0'·'빈칸' 만). 저장·커밋 없음."""
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    print(f"# 한투 추정가집계 시험 호출 · {clock().isoformat(timespec='seconds')}")
    try:
        tok = token(post, key, secret)
    except Exception as e:
        print(f"❌ {str(e)[:80]}")
        return 1
    h, worst = headers(key, secret, tok), 0

    def kind(v):
        v = num(v)
        return "빈칸" if v is None else ("0" if v == 0 else "값")

    for i, code in enumerate(codes):
        if i:
            sleep(PAUSE)
        rec, n = call_one(get, h, code, clock, sleep)
        print(f"- {code}: rt_cd={rec['rt_cd']} {rec['msg_cd']} · {rec.get('msg1', '')}{rec.get('error', '')}"
              + (f" · 속도 제한 재시도 {n}회" if n else ""))
        if rec["rt_cd"] != "0":
            worst = 1
            continue
        o1, o2 = as_rows(rec.get("output1")), rec["output2"]
        print(f"  output1 {len(o1)}행 · 필드 {sorted({k for x in o1 for k in x})}")
        print(f"  output2 {len(o2)}행 · 필드 {sorted({k for x in o2 for k in x})}")
        for x in o2:
            print(f"  · 입력구분 {x.get('bsop_hour_gb')} · "
                  + " · ".join(f"{label} {kind(x.get(FIELDS[f]))}"
                               for label, f in (("외국인", "frg"), ("기관", "org"), ("합", "sum"))))
    return worst


# ── 대조 (판정 때 PC 에서 한 번) ───────────────────────────────────────
def load_private(paths):
    """비공개 원본들 → {날짜: 원본}. 같은 날 실행이 여럿이면 시세를 가장 먼저 받은 것(15:05 에 가까운 것)만 쓴다."""
    by_day = {}
    for p in sorted(paths):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            d = json.load(fh)
        if d.get("version") != VERSION:
            continue
        cur = by_day.get(d["day"])
        if cur is None or d.get("marketRequestedAt", "") < cur.get("marketRequestedAt", ""):
            by_day[d["day"]] = d
    return by_day


def both(p, side):
    """K12 의 수급 조건 — 기관 > 0 그리고 외국인 > 0. side 는 'p'(잠정) 또는 'f'(확정)."""
    o, f = p[f"{side}_org"], p[f"{side}_frg"]
    return o is not None and f is not None and o > 0 and f > 0


def pairs_of(by_day, finals):
    """종목·날짜 짝. 잠정 = 15:05 응답의 가장 늦은 입력분(호출 실패면 None), 확정 = finals[(날짜, 코드)] = (외국인, 기관)."""
    out = []
    for day, d in sorted(by_day.items()):
        for r in d.get("records", []):
            q, ok = r.get("quote", {}), r.get("rt_cd") == "0"
            fin = finals.get((day, r["code"]))
            chg, amt, cap = num(q.get("prevChangeRate")), num(q.get("tradeAmount")), num(q.get("marketSum"))
            out.append({"day": day, "code": r["code"],
                        "p_org": latest(r.get("output2", []), FIELDS["org"])[0] if ok else None,
                        "p_frg": latest(r.get("output2", []), FIELDS["frg"])[0] if ok else None,
                        "f_frg": fin[0] if fin else None, "f_org": fin[1] if fin else None,
                        "volume": num(q.get("tradeVolume")), "change": chg,
                        "k12": chg is not None and chg < LIMIT_CHANGE,
                        "u2": (amt or 0) >= U2_AMOUNT and (cap or 0) >= U2_CAP, "vm": None})
    return out


def share(items, hit):
    return sum(1 for p in items if hit(p)) / len(items) if items else None


def boot_ci(items, stat, n=BOOT_N, seed=BOOT_SEED):
    """날짜 묶음 부트스트랩 95% 구간 — 같은 날 종목끼리 수급이 같이 움직이므로 종목이 아니라 날짜를 뽑는다."""
    by = collections.defaultdict(list)
    for it in items:
        by[it["day"]].append(it)
    keys, rnd, vals = sorted(by), random.Random(seed), []
    if not keys:
        return None, None
    for _ in range(n):
        v = stat([it for k in (rnd.choice(keys) for _ in keys) for it in by[k]])
        if v is not None:
            vals.append(v)
    vals.sort()
    return (vals[int(0.025 * len(vals))], vals[min(len(vals) - 1, int(0.975 * len(vals)))]) if vals else (None, None)


def big(p, side_field):
    """확정 순매수 크기가 15:05 거래량의 1% 이상인가 — 0 근처 부호는 작은 오차로도 뒤집힌다."""
    return bool(p["volume"]) and abs(p[side_field]) / p["volume"] >= 0.01


# 이름 → (맞음 판정, 분모 조건). 분모 조건이 None 이면 그 묶음 전체.
METRICS = {
    "기관 부호 일치": (lambda p: (p["p_org"] > 0) == (p["f_org"] > 0), None),
    "외국인 부호 일치": (lambda p: (p["p_frg"] > 0) == (p["f_frg"] > 0), None),
    "동반 조건 일치": (lambda p: both(p, "p") == both(p, "f"), None),
    "정밀도(잠정 통과 중 확정도 통과)": (lambda p: both(p, "f"), lambda p: both(p, "p")),
    "재현율(확정 통과 중 잠정도 통과)": (lambda p: both(p, "p"), lambda p: both(p, "f")),
    "확정 통과 비율(기준선)": (lambda p: both(p, "f"), None),
    "기관 부호 일치 — 확정 크기 1%↑": (lambda p: (p["p_org"] > 0) == (p["f_org"] > 0), lambda p: big(p, "f_org")),
    "기관 부호 일치 — 확정 크기 1% 미만": (lambda p: (p["p_org"] > 0) == (p["f_org"] > 0),
                                   lambda p: not big(p, "f_org")),
    "외국인 부호 일치 — 확정 크기 1%↑": (lambda p: (p["p_frg"] > 0) == (p["f_frg"] > 0), lambda p: big(p, "f_frg")),
    "외국인 부호 일치 — 확정 크기 1% 미만": (lambda p: (p["p_frg"] > 0) == (p["f_frg"] > 0),
                                    lambda p: not big(p, "f_frg")),
}


def metric(items, hit, among):
    sel = [p for p in items if among is None or among(p)]
    lo, hi = boot_ci(sel, lambda s: share(s, hit))
    return {"value": share(sel, hit), "n": len(sel), "days": len({p["day"] for p in sel}), "ci95": [lo, hi]}


def picks(pairs):
    """하루 한 종목(K12) — 15:05 후보 중 등락률 < 29.5% 에서 거래량 배수 최대(같으면 종목코드 순).
    잠정 조건으로 고른 것과, **같은 후보 안에서** 확정 조건으로 고른 것을 비교한다."""
    days = collections.defaultdict(list)
    for p in pairs:
        if p["k12"] and p["vm"] is not None:
            days[p["day"]].append(p)

    def pick(items, side):
        ok = sorted((p for p in items if both(p, side)), key=lambda p: (-p["vm"], p["code"]))
        return ok[0] if ok else None

    out = {"days": len(days), "provisionalPickDays": 0, "finalPickDays": 0, "sameCodeDays": 0,
           "provisionalPickFinalPass": 0, "provisionalPickFinalMissing": 0}
    for items in days.values():
        a, b = pick(items, "p"), pick(items, "f")
        out["provisionalPickDays"] += a is not None
        out["finalPickDays"] += b is not None
        out["sameCodeDays"] += a is not None and b is not None and a["code"] == b["code"]
        if a is not None:
            if a["f_org"] is None or a["f_frg"] is None:
                out["provisionalPickFinalMissing"] += 1
            else:
                out["provisionalPickFinalPass"] += both(a, "f")
    return out


def summarize(pairs):
    full = [p for p in pairs if None not in (p["p_org"], p["p_frg"], p["f_org"], p["f_frg"])]
    groups = {"전체 호출 종목": full,
              "K12 후보(15:05 등락률 < 29.5%)": [p for p in full if p["k12"]],
              "K12 후보 중 사용자 필터(500억·1,000억)": [p for p in full if p["k12"] and p["u2"]]}
    return {"version": VERSION, "pairs": len(pairs), "complete": len(full), "days": len({p["day"] for p in full}),
            "noProvisional": sum(1 for p in pairs if p["p_org"] is None or p["p_frg"] is None),
            "noFinal": sum(1 for p in pairs if p["f_org"] is None or p["f_frg"] is None),
            "groups": {g: {"n": len(items), "days": len({p["day"] for p in items}),
                           "metrics": {name: metric(items, hit, among) for name, (hit, among) in METRICS.items()}}
                       for g, items in groups.items()},
            "picks": picks(pairs)}


def kis_caller(get, key, secret, tok, sleep=time.sleep):
    """(경로, tr, 입력) → 정상 응답 dict, 실패면 None. 속도 제한 · 통신 오류는 1초 쉬고 두 번까지 다시."""
    def call(path, tr_id, params):
        h = dict(headers(key, secret, tok), tr_id=tr_id)
        for attempt in range(3):
            try:
                js = get(BASE + path, headers=h, params=params, timeout=15).json()
            except Exception:
                js = None
            js = js if isinstance(js, dict) else None
            if js is not None and str(js.get("rt_cd")) == "0":
                return js
            if attempt < 2 and (js is None or js.get("msg_cd") == RATE_LIMIT):
                sleep(1.0)
                continue
            return None
    return call


def fetch_finals(need, kis, sleep=time.sleep):
    """need = {코드: {날짜}} → ({(날짜, 코드): (외국인, 기관)}, 실패 종목 수). 한투 종목 투자자의 일별 확정 순매수 수량.
    최근 30거래일만 주므로 판정은 첫 기록일부터 30거래일 안에 한다. 응답에 없는 날의 짝은 '확정 없음' 으로 남는다."""
    out, fails = {}, 0
    for code, days in sorted(need.items()):
        want = {d.replace("-", "") for d in days}
        js = kis(INVESTOR_PATH, INVESTOR_TR, {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        if js is None:
            fails += 1
        else:
            for x in as_rows(js.get("output")):
                b = str(x.get("stck_bsop_date", ""))
                if b in want:
                    out[(f"{b[:4]}-{b[4:6]}-{b[6:]}", code)] = (num(x.get("frgn_ntby_qty")), num(x.get("orgn_ntby_qty")))
        sleep(PAUSE)
    return out, fails


def volume_multiples(need, kis, sleep=time.sleep):
    """need = [(날짜, 코드, 15:05 거래량)] → {(날짜, 코드): 15:05 거래량 ÷ 그날 전 20거래일 평균 거래량}.
    그날 봉은 쓰지 않는다(15:05 에 모르는 값). 20봉이 안 되면 뺀다. 거래량은 한투 일봉 누적(수정주가 기준)."""
    by, out = collections.defaultdict(list), {}
    for day, code, vol in need:
        by[code].append((day, vol))
    for code, items in sorted(by.items()):
        first = datetime.date.fromisoformat(min(d for d, _ in items)) - datetime.timedelta(days=DAILY_LOOKBACK)
        js = kis(DAILY_PATH, DAILY_TR, {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
                                        "FID_INPUT_DATE_1": first.strftime("%Y%m%d"),
                                        "FID_INPUT_DATE_2": max(d for d, _ in items).replace("-", ""),
                                        "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
        bars = sorted((f"{b[:4]}-{b[4:6]}-{b[6:]}", num(x.get("acml_vol")))
                      for x in as_rows((js or {}).get("output2")) for b in [str(x.get("stck_bsop_date", ""))]
                      if len(b) == 8 and num(x.get("acml_vol")) is not None)
        for day, vol in items:
            prev = [v for d, v in bars if d < day][-20:]
            if len(prev) == 20 and sum(prev) > 0 and vol:
                out[(day, code)] = vol / (sum(prev) / 20)
        sleep(PAUSE)
    return out


def main_reconcile(src, out_dir, env, get, post, clock, sleep):
    today = clock().strftime("%Y-%m-%d")
    by_day = {d: v for d, v in load_private(glob.glob(os.path.join(src, "research_flowest_*.json.gz"))).items()
              if d < today}                       # 오늘 것은 확정 전이다
    if not by_day:
        print("❌ 대조할 비공개 원본이 없다(오늘 것은 확정 전이라 뺀다)")
        return 1
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    kis = kis_caller(get, key, secret, token(post, key, secret), sleep)
    need = collections.defaultdict(set)
    for p in pairs_of(by_day, {}):
        need[p["code"]].add(p["day"])
    finals, fails = fetch_finals(need, kis, sleep)
    pairs = pairs_of(by_day, finals)
    vm = volume_multiples([(p["day"], p["code"], p["volume"]) for p in pairs
                           if p["k12"] and (both(p, "p") or both(p, "f"))], kis, sleep)
    for p in pairs:
        p["vm"] = vm.get((p["day"], p["code"]))
    s = dict(summarize(pairs), finalFetchFailures=fails, reconciledAt=clock().isoformat(timespec="seconds"),
             recordDays=sorted(by_day))
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(s, fh, ensure_ascii=False, indent=1)
    with open(os.path.join(out_dir, "pairs.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(pairs[0]) if pairs else ["day"], lineterminator="\n")
        w.writeheader()
        w.writerows(pairs)
    print(f"🧮 대조 {len(by_day)}일 · 짝 {s['pairs']} · 완전 {s['complete']} · 잠정 없음 {s['noProvisional']} · "
          f"확정 없음 {s['noFinal']} · 확정 받기 실패 종목 {fails}")
    for g, v in s["groups"].items():
        print(f"## {g} — {v['n']}쌍 · {v['days']}일")
        for name, m in v["metrics"].items():
            val = "—" if m["value"] is None else f"{m['value']:.1%}"
            ci = "" if m["ci95"][0] is None else f" (95% {m['ci95'][0]:.1%}~{m['ci95'][1]:.1%})"
            print(f"- {name}: {val}{ci} · 분모 {m['n']}")
    print(f"## 하루 한 종목 — {json.dumps(s['picks'], ensure_ascii=False)}")
    print(f"📄 {out_dir}/summary.json · pairs.csv (비공개 — 저장소에 올리지 않는다)")
    return 0


def main(argv=None, env=None, get=None, post=None, clock=None, sleep=time.sleep, uploader=None,
         runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    ap = argparse.ArgumentParser(description="15:05 장중 잠정 수급 기록 (한투 추정가집계)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--probe", action="store_true", help="시험 호출 — 응답 구조만 출력, 저장 없음")
    mode.add_argument("--reconcile", metavar="DIR", help="비공개 원본 폴더 → 확정치와 일치율 요약(로컬)")
    ap.add_argument("--codes", default=",".join(PROBE_CODES), help="시험 호출 종목(쉼표)")
    ap.add_argument("--out", default=os.path.join(R.PRIVATE_DIR, "flowest_reconcile"), help="대조 결과 폴더")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    clock = clock or (lambda: datetime.datetime.now(KST))
    if get is None or post is None:
        import requests
        get, post = get or requests.get, post or requests.post
    if a.probe:
        return probe(env, get, post, [c.strip() for c in a.codes.split(",") if c.strip()], clock, sleep)
    if a.reconcile:
        return main_reconcile(a.reconcile, a.out, env, get, post, clock, sleep)
    return main_record(env, get, post, clock, sleep, uploader, runs_dir, private_dir)


if __name__ == "__main__":
    sys.exit(main())
