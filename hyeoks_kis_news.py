# -*- coding: utf-8 -*-
# ==========================================================================
# 📰 한투 시황·공시 제목 수집 `kis-news-v2` — 5분 간격 · 최근 40건 · 넘침 의심 알림
# --------------------------------------------------------------------------
# 사전 고정: docs/사전고정_2026-10-08_한투뉴스제목수집_v1.md (이 코드보다 먼저 커밋).
# 사용자 지시: "우선 10분간격으로 40건이라도 적용해보자. 오버되면 최신순으로 자르고 도입하고 그런경우가 있다면 알려줘."
#   2026-10-09 사용자: "한투 뉴스 간격은 5분으로 바꿔줘" → v2 (10/8 첫날 넘침 의심 1회). 넘침 판정·저장은 v1 과 같다.
#   → 40건이 꽉 찼는데 이미 본 것이 하나도 없으면 '넘침 의심'. 받은 최신 40건만 두고, 놓쳤을 수 있는 구간을 기록한다.
# 저장: 원본은 구글 드라이브 비공개, 공개는 건수만(kis_news_runs.csv · private_receipts.csv). 로그에 제목·종목명 없음.
# 운영 KIS 토큰(구글 시트 칸)은 읽지도 쓰지도 않는다 — 실행당 토큰을 따로 1회 발급한다.
# 운영 선정·주문·시트와 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import datetime
import gzip
import hashlib
import json
import os
import sys
import time

import hyeoks_research_daily as R

VERSION = "kis-news-v2"
BASE = "https://openapi.koreainvestment.com:9443"
PATH = "/uapi/domestic-stock/v1/quotations/news-title"
TR_ID = "FHKST01011800"
PARAMS = {"FID_NEWS_OFER_ENTP_CODE": "", "FID_COND_MRKT_CLS_CODE": "", "FID_INPUT_ISCD": "", "FID_TITL_CNTT": "",
          "FID_INPUT_DATE_1": "", "FID_INPUT_HOUR_1": "", "FID_RANK_SORT_CLS_CODE": "", "FID_INPUT_SRNO": ""}
PAGE = 40
STOP_AFTER = 3                    # rt_cd ≠ 0 이 연속 3회면 그날 중단
KST = R.KST
RUN_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "polls", "pollFailures", "news", "withStockCode",
            "publishedBy1505", "overflowPolls", "overflowWindows", "status", "stopReason", "privateFile", "privateStatus"]


def news_key(x):
    s = (x.get("cntt_usiq_srno") or "").strip()
    if s:
        return s
    raw = "|".join(str(x.get(k, "")) for k in ("data_dt", "data_tm", "news_ofer_entp_code", "hts_pbnt_titl_cntt"))
    return "h" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def pub_at(x):
    d, t = (x.get("data_dt") or "").strip(), (x.get("data_tm") or "").strip().rjust(6, "0")
    if len(d) != 8:
        return ""
    return f"{d[:4]}-{d[4:6]}-{d[6:]}T{t[:2]}:{t[2:4]}:{t[4:6]}+09:00"


def token(post, key, secret):
    r = post(f"{BASE}/oauth2/tokenP", json={"grant_type": "client_credentials", "appkey": key, "appsecret": secret},
             headers={"content-type": "application/json"}, timeout=10)
    tok = r.json().get("access_token") if getattr(r, "status_code", 0) == 200 else None
    if not tok:
        raise RuntimeError(f"토큰 발급 실패 HTTP {getattr(r, 'status_code', '?')}")
    return tok


def fetch(get, key, secret, tok):
    h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}", "appkey": key,
         "appsecret": secret, "tr_id": TR_ID, "custtype": "P"}
    r = get(BASE + PATH, headers=h, params=PARAMS, timeout=15)
    if getattr(r, "status_code", 200) != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    js = r.json()
    if str(js.get("rt_cd")) != "0":
        raise PermissionError(f"rt_cd {js.get('rt_cd')} {js.get('msg_cd', '')}")
    rows = js.get("output") or []
    return [rows] if isinstance(rows, dict) else rows


def run(day, fetch_once, until, every, clock, sleep, run_id=""):
    """(기록 행들, 공개 상태). fetch_once() → 최근 목록. 넘침 의심을 센다."""
    seen, rows, polls, fails, bad_run = set(), [], 0, 0, 0
    newest_seen, overflow = "", []
    st = {"day": day, "runId": run_id, "version": VERSION, "startedAt": clock().isoformat(timespec="seconds"),
          "status": "완료", "stopReason": ""}
    while True:
        now = clock()
        if now > until:
            break
        try:
            items = fetch_once()
            bad_run = 0
            fresh = [x for x in items if news_key(x) not in seen]
            if polls and len(items) >= PAGE and len(fresh) == len(items):
                oldest = min((pub_at(x) for x in items if pub_at(x)), default="")
                overflow.append({"poll": polls + 1, "at": now.isoformat(timespec="seconds"),
                                 "gapFrom": newest_seen, "gapTo": oldest})
                print(f"⚠️ 넘침 의심 — {polls + 1}번째 조회: 40건 모두 새 뉴스. 놓쳤을 수 있는 구간 {newest_seen[11:19]}~{oldest[11:19]}")
            for x in fresh:
                k = news_key(x)
                seen.add(k)
                row = dict(x, key=k, published_at=pub_at(x), received_at=now.isoformat(timespec="seconds"),
                           poll_no=polls + 1)
                rows.append(row)
            pubs = [pub_at(x) for x in items if pub_at(x)]
            if pubs:
                newest_seen = max([newest_seen] + pubs)
            polls += 1
        except PermissionError as e:
            fails += 1
            bad_run += 1
            print(f"⚠️ 응답 오류 {str(e)[:60]}")
            if bad_run >= STOP_AFTER:
                st.update(status="중단", stopReason=f"응답 오류 연속 {STOP_AFTER}회")
                break
        except Exception as e:
            fails += 1
            print(f"⚠️ 조회 실패 {type(e).__name__}")
        nxt = now + datetime.timedelta(seconds=every)
        if nxt > until:
            break
        sleep(max(0.0, (nxt - clock()).total_seconds()))
    cut = f"{day}T15:05:00+09:00"
    st.update(finishedAt=clock().isoformat(timespec="seconds"), polls=polls, pollFailures=fails, news=len(rows),
              withStockCode=sum(1 for r in rows if any((r.get(f"iscd{i}") or "").strip() for i in range(1, 11))),
              publishedBy1505=sum(1 for r in rows if r["published_at"] and r["published_at"] <= cut
                                  and r["received_at"] <= cut),
              overflowPolls=len(overflow), overflowWindows=";".join(f"{o['gapFrom'][11:19]}~{o['gapTo'][11:19]}"
                                                                    for o in overflow))
    if not polls and st["status"] == "완료":
        st["status"] = "조회없음"
    return rows, st, overflow


def store(rows, overflow, st, uploader, runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    name = f"research_kisnews_{st['day']}_{st['runId'] or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"version": VERSION, "day": st["day"], "runId": st["runId"], "rows": rows,
                                     "overflow": overflow}, ensure_ascii=False).encode("utf-8"), mtime=0)
    rc = R.upload_receipt("kisnews", st["runId"], name, data, uploader, private_dir)
    R.append_receipts([rc], runs_dir)
    st.update(privateFile=name, privateStatus=rc["status"])
    return rc


def log_run(st, runs_dir=R.RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "kis_news_runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def main(argv=None, env=None, get=None, post=None, clock=None, sleep=time.sleep, uploader=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="15:08")
    ap.add_argument("--every", type=int, default=300)
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    clock = clock or (lambda: datetime.datetime.now(KST))
    now = clock()
    day = now.strftime("%Y-%m-%d")
    ok, why = R.trading_day(day)
    if not ok:
        print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''} — 수집 생략")
        return 0
    until, capped = R.poll_deadline(now, a.until)
    if capped:
        print(f"ℹ️ 깃허브 작업 한도 때문에 {until:%H:%M} 까지만 조회한다 — 뒤 구간은 다음 실행이 맡는다")
    if now > until:
        print(f"ℹ️ {a.until} 이 지났다 — 앞 실행이 끝냈거나 늦게 시작했다. 아무것도 하지 않는다")
        return 0
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    if get is None or post is None:
        import requests
        get, post = get or requests.get, post or requests.post
    try:
        tok = token(post, key, secret)
    except Exception as e:
        print(f"❌ {str(e)[:80]}")
        st = {"day": day, "runId": env.get("GITHUB_RUN_ID", ""), "version": VERSION, "status": "중단",
              "stopReason": "토큰 발급 실패", "polls": 0}
        log_run(st)
        return 1
    rows, st, overflow = run(day, lambda: fetch(get, key, secret, tok), until, max(300, a.every), clock, sleep,
                             run_id=env.get("GITHUB_RUN_ID", ""))
    store(rows, overflow, st, uploader or R.drive_uploader())
    log_run(st)
    print("📰 " + json.dumps({k: st.get(k) for k in RUN_COLS}, ensure_ascii=False))
    return 0 if st["status"] in ("완료", "조회없음") and st.get("privateStatus") == "접수" else 1


if __name__ == "__main__":
    sys.exit(main())
