# -*- coding: utf-8 -*-
# ==========================================================================
# 📰 한투 시황·공시 제목 수집 `kis-news-v3` — 5분 간격 · 최근 40건 · 넘치면 이어 받기 · 남은 넘침 알림
# --------------------------------------------------------------------------
# 사전 고정: docs/사전고정_2026-10-08_한투뉴스제목수집_v1.md (이 코드보다 먼저 커밋).
# 사용자 지시: "우선 10분간격으로 40건이라도 적용해보자. 오버되면 최신순으로 자르고 도입하고 그런경우가 있다면 알려줘."
#   2026-10-09 사용자: "한투 뉴스 간격은 5분으로 바꿔줘" → v2 (10/8 첫날 넘침 의심 1회). 넘침 판정·저장은 v1 과 같다.
#   → 40건이 꽉 찼는데 이미 본 것이 하나도 없으면 '넘침 의심'. 받은 최신 40건만 두고, 놓쳤을 수 있는 구간을 기록한다.
#   2026-10-12 사용자: 넘침 때만 이어 받기(허브 #19) → v3. 넘치면 그 목록의 가장 이른 게시 시각을 입력해
#   더 이전 40건을 받는다(시험 run 38250737183 — 경계 1건 겹침·39건 이전). 이전 조회에서 본 뉴스에 닿을 때까지,
#   한 조회당 최대 CONT_MAX 회. 그래도 못 닿으면 남은 구간만 '넘침' 으로 센다.
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

VERSION = "kis-news-v3"
BASE = "https://openapi.koreainvestment.com:9443"
PATH = "/uapi/domestic-stock/v1/quotations/news-title"
TR_ID = "FHKST01011800"
PARAMS = {"FID_NEWS_OFER_ENTP_CODE": "", "FID_COND_MRKT_CLS_CODE": "", "FID_INPUT_ISCD": "", "FID_TITL_CNTT": "",
          "FID_INPUT_DATE_1": "", "FID_INPUT_HOUR_1": "", "FID_RANK_SORT_CLS_CODE": "", "FID_INPUT_SRNO": ""}
PAGE = 40
STOP_AFTER = 3                    # rt_cd ≠ 0 이 연속 3회면 그날 중단
CONT_MAX = 3                      # 넘친 조회 하나에서 더 거슬러 받는 최대 횟수(v3)
CONT_PAUSE = 0.2                  # 이어 받기 호출 사이 쉼(초)
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


def before_params(before):
    """게시 시각(ISO) → 그 시각 이전 목록을 받는 입력. 시험에서 00YYYYMMDD·0000HHMMSS 형식이 동작했다."""
    if not before:
        return PARAMS
    return dict(PARAMS, FID_INPUT_DATE_1="00" + before[:10].replace("-", ""),
                FID_INPUT_HOUR_1="0000" + before[11:19].replace(":", ""))


def fetch(get, key, secret, tok, before=""):
    h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}", "appkey": key,
         "appsecret": secret, "tr_id": TR_ID, "custtype": "P"}
    r = get(BASE + PATH, headers=h, params=before_params(before), timeout=15)
    if getattr(r, "status_code", 200) != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    js = r.json()
    if str(js.get("rt_cd")) != "0":
        raise PermissionError(f"rt_cd {js.get('rt_cd')} {js.get('msg_cd', '')}")
    rows = js.get("output") or []
    return [rows] if isinstance(rows, dict) else rows


def continue_back(fresh, seen, newest_seen, fetch_before, sleep):
    """넘친 조회를 이전 시각으로 거슬러 이어 받는다(v3). (합친 새 뉴스, 이전 조회까지 닿았는지, 호출 수)."""
    got, keys, calls = list(fresh), {news_key(x) for x in fresh}, 0
    gap_to = min((pub_at(x) for x in got if pub_at(x)), default="")
    while gap_to and calls < CONT_MAX:
        sleep(CONT_PAUSE)
        try:
            page = fetch_before(gap_to)
        except Exception as e:
            print(f"⚠️ 이어 받기 실패 {type(e).__name__}")
            break
        calls += 1
        reached = any(news_key(x) in seen for x in page) or             any(pub_at(x) and newest_seen and pub_at(x) <= newest_seen for x in page)
        new = [x for x in page if news_key(x) not in seen and news_key(x) not in keys]
        got += new
        keys.update(news_key(x) for x in new)
        if reached:
            return got, True, calls
        older = min((pub_at(x) for x in new if pub_at(x)), default="")
        if not older or older >= gap_to:
            break                     # 더 거슬러 가지 못한다
        gap_to = older
    return got, False, calls


def run(day, fetch_once, until, every, clock, sleep, run_id="", fetch_before=None):
    """(기록 행들, 공개 상태). fetch_once() → 최근 목록. fetch_before(시각) → 그 이전 목록(v3).
    넘치면 이어 받고, 이어 받아도 이전 조회에 닿지 못한 것만 넘침으로 센다."""
    seen, rows, polls, fails, bad_run = set(), [], 0, 0, 0
    newest_seen, overflow = "", []
    cont_calls, recovered = 0, 0
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
                closed, calls = False, 0
                if fetch_before is not None:
                    fresh, closed, calls = continue_back(fresh, seen, newest_seen, fetch_before, sleep)
                    cont_calls += calls
                oldest = min((pub_at(x) for x in fresh if pub_at(x)), default="")
                if closed:
                    recovered += 1
                    print(f"↩️ 넘침 — {polls + 1}번째 조회: 이어 받기 {calls}회로 이전 조회까지 닿았다(새 뉴스 {len(fresh)}건)")
                else:
                    overflow.append({"poll": polls + 1, "at": now.isoformat(timespec="seconds"),
                                     "gapFrom": newest_seen, "gapTo": oldest, "contCalls": calls})
                    print(f"⚠️ 넘침 의심 — {polls + 1}번째 조회: 40건 모두 새 뉴스"
                          f"{f'(이어 받기 {calls}회 뒤에도)' if calls else ''}. "
                          f"놓쳤을 수 있는 구간 {newest_seen[11:19]}~{oldest[11:19]}")
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
                                                                    for o in overflow),
              contCalls=cont_calls, recoveredPolls=recovered)
    if not polls and st["status"] == "완료":
        st["status"] = "조회없음"
    return rows, st, overflow


def store(rows, overflow, st, uploader, runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    name = f"research_kisnews_{st['day']}_{st['runId'] or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"version": VERSION, "day": st["day"], "runId": st["runId"], "rows": rows,
                                     "overflow": overflow, "contCalls": st.get("contCalls", 0),
                                     "recoveredPolls": st.get("recoveredPolls", 0)},
                                    ensure_ascii=False).encode("utf-8"), mtime=0)
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
                             run_id=env.get("GITHUB_RUN_ID", ""),
                             fetch_before=lambda before: fetch(get, key, secret, tok, before))
    store(rows, overflow, st, uploader or R.drive_uploader())
    log_run(st)
    print("📰 " + json.dumps({k: st.get(k) for k in RUN_COLS}, ensure_ascii=False))
    print(f"↩️ 이어 받기 {st.get('contCalls', 0)}회 · 넘침을 메운 조회 {st.get('recoveredPolls', 0)}회 · 남은 넘침 {st.get('overflowPolls', 0)}회")
    return 0 if st["status"] in ("완료", "조회없음") and st.get("privateStatus") == "접수" else 1


if __name__ == "__main__":
    sys.exit(main())
