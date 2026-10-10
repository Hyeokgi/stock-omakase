# -*- coding: utf-8 -*-
# ==========================================================================
# 🗞️ OpenDART 공시 재료 수집 `dart-disclosures-v1` — 장중 반복 조회로 '처음 본 시각' 을 남긴다
# --------------------------------------------------------------------------
# 사전 고정: docs/사전고정_2026-10-08_DART공시수집_v1.md (이 코드보다 먼저 커밋).
# 공시 목록에는 접수 날짜만 있고 시각이 없다 → 10분 간격으로 조회해 접수번호가 처음 나타난 조회 시각을 기록한다.
#   first_seen_at = 실제 공시 시각의 상한, prev_poll_at = 하한. 15:05 판단에는 first_seen_at ≤ 15:05 인 것만 쓴다.
# 저장: 원시 목록은 구글 드라이브 비공개(기존 쓰기 전용 창구), 공개 저장소에는 건수만(dart_runs.csv · private_receipts.csv).
# 멈춤: 한도 초과(020)·키 문제(010/011/012/901)면 그날 중단 — 우회·키 교체 안 함. 013(결과 없음)은 정상.
# 운영 선정·주문·시트와 무관. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import datetime
import gzip
import json
import os
import sys
import time

import hyeoks_research_daily as R

VERSION = "dart-disclosures-v1"
URL = "https://opendart.fss.or.kr/api/list.json"
KST = R.KST
RUNS_DIR = R.RUNS_DIR
CORP_CLS = ("Y", "K")                     # 유가증권 · 코스닥
PAGE_COUNT = 100
PAGE_CAP = 30
GAP_WARN_MIN = 20                         # 15:05 직전 조회가 이보다 오래되면 '불완전'
STOP_STATUS = {"020": "한도초과", "010": "키미등록", "011": "키사용불가", "012": "접근불가IP", "901": "키만료"}
KEEP = ("rcept_no", "corp_code", "stock_code", "corp_name", "corp_cls", "report_nm", "rcept_dt", "rm", "flr_nm")
RUN_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "polls", "pollFailures", "disclosures",
            "withStockCode", "seenBy1505", "lastPollBefore1505", "coverage1505", "capped", "status", "stopReason",
            "privateFile", "privateStatus"]


class Stop(Exception):
    """그날 수집을 멈춰야 하는 응답(한도·키)."""


def _ymd(d):
    return d.replace("-", "")


def fetch_page(get, key, bgn, end, cls, page):
    """(목록, 전체 페이지 수). 013 은 빈 목록. 멈춤 상태면 Stop, 그 밖의 오류는 RuntimeError."""
    r = get(URL, params={"crtfc_key": key, "bgn_de": _ymd(bgn), "end_de": _ymd(end), "corp_cls": cls,
                         "page_no": page, "page_count": PAGE_COUNT, "sort": "date", "sort_mth": "desc"}, timeout=20)
    if getattr(r, "status_code", 200) != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    js = r.json()
    st = str(js.get("status", ""))
    if st in STOP_STATUS:
        raise Stop(f"{st} {STOP_STATUS[st]}")
    if st == "013":
        return [], 0
    if st != "000":
        raise RuntimeError(f"status {st}")
    return js.get("list") or [], int(js.get("total_page") or 1)


def poll(get, key, bgn, end, seen, pause=0.2, sleep=time.sleep):
    """한 번 조회. (새로 본 항목들, 상한 도달 여부). 최신순이라 한 페이지가 전부 이미 본 것이면 거기서 멈춘다.
    조회 도중 실패하면 예외 — `seen` 은 건드리지 않는다(부분 결과를 '본 것' 으로 만들지 않는다)."""
    fresh, capped, local = [], False, set()
    for cls in CORP_CLS:
        page, total = 1, 1
        while page <= total:
            if page > PAGE_CAP:
                capped = True
                break
            items, total = fetch_page(get, key, bgn, end, cls, page)
            new = [x for x in items if x.get("rcept_no") and x["rcept_no"] not in seen and x["rcept_no"] not in local]
            local.update(x["rcept_no"] for x in new)
            fresh += new
            if items and not new:
                break
            page += 1
            sleep(pause)
    return fresh, capped


def coverage(poll_times, day):
    """15:05 범위 판정 — (15:05 이전 마지막 조회 시각, '완전'/'불완전'/'조회없음')."""
    cut = datetime.datetime.fromisoformat(f"{day}T15:05:00+09:00")
    before = [t for t in poll_times if t <= cut]
    if not before:
        return "", "조회없음"
    last = max(before)
    return last.isoformat(timespec="seconds"), ("완전" if cut - last <= datetime.timedelta(minutes=GAP_WARN_MIN) else "불완전")


def run(day, prev_day, get, key, until, every, clock, sleep, run_id=""):
    """조회 반복. (기록 행들, 공개 상태 dict). 기록 행에는 처음 본 시각과 직전 조회 시각을 붙인다."""
    seen, rows, polls, fails, capped_any = set(), [], [], 0, False
    st = {"day": day, "runId": run_id, "version": VERSION, "startedAt": clock().isoformat(timespec="seconds"),
          "status": "완료", "stopReason": ""}
    prev = None
    while True:
        now = clock()
        if now > until:
            break
        bgn = prev_day if not polls else day        # 첫 조회는 전 거래일부터(밤사이 공시)
        try:
            fresh, capped = poll(get, key, bgn, day, seen, sleep=sleep)
            seen.update(x["rcept_no"] for x in fresh)
            capped_any |= capped
            for x in fresh:
                row = {k: x.get(k, "") for k in KEEP}
                row.update(first_seen_at=now.isoformat(timespec="seconds"),
                           prev_poll_at=prev.isoformat(timespec="seconds") if prev else "", poll_no=len(polls) + 1)
                rows.append(row)
            polls.append(now)
            prev = now
        except Stop as e:
            st.update(status="중단", stopReason=str(e))
            break
        except Exception as e:                       # 그 조회만 실패 — 다음 조회에서 다시
            fails += 1
            print(f"⚠️ 조회 실패 {type(e).__name__}: {str(e)[:60]}")
        nxt = now + datetime.timedelta(seconds=every)
        if nxt > until:
            break
        sleep(max(0.0, (nxt - clock()).total_seconds()))
    last1505, cov = coverage(polls, day)
    cut = f"{day}T15:05:00+09:00"
    st.update(finishedAt=clock().isoformat(timespec="seconds"), polls=len(polls), pollFailures=fails,
              disclosures=len(rows), withStockCode=sum(1 for r in rows if (r.get("stock_code") or "").strip()),
              seenBy1505=sum(1 for r in rows if r["first_seen_at"] <= cut), lastPollBefore1505=last1505,
              coverage1505=cov, capped=capped_any)
    if not polls and st["status"] == "완료":
        st["status"] = "조회없음"
    return rows, st


def store(rows, st, uploader, runs_dir=RUNS_DIR, private_dir=R.PRIVATE_DIR):
    name = f"research_dart_{st['day']}_{st['runId'] or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"version": VERSION, "day": st["day"], "runId": st["runId"], "rows": rows},
                                    ensure_ascii=False).encode("utf-8"), mtime=0)
    rc = R.upload_receipt("dart", st["runId"], name, data, uploader, private_dir)
    R.append_receipts([rc], runs_dir)
    st.update(privateFile=name, privateStatus=rc["status"])
    return rc


def log_run(st, runs_dir=RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "dart_runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def prev_trading_day(day):
    d = datetime.date.fromisoformat(day)
    for _ in range(15):
        d -= datetime.timedelta(days=1)
        if R.trading_day(d.isoformat())[0]:
            return d.isoformat()
    return day


def main(argv=None, env=None, get=None, clock=None, sleep=time.sleep, uploader=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="15:08")
    ap.add_argument("--every", type=int, default=600)
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
    key = env.get("DART_API_KEY", "")
    if not key:
        print("❌ DART_API_KEY 없음")
        return 1
    if get is None:
        import requests
        get = requests.get
    rows, st = run(day, prev_trading_day(day), get, key, until, max(300, a.every), clock, sleep,
                   run_id=env.get("GITHUB_RUN_ID", ""))
    store(rows, st, uploader or R.drive_uploader())
    log_run(st)
    print("🗞️ " + json.dumps({k: st.get(k) for k in RUN_COLS}, ensure_ascii=False))
    return 0 if st["status"] in ("완료", "조회없음") and st.get("privateStatus") == "접수" else 1


if __name__ == "__main__":
    sys.exit(main())
