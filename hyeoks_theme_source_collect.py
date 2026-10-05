# -*- coding: utf-8 -*-
# ==========================================================================
# 🗂️ 외부 테마 자료원 병행 수집 — stockinfo7.com 테마랭킹 (연구 전용)
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-05): "stockinfo7는 그대로 수집해. 상용화 할 생각은 없어."
#   → 운영자 이용 범위 답변을 기다리지 않고 **개인 비영리 연구** 로 수집한다(사용자 결정).
#   Codex 권고(2026-10-05 후속) 중 보관 원칙은 따른다: 화면 원자료는 **비공개**(구글 드라이브),
#   공개 저장소에는 구조 메타(시각·상태·건수·지문)만. 원본 재배포는 하지 않는다.
#
# 하는 것: 거래일 오후(기본 ~16:35 KST까지) `/theme/rank/list` 를 10분 간격으로 읽는다.
#   · 관측마다 공개 메타 한 줄: 요청 시작·수신 완료(KST) · HTTP · 분류(접속실패/HTTP오류/파싱실패/첫관측/동일/갱신)
#     · 기준 시각 문구 · 카드·종목 행 수 · 내용 지문 → `data/research_runs/stockinfo7_obs.csv`
#   · 첫관측·갱신일 때만 화면 전체(파싱 결과 + 원본 HTML)를 모아 두었다가 드라이브에 올린다
#     — 15:10 이후 첫 관측 때 한 번, 끝날 때 한 번(중단돼도 `finally` 로).
#   · 이 화면은 상승률 상위 종목이 속한 테마만 보여 주는 **선별 화면**이다. 테마 전체 구성 정본이 아니다.
# 하지 않는 것: 선정 조건 변경·주문·시트 쓰기·운영 Gate 연결. 지문 대상 파일이 아니다.
# 공개 메타의 '15시' 문구 수신 시각으로 15:05 판단 시점과의 정렬을 나중에 따진다(간격 관측이라 구간으로만 안다).
# ==========================================================================
import argparse
import csv
import datetime
import gzip
import json
import os
import re
import sys
import time

from hyeoks_theme_source_probe import BASE, UA, KST, classify, parse_rank, rank_state

RUNS_DIR = "data/research_runs"
PRIVATE_DIR = "data/research_private"
OBS_COLS = ["day", "runId", "requestedAt", "receivedAt", "parsedAt", "http", "class", "header", "screenDay",
            "screenHour", "cards", "rows", "digest", "privateFile"]
RUNLOG_COLS = ["day", "runId", "startedAt", "finishedAt", "status", "observations", "valid", "updates", "errors",
               "uploads", "uploadFailed", "stopReason", "stateCommit"]
EVERY_S = 600
MIN_EVERY_S = 300
UNTIL = (16, 35)
CHECKPOINT = (15, 10)
# 🔴 2026-10-05 (Codex 진행 계획 ⑤) — 제공처가 막거나 줄이라고 하면 우회하지 않고 그날 수집을 멈춘다.
HOLD_HTTP = {401: "인증요구", 403: "접근거부", 429: "요청제한"}


def drive_uploader():
    from hyeoks_run_freeze import upload, DEFAULT_GAS_URL, UPLOAD_BACKOFF_LATE

    def up(name, data):
        return upload(DEFAULT_GAS_URL, name, data, backoff=UPLOAD_BACKOFF_LATE)
    return up


def _append(path, cols, rows):
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerows(rows)


def append_obs(rows, runs_dir=RUNS_DIR):
    _append(os.path.join(runs_dir, "stockinfo7_obs.csv"), OBS_COLS, rows)


def screen_time(header):
    """'2026-10-06 15시' → ('2026-10-06', 15). 못 읽으면 ('', None)."""
    m = re.match(r"(\d{4}-\d{2}-\d{2}) (\d{1,2})시$", header or "")
    return (m.group(1), int(m.group(2))) if m else ("", None)


def completed_today(day, run_id, runs_dir=RUNS_DIR):
    """다른 실행이 오늘 관측 창을 정상으로 끝냈는가(Codex R10). 실패·부분 실행은 완료로 보지 않는다."""
    path = os.path.join(runs_dir, "stockinfo7_runs.csv")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r.get("day") == day and r.get("runId") != run_id and r.get("status") in ("완료", "보류"):
                return r
    return None


def flush_private(day, run_id, buf, uploader, stamp, private_dir=PRIVATE_DIR):
    """모아 둔 화면을 한 파일로 드라이브에 올린다. 영수증 dict (이름·크기·sha256·드라이브 id·상태)."""
    if not buf:
        return None
    from hyeoks_research_daily import upload_receipt
    name = f"research_stockinfo7_{day}_{stamp}_{run_id or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"source": BASE + "/theme/rank/list", "day": day, "runId": run_id,
                                     "observations": buf}, ensure_ascii=False).encode("utf-8"), mtime=0)
    return upload_receipt("stockinfo7", run_id, name, data, uploader, private_dir)


def collect(day, get, now=None, sleep=time.sleep, until=UNTIL, every=EVERY_S, run_id="", uploader=None,
            runs_dir=RUNS_DIR, private_dir=PRIVATE_DIR):
    """`until`(KST 시·분)까지 `every` 초 간격으로 관측한다. 결과 요약 dict."""
    from hyeoks_research_daily import append_receipts
    now = now or (lambda: datetime.datetime.now(KST))
    uploader = uploader or drive_uploader()
    every = max(MIN_EVERY_S, every) if every else MIN_EVERY_S
    prev, buf, pending_meta = None, [], []
    archived = {}                     # 내용 지문 → 그 화면을 담은 비공개 파일 (Codex R9)
    out = {"observations": 0, "valid": 0, "updates": 0, "errors": 0, "uploads": [], "uploadFailed": 0,
           "stopReason": "", "windowDone": False}
    checkpoint_done = False

    def flush(stamp):
        rc = flush_private(day, run_id, buf, uploader, stamp, private_dir)
        if rc:
            out["uploads"].append(rc)
            append_receipts([rc], runs_dir)
            if rc["status"] != "접수":
                out["uploadFailed"] += 1
            for o in buf:
                archived[o["meta"]["digest"]] = rc["name"] if rc["status"] == "접수" else f"보관실패:{rc['name']}"
        for m in pending_meta:
            if m["class"] in ("첫관측", "동일", "갱신"):
                # 같은 내용이 마지막으로 보관된 파일을 가리킨다 — '동일' 관측도 원본으로 바로 이어진다
                m["privateFile"] = archived.get(m["digest"], "")
        append_obs(pending_meta, runs_dir)
        buf.clear()
        pending_meta.clear()

    try:
        while True:
            t0 = now()
            if (t0.hour, t0.minute) >= until:
                out["windowDone"] = True
                break
            meta = {"day": day, "runId": run_id, "requestedAt": t0.isoformat(timespec="seconds")}
            try:
                r = get(BASE + "/theme/rank/list", headers={"User-Agent": UA}, timeout=20)
                t1 = now()
                st = rank_state(r.text) if r.status_code == 200 else {"header": "", "cards": 0, "rows": 0, "digest": ""}
                t2 = now()
                kind = classify(r.status_code, st, prev)
                sday, shour = screen_time(st["header"])
                if kind in ("첫관측", "동일", "갱신") and sday != day:
                    kind = "날짜불일치"           # 다른 날짜 화면은 오늘 비교에 넣지 않는다 (Codex R8)
                login = r.status_code == 200 and re.search(r'(?i)type="password"|/member/page/login', r.text or "")
                meta.update(receivedAt=t1.isoformat(timespec="seconds"), parsedAt=t2.isoformat(timespec="seconds"),
                            http=r.status_code, header=st["header"], screenDay=sday,
                            screenHour="" if shour is None else shour, cards=st["cards"], rows=st["rows"],
                            digest=st["digest"])
                if kind in ("첫관측", "갱신"):
                    header, cards = parse_rank(r.text)
                    buf.append({"meta": dict(meta, **{"class": kind}), "header": header, "cards": cards, "html": r.text})
                if kind in ("첫관측", "동일", "갱신"):
                    prev = st
                    out["valid"] += 1
                    out["updates"] += kind == "갱신"
                else:
                    out["errors"] += 1
                if r.status_code in HOLD_HTTP:
                    out["stopReason"] = f"{HOLD_HTTP[r.status_code]}(HTTP {r.status_code})"
                elif login and not st["cards"]:
                    out["stopReason"] = "로그인요구"
            except Exception as e:
                kind = "접속실패"
                meta.update(receivedAt="", parsedAt="", http="", header=type(e).__name__)
                out["errors"] += 1
            meta["class"] = kind
            pending_meta.append(meta)
            out["observations"] += 1
            print(f"{meta['requestedAt']} {kind} {meta.get('header', '')} 카드 {meta.get('cards', '')} 지문 {meta.get('digest', '')}",
                  flush=True)
            if out["stopReason"]:
                print(f"⛔ 수집 보류: {out['stopReason']} — 우회하지 않고 오늘 수집을 멈춘다")
                break
            t = now()
            if not checkpoint_done and (t.hour, t.minute) >= CHECKPOINT:
                checkpoint_done = True
                flush(t.strftime("%H%M%S"))
            nxt = t + datetime.timedelta(seconds=every)
            if (nxt.hour, nxt.minute) >= until:
                out["windowDone"] = True
                break
            sleep(every)
    finally:
        flush(now().strftime("%H%M%S"))
    return out


def run_status(out):
    if out.get("stopReason"):
        return "보류"
    if out.get("windowDone") and out.get("valid") and not out.get("uploadFailed"):
        return "완료"
    return "부분" if out.get("valid") else "실패"


def main(argv=None, env=None, now=None, get=None, runs_dir=RUNS_DIR):
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, default=EVERY_S)
    ap.add_argument("--until", default="16:35")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    now = now or datetime.datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    run_id = env.get("GITHUB_RUN_ID", "")
    from hyeoks_research_daily import trading_day
    ok, why = trading_day(today)
    if not ok:
        print(f"ℹ️ {today} 은 예정 거래일이 아니다{(' — ' + why) if why else ''}. 수집 생략")
        return 0
    hh, mm = (int(x) for x in a.until.split(":"))
    if (now.hour, now.minute) >= (hh, mm):
        print(f"ℹ️ 관측 끝 시각({a.until}) 이후 시작 — 아무것도 하지 않는다")
        return 0
    # GAS(주)·깃허브 예약(백업) 중복 (Codex R10): 다른 실행이 오늘 창을 정상으로 끝냈거나 보류했으면 다시 하지 않는다.
    # 앞 실행이 실패·부분이면 남은 시간을 이어서 관측한다. 상태는 워크플로가 받아 온 **최신 main** 의 요약 파일로 본다.
    done = completed_today(today, run_id, runs_dir)
    if done:
        print(f"ℹ️ 오늘 관측은 실행 {done['runId']} 이 이미 {done['status']} — 중복 수집 생략")
        return 0
    if get is None:
        import requests
        get = requests.get
    started = now
    out = collect(today, get, until=(hh, mm), every=a.every, run_id=run_id, runs_dir=runs_dir)
    status = run_status(out)
    _append(os.path.join(runs_dir, "stockinfo7_runs.csv"), RUNLOG_COLS, [{
        "day": today, "runId": run_id, "startedAt": started.isoformat(timespec="seconds"),
        "finishedAt": datetime.datetime.now(KST).isoformat(timespec="seconds"), "status": status,
        "observations": out.get("observations", 0), "valid": out.get("valid", 0), "updates": out.get("updates", 0),
        "errors": out.get("errors", 0), "uploads": len(out.get("uploads", [])), "uploadFailed": out.get("uploadFailed", 0),
        "stopReason": out.get("stopReason", ""),
        "stateCommit": env.get("RESEARCH_STATE_COMMIT", "")}])
    print("📊 " + json.dumps({k: v for k, v in out.items() if k != "uploads"}, ensure_ascii=False) + f" · 상태 {status}")
    return 0 if status in ("완료", "보류") else 1


if __name__ == "__main__":
    sys.exit(main())
