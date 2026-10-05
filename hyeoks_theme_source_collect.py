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
import sys
import time

from hyeoks_theme_source_probe import BASE, UA, KST, classify, parse_rank, rank_state

RUNS_DIR = "data/research_runs"
PRIVATE_DIR = "data/research_private"
OBS_COLS = ["day", "runId", "requestedAt", "receivedAt", "http", "class", "header", "cards", "rows", "digest", "privateFile"]
EVERY_S = 600
MIN_EVERY_S = 300
UNTIL = (16, 35)
CHECKPOINT = (15, 10)


def drive_uploader():
    from hyeoks_run_freeze import upload, DEFAULT_GAS_URL, UPLOAD_BACKOFF_LATE

    def up(name, data):
        return upload(DEFAULT_GAS_URL, name, data, backoff=UPLOAD_BACKOFF_LATE)
    return up


def append_obs(rows, runs_dir=RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "stockinfo7_obs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=OBS_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerows(rows)


def flush_private(day, run_id, buf, uploader, stamp, private_dir=PRIVATE_DIR):
    """모아 둔 화면을 한 파일로 드라이브에 올린다. (파일 이름, 성공 여부, 오류)."""
    if not buf:
        return None, True, ""
    name = f"research_stockinfo7_{day}_{stamp}_{run_id or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"source": BASE + "/theme/rank/list", "day": day, "runId": run_id,
                                     "observations": buf}, ensure_ascii=False).encode("utf-8"), mtime=0)
    os.makedirs(private_dir, exist_ok=True)
    with open(os.path.join(private_dir, name), "wb") as fh:
        fh.write(data)
    try:
        uploader(name, data)
        return name, True, ""
    except Exception as e:
        return name, False, f"{type(e).__name__}: {str(e)[:80]}"


def collect(day, get, now=None, sleep=time.sleep, until=UNTIL, every=EVERY_S, run_id="", uploader=None,
            runs_dir=RUNS_DIR, private_dir=PRIVATE_DIR):
    """`until`(KST 시·분)까지 `every` 초 간격으로 관측한다. 결과 요약 dict."""
    now = now or (lambda: datetime.datetime.now(KST))
    uploader = uploader or drive_uploader()
    every = max(MIN_EVERY_S, every) if every else MIN_EVERY_S
    prev, buf, pending_meta = None, [], []
    out = {"observations": 0, "valid": 0, "updates": 0, "errors": 0, "uploads": [], "uploadFailed": 0}
    checkpoint_done = False

    def flush(stamp):
        name, ok, err = flush_private(day, run_id, buf, uploader, stamp, private_dir)
        if name:
            out["uploads"].append({"name": name, "ok": ok, "error": err})
            if not ok:
                out["uploadFailed"] += 1
            for m in pending_meta:
                m["privateFile"] = name if ok else f"보관실패:{name}"
        append_obs(pending_meta, runs_dir)
        buf.clear()
        pending_meta.clear()

    try:
        while True:
            t0 = now()
            if (t0.hour, t0.minute) >= until:
                break
            meta = {"day": day, "runId": run_id, "requestedAt": t0.isoformat(timespec="seconds")}
            try:
                r = get(BASE + "/theme/rank/list", headers={"User-Agent": UA}, timeout=20)
                t1 = now()
                st = rank_state(r.text) if r.status_code == 200 else {"header": "", "cards": 0, "rows": 0, "digest": ""}
                kind = classify(r.status_code, st, prev)
                meta.update(receivedAt=t1.isoformat(timespec="seconds"), http=r.status_code, header=st["header"],
                            cards=st["cards"], rows=st["rows"], digest=st["digest"])
                if kind in ("첫관측", "갱신"):
                    header, cards = parse_rank(r.text)
                    buf.append({"meta": dict(meta, **{"class": kind}), "header": header, "cards": cards, "html": r.text})
                if kind in ("첫관측", "동일", "갱신"):
                    prev = st
                    out["valid"] += 1
                    out["updates"] += kind == "갱신"
                else:
                    out["errors"] += 1
            except Exception as e:
                kind = "접속실패"
                meta.update(receivedAt="", http="", header=type(e).__name__)
                out["errors"] += 1
            meta["class"] = kind
            pending_meta.append(meta)
            out["observations"] += 1
            print(f"{meta['requestedAt']} {kind} {meta.get('header', '')} 카드 {meta.get('cards', '')} 지문 {meta.get('digest', '')}",
                  flush=True)
            t = now()
            if not checkpoint_done and (t.hour, t.minute) >= CHECKPOINT:
                checkpoint_done = True
                flush(t.strftime("%H%M%S"))
            nxt = t + datetime.timedelta(seconds=every)
            if (nxt.hour, nxt.minute) >= until:
                break
            sleep(every)
    finally:
        flush(now().strftime("%H%M%S"))
    return out


def main(argv=None, env=None, now=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, default=EVERY_S)
    ap.add_argument("--until", default="16:35")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    now = now or datetime.datetime.now(KST)
    today = now.strftime("%Y-%m-%d")
    from hyeoks_research_daily import trading_day
    ok, why = trading_day(today)
    if not ok:
        print(f"ℹ️ {today} 은 예정 거래일이 아니다{(' — ' + why) if why else ''}. 수집 생략")
        return 0
    hh, mm = (int(x) for x in a.until.split(":"))
    # GAS 발사(주)와 깃허브 예약(백업)이 둘 다 오면 concurrency 그룹이 뒤 실행을 대기시킨다.
    # 앞 실행이 끝 시각까지 돈 뒤 시작한 실행은 할 일이 없다 — 파일을 건드리지 않고 정상 종료한다.
    if (now.hour, now.minute) >= (hh, mm):
        print(f"ℹ️ 관측 끝 시각({a.until}) 이후 시작 — 오늘 수집은 이미 끝났거나 창이 지났다. 아무것도 하지 않는다")
        return 0
    import requests
    out = collect(today, requests.get, until=(hh, mm), every=a.every, run_id=env.get("GITHUB_RUN_ID", ""))
    print("📊 " + json.dumps(out, ensure_ascii=False))
    return 0 if out["valid"] and not out["uploadFailed"] else 1


if __name__ == "__main__":
    sys.exit(main())
