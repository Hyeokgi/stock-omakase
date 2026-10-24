# -*- coding: utf-8 -*-
# 스캐너 후보 자료(Feature Store · 순위 풀)를 공개 저장소 대신 드라이브 비공개로 보관하고 공개에는 건수 색인만 남기는 모듈
"""후보 자료 비공개 보관 (2026-10-25).

왜
  `data/feature_store/*.csv.gz`(그날 스캔 종목 전부의 이름 · 코드 · 가격 · 점수 · 후보 여부)와
  `data/scanner_census/rank_pool.csv`(채널별 상위 10 후보의 코드 · 이름 · 점수)가 9/17~9/18 부터 매일
  공개 저장소에 커밋됐다. 허브 §8 '후보 풀 사본 공개 금지' 와 충돌한다(Codex 검토 2026-10-25 P1).
  사용자 결정 2026-10-25: 비공개 전환 · 과거분도 드라이브로 옮기고 저장소에서 지움 · 순위 풀도 함께.
  `data/run_freeze/` 와 같은 원칙이다 — 후보 풀은 시스템의 신호 그 자체라 드라이브(비공개)만 보존한다.

어떻게
  · 스캐너(omakase.py)는 지금처럼 러너에 파일을 쓴다. 두 경로는 .gitignore 라 커밋되지 않는다.
  · 같은 회차 뒤 단계에서 `python private_store.py --upload` 가 새 파일을 드라이브(GAS 쓰기 전용 창구,
    hyeoks_run_freeze.upload)로 올리고, 공개 색인 `data/scanner_census/private_index.csv` 에
    종류 · 날짜 · 채널 · 행 수 · 바이트 · sha256 · 드라이브 접수 여부만 남긴다. 종목 정보는 한 칸도 없다.
  · '그날 이미 기록됨' 판단은 예전에 커밋된 파일이 맡았다. 이제 이 색인의 '접수' 행이 맡는다
    (feature_store.record · rank_pool.already_recorded 가 기본 경로일 때만 본다).
    업로드가 실패하면 '실패' 행만 남고 접수로 치지 않으므로, 같은 날 뒤 회차가 다시 기록해 올린다
    (그때는 더 늦은 시각의 값이다 — captured_at 이 파일 안에 남는다).

하지 않는 것
  · 선정 · 점수 · 영수증 형식은 바꾸지 않는다(영수증의 feature_store 행 수 · 순위 풀 행 수는 그대로).
  · 드라이브에서 읽어 오지 않는다(창구가 쓰기 전용). 분석 때는 드라이브에서 내려받는다.
"""
import argparse
import csv
import datetime
import glob
import gzip
import hashlib
import io
import os
import sys

KST = datetime.timezone(datetime.timedelta(hours=9))
INDEX = "data/scanner_census/private_index.csv"
FS_DIR = "data/feature_store"
POOL_PATH = "data/scanner_census/rank_pool.csv"
COLS = ["kind", "date", "channel", "rows", "bytes", "sha256", "run_id", "name", "status", "driveId", "error", "at"]
DONE = "접수"


def load(path=None):
    try:
        with open(path or INDEX, encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    except FileNotFoundError:
        return []


def recorded(kind, day, channel="", path=None):
    """그날(· 채널) 자료가 드라이브에 접수됐나. 업로드 실패 행은 치지 않는다."""
    return any(r.get("kind") == kind and r.get("date") == day and (r.get("channel") or "") == channel
               and r.get("status") == DONE for r in load(path))


def _append(rows, path):
    if not rows:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerows(rows)


def _put(uploader, name, data):
    """(상태, 드라이브 id, 오류). 업로드 예외를 밖으로 내지 않는다."""
    try:
        return DONE, str(uploader(name, data) or ""), ""
    except Exception as e:                     # noqa: BLE001 — 스캐너 회차를 죽이지 않는다
        return "실패", "", f"{type(e).__name__}: {str(e)[:80]}"


def _gz_rows(data):
    with gzip.open(io.BytesIO(data), "rt", encoding="utf-8") as fh:
        return max(0, sum(1 for _ in csv.reader(fh)) - 1)


def pending_feature_store(fs_dir=None, index=None):
    """[(날짜, 파일 경로)] — 러너에 있고 아직 접수되지 않은 날."""
    out = []
    for p in sorted(glob.glob(os.path.join(fs_dir or FS_DIR, "*.csv.gz"))):
        day = os.path.basename(p)[:-len(".csv.gz")]
        if len(day) == 10 and not recorded("feature_store", day, path=index):
            out.append((day, p))
    return out


def pending_rank_pool(pool_path=None, index=None):
    """(헤더, {날짜: [행]}) — 아직 접수되지 않은 (날짜, 채널)의 행만."""
    try:
        with open(pool_path or POOL_PATH, encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
    except FileNotFoundError:
        return [], {}
    if not rows:
        return [], {}
    header, by_day = rows[0], {}
    for r in rows[1:]:
        if len(r) >= 2 and not recorded("rank_pool", r[0], r[1], path=index):
            by_day.setdefault(r[0], []).append(r)
    return header, by_day


def upload_pending(uploader, run_id, fs_dir=None, pool_path=None, index=None, now=None, tag=None):
    """러너의 새 후보 자료를 올리고 공개 색인에 적는다. (접수 수, 실패 수)."""
    at = (now or datetime.datetime.now(KST)).isoformat(timespec="seconds")
    tag = tag or run_id or "local"
    done = []
    for day, p in pending_feature_store(fs_dir, index):
        with open(p, "rb") as fh:
            data = fh.read()
        name = f"feature_store_{day}_{tag}.csv.gz"
        st, fid, err = _put(uploader, name, data)
        done.append({"kind": "feature_store", "date": day, "channel": "", "rows": _gz_rows(data), "bytes": len(data),
                     "sha256": hashlib.sha256(data).hexdigest(), "run_id": run_id, "name": name,
                     "status": st, "driveId": fid, "error": err, "at": at})
    header, by_day = pending_rank_pool(pool_path, index)
    for day, rows in sorted(by_day.items()):
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
        data = gzip.compress(buf.getvalue().encode("utf-8"), mtime=0)
        name = f"rank_pool_{day}_{tag}.csv.gz"
        st, fid, err = _put(uploader, name, data)
        sha = hashlib.sha256(data).hexdigest()
        for ch in sorted({r[1] for r in rows}):
            done.append({"kind": "rank_pool", "date": day, "channel": ch, "rows": sum(1 for r in rows if r[1] == ch),
                         "bytes": len(data), "sha256": sha, "run_id": run_id, "name": name,
                         "status": st, "driveId": fid, "error": err, "at": at})
    _append(done, index or INDEX)
    ok = sum(1 for r in done if r["status"] == DONE)
    return ok, len(done) - ok


def drive_uploader():
    from hyeoks_run_freeze import upload, DEFAULT_GAS_URL, UPLOAD_BACKOFF_LATE

    def up(name, data):
        return upload(DEFAULT_GAS_URL, name, data, backoff=UPLOAD_BACKOFF_LATE)
    return up


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--upload", action="store_true", help="이 회차가 러너에 만든 후보 자료를 드라이브로 올린다")
    ap.add_argument("--migrate", metavar="TAG", help="저장소에 있던 과거 자료를 한 번 옮긴다(PC 에서, 예: migrate-20261025)")
    a = ap.parse_args(argv)
    if not (a.upload or a.migrate):
        ap.error("--upload 또는 --migrate")
    run_id = os.environ.get("GITHUB_RUN_ID", "") if a.upload else a.migrate
    ok, bad = upload_pending(drive_uploader(), run_id, tag=a.migrate)
    print(f"🔒 후보 자료 비공개 보관 — 접수 {ok} · 실패 {bad}"            # 종목 정보는 찍지 않는다
          + ("" if ok or bad else " (새 자료 없음)"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
