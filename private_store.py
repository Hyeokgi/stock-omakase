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
import json
import os
import subprocess
import sys

KST = datetime.timezone(datetime.timedelta(hours=9))
INDEX = "data/scanner_census/private_index.csv"
FS_DIR = "data/feature_store"
POOL_PATH = "data/scanner_census/rank_pool.csv"
COLS = ["kind", "date", "channel", "rows", "bytes", "sha256", "run_id", "name", "status", "driveId", "error", "at"]
DONE = "접수"
# 🔒 2026-10-25 전체 검토 A — 스캐너는 첫 적재 회차에서만 후보 자료를 만든다(omakase.py `_entered_today`).
#    그 회차의 업로드가 실패하면 뒤 회차는 다시 만들지 않으므로 원본이 사라졌다. 이제 업로드가 실패하면 **같은 바이트를**
#    AES-256(openssl, 키는 비밀값 PRIVATE_STORE_KEY)으로 봉인해 artifact 로 14일 남기고(main.yml), 색인에 '암호사본' 을 적는다.
#    공개 artifact 라도 키 없이는 읽을 수 없다. PC 에서 `--restore <내려받은 폴더>` 로 풀어 드라이브로 옮긴다.
SEALED = "암호사본"
KEY_ENV = "PRIVATE_STORE_KEY"
FALLBACK_DIR = ".private_fallback"
OPENSSL = ["openssl", "enc", "-aes-256-cbc", "-pbkdf2", "-iter", "200000", "-salt", "-pass", f"env:{KEY_ENV}"]


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


def preserved(kind, day, channel=None, path=None):
    """그날 자료가 드라이브 접수 또는 봉인 사본으로 남았나. channel=None 이면 채널을 가리지 않는다."""
    return any(r.get("kind") == kind and r.get("date") == day
               and (channel is None or (r.get("channel") or "") == channel)
               and r.get("status") in (DONE, SEALED) for r in load(path))


def failed(kind, day, path=None):
    """그날 자료 중 드라이브에도 봉인 사본에도 남지 못한 것이 있나."""
    return any(r.get("kind") == kind and r.get("date") == day and r.get("status") not in (DONE, SEALED)
               for r in load(path))


def _openssl(data, key, decrypt=False):
    env = dict(os.environ, **{KEY_ENV: key})
    res = subprocess.run(OPENSSL + (["-d"] if decrypt else []), input=data, capture_output=True, env=env, timeout=60)
    if res.returncode != 0:
        raise RuntimeError(f"openssl 실패 {res.returncode}")
    return res.stdout


def seal(data, key):
    return _openssl(data, key)


def unseal(blob, key):
    return _openssl(blob, key, decrypt=True)


def keep_sealed(name, data, meta, key, out_dir=None):
    """업로드에 실패한 원본을 봉인해 남긴다. (성공, 오류). 키가 없으면 평문을 남기지 않는다."""
    if not key:
        return False, f"{KEY_ENV} 없음 — 봉인 사본을 만들지 않는다"
    out_dir = out_dir or FALLBACK_DIR
    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, name + ".enc"), "wb") as fh:
            fh.write(seal(data, key))
        mpath = os.path.join(out_dir, "manifest.json")
        items = []
        if os.path.exists(mpath):
            with open(mpath, encoding="utf-8") as fh:
                items = json.load(fh)
        items.append(dict(meta, file=name + ".enc", sha256=hashlib.sha256(data).hexdigest()))
        with open(mpath, "w", encoding="utf-8") as fh:
            json.dump(items, fh, ensure_ascii=False, indent=1)
        return True, ""
    except Exception as e:                     # noqa: BLE001
        return False, f"봉인 실패 {type(e).__name__}"


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


def _put_once(uploader, name, data):
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


def upload_pending(uploader, run_id, fs_dir=None, pool_path=None, index=None, now=None, tag=None,
                   key=None, fallback_dir=None):
    """러너의 새 후보 자료를 올리고 공개 색인에 적는다. (접수 수, 접수 못 한 수). 못 올린 것은 봉인 사본을 시도한다."""
    at = (now or datetime.datetime.now(KST)).isoformat(timespec="seconds")
    tag = tag or run_id or "local"
    key = os.environ.get(KEY_ENV, "") if key is None else key
    done = []

    def _put(uploader, name, data, meta):
        st, fid, err = _put_once(uploader, name, data)
        if st != DONE:
            ok, why = keep_sealed(name, data, meta, key, fallback_dir)
            st, err = (SEALED, f"{err} → 봉인 사본") if ok else (st, f"{err} · {why}")
        return st, fid, err
    for day, p in pending_feature_store(fs_dir, index):
        with open(p, "rb") as fh:
            data = fh.read()
        name = f"feature_store_{day}_{tag}.csv.gz"
        st, fid, err = _put(uploader, name, data, {"kind": "feature_store", "date": day, "channels": {}, "name": name})
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
        chans = {ch: sum(1 for r in rows if r[1] == ch) for ch in sorted({r[1] for r in rows})}
        st, fid, err = _put(uploader, name, data, {"kind": "rank_pool", "date": day, "channels": chans, "name": name})
        sha = hashlib.sha256(data).hexdigest()
        for ch in sorted({r[1] for r in rows}):
            done.append({"kind": "rank_pool", "date": day, "channel": ch, "rows": sum(1 for r in rows if r[1] == ch),
                         "bytes": len(data), "sha256": sha, "run_id": run_id, "name": name,
                         "status": st, "driveId": fid, "error": err, "at": at})
    _append(done, index or INDEX)
    ok = sum(1 for r in done if r["status"] == DONE)
    return ok, len(done) - ok


def restore(folder, uploader, key=None, index=None, now=None):
    """PC 에서 — 내려받은 봉인 사본 폴더를 풀어 드라이브로 올리고 색인에 접수를 적는다. (접수 수, 실패 수)."""
    key = os.environ.get(KEY_ENV, "") if key is None else key
    if not key:
        raise SystemExit(f"{KEY_ENV} 가 필요하다(사용자 환경 변수)")
    at = (now or datetime.datetime.now(KST)).isoformat(timespec="seconds")
    with open(os.path.join(folder, "manifest.json"), encoding="utf-8") as fh:
        items = json.load(fh)
    rows, bad = [], 0
    for it in items:
        try:
            with open(os.path.join(folder, it["file"]), "rb") as fh:
                data = unseal(fh.read(), key)
            if hashlib.sha256(data).hexdigest() != it["sha256"]:
                raise ValueError("sha256 불일치")
        except Exception as e:                 # noqa: BLE001
            print(f"❌ {it['file']} 풀기 실패 {type(e).__name__}")
            bad += 1
            continue
        st, fid, err = _put_once(uploader, it["name"], data)
        bad += st != DONE
        chans = it.get("channels") or {"": _gz_rows(data)}
        for ch, n in chans.items():
            rows.append({"kind": it["kind"], "date": it["date"], "channel": ch, "rows": n, "bytes": len(data),
                         "sha256": it["sha256"], "run_id": "restore", "name": it["name"], "status": st,
                         "driveId": fid, "error": err, "at": at})
    _append(rows, index or INDEX)
    return sum(1 for r in rows if r["status"] == DONE), bad


def drive_uploader():
    from hyeoks_run_freeze import upload, DEFAULT_GAS_URL, UPLOAD_BACKOFF_LATE

    def up(name, data):
        return upload(DEFAULT_GAS_URL, name, data, backoff=UPLOAD_BACKOFF_LATE)
    return up


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--upload", action="store_true", help="이 회차가 러너에 만든 후보 자료를 드라이브로 올린다")
    ap.add_argument("--migrate", metavar="TAG", help="저장소에 있던 과거 자료를 한 번 옮긴다(PC 에서, 예: migrate-20261025)")
    ap.add_argument("--restore", metavar="DIR", help="내려받은 봉인 사본 artifact 폴더를 풀어 드라이브로 옮긴다(PC 에서)")
    a = ap.parse_args(argv)
    if a.restore:
        ok, bad = restore(a.restore, drive_uploader())
        print(f"🔓 봉인 사본 복구 — 접수 {ok} · 실패 {bad}")
        return 1 if bad else 0
    if not (a.upload or a.migrate):
        ap.error("--upload · --migrate · --restore 중 하나")
    run_id = os.environ.get("GITHUB_RUN_ID", "") if a.upload else a.migrate
    ok, bad = upload_pending(drive_uploader(), run_id, tag=a.migrate)
    sealed = sum(1 for r in load() if r.get("status") == SEALED and r.get("run_id") == run_id)
    print(f"🔒 후보 자료 비공개 보관 — 접수 {ok} · 못 올림 {bad}"            # 종목 정보는 찍지 않는다
          + (f" (그중 봉인 사본 {sealed} — artifact 를 내려받아 --restore)" if sealed else "")
          + ("" if ok or bad else " (새 자료 없음)"))
    if bad:
        print(f"::warning::후보 자료 드라이브 업로드 실패 {bad}건 — 봉인 사본 {sealed}건")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
