# -*- coding: utf-8 -*-
# ==========================================================================
# 🧭 테마·대장 경로 연구 — 확정 일봉 수집 + [연구] 일일 보고
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-04): "일봉은 깃허브에, 연구보고는 켜줘".
# 2026-10-05 ① Codex 교차 검토 R1~R6 반영(018042b).
# 2026-10-05 ② Codex 후속 권고 + 사용자 지시("코덱스의 제안을 보고 적용해보자"):
#   · **가격 원자료는 공개 저장소에 커밋하지 않는다** → 구글 드라이브 비공개(동결 번들과 같은 쓰기 전용 창구).
#     공개 저장소에는 코드·시험·**집계 요약**(건수·상태·파일 이름)만 남긴다.
#   · 드라이브 창구는 쓰기 전용이라 러너가 지난 자료를 읽을 수 없다 → **상태 없는 수집**으로 바꿨다.
#     매 실행 모든 대상 종목의 최근 300봉을 새로 받는다. 덕분에
#       - 초기 수집 완료 판정이 필요 없다(Codex 후속 B): 매일 같은 창을 다시 받고, 기간 충족 여부는 실행마다 종목별로 남긴다.
#       - 오래된 봉의 수정주가 변경도 매일 탐지된다(Codex ②: 최근 5봉 재수집으로는 못 잡던 것).
#   · 시간 예산을 넘기면 멈춘 위치를 공개 `resume.json`(정수 둘)에 남기고 다음 실행이 거기서 잇는다(Codex 후속 A).
#     2026-10-05 ③ Codex R7: 우선 구간도 회전한다 — '항상 먼저' 와 '모두 매일 확보' 는 다르다.
#   · 2026-10-05 ③ Codex R9·R11: 비공개 파일마다 영수증(크기·sha256·드라이브 id·접수 상태)을 공개 `private_receipts.csv` 에,
#     보고 발송 결과를 `report_log.csv` 에 남긴다. 수집은 끝났는데 보고만 실패했으면 백업 실행이 보고만 다시 보낸다.
#     우선 구간(지수 + 그날 A ∪ 최근 20 관측일 대장 후보)은 항상 먼저 받는다 — 당일 봉이 굶지 않게.
#
# 1) 일봉 (`--bars`)
#    · 대상: 우선 = 지수 + 그날 15:05 스냅샷의 A ∪ 최근 20 관측일의 대장 후보,
#            나머지 = 저장된 모든 스냅샷 날의 A(배지가 사라진 과거 후보도 계속 추적).
#    · 원천: 네이버 fchart 일봉 (`omakase.get_daily_bars` 와 같은 엔드포인트). **수정주가 여부는 확인되지 않았다.**
#    · 날짜 셋을 나눈다: 대상 거래일 · 실제 실행 시각 · 봉 날짜 · 수신 시각.
#      대상일 **뒤** 봉은 넣지 않는다. 미확정 제외는 **실제 오늘** 봉에만 적용한다.
#    · 비공개 파일: `research_bars_<대상일>_<실행ID>[_partN].csv.gz` (봉) + `..._status.json.gz` (종목별 상태).
#    · 공개 요약: `data/research_runs/last_run.json`, `runs.csv`, `resume.json` — 가격 없음.
# 2) 보고 (`--report [--send]`)
#    · 텔레그램 기존 채널에 `[연구]` 머리말. 수집 상태와 **구조 집계만**. 같은 실행 ID 의 상태만 읽는다.
#    · 🔒 대장 후보의 이후 수익률·가격 경로는 담지 않는다(잠긴 `leader-hold-v1` 확증 구간 ≥ 2026-10-06).
#
# 하지 않는 것: 주문, 시트 쓰기, 선정 조건 변경. 지문 대상 파일이 아니다.
# 연구 상태(OK/DEGRADED/FAILED)는 생산 안정화 판정(Gate)에 연결하지 않는다.
# ==========================================================================
import argparse
import csv
import datetime
import glob
import gzip
import io
import json
import math
import os
import sys
import time
import xml.etree.ElementTree as ET

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, KST, read_snapshot

RUNS_DIR = "data/research_runs"           # 공개 — 건수·상태·파일 이름만
PRIVATE_DIR = "data/research_private"     # 러너 사본(.gitignore). 보존은 드라이브가 맡는다
RUN_COLS = ["runId", "targetDay", "startedAt", "finishedAt", "status", "privateStatus", "plannedCodes", "requested",
            "responded", "covered", "missingTarget", "failed", "notAttempted", "rowsStored", "rejected",
            "resumeOffset", "nextResumeOffset", "avgFetchMs", "p95FetchMs", "maxFetchMs"]
RECEIPT_COLS = ["at", "kind", "runId", "name", "bytes", "sha256", "driveId", "status", "error", "verified"]
REPORT_COLS = ["day", "runId", "collectRunId", "at", "status", "error"]
FCHART = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={n}&requestType=0"
FETCH_N = 300                 # 매 실행 받는 창 — 상태 없는 수집
LEADER_LOOKBACK = 20          # 우선 구간에 넣는 대장 후보의 관측일 수 (임시 범위)
INDEXES = ("KOSPI", "KOSDAQ")
MARKET_CLOSE_SAFE = (16, 0)   # 이 시각(KST) 전에는 실제 오늘 봉을 미확정으로 보고 넣지 않는다
TIME_BUDGET_S = 20 * 60       # 워크플로 제한(40분)보다 넉넉히 짧게 — 업로드·커밋·보고 시간을 남긴다
DEGRADED_COVER = 0.9          # 대상일 봉 확보 비율이 이보다 낮으면 DEGRADED
PART_LIMIT_BYTES = 3_000_000  # 드라이브 업로드 한 파일의 gz 크기 상한(넘으면 종목 단위로 나눈다)
TAG = "[연구]"


# ── fchart 파싱 ───────────────────────────────────────────────────────
def _valid_date(s):
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8])).isoformat()
    except ValueError:
        return None


def parse_fchart_detail(text):
    """fchart XML → (봉 목록, 정보). 봉 = (date, open, high, low, close, volume).
    정보 = {"symbol": 응답의 종목 기호, "rejects": {사유: 수}, "zero_volume": 무거래 봉 수}.
    날짜·유한값·음수·가격 관계를 검사한다(R5). 거래량 0 봉은 보존하되 따로 센다."""
    info = {"symbol": None, "rejects": {}, "zero_volume": 0}
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return [], info
    cd = root.find(".//chartdata")
    if cd is not None:
        info["symbol"] = cd.get("symbol")
    out = []

    def rej(why):
        info["rejects"][why] = info["rejects"].get(why, 0) + 1

    for item in root.findall(".//item"):
        d = (item.get("data") or "").split("|")
        if len(d) < 6:
            rej("형식")
            continue
        day = _valid_date(d[0])
        if not day:
            rej("날짜오류")
            continue
        try:
            o, h, l, c, v = (float(x) for x in d[1:6])
        except ValueError:
            rej("숫자아님")
            continue
        if not all(math.isfinite(x) for x in (o, h, l, c, v)):
            rej("비유한값")
            continue
        if min(o, h, l, c, v) < 0:
            rej("음수")
            continue
        if min(o, h, l, c) == 0:
            rej("0가격")
            continue
        if h < max(o, l, c) or l > min(o, h, c):
            rej("가격관계")
            continue
        if v == 0:
            info["zero_volume"] += 1
        out.append((day, o, h, l, c, v))
    return out, info


def parse_fchart(text):
    """fchart XML → [(date, open, high, low, close, volume)]. 형식이 다르거나 검사에 걸린 봉은 뺀다."""
    return parse_fchart_detail(text)[0]


def fetch(code, n, get):
    """(봉 목록, 오류 문자열, 정보). `get` 은 requests.get 호환 — 시험에서 바꿔 끼운다."""
    try:
        r = get(FCHART.format(code=code, n=n), timeout=10)
        if getattr(r, "status_code", 200) != 200:
            return [], f"HTTP {r.status_code}", {}
        bars, info = parse_fchart_detail(r.text)
        sym = info.get("symbol")
        if sym and sym != code:
            return [], f"종목 불일치({sym})", info
        return bars, ("" if bars else "빈 응답"), info
    except Exception as e:                       # 네트워크 오류는 종목 단위로 기록하고 계속한다
        return [], f"{type(e).__name__}: {e}"[:120], {}


def num(x):
    """원천 숫자를 반올림 없이 문자열로. 정수면 정수 표기."""
    x = float(x)
    return str(int(x)) if x.is_integer() else repr(x)


# ── 대상 종목 ─────────────────────────────────────────────────────────
def snapshot_days(snap_dir=SNAP_DIR):
    return sorted(os.path.basename(p)[:10] for p in glob.glob(os.path.join(snap_dir, "2026-*_1505.csv.gz"))
                  if os.path.basename(p)[:10][:4].isdigit())


def universe(day, snap_dir=SNAP_DIR, lookback=LEADER_LOOKBACK):
    """`day` 15:05 스냅샷의 A ∪ 직전 `lookback` 관측일(그날 포함)의 대장 후보."""
    days = [d for d in snapshot_days(snap_dir) if d <= day]
    codes = set()
    if day in days:
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{day}_1505.csv.gz"))
        A, _ = T.build_A(rows)
        codes |= {x["code"] for x in A}
    for d in days[-lookback:]:
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{d}_1505.csv.gz"))
        A, _ = T.build_A(rows)
        codes |= {x["code"] for x in T.build_B_current(A)[0]}
    return sorted(codes)


def backfill_universe(snap_dir=SNAP_DIR, upto=None):
    codes = set()
    for d in snapshot_days(snap_dir):
        if upto and d > upto:
            continue
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{d}_1505.csv.gz"))
        codes |= {x["code"] for x in T.build_A(rows)[0]}
    return sorted(codes)


# ── 공개 요약 파일 ────────────────────────────────────────────────────
def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


# ── 수집 순서 (우선 구간 + 이어 받는 나머지) ─────────────────────────────
def _rotate(xs, k):
    if not xs:
        return xs
    k %= len(xs)
    return xs[k:] + xs[:k]


def registry_codes(day, snap_dir=SNAP_DIR):
    """에피소드 등록부의 추적 종목 — 미종결 에피소드 종목 + 그 주 대조군 + 그날 보조 대조군.
    🔴 2026-10-10 사용자 결정(허브 #14): 연구 일봉 우선 구간에 더한다(교체 아님). 배지가 사라지거나
       거래대금이 줄어 그날 A 에서 빠진 대장·대조군도 먼저 받는다. 실패하면 빈 목록 — 우선 구간만 예전대로."""
    try:
        import hyeoks_episode_registry as E
        days = E.load_obs(snap_dir, until=day)
        if not days:
            return []
        episodes, _, _, _ = E.build(days)
        secondary, _ = E.secondary_controls(days[-1])
        return E.tracked_codes(episodes, secondary)
    except Exception as e:
        print(f"⚠️ 에피소드 등록부 우선 구간 생략 — {type(e).__name__}: {str(e)[:80]}")
        return []


def plan_order(day, snap_dir=SNAP_DIR, offset=0, priority_offset=0):
    """(우선 목록, 나머지 목록). 지수는 맨 앞 고정, 우선 종목은 `priority_offset`, 나머지는 `offset` 부터 돌린다.
    🔴 Codex R7 — 우선 구간 뒤쪽이 매번 예산에 걸려 영원히 밀리지 않게 우선 구간도 회전한다.
    우선 = 그날 A ∪ 최근 대장 후보 ∪ 에피소드 등록부 추적 종목(2026-10-10)."""
    prio = [c for c in sorted(set(universe(day, snap_dir)) | set(registry_codes(day, snap_dir)))
            if c not in INDEXES]
    first = list(INDEXES) + _rotate(prio, priority_offset)
    seen = set(first)
    rest = _rotate([c for c in backfill_universe(snap_dir, upto=day) if c not in seen], offset)
    return first, rest


def completeness(n_bars, rejects, has_target):
    """종목별 기간 충족 분류(Codex 후속 B). 짧은 응답은 '상장이 짧음' 으로 인증하지 않는다."""
    if not has_target:
        return "대상일없음"
    if rejects:
        return "검사탈락"
    if n_bars < FETCH_N:
        return "짧은응답"
    return "완전"


# ── 비공개 보관 ───────────────────────────────────────────────────────
def private_files(day, run_id, rows, status):
    """[(파일 이름, gz 바이트)]. 봉 CSV 가 크면 종목 단위로 나눈다."""
    def bars_gz(chunk):
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["date", "code", "open", "high", "low", "close", "volume", "fetchedAt"])
        w.writerows(chunk)
        return gzip.compress(buf.getvalue().encode("utf-8"), mtime=0)

    base = f"research_bars_{day}_{run_id or 'local'}"
    codes = sorted({r[1] for r in rows})
    parts = [codes] if codes else []
    while True:
        blobs = [bars_gz([r for r in rows if r[1] in set(p)]) for p in parts]
        big = [i for i, b in enumerate(blobs) if len(b) > PART_LIMIT_BYTES and len(parts[i]) > 1]
        if not big:
            break
        i = big[0]
        p = parts.pop(i)
        parts[i:i] = [p[:len(p) // 2], p[len(p) // 2:]]
    out = []
    for i, b in enumerate(blobs):
        name = f"{base}.csv.gz" if len(blobs) == 1 else f"{base}_part{i + 1}of{len(blobs)}.csv.gz"
        out.append((name, b))
    out.append((f"{base}_status.json.gz",
                gzip.compress(json.dumps(status, ensure_ascii=False).encode("utf-8"), mtime=0)))
    return out


def drive_uploader():
    """동결 번들과 같은 드라이브 쓰기 전용 창구. 실패하면 예외를 올린다."""
    from hyeoks_run_freeze import upload, DEFAULT_GAS_URL, UPLOAD_BACKOFF_LATE

    def up(name, data):
        return upload(DEFAULT_GAS_URL, name, data, backoff=UPLOAD_BACKOFF_LATE)
    return up


def upload_receipt(kind, run_id, name, data, uploader, local_dir=PRIVATE_DIR):
    """러너 사본을 남기고 드라이브에 올린다. 영수증 dict.
    `status` 는 업로드 **접수**(드라이브 id 수신)까지다 — 원격 재다운로드 대조(`verified`)와 구분한다(Codex R9)."""
    import hashlib
    os.makedirs(local_dir, exist_ok=True)
    with open(os.path.join(local_dir, name), "wb") as fh:
        fh.write(data)
    rc = {"at": datetime.datetime.now(KST).isoformat(timespec="seconds"), "kind": kind, "runId": run_id,
          "name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(), "driveId": "",
          "status": "", "error": "", "verified": ""}
    try:
        rc["driveId"] = str(uploader(name, data) or "")
        rc["status"] = "접수"
    except Exception as e:
        rc["status"] = "실패"
        rc["error"] = f"{type(e).__name__}: {str(e)[:80]}"
    return rc


def append_receipts(rows, runs_dir=RUNS_DIR):
    """공개 영수증 장부 — 파일 이름·크기·sha256·드라이브 id·상태. 내용은 없다."""
    if not rows:
        return
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "private_receipts.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RECEIPT_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerows(rows)


def store_private(files, uploader, local_dir=PRIVATE_DIR, run_id="", runs_dir=None):
    """(상태, [영수증]). 영수증은 `runs_dir` 가 주어지면 공개 장부에도 남긴다."""
    done = [upload_receipt("bars", run_id, name, data, uploader, local_dir) for name, data in files]
    if runs_dir:
        append_receipts(done, runs_dir)
    ok = all(r["status"] == "접수" for r in done)
    return ("보관 완료" if ok else "보관 실패"), done


# ── 수집 ─────────────────────────────────────────────────────────────
def run_bars(day, get, runs_dir=RUNS_DIR, snap_dir=SNAP_DIR, now=None, pause=0.05, sleep=time.sleep,
             clock=None, budget_s=TIME_BUDGET_S, run_id="", uploader=None, private_dir=PRIVATE_DIR):
    """대상 거래일 `day` 의 일봉 수집 → 비공개 보관. 공개용 상태 dict 를 돌려준다(가격 없음)."""
    now = now or datetime.datetime.now(KST)
    clock = clock or (lambda: datetime.datetime.now(KST))
    t0 = time.monotonic()
    run_today = now.astimezone(KST).strftime("%Y-%m-%d")
    unsettled = (now.astimezone(KST).hour, now.astimezone(KST).minute) < MARKET_CLOSE_SAFE
    resume = _read_json(os.path.join(runs_dir, "resume.json"), {"offset": 0})
    offset = int(resume.get("offset", 0))
    poffset = int(resume.get("priorityOffset", 0))
    first, rest = plan_order(day, snap_dir, offset, poffset)
    n_idx = len([c for c in first if c in INDEXES])
    order = first + rest

    st = {"runId": run_id, "targetDay": day, "startedAt": now.isoformat(timespec="seconds"),
          "plannedCodes": len(order), "priorityCodes": len(first), "requested": 0, "responded": 0, "covered": 0,
          "missingTarget": 0, "failed": 0, "notAttempted": 0, "rejected": {}, "zeroVolume": 0,
          "unsettledSkipped": 0, "completeness": {}, "failSample": {}, "rowsStored": 0,
          "resumeOffset": offset, "nextResumeOffset": offset,
          "priorityResumeOffset": poffset, "nextPriorityResumeOffset": poffset}
    durations = []
    rows, per_code = [], {}
    stopped_at = None
    try:
        for i, code in enumerate(order):
            if time.monotonic() - t0 > budget_s:
                stopped_at = i
                break
            tf = time.perf_counter()
            res = fetch(code, FETCH_N, get)
            durations.append((time.perf_counter() - tf) * 1000)
            bars, err = res[0], res[1]
            info = res[2] if len(res) > 2 else {}
            recv = clock().isoformat(timespec="seconds")
            sleep(pause)                              # 원천에 부담을 주지 않도록 종목 사이를 띄운다
            st["requested"] += 1
            rej = sum((info.get("rejects") or {}).values())
            for k, v in (info.get("rejects") or {}).items():
                st["rejected"][k] = st["rejected"].get(k, 0) + v
            st["zeroVolume"] += info.get("zero_volume", 0)
            if err:
                st["failed"] += 1
                per_code[code] = {"err": err, "recv": recv}
                if len(st["failSample"]) < 3:
                    st["failSample"][code] = err
                continue
            st["responded"] += 1
            kept = 0
            for (d, o, h, l, c, v) in bars:
                if d > day:
                    continue                          # 과거 복구가 대상일 뒤 자료를 섞지 않게
                if d == run_today and unsettled:
                    st["unsettledSkipped"] += 1
                    continue
                rows.append([d, code, num(o), num(h), num(l), num(c), num(v), recv])
                kept += 1
            has_target = any(b[0] == day for b in bars) and not (day == run_today and unsettled)
            kind = completeness(len(bars), rej, has_target)
            st["completeness"][kind] = st["completeness"].get(kind, 0) + 1
            if has_target:
                st["covered"] += 1
            else:
                st["missingTarget"] += 1
            per_code[code] = {"bars": len(bars), "kept": kept, "first": bars[0][0], "last": bars[-1][0],
                              "rejects": info.get("rejects") or {}, "zeroVolume": info.get("zero_volume", 0),
                              "class": kind, "recv": recv}
    finally:
        # 중단돼도 받은 것은 비공개로 남긴다
        if stopped_at is not None:
            st["notAttempted"] = len(order) - stopped_at
            n_prio = len(first) - n_idx
            if stopped_at < len(first):
                # 우선 구간 안에서 멈췄다 — 다음 실행은 우선 구간도 멈춘 곳부터 (Codex R7)
                done_prio = max(0, stopped_at - n_idx)
                st["nextPriorityResumeOffset"] = (poffset + done_prio) % n_prio if n_prio else 0
            else:
                done_rest = stopped_at - len(first)
                st["nextResumeOffset"] = (offset + done_rest) % len(rest) if rest else 0
        if durations:
            ds = sorted(durations)
            st["avgFetchMs"] = round(sum(ds) / len(ds))
            st["p95FetchMs"] = round(ds[min(len(ds) - 1, int(len(ds) * 0.95))])
            st["maxFetchMs"] = round(ds[-1])
        st["finishedAt"] = clock().isoformat(timespec="seconds")
        st["rowsStored"] = len(rows)
        meta = {k: v for k, v in st.items()}
        files = private_files(day, run_id, rows, {"run": meta, "codes": per_code}) if (rows or per_code) else []
        if files:
            st["privateStatus"], st["privateFiles"] = store_private(files, uploader or drive_uploader(), private_dir,
                                                                    run_id, runs_dir)
        else:
            st["privateStatus"], st["privateFiles"] = "보관할 자료 없음", []
        _write_json(os.path.join(runs_dir, "resume.json"),
                    {"offset": st["nextResumeOffset"], "priorityOffset": st["nextPriorityResumeOffset"]})
    st["status"] = status_of(st)
    return st


def status_of(st):
    planned = st["plannedCodes"]
    if (planned and st["responded"] == 0) or st.get("privateStatus") == "보관 실패":
        return "FAILED"
    if st["failed"] or st["notAttempted"] or (planned and st["covered"] / planned < DEGRADED_COVER):
        return "DEGRADED"
    return "OK"


def save_run(st, runs_dir=RUNS_DIR):
    """공개 요약만 — 가격·종목별 상태는 넣지 않는다."""
    public = {k: v for k, v in st.items() if k not in ("failSample",)}
    public["privateFiles"] = [{k: f[k] for k in ("name", "bytes", "sha256", "driveId", "status", "error") if k in f}
                              for f in st.get("privateFiles", [])]
    _write_json(os.path.join(runs_dir, "last_run.json"), public)
    path = os.path.join(runs_dir, "runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        row = dict(public)
        row["rejected"] = sum(st.get("rejected", {}).values())
        w.writerow(row)


def load_run(day, run_id, runs_dir=RUNS_DIR):
    """보고용 — 같은 실행 ID·같은 대상일의 수집 상태만. 아니면 None (오래된 상태를 쓰지 않는다)."""
    st = _read_json(os.path.join(runs_dir, "last_run.json"), None)
    if not st or st.get("targetDay") != day:
        return None
    if run_id and st.get("runId") != run_id:
        return None
    return st


DECISION_HM = "15:05"   # 종베 판단 시각 (운영대전제)
# 🔴 2026-10-10 사전 고정 v2(사용자 결정) — docs/사전고정_2026-10-10_stockinfo7_판단화면_v2.md.
#    15시본은 10/6~10/8 사흘 모두 없었다(화면 시각이 매일 다름). 이 날짜부터는 15:05 까지 파싱이 끝난
#    **가장 최근 당일 화면**을 시각과 함께 쓴다. 그 전 날짜는 v1(15시본만) 그대로 — 소급하지 않는다.
STOCKINFO7_RULE_V2_FROM = "2026-10-12"


def theme_obs_summary(day, runs_dir=RUNS_DIR):
    """stockinfo7 공개 관측 메타(이름 없음)에서 그날 요약 한 줄. 파일이 없으면 None.

    🔴 2026-10-05 Codex R8 + 사전 고정 규칙(10/6 첫 실측 **전**에 정함):
      주 분석 화면 = **그날 날짜·15시 기준 화면 중 15:05 까지 수신·파싱이 끝난 가장 최근 관측**. 없으면 결측.
      14시본은 별도 비교용으로만 남기고 15시본 빈칸에 대신 넣지 않는다. 15:05 뒤에 받은 15시본을 소급 사용하지 않는다.
      화면 날짜가 그날이 아니면(날짜불일치) 오늘 비교에 넣지 않는다."""
    path = os.path.join(runs_dir, "stockinfo7_obs.csv")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if r.get("day") == day]
    if not rows:
        return ""
    kinds = {}
    for r in rows:
        kinds[r["class"]] = kinds.get(r["class"], 0) + 1
    valid = [r for r in rows if r.get("class") in ("첫관측", "동일", "갱신")]

    def sday_hour(r):
        sd, sh = r.get("screenDay", ""), r.get("screenHour", "")
        if not sd:                      # 이전 형식 행 — 헤더에서 읽는다
            parts = (r.get("header") or "").replace("시", "").split()
            sd, sh = (parts[0], parts[1]) if len(parts) == 2 else ("", "")
        return sd, int(sh) if str(sh).isdigit() else None

    def ready(r):
        return (r.get("parsedAt") or r.get("receivedAt") or "")[:19]

    cutoff = f"{day}T{DECISION_HM}:00"
    usable = [r for r in valid if sday_hour(r)[0] == day and ready(r) and ready(r) <= cutoff]
    latest = max(usable, key=ready) if usable else None
    if day >= STOCKINFO7_RULE_V2_FROM:
        hour = sday_hour(latest)[1] if latest else None
        if latest and hour is not None:
            hh, mm = (int(x) for x in DECISION_HM.split(":"))
            age = (hh * 60 + mm) - hour * 60          # 화면 기준 시각이 15:05 보다 몇 분 앞서나
            main_line = (f"{DECISION_HM} 판단 화면 {hour}시본 ({ready(latest)[11:19]} 확보 · "
                         f"화면 기준 {age}분 전) [규칙 v2]")
        else:
            main_line = f"{DECISION_HM} 판단 화면 결측 — {DECISION_HM} 전 당일 화면 없음 [규칙 v2]"
    elif latest and sday_hour(latest)[1] == 15:
        main_line = f"{DECISION_HM} 판단 화면 15시본 ({ready(latest)[11:19]} 확보)"
    elif latest:
        main_line = f"{DECISION_HM} 판단 화면 결측 — 그 시각 최신은 {sday_hour(latest)[1]}시본(대체하지 않음)"
    else:
        main_line = f"{DECISION_HM} 판단 화면 결측 — {DECISION_HM} 전 유효 화면 없음"
    first15 = min((ready(r) for r in valid if sday_hour(r) == (day, 15) and ready(r)), default="")
    late = f" · 15시본 첫 확보 {first15[11:19]}" if first15 and first15 > cutoff else ""
    errs = sum(v for k, v in kinds.items() if k not in ("첫관측", "동일", "갱신"))
    stale = kinds.get("날짜불일치", 0)
    return (f"관측 {len(rows)}회 · 갱신 {kinds.get('갱신', 0)} · 오류 {errs}" + (f"(날짜불일치 {stale})" if stale else "")
            + f" · {main_line}{late}")


# ── 보고 (구조 집계만) ────────────────────────────────────────────────
def structure_today(day, snap_dir=SNAP_DIR):
    """그날과 직전 관측일의 대장 후보·테마 구조. 가격 성과는 계산하지 않는다."""
    days = [d for d in snapshot_days(snap_dir) if d <= day]
    if not days or days[-1] != day:
        return None
    prev = days[-2] if len(days) >= 2 else None

    def leaders(d):
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{d}_1505.csv.gz"))
        return {x["theme"]: x["code"] for x in T.build_B_current(T.build_A(rows)[0])[0]}
    now_l = leaders(day)
    out = {"day": day, "prev": prev, "leaders": len(now_l)}
    if prev:
        prev_l = leaders(prev)
        out["kept_leader"] = len(set(prev_l.values()) & set(now_l.values()))
        out["prev_leaders"] = len(prev_l)
        kept = [t for t in prev_l if t in now_l]
        out["swapped"] = sum(1 for t in kept if now_l[t] != prev_l[t])
        out["themes_kept"] = len(kept)
    return out


def locked_status(snap_dir=SNAP_DIR):
    """잠긴 사전등록의 확증 구간 상태 — 수(數)만. 수익률 숫자는 내지 않는다.
    본 분석기와 같은 120거래일 상한을 쓴다(R6). 상한을 달력으로 정할 수 없으면 달력 범위 밖 날은 세지 않는다."""
    days, _ = T.collect(snap_dir, with_returns=True)
    cal = T._calendar(snap_dir)
    cap, cap_why = T.trading_day_n(T.CONFIRM_FROM, T.CAP_TRADING_DAYS, cal)
    limit = cap or getattr(cal, "end", None)
    valid, bad = T.valid_hold_days(days, T.CONFIRM_FROM, limit)
    if not cap and "상한초과" in bad:
        bad["달력범위밖"] = bad.pop("상한초과")
    conf = [d for d in days if d["date"] >= T.CONFIRM_FROM and not d.get("skip")]
    return {"observed": len(conf), "valid": len(valid), "bad": bad, "cap": cap, "capWhy": cap_why}


def materials_summary(day, runs_dir=RUNS_DIR):
    """재료 층 한 줄 — DART 공시·한투 뉴스 제목의 그날 건수(이름·제목 없음). 넘침 의심이 있으면 ⚠️ 로 알린다."""
    def rows(name):
        path = os.path.join(runs_dir, name)
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as fh:
            return [r for r in csv.DictReader(fh) if r.get("day") == day]

    def n(r, k):
        try:
            return int(float(r.get(k) or 0))
        except ValueError:
            return 0
    parts = []
    dart = rows("dart_runs.csv")
    if dart:
        best = max(dart, key=lambda r: n(r, "polls"))
        parts.append(f"DART 공시 {n(best, 'disclosures')}건(15:05까지 {n(best, 'seenBy1505')} · "
                     f"15:05 범위 {best.get('coverage1505') or '?'} · {best.get('status') or '?'})")
    news = rows("kis_news_runs.csv")
    if news:
        best = max(news, key=lambda r: n(r, "polls"))
        over = sum(n(r, "overflowPolls") for r in news)
        parts.append(f"한투 뉴스 {n(best, 'news')}건(15:05까지 {n(best, 'publishedBy1505')} · 종목코드 {n(best, 'withStockCode')} · "
                     f"{best.get('status') or '?'})" + (f" ⚠️ 넘침 의심 {over}회 — 최신 40건만 받음" if over else ""))
    return " · ".join(parts)


def report_text(day, bars, struct, locked, archive="", theme_obs=None, materials=None):
    """허용된 상태 필드만 쓴다. 수익률·승률·후보별 가격 경로는 넣지 않는다."""
    L = [f"{TAG} 테마·대장 경로 연구 일일 보고 · 대상일 {day}"]
    if bars:
        L.append(f"· 일봉 수집 {bars['status']} · 실행 {bars.get('startedAt', '?')} ~ {bars.get('finishedAt', '?')}")
        L.append(f"· 일봉: 계획 {bars['plannedCodes']} · 요청 {bars['requested']} · 응답 {bars['responded']} · "
                 f"대상일 확보 {bars['covered']} · 대상일 봉 없음 {bars['missingTarget']} · 실패 {bars['failed']} · "
                 f"미처리 {bars['notAttempted']}")
        c = bars.get("completeness", {})
        L.append(f"· 기간 충족: 완전 {c.get('완전', 0)} · 짧은 응답 {c.get('짧은응답', 0)} · 검사 탈락 포함 {c.get('검사탈락', 0)} · "
                 f"품질 제외 봉 {sum(bars.get('rejected', {}).values())} · 무거래 봉 {bars.get('zeroVolume', 0)}")
        L.append(f"· 비공개 보관(드라이브): {bars.get('privateStatus', '?')} · 파일 {len(bars.get('privateFiles', []))}개 · "
                 f"봉 {bars.get('rowsStored', 0)}행")
        if bars.get("unsettledSkipped"):
            L.append(f"  (장 마감 전 실제 오늘 봉 {bars['unsettledSkipped']}건은 미확정이라 넣지 않음)")
    else:
        L.append("· 일봉 수집: 이번 실행의 수집 기록 없음 — 수집이 돌지 않았거나 다른 실행의 기록이다")
    L.append(f"· 공개 요약 커밋: {archive or '확인 안 됨'}")
    if theme_obs is not None:
        L.append("· stockinfo7: " + (theme_obs or "오늘 관측 기록 없음"))
    if materials is not None:
        L.append("· 재료: " + (materials or "오늘 수집 기록 없음"))
    if struct:
        L.append(f"· 오늘 대장 후보 {struct['leaders']}개 (현재 정의의 표시 — 실제 주도권 판정 아님)")
        if struct.get("prev"):
            L.append(f"  직전 관측일({struct['prev']}) 대장 {struct['prev_leaders']}개 중 오늘도 대장 {struct['kept_leader']}개 · "
                     f"이어진 테마 {struct['themes_kept']}개 중 대장 표시 교체 {struct['swapped']}개")
    else:
        L.append("· 오늘 15:05 스냅샷 없음 — 구조 집계 생략")
    if locked:
        cap = f"상한 {locked['cap']}" if locked.get("cap") else "상한 미확정(달력 범위 밖)"
        L.append(f"· 잠긴 사전등록 leader-hold-v1: 확증 관측 {locked['observed']}일 · "
                 f"유효 비교일 {locked['valid']}/{T.EVAL_VALID_DAYS} · {cap} (수익률 비공개)")
    L.append("관측 전용 · 자동주문 없음 · 매수 추천 아님 · 대장 후보의 이후 수익률은 방화벽 때문에 넣지 않는다")
    return "\n".join(L)


def send(text, post, token):
    from telegram_target import chat_id
    if not token:
        return False, "TELEGRAM_BOT_TOKEN 없음 — 보내지 않았다"
    try:
        r = post(f"https://api.telegram.org/bot{token}/sendMessage",
                 data={"chat_id": chat_id(), "text": text}, timeout=15)
        ok = getattr(r, "status_code", 0) == 200
        return ok, ("" if ok else f"HTTP {getattr(r, 'status_code', '?')}")
    except Exception as e:
        return False, type(e).__name__        # 토큰이 들어간 URL 을 오류 문구에 남기지 않는다


# 🔴 2026-10-10 — 깃허브 작업은 최대 360분이다. 09시 전후에 시작해 15:08 까지 도는 장중 수집(DART·한투 뉴스)은
#    한도에 걸려 저장 전에 죽을 수 있다. 조회 끝 시각을 시작 + 340분(설치·저장·커밋 몫 20분 제외)으로 묶는다.
#    잘린 뒤 구간은 다음 실행(12:25 stockinfo7 연동 등)이 맡는다.
JOB_CAP_MIN = 340


def poll_deadline(now, hhmm, cap_min=JOB_CAP_MIN):
    """(조회 끝 시각, 한도 때문에 잘렸는지). 끝 = min(오늘 hhmm, now + cap_min분)."""
    hh, mm = (int(x) for x in hhmm.split(":"))
    until = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    cap = now + datetime.timedelta(minutes=cap_min)
    return (cap, True) if until > cap else (until, False)


def trading_day(day):
    from hyeoks_trading_calendar import scheduled_session
    try:
        return scheduled_session(day), ""
    except ValueError as e:
        return False, f"달력 범위 밖: {e}"


def target_day(now, is_session=None):
    """실제 실행 시각 기준 **가장 최근에 장이 확정된 거래일**.
    오늘이 거래일이고 16:00 이 지났으면 오늘, 아니면 그 전 거래일. 예약이 몇 시간 늦어도 같은 규칙이다.
    달력으로 정할 수 없으면 None."""
    is_session = is_session or (lambda d: trading_day(d)[0])
    now = now.astimezone(KST)
    d = now.date()
    if (now.hour, now.minute) < MARKET_CLOSE_SAFE:
        d -= datetime.timedelta(days=1)
    for _ in range(15):
        if is_session(d.isoformat()):
            return d.isoformat()
        d -= datetime.timedelta(days=1)
    return None


def send_status(sent, err):
    """발송성공 / 발송실패(안 갔다고 볼 수 있음 — 재시도 가능) / 응답불명(갔을 수도 있음 — 자동 재발송 안 함)."""
    if sent:
        return "발송성공"
    if err.startswith("HTTP") or "없음" in err or err in ("ConnectTimeout", "ConnectionError", "NewConnectionError",
                                                          "ProxyError", "SSLError", "InvalidURL"):
        return "발송실패"
    return "응답불명"


def log_report(day, run_id, collect_run_id, status, error, runs_dir=RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "report_log.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=REPORT_COLS, lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow({"day": day, "runId": run_id, "collectRunId": collect_run_id,
                    "at": datetime.datetime.now(KST).isoformat(timespec="seconds"), "status": status, "error": error})


def report_state(day, runs_dir=RUNS_DIR):
    """그날 보고의 가장 나은 상태: 발송성공 > 응답불명 > 발송실패 > ''."""
    path = os.path.join(runs_dir, "report_log.csv")
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as fh:
        seen = {r["status"] for r in csv.DictReader(fh) if r.get("day") == day}
    for s in ("발송성공", "응답불명", "발송실패"):
        if s in seen:
            return s
    return ""


def main(argv=None, now=None, env=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--bars", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--send", action="store_true")
    ap.add_argument("--date", default="")
    a = ap.parse_args(argv)
    env = os.environ if env is None else env
    now = now or datetime.datetime.now(KST)
    run_id = env.get("GITHUB_RUN_ID", "")
    reuse = None
    if a.date:
        day = a.date
        ok, why = trading_day(day)
        if not ok:
            print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''}. 수집·보고 생략")
            return 0
    else:
        # 예약 실행이 휴장일(평일 공휴일) 저녁에 오면 같은 거래일을 다시 보고하지 않는다.
        # 08시 전 실행은 전날 저녁 예약이 늦게 온 것으로 보고 **전날**로 판단한다
        # (2026-10-06 실측: 10/5 휴장일 19:40 예약이 8시간 42분 늦은 04:22 에 와 10/2 를 보고했다).
        local = now.astimezone(KST)
        sched = (local - datetime.timedelta(days=1)) if local.hour < 8 else local
        sched_day = sched.strftime("%Y-%m-%d")
        if not trading_day(sched_day)[0]:
            print(f"ℹ️ 예약일({sched_day})은 예정 거래일이 아니다 — 수집·보고 생략 (다음 실행이 300봉 창으로 다시 받는다)")
            return 0
        day = target_day(now)
        if not day:
            print("ℹ️ 대상 거래일을 달력으로 정할 수 없다 — 수집·보고 생략")
            return 0
        # GAS 발사(주) + 깃허브 예약(백업)이 둘 다 오면 같은 대상일을 두 번 수집·보고하지 않는다.
        # 다른 실행이 이미 OK/DEGRADED 로 끝냈으면 생략. FAILED 였으면 다시 한다. 날짜를 지정한 수동 실행은 항상 한다.
        # 🔴 Codex R11 — 수집 완료와 보고 완료를 나눈다. 보고만 실패했으면 수집은 반복하지 않고 보고만 다시 보낸다.
        last = _read_json(os.path.join(RUNS_DIR, "last_run.json"), None)
        if (last and last.get("targetDay") == day and last.get("runId") != run_id
                and last.get("status") in ("OK", "DEGRADED")):
            rep = report_state(day, RUNS_DIR)
            if a.bars and not a.report:
                print(f"ℹ️ {day} 일봉은 실행 {last.get('runId')} 이 이미 수집했다({last.get('status')}) — 수집 생략")
                return 0
            if rep in ("발송성공", "응답불명"):
                print(f"ℹ️ {day} 은 수집·보고가 이미 끝났다(수집 {last.get('runId')}, 보고 {rep}) — 중복 실행 생략. "
                      "응답불명은 중복 발송을 피하려고 자동 재발송하지 않는다. 다시 하려면 date 를 지정해 수동 실행")
                return 0
            print(f"ℹ️ {day} 일봉은 실행 {last.get('runId')} 수집분을 쓰고, 보고만 다시 보낸다(이전 보고 상태: {rep or '기록 없음'})")
            a.bars = False
            reuse = last
    code = 0
    bars = None
    if a.bars:
        import requests
        bars = run_bars(day, requests.get, runs_dir=RUNS_DIR, now=now, run_id=run_id)
        bars["stateCommit"] = env.get("RESEARCH_STATE_COMMIT", "")
        save_run(bars, RUNS_DIR)
        print("📈 일봉 " + json.dumps({k: v for k, v in bars.items() if k != "privateFiles"}, ensure_ascii=False))
        print("🔒 비공개 보관: " + bars["privateStatus"] + " · " + ", ".join(f["name"] for f in bars["privateFiles"]))
        if bars["status"] == "FAILED":
            code = 1
    if a.report:
        bars = bars or reuse or load_run(day, run_id, RUNS_DIR)
        text = report_text(day, bars, structure_today(day), locked_status(),
                           archive=env.get("RESEARCH_ARCHIVE", ""), theme_obs=theme_obs_summary(day, RUNS_DIR),
                           materials=materials_summary(day, RUNS_DIR))
        print(text)
        if a.send:
            import requests
            sent, err = send(text, requests.post, env.get("TELEGRAM_BOT_TOKEN"))
            status = send_status(sent, err)
            log_report(day, run_id, (bars or {}).get("runId", ""), status, err, RUNS_DIR)
            print("✅ [연구] 보고 발송 (HTTP 200 — 도착은 채널에서 확인)" if sent else f"⚠️ [연구] 보고 {status}: {err}")
            if not sent:
                code = 1
    return code


if __name__ == "__main__":
    sys.exit(main())
