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
#   · 시간 예산을 넘기면 멈춘 위치를 공개 `resume.json`(정수 하나)에 남기고 다음 실행이 거기서 잇는다(Codex 후속 A).
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
            "resumeOffset", "nextResumeOffset"]
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
def plan_order(day, snap_dir=SNAP_DIR, offset=0):
    """(우선 목록, 나머지 목록). 나머지는 `offset` 에서 시작하도록 돌린다."""
    first = list(INDEXES) + universe(day, snap_dir)
    seen = set(first)
    rest = [c for c in backfill_universe(snap_dir, upto=day) if c not in seen]
    if rest:
        k = offset % len(rest)
        rest = rest[k:] + rest[:k]
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


def store_private(files, uploader, local_dir=PRIVATE_DIR):
    """러너 사본을 남기고 드라이브에 올린다. (상태, [{name, bytes, id 또는 error}])."""
    os.makedirs(local_dir, exist_ok=True)
    done, ok = [], True
    for name, data in files:
        with open(os.path.join(local_dir, name), "wb") as fh:
            fh.write(data)
        try:
            fid = uploader(name, data)
            done.append({"name": name, "bytes": len(data), "id": fid})
        except Exception as e:
            ok = False
            done.append({"name": name, "bytes": len(data), "error": f"{type(e).__name__}: {str(e)[:80]}"})
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
    first, rest = plan_order(day, snap_dir, offset)
    order = first + rest

    st = {"runId": run_id, "targetDay": day, "startedAt": now.isoformat(timespec="seconds"),
          "plannedCodes": len(order), "priorityCodes": len(first), "requested": 0, "responded": 0, "covered": 0,
          "missingTarget": 0, "failed": 0, "notAttempted": 0, "rejected": {}, "zeroVolume": 0,
          "unsettledSkipped": 0, "completeness": {}, "failSample": {}, "rowsStored": 0,
          "resumeOffset": offset, "nextResumeOffset": offset}
    rows, per_code = [], {}
    stopped_at = None
    try:
        for i, code in enumerate(order):
            if time.monotonic() - t0 > budget_s:
                stopped_at = i
                break
            res = fetch(code, FETCH_N, get)
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
            done_rest = max(0, stopped_at - len(first))
            st["nextResumeOffset"] = (offset + done_rest) % len(rest) if rest else 0
        st["finishedAt"] = clock().isoformat(timespec="seconds")
        st["rowsStored"] = len(rows)
        meta = {k: v for k, v in st.items()}
        files = private_files(day, run_id, rows, {"run": meta, "codes": per_code}) if (rows or per_code) else []
        if files:
            st["privateStatus"], st["privateFiles"] = store_private(files, uploader or drive_uploader(), private_dir)
        else:
            st["privateStatus"], st["privateFiles"] = "보관할 자료 없음", []
        _write_json(os.path.join(runs_dir, "resume.json"), {"offset": st["nextResumeOffset"]})
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
    public["privateFiles"] = [{k: f[k] for k in ("name", "bytes", "id", "error") if k in f}
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


def theme_obs_summary(day, runs_dir=RUNS_DIR):
    """stockinfo7 공개 관측 메타(이름 없음)에서 그날 요약 한 줄. 파일이 없으면 None."""
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
    h15 = [r for r in rows if r.get("header", "").endswith(" 15시")]
    first15 = min((r["receivedAt"] for r in h15), default="")
    before = "15:05 전 수신" if first15 and first15[11:16] < "15:05" else ("15:05 이후 처음 수신" if first15 else "15시본 관측 없음")
    return (f"관측 {len(rows)}회 · 갱신 {kinds.get('갱신', 0)} · 오류 {sum(v for k, v in kinds.items() if k not in ('첫관측', '동일', '갱신'))} · "
            f"15시본 {first15[11:19] if first15 else '—'} ({before})")


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


def report_text(day, bars, struct, locked, archive="", theme_obs=None):
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
    if a.date:
        day = a.date
        ok, why = trading_day(day)
        if not ok:
            print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''}. 수집·보고 생략")
            return 0
    else:
        # 예약 실행이 휴장일(평일 공휴일) 저녁에 오면 같은 거래일을 다시 보고하지 않는다.
        today = now.astimezone(KST).strftime("%Y-%m-%d")
        if (now.astimezone(KST).hour >= 8) and not trading_day(today)[0]:
            print(f"ℹ️ 오늘({today})은 예정 거래일이 아니다 — 수집·보고 생략 (다음 실행이 300봉 창으로 다시 받는다)")
            return 0
        day = target_day(now)
        if not day:
            print("ℹ️ 대상 거래일을 달력으로 정할 수 없다 — 수집·보고 생략")
            return 0
        # GAS 발사(주) + 깃허브 예약(백업)이 둘 다 오면 같은 대상일을 두 번 수집·보고하지 않는다.
        # 다른 실행이 이미 OK/DEGRADED 로 끝냈으면 생략. FAILED 였으면 다시 한다. 날짜를 지정한 수동 실행은 항상 한다.
        last = _read_json(os.path.join(RUNS_DIR, "last_run.json"), None)
        if (last and last.get("targetDay") == day and last.get("runId") != run_id
                and last.get("status") in ("OK", "DEGRADED")):
            print(f"ℹ️ {day} 은 실행 {last.get('runId')} 이 이미 수집·보고했다({last.get('status')}) — 중복 실행 생략. "
                  "다시 하려면 date 를 지정해 수동 실행")
            return 0
    code = 0
    bars = None
    if a.bars:
        import requests
        bars = run_bars(day, requests.get, runs_dir=RUNS_DIR, now=now, run_id=run_id)
        save_run(bars, RUNS_DIR)
        print("📈 일봉 " + json.dumps({k: v for k, v in bars.items() if k != "privateFiles"}, ensure_ascii=False))
        print("🔒 비공개 보관: " + bars["privateStatus"] + " · " + ", ".join(f["name"] for f in bars["privateFiles"]))
        if bars["status"] == "FAILED":
            code = 1
    if a.report:
        bars = bars or load_run(day, run_id, RUNS_DIR)
        text = report_text(day, bars, structure_today(day), locked_status(),
                           archive=env.get("RESEARCH_ARCHIVE", ""), theme_obs=theme_obs_summary(day, RUNS_DIR))
        print(text)
        if a.send:
            import requests
            sent, err = send(text, requests.post, env.get("TELEGRAM_BOT_TOKEN"))
            print("✅ [연구] 보고 발송 (HTTP 200 — 도착은 채널에서 확인)" if sent else f"⚠️ [연구] 보고 발송 실패: {err}")
            if not sent:
                code = 1
    return code


if __name__ == "__main__":
    sys.exit(main())
