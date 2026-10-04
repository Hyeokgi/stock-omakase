# -*- coding: utf-8 -*-
# ==========================================================================
# 🧭 테마·대장 경로 연구 — 확정 일봉 수집 + [연구] 일일 보고
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-04): "일봉은 깃허브에, 연구보고는 켜줘".
# 2026-10-05 개정: Codex 교차 검토(docs/collaboration/2026-10-05_Codex_테마대장경로_인계검토_회신.md)
#   R1~R6 반영. 첫 예약 실행(10/6) 전이다 — 실제 원천 응답·원격 보관·텔레그램 도착은 아직 확인되지 않았다.
#
# 1) 일봉 (`--bars`)
#    · 대상: 그날 15:05 스냅샷의 A(거래대금 ≥ 50억, 거래정지·관리종목 제외) ∪ 최근 20 관측일의 대장 후보
#      ∪ 지난 실행에서 끝내지 못한 종목(미처리). 20 관측일은 임시 범위다 — 연구 종료 기간이 아니다.
#    · 원천: 네이버 fchart 일봉 (`omakase.get_daily_bars` 와 같은 엔드포인트). **수정주가 여부는 확인되지 않았다.**
#    · 저장: `data/daily_bars/YYYY-MM.csv.gz` — (date, code) 당 처음 받은 값을 보존한다.
#      나중에 다른 값이 오면 덮어쓰지 않고 `revisions.csv.gz` 에 남긴다(기업행사 조정 탐지).
#      숫자는 원천 그대로(유효숫자 반올림 없이) 적는다. `fetchedAt` = 그 종목 응답을 **받은** 시각(KST).
#    · 종목별 수집 범위(`coverage.json`): 초기 300봉을 받은 적이 있는지, 마지막 저장 봉 날짜, 마지막 오류.
#      → 요청량은 파일 존재가 아니라 **종목별 필요 기간**으로 정한다(R1).
#    · 날짜 셋을 나눈다(R2): 대상 거래일 · 실제 실행 시각 · 봉 날짜 · 수신 시각.
#      대상일 **뒤** 봉은 저장하지 않는다(과거 복구가 이후 자료를 섞지 않게). 미확정 제외는 **실제 오늘** 봉에만 적용한다.
#    · 묶음마다 저장하고 시간 예산을 넘기면 멈춘다(R4). 못 한 종목은 다음 실행이 이어서 한다.
#    · 실행 상태(`last_run.json`, `runs.csv`): 실행 ID·대상일·요청/응답/대상일 확보/실패/미처리·품질 제외·상태(R3).
# 2) 보고 (`--report [--send]`)
#    · 텔레그램 기존 채널에 `[연구]` 머리말로 보낸다. 수집 상태와 **구조 집계만** 담는다.
#    · 보고는 **같은 실행 ID** 의 수집 상태만 읽는다. 오래된 상태를 오늘 성공으로 쓰지 않는다.
#    · 🔒 가격 수익률·대장 후보의 이후 성과는 담지 않는다 — 잠긴 사전등록 `leader-hold-v1` 의
#      확증 구간(진입일 ≥ 2026-10-06)과 겹치기 때문이다. 잠긴 연구는 유효 비교일 '수' 만 적는다.
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

BARS_DIR = "data/daily_bars"
BAR_COLS = ["date", "code", "open", "high", "low", "close", "volume", "fetchedAt"]
REV_COLS = ["date", "code", "field", "old", "new", "oldFetchedAt", "newFetchedAt"]
RUN_COLS = ["runId", "targetDay", "startedAt", "finishedAt", "status", "requested", "responded", "covered",
            "missingTarget", "failed", "notAttempted", "rejected", "added", "revised", "backfilledCodes", "plannedCodes"]
FIELDS = ("open", "high", "low", "close", "volume")
FCHART = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={n}&requestType=0"
BACKFILL_N, DAILY_N = 300, 5
LEADER_LOOKBACK = 20          # 대장 배지가 사라진 뒤에도 이만큼의 관측일 동안 계속 받는다 (임시 범위)
INDEXES = ("KOSPI", "KOSDAQ")
MARKET_CLOSE_SAFE = (16, 0)   # 이 시각(KST) 전에는 실제 오늘 봉을 미확정으로 보고 저장하지 않는다
CHUNK = 50                    # 이만큼 받을 때마다 저장한다
TIME_BUDGET_S = 20 * 60       # 워크플로 제한(40분)보다 넉넉히 짧게 — 저장·커밋·보고 시간을 남긴다
DEGRADED_COVER = 0.9          # 대상일 봉 확보 비율이 이보다 낮으면 DEGRADED
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


# ── 파일 ─────────────────────────────────────────────────────────────
def _read_gz(path):
    if not os.path.exists(path):
        return []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_gz(path, cols, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, lineterminator="\n")
    w.writeheader()
    w.writerows(rows)
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:   # mtime 고정은 하지 않는다 — 내용이 같으면 커밋 안 함은 git 이 판단
        fh.write(buf.getvalue())
    os.replace(tmp, path)


def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, sort_keys=True)
        fh.write("\n")
    os.replace(tmp, path)


def load_coverage(bars_dir=BARS_DIR):
    return _read_json(os.path.join(bars_dir, "coverage.json"), {"version": 1, "codes": {}})


def save_coverage(cov, bars_dir=BARS_DIR):
    os.makedirs(bars_dir, exist_ok=True)
    _write_json(os.path.join(bars_dir, "coverage.json"), cov)


# ── 저장 (처음 값 보존 · 수정 기록) ───────────────────────────────────
def store(bars_by_code, fetched_at, bars_dir=BARS_DIR, target=None, now=None):
    """새 (date, code) 는 추가, 기존과 값이 다르면 덮어쓰지 않고 수정 기록.

    `target` = 대상 거래일. 그 **뒤** 봉은 버린다(과거 복구가 이후 자료를 섞지 않게).
    `now` = 실제 실행 시각. **실제 오늘** 봉은 장 마감 확정 시각 전이면 버린다.
    `fetched_at` = 문자열 하나 또는 {code: 수신 시각}.
    반환 (추가 수, 수정 수, 미확정 제외 수)."""
    os.makedirs(bars_dir, exist_ok=True)
    now = now or datetime.datetime.now(KST)
    run_today = now.astimezone(KST).strftime("%Y-%m-%d")
    unsettled = (now.astimezone(KST).hour, now.astimezone(KST).minute) < MARKET_CLOSE_SAFE
    by_month = {}
    skipped = 0
    for code, bars in bars_by_code.items():
        fa = fetched_at.get(code, "") if isinstance(fetched_at, dict) else fetched_at
        for (d, o, h, l, c, v) in bars:
            if target and d > target:
                continue
            if d == run_today and unsettled:
                skipped += 1
                continue
            by_month.setdefault(d[:7], []).append({"date": d, "code": code, "open": num(o), "high": num(h),
                                                   "low": num(l), "close": num(c), "volume": num(v),
                                                   "fetchedAt": fa})
    added = revised = 0
    revs = []
    for month, new_rows in sorted(by_month.items()):
        path = os.path.join(bars_dir, f"{month}.csv.gz")
        old = {(r["date"], r["code"]): r for r in _read_gz(path)}
        changed = False
        for r in new_rows:
            k = (r["date"], r["code"])
            if k not in old:
                old[k] = r
                added += 1
                changed = True
                continue
            diff = [f for f in FIELDS if float(old[k][f]) != float(r[f])]
            for f in diff:
                revs.append({"date": r["date"], "code": r["code"], "field": f, "old": old[k][f], "new": r[f],
                             "oldFetchedAt": old[k]["fetchedAt"], "newFetchedAt": r["fetchedAt"]})
            if diff:
                revised += 1
        if changed:
            _write_gz(path, BAR_COLS, sorted(old.values(), key=lambda x: (x["date"], x["code"])))
    if revs:
        rp = os.path.join(bars_dir, "revisions.csv.gz")
        _write_gz(rp, REV_COLS, _read_gz(rp) + revs)
    return added, revised, skipped


def stored_dates(bars_dir=BARS_DIR):
    return sorted({r["date"] for p in glob.glob(os.path.join(bars_dir, "20*.csv.gz")) for r in _read_gz(p)})


# ── 수집 ─────────────────────────────────────────────────────────────
def request_size(entry, target):
    """종목별 요청 봉 수. 초기 수집 전이면 300, 아니면 마지막 저장 봉부터 대상일까지 메울 만큼."""
    if not entry or not entry.get("backfilled"):
        return BACKFILL_N
    last = entry.get("last")
    if not last:
        return BACKFILL_N
    gap = (datetime.date.fromisoformat(target) - datetime.date.fromisoformat(last)).days
    return min(BACKFILL_N, max(DAILY_N, gap + DAILY_N))


def pending_codes(cov):
    """지난 실행이 끝내지 못한 종목 — 초기 수집 미완료 또는 마지막 시도 실패."""
    return sorted(c for c, e in cov.get("codes", {}).items()
                  if not e.get("backfilled") or e.get("last_error"))


def plan_codes(day, cov, snap_dir=SNAP_DIR):
    """이번 실행 대상. 초기 수집 기록이 없으면 저장된 모든 스냅샷 날의 A 를 넣는다."""
    codes = set(universe(day, snap_dir)) | set(pending_codes(cov))
    if not any(e.get("backfilled") for c, e in cov.get("codes", {}).items() if c not in INDEXES):
        codes |= set(backfill_universe(snap_dir, upto=day))
    return sorted(codes)


def run_bars(day, get, bars_dir=BARS_DIR, snap_dir=SNAP_DIR, now=None, pause=0.05, sleep=time.sleep,
             clock=None, budget_s=TIME_BUDGET_S, run_id=""):
    """대상 거래일 `day` 의 일봉 수집. 묶음마다 저장하고 예산을 넘기면 멈춘다. 상태 dict 를 돌려준다."""
    now = now or datetime.datetime.now(KST)
    clock = clock or (lambda: datetime.datetime.now(KST))
    t0 = time.monotonic()
    cov = load_coverage(bars_dir)
    cov.setdefault("codes", {})
    codes = plan_codes(day, cov, snap_dir)
    order = list(codes) + list(INDEXES)
    for c in order:
        cov["codes"].setdefault(c, {"backfilled": False})
    save_coverage(cov, bars_dir)

    st = {"runId": run_id, "targetDay": day, "startedAt": now.isoformat(timespec="seconds"),
          "requested": 0, "responded": 0, "covered": 0, "missingTarget": 0, "failed": 0, "notAttempted": 0,
          "rejected": {}, "zeroVolume": 0, "added": 0, "revised": 0, "unsettledSkipped": 0,
          "failSample": {}, "missingSample": [], "backfillRequests": 0, "plannedCodes": len(order)}
    got, fetched = {}, {}

    def flush():
        if not got:
            return
        a, r, s = store(got, dict(fetched), bars_dir, day, now)
        st["added"] += a
        st["revised"] += r
        st["unsettledSkipped"] += s
        got.clear()
        fetched.clear()
        save_coverage(cov, bars_dir)

    try:
        for i, code in enumerate(order):
            if time.monotonic() - t0 > budget_s:
                st["notAttempted"] = len(order) - i
                break
            entry = cov["codes"][code]
            n = request_size(entry, day)
            res = fetch(code, n, get)
            bars, err = res[0], res[1]
            info = res[2] if len(res) > 2 else {}
            recv = clock().isoformat(timespec="seconds")
            sleep(pause)                              # 원천에 부담을 주지 않도록 종목 사이를 띄운다
            st["requested"] += 1
            st["backfillRequests"] += n == BACKFILL_N
            for k, v in (info.get("rejects") or {}).items():
                st["rejected"][k] = st["rejected"].get(k, 0) + v
            st["zeroVolume"] += info.get("zero_volume", 0)
            entry["lastAttempt"] = recv
            if err:
                st["failed"] += 1
                entry["last_error"] = err
                if len(st["failSample"]) < 3:
                    st["failSample"][code] = err
                continue
            st["responded"] += 1
            usable = [b for b in bars if b[0] <= day]
            if n == BACKFILL_N:
                entry["backfilled"] = True
                entry["shortHistory"] = len(bars) < BACKFILL_N
            if usable:
                entry["last"] = max(entry.get("last") or "", usable[-1][0])
                entry["first"] = min(entry.get("first") or "9999", usable[0][0])
            if any(b[0] == day for b in usable):
                st["covered"] += 1
                entry["last_error"] = ""
            else:
                st["missingTarget"] += 1
                entry["last_error"] = "대상일 봉 없음"
                if len(st["missingSample"]) < 5:
                    st["missingSample"].append(code)
            got[code] = bars
            fetched[code] = recv
            if len(got) >= CHUNK:
                flush()
    finally:
        flush()                                       # 중단돼도 받은 것은 남긴다(R4)
        save_coverage(cov, bars_dir)

    st["finishedAt"] = clock().isoformat(timespec="seconds")
    st["backfilledCodes"] = sum(1 for c, e in cov["codes"].items() if e.get("backfilled"))
    st["pendingAfter"] = len(pending_codes(cov))
    st["status"] = status_of(st)
    return st


def status_of(st):
    planned = st["plannedCodes"]
    if planned and st["responded"] == 0:
        return "FAILED"
    if st["failed"] or st["notAttempted"] or (planned and st["covered"] / planned < DEGRADED_COVER):
        return "DEGRADED"
    return "OK"


def save_run(st, bars_dir=BARS_DIR):
    os.makedirs(bars_dir, exist_ok=True)
    _write_json(os.path.join(bars_dir, "last_run.json"), st)
    path = os.path.join(bars_dir, "runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        row = dict(st)
        row["rejected"] = sum(st.get("rejected", {}).values())
        w.writerow(row)


def load_run(day, run_id, bars_dir=BARS_DIR):
    """보고용 — 같은 실행 ID·같은 대상일의 수집 상태만. 아니면 None (오래된 상태를 쓰지 않는다)."""
    st = _read_json(os.path.join(bars_dir, "last_run.json"), None)
    if not st or st.get("targetDay") != day:
        return None
    if run_id and st.get("runId") != run_id:
        return None
    return st


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


def report_text(day, bars, struct, locked, bars_dir=BARS_DIR, archive=""):
    """허용된 상태 필드만 쓴다. 수익률·승률·후보별 가격 경로는 넣지 않는다."""
    L = [f"{TAG} 테마·대장 경로 연구 일일 보고 · 대상일 {day}"]
    if bars:
        L.append(f"· 수집 상태 {bars['status']} · 실행 {bars.get('startedAt', '?')} ~ {bars.get('finishedAt', '?')}")
        L.append(f"· 일봉: 계획 {bars['plannedCodes']} · 요청 {bars['requested']} · 응답 {bars['responded']} · "
                 f"대상일 확보 {bars['covered']} · 대상일 봉 없음 {bars['missingTarget']} · 실패 {bars['failed']} · "
                 f"미처리 {bars['notAttempted']}")
        L.append(f"· 저장: 새 행 {bars['added']} · 값 변경 기록 {bars['revised']} · "
                 f"품질 제외 {sum(bars.get('rejected', {}).values())} · 무거래 봉 {bars.get('zeroVolume', 0)} · "
                 f"초기 수집 완료 {bars.get('backfilledCodes', 0)}/{bars['plannedCodes']} · 다음 실행 미처리 {bars.get('pendingAfter', 0)}")
        if bars.get("unsettledSkipped"):
            L.append(f"  (장 마감 전 실제 오늘 봉 {bars['unsettledSkipped']}건은 미확정이라 저장 안 함)")
    else:
        L.append("· 수집 상태: 이번 실행의 수집 기록 없음 — 수집이 돌지 않았거나 다른 실행의 기록이다")
    L.append(f"· 원격 보관: {archive or '확인 안 됨'}")
    sd = stored_dates(bars_dir)
    L.append(f"· 저장된 일봉 날짜: {sd[0] if sd else '—'} ~ {sd[-1] if sd else '—'} ({len(sd)}일)")
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
            print(f"ℹ️ 오늘({today})은 예정 거래일이 아니다 — 수집·보고 생략 (빠진 봉은 다음 실행이 메운다)")
            return 0
        day = target_day(now)
        if not day:
            print("ℹ️ 대상 거래일을 달력으로 정할 수 없다 — 수집·보고 생략")
            return 0
    code = 0
    bars = None
    if a.bars:
        import requests
        bars = run_bars(day, requests.get, bars_dir=BARS_DIR, now=now, run_id=run_id)
        save_run(bars, BARS_DIR)
        print(f"📈 일봉 {json.dumps(bars, ensure_ascii=False)}")
        if bars["status"] == "FAILED":
            code = 1
    if a.report:
        bars = bars or load_run(day, run_id, BARS_DIR)
        text = report_text(day, bars, structure_today(day), locked_status(), bars_dir=BARS_DIR,
                           archive=env.get("RESEARCH_ARCHIVE", ""))
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
