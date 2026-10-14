# -*- coding: utf-8 -*-
# ==========================================================================
# 🕐 급등 사건 과거 1분봉 일괄 수집 `minute-backfill-v1` — 한투 주식일별분봉조회 (연구 전용, 수동 실행)
# --------------------------------------------------------------------------
# 설계: docs/collaboration/2026-10-14_Claude_급등사건_분봉수집설계.md
# 왜: '급등 대장주 5거래일 안 +10%' 공통 조건 연구(PC)는 사건일 t 와 t+1 의 1분봉이 필요하다. 한투는 과거 분봉을
#     **최대 1년만** 보관한다(2025-10-15 사건부터 하루씩 사라진다) — 그래서 오래된 달부터 받는다.
# 사건 목록(종목코드·날짜)은 공개 저장소에 두지 않는다. PC 가 `--pack` 으로 묶은 문자열(gzip + base64, 약 10KB)을
#     GitHub 비밀값 `MINUTE_EVENTS` 로 넘긴다. 공개 장부에는 목록 sha256 앞 12자·건수만 남긴다(PC 목록과 대조용).
# 모드
#   --pack EVENTS --calendar CAL --out FILE — (PC) 사건 CSV(code, date[, t1]) → 비밀값 문자열 파일.
#             t1 이 없으면 달력 CSV 의 date 열에서 t 다음 날(이 저장소의 거래일 달력은 2026-08 부터라 러너가 못 정한다)
#   --probe — 시험 호출. 삼성전자로 과거 분봉(보관 끝 근처 날짜 포함)·프로그램매매 시간대별의 날짜 입력 반응을
#             **구조만** 출력한다(가격·수량 없음). 저장·커밋 없음
#   (기본) 수집 — `--months all|YYYY-MM,...`(사건일 t 의 달). 그달 사건의 t·t+1 의 09:00~15:30 1분봉.
#             원본은 드라이브 비공개, 공개 `data/research_runs/minute_backfill_runs.csv` 는 건수만.
#             `all` 은 같은 목록으로 이미 끝난 달을 건너뛴다(이어 받기). 달을 적어 주면 그 달은 다시 받는다
#   --merge DIR --out OUT — (PC) 드라이브에서 받은 원본 → 영수증 sha256 대조 → 종목·날짜마다 가장 완전한 것 → minute.csv.gz
# 운영 KIS 토큰(시트 칸)·선정·시트와 무관, 지문 대상이 아니다. 실행당 토큰 1회(hyeoks_kis_news 와 같다).
# 종목코드·가격은 공개 장부·로그·커밋 메시지에 남기지 않는다.
# ==========================================================================
import argparse
import base64
import bisect
import collections
import csv
import datetime
import glob
import gzip
import hashlib
import io
import json
import os
import sys
import time

import hyeoks_research_daily as R
from hyeoks_kis_news import BASE, token

VERSION = "minute-backfill-v1"
PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
TR_ID = "FHKST03010230"            # 주식일별분봉조회 — 한 번에 최대 120봉, 과거는 날짜·시각 입력, 최대 1년 보관(공식 예제)
PROGRAM_PATH = "/uapi/domestic-stock/v1/quotations/program-trade-by-stock"
PROGRAM_TR = "FHPPG04650101"       # 종목별 프로그램매매추이(체결) — 공식 예제에 날짜 입력이 없다. 시험 호출에서만 본다
MARKET = "J"                       # KRX — 연구 일봉(네이버 KRX)과 같은 시장
PW_INCU = "N"                      # 과거 데이터 포함 여부 — 공식 예제 기본값. 시험 호출이 N·Y 를 비교한다
START_HOUR = "160000"              # 이 시각부터 거꾸로 받는다. 15:30 봉이 빠지지 않게 마감 뒤로 잡는다
OPEN, CLOSE = "090000", "153000"
MAX_PAGES = 8                      # 하루 381봉 ÷ 120 = 4회. 겹침 여유
FULL_FIRST = "0905"                # 첫 봉이 이 시각 이하이고 마지막이 15:30 이면 '완전'
PAUSE_DAY, PAUSE_NIGHT = 0.25, 0.1 # 07~20시는 운영 omakase 와 같은 앱키를 나눠 쓴다(한투 한도 초당 20건)
RATE_LIMIT = "EGW00201"            # 초당 거래건수 초과
STOP_AFTER = 3                     # 속도 제한이 아닌 응답 오류가 종목·날짜 연속 3개면 그 실행을 멈춘다
TIME_BUDGET_S = 320 * 60           # 워크플로 350분 안에서 업로드·커밋 몫을 남긴다
SECRET_LIMIT = 48_000              # GitHub 비밀값 한도(48KB)
EVENTS_ENV = "MINUTE_EVENTS"
FIELDS = ["code", "date", "time", "open", "high", "low", "close", "volume"]
PROBE_CODE = "005930"
RUNS_FILE = "minute_backfill_runs.csv"
RUN_COLS = ["day", "runId", "version", "month", "eventsSha", "eventsTotal", "monthEvents", "codeDays", "attempted",
            "full", "partial", "empty", "failed", "rows", "rejected", "calls", "rateLimitRetries", "startedAt",
            "finishedAt", "status", "stopReason", "privateFiles", "privateStatus"]
KST = R.KST


def _int(x):
    try:
        return int(float(str(x).replace(",", "").strip()))
    except (TypeError, ValueError):
        return None


def _rows(v):
    return [v] if isinstance(v, dict) else [x for x in (v or []) if isinstance(x, dict)]


# ── 사건 목록 (PC 에서 묶고 러너에서 푼다) ───────────────────────────────
def canonical(events):
    """[(code, t, t1)] → 날짜·코드 순 'code,t,t1' 줄. 비밀값과 sha256 의 기준."""
    return "".join(f"{c},{t},{t1}\n" for c, t, t1 in sorted(set(events), key=lambda e: (e[1], e[0])))


def events_sha(events):
    return hashlib.sha256(canonical(events).encode("utf-8")).hexdigest()[:12]


def _valid(c, t, t1):
    return (len(c) == 6 and c.isalnum() and all(len(d) == 8 and d.isdigit() for d in (t, t1)) and t1 > t)


def unpack(b64):
    """비밀값 → [(code, t, t1)]. 한 줄이라도 형식이 틀리면 ValueError — 조용히 일부만 받지 않는다."""
    text = gzip.decompress(base64.b64decode(b64.strip())).decode("utf-8")
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        parts = line.strip().split(",")
        if len(parts) != 3 or not _valid(*parts):
            raise ValueError(f"사건 목록 {i}번째 줄 형식 아님")
        out.append(tuple(parts))
    if not out:
        raise ValueError("사건 목록이 비었다")
    return out


def pack(events_csv, calendar_csv, out_path):
    """(PC) 사건 CSV → 비밀값 문자열 파일. 종목코드는 출력하지 않는다(건수·달·sha 만)."""
    with open(calendar_csv, encoding="utf-8") as fh:
        cal = sorted({str(r["date"]).replace("-", "").strip() for r in csv.DictReader(fh)})
    events = []
    with open(events_csv, encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            c = str(r["code"]).strip()
            c = c.zfill(6) if c.isdigit() else c
            t = str(r["date"]).replace("-", "").strip()
            t1 = str(r.get("t1") or "").replace("-", "").strip()
            if not t1:
                i = bisect.bisect_right(cal, t)
                if i >= len(cal):
                    raise ValueError(f"{t} 다음 거래일이 달력에 없다")
                t1 = cal[i]
            if not _valid(c, t, t1):
                raise ValueError(f"형식 아님: 날짜 {t}")
            events.append((c, t, t1))
    b64 = base64.b64encode(gzip.compress(canonical(events).encode("utf-8"), mtime=0)).decode("ascii")
    with open(out_path, "w", encoding="ascii", newline="") as fh:
        fh.write(b64)
    ev = unpack(b64)
    months = collections.Counter(f"{t[:4]}-{t[4:6]}" for _, t, _ in ev)
    print(f"📦 사건 {len(ev)}건 · 종목 {len({c for c, _, _ in ev})} · 날 {len({t for _, t, _ in ev})} · "
          f"목록 {events_sha(ev)} · 비밀값 {len(b64):,}자(한도 {SECRET_LIMIT:,})")
    print("   달별 " + " · ".join(f"{m} {n}" for m, n in sorted(months.items())))
    if len(b64) > SECRET_LIMIT:
        print("❌ GitHub 비밀값 한도를 넘는다 — 목록을 나눠야 한다")
        return 1
    print(f"   → {out_path} 내용을 저장소 Settings → Secrets and variables → Actions 의 `{EVENTS_ENV}` 에 붙여 넣는다")
    return 0


def months_of(events):
    return sorted({f"{t[:4]}-{t[4:6]}" for _, t, _ in events})


def code_days(events, month):
    """그달(사건일 t 기준) 사건의 (날짜, 코드) — t 와 t+1, 중복 없이 오래된 날부터(보관 1년이라 먼저 사라진다)."""
    return sorted({(d, c) for c, t, t1 in events if f"{t[:4]}-{t[4:6]}" == month for d in (t, t1)})


# ── 한투 호출 ─────────────────────────────────────────────────────────
def headers(key, secret, tok, tr_id=TR_ID):
    return {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}", "appkey": key,
            "appsecret": secret, "tr_id": tr_id, "custtype": "P"}


def kis_get(get, h, params, sleep, path=PATH):
    """한 번 부른다 — (응답, 오류, 속도 제한 재시도 수). 속도 제한은 1초 쉬고 두 번까지, 통신·형식 오류는 한 번 다시."""
    retries, err = 0, ""
    for attempt in range(3):
        try:
            js = get(BASE + path, headers=h, params=params, timeout=15).json()
            if not isinstance(js, dict) or "rt_cd" not in js:
                raise ValueError("형식 아님")
        except Exception as e:
            err = type(e).__name__
            if attempt == 0:
                sleep(1.0)
                continue
            break
        if str(js.get("rt_cd")) != "0" and str(js.get("msg_cd", "")) == RATE_LIMIT and attempt < 2:
            retries += 1
            sleep(1.0)
            continue
        return js, "", retries
    return None, err, retries


def minute_params(code, day, hour, pw=PW_INCU):
    return {"FID_COND_MRKT_DIV_CODE": MARKET, "FID_INPUT_ISCD": code, "FID_INPUT_HOUR_1": hour,
            "FID_INPUT_DATE_1": day, "FID_PW_DATA_INCU_YN": pw, "FID_FAKE_TICK_INCU_YN": ""}


def parse_rows(rows, day):
    """output2 → ({HHMMSS: (시가, 고가, 저가, 종가, 거래량)}, 그날 행 중 가장 이른 시각, 다른 날짜 행 수, 거부 수).
    09:00~15:30 밖 시각은 담지 않는다(거부도 아니다). 가격 관계가 틀린 봉은 거부로 센다."""
    got, earliest, other, rej = {}, None, 0, 0
    for x in rows:
        if str(x.get("stck_bsop_date", "")).strip() != day:
            other += 1
            continue
        t = str(x.get("stck_cntg_hour", "")).strip()
        if not (t.isdigit() and len(t) <= 6):
            rej += 1
            continue
        t = t.rjust(6, "0")
        earliest = t if earliest is None or t < earliest else earliest
        if not (OPEN <= t <= CLOSE):
            continue
        o, hi, lo, c, v = (_int(x.get(k)) for k in ("stck_oprc", "stck_hgpr", "stck_lwpr", "stck_prpr", "cntg_vol"))
        if None in (o, hi, lo, c, v) or min(o, hi, lo, c) <= 0 or v < 0 or hi < max(o, c, lo) or lo > min(o, c, hi):
            rej += 1
            continue
        got[t] = (o, hi, lo, c, v)
    return got, earliest, other, rej


def day_status(times, error=""):
    """오류가 났으면 받은 봉이 있어도 '실패' — 그 달을 다시 받게 한다(합치기가 더 완전한 쪽을 고른다)."""
    if error:
        return "실패"
    if not times:
        return "빈 날"
    return "완전" if times[0][:4] <= FULL_FIRST and times[-1][:4] == CLOSE[:4] else "일부"


def fetch_day(get, h, code, day, sleep, pause):
    """종목·날짜 하나의 09:00~15:30 1분봉 — (행들, 상태). 120봉씩 거꾸로 받고, 그 페이지의 가장 이른 시각을
    다음 입력으로 쓴다(입력 시각이 포함이든 아니든 겹침은 시각으로 합친다). 더 거슬러 가지 못하면 멈춘다."""
    bars, hour, prev = {}, START_HOUR, None
    st = {"code": code, "date": day, "calls": 0, "rateLimitRetries": 0, "otherDateRows": 0, "rejected": 0,
          "rt_cd": "", "error": ""}
    for _ in range(MAX_PAGES):
        if st["calls"]:
            sleep(pause)
        js, err, n = kis_get(get, h, minute_params(code, day, hour), sleep)
        st["calls"] += 1
        st["rateLimitRetries"] += n
        if js is None:
            st["error"] = err
            break
        st["rt_cd"] = str(js.get("rt_cd"))
        if st["rt_cd"] != "0":
            st["error"] = str(js.get("msg_cd", "")) or "rt_cd"
            break
        got, earliest, other, rej = parse_rows(_rows(js.get("output2")), day)
        bars.update(got)
        st["otherDateRows"] += other
        st["rejected"] += rej
        if earliest is None or earliest <= OPEN or (prev is not None and earliest >= prev):
            break
        prev = hour = earliest
    times = sorted(bars)
    st.update(rows=len(times), first=times[0][:4] if times else "", last=times[-1][:4] if times else "",
              status=day_status(times, st["error"]))
    return [[code, day, t[:4], *bars[t]] for t in times], st


def pause_for(now):
    return PAUSE_DAY if 7 <= now.hour < 20 else PAUSE_NIGHT


# ── 수집 ─────────────────────────────────────────────────────────────
def collect_month(month, events, get, h, clock, sleep, run_id="", stop_at=None, sha=""):
    """한 달 — (행들, 종목·날짜 상태들, 공개 상태). 공개 상태에는 건수만 담는다."""
    todo = code_days(events, month)
    st = {"day": clock().strftime("%Y-%m-%d"), "runId": run_id, "version": VERSION, "month": month,
          "eventsSha": sha, "eventsTotal": len(events),
          "monthEvents": sum(1 for _, t, _ in events if f"{t[:4]}-{t[4:6]}" == month), "codeDays": len(todo),
          "startedAt": clock().isoformat(timespec="seconds"), "status": "완료", "stopReason": ""}
    rows, stats, bad = [], [], 0
    for i, (day, code) in enumerate(todo):
        if stop_at is not None and clock() >= stop_at:
            st.update(status="중단", stopReason=f"시간 예산 — {i}/{len(todo)}")
            break
        if i:
            sleep(pause_for(clock()))
        got, s = fetch_day(get, h, code, day, sleep, pause_for(clock()))
        rows += got
        stats.append(s)
        bad = bad + 1 if s["rt_cd"] not in ("0", "") and s["error"] != RATE_LIMIT else 0
        if bad >= STOP_AFTER:
            st.update(status="중단", stopReason=f"응답 오류 연속 {STOP_AFTER}회 ({s['error']})")
            break
    count = collections.Counter(s["status"] for s in stats)
    st.update(finishedAt=clock().isoformat(timespec="seconds"), attempted=len(stats), full=count["완전"],
              partial=count["일부"], empty=count["빈 날"], failed=count["실패"], rows=len(rows),
              rejected=sum(s["rejected"] for s in stats), calls=sum(s["calls"] for s in stats),
              rateLimitRetries=sum(s["rateLimitRetries"] for s in stats))
    if st["status"] == "완료" and st["failed"]:
        st.update(status="일부 실패", stopReason=f"실패 {st['failed']}개 — 이 달을 다시 받으면 된다")
    return rows, stats, st


def _csv_gz(rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(FIELDS)
    w.writerows(rows)
    return gzip.compress(buf.getvalue().encode("utf-8"), mtime=0)


def private_files(month, run_id, rows, stats, meta, limit=R.PART_LIMIT_BYTES):
    """[(이름, gz 바이트)]. 한 파일이 업로드 상한을 넘으면 종목·날짜 단위로 반씩 나눈다(연구 일봉과 같은 방식)."""
    base = f"research_minute_{month}_{run_id or 'local'}"
    units = sorted({(r[1], r[0]) for r in rows})
    by = collections.defaultdict(list)
    for r in rows:
        by[(r[1], r[0])].append(r)
    parts = [units] if units else []
    while True:
        blobs = [_csv_gz([r for u in p for r in by[u]]) for p in parts]
        big = [i for i, b in enumerate(blobs) if len(b) > limit and len(parts[i]) > 1]
        if not big:
            break
        p = parts.pop(big[0])
        parts[big[0]:big[0]] = [p[:len(p) // 2], p[len(p) // 2:]]
    out = [(f"{base}.csv.gz" if len(blobs) == 1 else f"{base}_part{i + 1}of{len(blobs)}.csv.gz", b)
           for i, b in enumerate(blobs)]
    status = dict(meta, version=VERSION, month=month, runId=run_id, codeDays=stats)
    out.append((f"{base}_status.json.gz", gzip.compress(json.dumps(status, ensure_ascii=False).encode("utf-8"),
                                                        mtime=0)))
    return out


def store_month(month, rows, stats, st, uploader, runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    files = private_files(month, st["runId"], rows, stats, {"eventsSha": st["eventsSha"]})
    rcs = [R.upload_receipt("minute", st["runId"], name, data, uploader, private_dir) for name, data in files]
    R.append_receipts(rcs, runs_dir)
    st.update(privateFiles=len(rcs), privateStatus="접수" if all(r["status"] == "접수" for r in rcs) else "실패")
    return rcs


def log_run(st, runs_dir=R.RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, RUNS_FILE)
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def done_months(sha, runs_dir=R.RUNS_DIR):
    """같은 사건 목록으로 끝까지 받고 보관까지 된 달 — `all` 이 건너뛴다."""
    path = os.path.join(runs_dir, RUNS_FILE)
    if not os.path.exists(path):
        return set()
    with open(path, encoding="utf-8") as fh:
        return {r["month"] for r in csv.DictReader(fh)
                if r.get("eventsSha") == sha and r.get("status") == "완료" and r.get("privateStatus") == "접수"}


def main_collect(months_arg, env, get, post, clock, sleep, uploader=None, runs_dir=R.RUNS_DIR,
                 private_dir=R.PRIVATE_DIR, budget_s=TIME_BUDGET_S):
    b64 = env.get(EVENTS_ENV, "")
    if not b64:
        print(f"❌ 비밀값 {EVENTS_ENV} 없음 — PC 에서 --pack 으로 만든 문자열을 넣어야 한다")
        return 1
    try:
        events = unpack(b64)
    except Exception as e:
        print(f"❌ 사건 목록을 풀 수 없다 — {type(e).__name__}: {str(e)[:60]}")
        return 1
    sha, months = events_sha(events), months_of(events)
    if months_arg == "all":
        todo = [m for m in months if m not in done_months(sha, runs_dir)]
    else:
        todo = [m.strip() for m in months_arg.split(",") if m.strip()]
        if any(m not in months for m in todo):
            print(f"❌ 목록에 없는 달: {', '.join(m for m in todo if m not in months)} (있는 달 {months[0]}~{months[-1]})")
            return 1
    print(f"🕐 사건 {len(events)}건 · 목록 {sha} · 달 {len(months)}개 · 이번 {', '.join(todo) or '없음(모두 끝남)'}")
    if not todo:
        return 0
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    try:
        tok = token(post, key, secret)
    except Exception as e:
        print(f"❌ {str(e)[:80]}")
        return 1
    h, run_id, worst = headers(key, secret, tok), env.get("GITHUB_RUN_ID", ""), 0
    stop_at = clock() + datetime.timedelta(seconds=budget_s)
    for m in todo:
        rows, stats, st = collect_month(m, events, get, h, clock, sleep, run_id, stop_at, sha)
        store_month(m, rows, stats, st, uploader or R.drive_uploader(), runs_dir, private_dir)
        log_run(st, runs_dir)
        print(f"   {m} — 종목·날짜 {st['attempted']}/{st['codeDays']} · 완전 {st['full']} · 일부 {st['partial']} · "
              f"빈 날 {st['empty']} · 실패 {st['failed']} · 봉 {st['rows']:,} · 호출 {st['calls']:,} · "
              f"{st['status']}{(' — ' + st['stopReason']) if st['stopReason'] else ''} · 보관 {st['privateStatus']}")
        if st["status"] != "완료" or st["privateStatus"] != "접수":
            worst = 1
        if st["status"] == "중단":
            break
    return worst


# ── 시험 호출 ─────────────────────────────────────────────────────────
def probe(env, get, post, clock, sleep, code=PROBE_CODE):
    """구조만 출력한다 — 가격·수량은 찍지 않는다. ① 과거 분봉: 보관 끝 근처(1년 전 앞뒤)와 첫 사건 달 첫날, PW N·Y
    ② 하루 전체 받기(fetch_day) ③ 프로그램매매 시간대별: 날짜 입력이 있을 때와 없을 때 응답이 같은지."""
    key, secret = env.get("KIS_APP_KEY", ""), env.get("KIS_APP_SECRET", "")
    if not (key and secret):
        print("❌ KIS_APP_KEY / KIS_APP_SECRET 없음")
        return 1
    now = clock()
    print(f"# 한투 과거 분봉·프로그램매매 시험 호출 · {now.isoformat(timespec='seconds')} · 종목 {code}")
    try:
        tok = token(post, key, secret)
    except Exception as e:
        print(f"❌ {str(e)[:80]}")
        return 1
    h, worst = headers(key, secret, tok), 0
    edge = now.date().replace(year=now.year - 1)
    days = sorted({(edge + datetime.timedelta(days=k)).strftime("%Y%m%d") for k in (-1, 0, 1, 2)} | {"20251015"})
    print("## 과거 분봉 — 날짜·PW 마다 1회(16:00 부터 거꾸로 120봉)")
    for d in days:
        for pw in ("N", "Y"):
            sleep(PAUSE_DAY)
            js, err, n = kis_get(get, h, minute_params(code, d, START_HOUR, pw), sleep)
            if js is None:
                print(f"- {d} PW={pw}: 실패 {err}")
                worst = 1
                continue
            o2 = _rows(js.get("output2"))
            dates = collections.Counter(str(x.get("stck_bsop_date", "")) for x in o2)
            same = [str(x.get("stck_cntg_hour", "")) for x in o2 if str(x.get("stck_bsop_date", "")) == d]
            print(f"- {d} PW={pw}: rt_cd={js.get('rt_cd')} {js.get('msg_cd', '')} · {len(o2)}행 · 날짜별 {dict(dates)} · "
                  f"그날 시각 {min(same) if same else '-'}~{max(same) if same else '-'}")
    print(f"  output1 필드 {sorted({k for x in _rows(js.get('output1')) for k in x}) if js else '-'}")
    print(f"  output2 필드 {sorted({k for x in _rows(js.get('output2')) for k in x}) if js else '-'}")
    print("## 하루 전체 받기(수집과 같은 함수)")
    for d in days:
        _rows_got, s = fetch_day(get, h, code, d, sleep, PAUSE_DAY)
        print(f"- {d}: {s['status']} · {s['rows']}봉 · {s['first'] or '-'}~{s['last'] or '-'} · 호출 {s['calls']} · "
              f"다른 날짜 행 {s['otherDateRows']} · 거부 {s['rejected']}{(' · 오류 ' + s['error']) if s['error'] else ''}")
    print("## 프로그램매매 시간대별 — 날짜 입력 반응")
    ph = headers(key, secret, tok, PROGRAM_TR)
    base = {"FID_COND_MRKT_DIV_CODE": MARKET, "FID_INPUT_ISCD": code}
    seen = {}
    for label, extra in (("날짜 없음", {}), ("날짜 20251015", {"FID_INPUT_DATE_1": "20251015", "FID_INPUT_HOUR_1": "153000"})):
        sleep(PAUSE_DAY)
        js, err, _n = kis_get(get, ph, dict(base, **extra), sleep, PROGRAM_PATH)
        if js is None:
            print(f"- {label}: 실패 {err}")
            continue
        out = _rows(js.get("output"))
        hours = [str(x.get("bsop_hour", "")) for x in out if x.get("bsop_hour")]
        seen[label] = hashlib.sha256(json.dumps(out, sort_keys=True).encode()).hexdigest()[:12]
        print(f"- {label}: rt_cd={js.get('rt_cd')} {js.get('msg_cd', '')} · {len(out)}행 · "
              f"시각 {min(hours) if hours else '-'}~{max(hours) if hours else '-'} · 필드 {sorted({k for x in out for k in x})}")
    if len(seen) == 2:
        print("  → " + ("두 응답이 같다 — 날짜 입력을 무시한다(과거 날짜 미지원으로 본다)" if len(set(seen.values())) == 1
                        else "두 응답이 다르다 — 날짜 입력에 반응한다(값으로 과거 날짜인지 따로 확인해야 한다)"))
    return worst


# ── 합치기 (PC) ───────────────────────────────────────────────────────
def merge(src, out_dir, receipts=None, events_csv=None):
    """드라이브에서 받은 원본 → 종목·날짜마다 봉이 가장 많은 것(같으면 이름 순 뒤 = 나중 실행) → minute.csv.gz.
    영수증 CSV 를 주면 sha256 이 맞는 파일만 쓴다."""
    want = {}
    if receipts:
        with open(receipts, encoding="utf-8") as fh:
            want = {r["name"]: r["sha256"] for r in csv.DictReader(fh) if r.get("kind") == "minute"}
    best, used, skipped = {}, [], []
    for p in sorted(glob.glob(os.path.join(src, "research_minute_*.csv.gz"))):
        name = os.path.basename(p)
        with open(p, "rb") as fh:
            data = fh.read()
        if receipts and want.get(name) != hashlib.sha256(data).hexdigest():
            skipped.append(name)
            continue
        groups = collections.defaultdict(list)
        for r in csv.DictReader(io.StringIO(gzip.decompress(data).decode("utf-8"))):
            groups[(r["code"], r["date"])].append([r[k] for k in FIELDS])
        for k, rs in groups.items():
            if k not in best or len(rs) >= len(best[k][0]):
                best[k] = (sorted(rs, key=lambda r: r[2]), name)
        used.append(name)
    os.makedirs(out_dir, exist_ok=True)
    with gzip.open(os.path.join(out_dir, "minute.csv.gz"), "wt", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(FIELDS)
        for k in sorted(best):
            w.writerows(best[k][0])
    status = {}
    with open(os.path.join(out_dir, "minute_status.csv"), "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["code", "date", "rows", "first", "last", "status", "source"])
        for k in sorted(best):
            rs, name = best[k]
            status[k] = day_status([r[2] for r in rs])
            w.writerow([k[0], k[1], len(rs), rs[0][2], rs[-1][2], status[k], name])
    count = collections.Counter(status.values())
    print(f"🧩 파일 {len(used)}개 사용 · sha 불일치·영수증 없음 {len(skipped)}개 제외 · 종목·날짜 {len(best):,} "
          f"(완전 {count['완전']:,} · 일부 {count['일부']:,})")
    if events_csv:
        with open(events_csv, encoding="utf-8") as fh:
            ev = [(str(r["code"]).strip().zfill(6), str(r["date"]).replace("-", ""), str(r.get("t1") or "").replace("-", ""))
                  for r in csv.DictReader(fh)]
        both = sum(1 for c, t, t1 in ev if status.get((c, t)) == "완전" and (not t1 or status.get((c, t1)) == "완전"))
        has_t = sum(1 for c, t, _ in ev if (c, t) in status)
        print(f"   사건 {len(ev):,}건 중 t 분봉 있음 {has_t:,} · t·t+1 모두 완전 {both:,}")
    print(f"📄 {out_dir}/minute.csv.gz · minute_status.csv (비공개 — 저장소에 올리지 않는다)")
    return 0 if not skipped else 1


def main(argv=None, env=None, get=None, post=None, clock=None, sleep=time.sleep, uploader=None,
         runs_dir=R.RUNS_DIR, private_dir=R.PRIVATE_DIR):
    ap = argparse.ArgumentParser(description="급등 사건 과거 1분봉 일괄 수집 (한투 주식일별분봉조회)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--pack", metavar="EVENTS_CSV", help="(PC) 사건 CSV → 비밀값 문자열 파일")
    mode.add_argument("--probe", action="store_true", help="시험 호출 — 구조만 출력, 저장 없음")
    mode.add_argument("--merge", metavar="DIR", help="(PC) 드라이브에서 받은 원본 폴더 → minute.csv.gz")
    ap.add_argument("--calendar", help="--pack: t1 이 없을 때 쓸 거래일 달력 CSV(date 열)")
    ap.add_argument("--out", help="--pack: 비밀값 파일 · --merge: 결과 폴더")
    ap.add_argument("--receipts", help="--merge: 공개 영수증 private_receipts.csv (sha256 대조)")
    ap.add_argument("--events", help="--merge: 사건 CSV(code, date[, t1]) — 받은 비율을 센다")
    ap.add_argument("--months", default="all", help="수집: all 또는 YYYY-MM,YYYY-MM (사건일 t 의 달)")
    a = ap.parse_args(argv)
    if a.pack:
        if not (a.calendar and a.out):
            ap.error("--pack 에는 --calendar 와 --out 이 필요하다")
        return pack(a.pack, a.calendar, a.out)
    if a.merge:
        if not a.out:
            ap.error("--merge 에는 --out 이 필요하다")
        return merge(a.merge, a.out, a.receipts, a.events)
    env = os.environ if env is None else env
    clock = clock or (lambda: datetime.datetime.now(KST))
    if get is None or post is None:
        import requests
        get, post = get or requests.get, post or requests.post
    if a.probe:
        return probe(env, get, post, clock, sleep)
    return main_collect(a.months, env, get, post, clock, sleep, uploader, runs_dir, private_dir)


if __name__ == "__main__":
    sys.exit(main())
