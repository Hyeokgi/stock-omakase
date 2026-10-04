# -*- coding: utf-8 -*-
# ==========================================================================
# 🧭 테마·대장 경로 연구 — 확정 일봉 수집 + [연구] 일일 보고
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-04): "일봉은 깃허브에, 연구보고는 켜줘".
#
# 1) 일봉 (`--bars`)
#    · 대상: 그날 15:05 스냅샷의 A(거래대금 ≥ 50억, 거래정지·관리종목 제외) ∪ 최근 20 관측일의 대장 후보.
#      대장 배지가 사라져도 추적을 이어 가기 위해서다(Codex 2026-10-04 제언: 최초 후보 추적 유지).
#    · 원천: 네이버 fchart 일봉 (`omakase.get_daily_bars` 와 같은 엔드포인트). **수정주가 여부는 확인되지 않았다.**
#    · 저장: `data/daily_bars/YYYY-MM.csv.gz` — (date, code) 당 처음 받은 값을 보존한다.
#      나중에 다른 값이 오면 덮어쓰지 않고 `revisions.csv.gz` 에 남긴다(기업행사 조정 탐지).
#    · 저장소가 비어 있으면 한 번 백필한다(저장된 모든 스냅샷 날의 대상, 300봉).
#    · 장중에는 당일 봉을 저장하지 않는다(미확정).
# 2) 보고 (`--report [--send]`)
#    · 텔레그램 기존 채널에 `[연구]` 머리말로 보낸다. 수집 상태와 **구조 집계만** 담는다.
#    · 🔒 가격 수익률·대장 후보의 이후 성과는 담지 않는다 — 잠긴 사전등록 `leader-hold-v1` 의
#      확증 구간(진입일 ≥ 2026-10-06)과 겹치기 때문이다. 잠긴 연구는 유효 비교일 '수' 만 적는다.
#
# 하지 않는 것: 주문, 시트 쓰기, 선정 조건 변경. 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import csv
import datetime
import glob
import gzip
import io
import os
import sys
import time
import xml.etree.ElementTree as ET

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, KST, read_snapshot

BARS_DIR = "data/daily_bars"
BAR_COLS = ["date", "code", "open", "high", "low", "close", "volume", "fetchedAt"]
REV_COLS = ["date", "code", "field", "old", "new", "oldFetchedAt", "newFetchedAt"]
FCHART = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count={n}&requestType=0"
BACKFILL_N, DAILY_N = 300, 5
LEADER_LOOKBACK = 20          # 대장 배지가 사라진 뒤에도 이만큼의 관측일 동안 계속 받는다
INDEXES = ("KOSPI", "KOSDAQ")
MARKET_CLOSE_SAFE = (16, 0)   # 이 시각(KST) 전에는 당일 봉을 미확정으로 보고 저장하지 않는다
TAG = "[연구]"


# ── fchart 파싱 ───────────────────────────────────────────────────────
def parse_fchart(text):
    """fchart XML → [(date, open, high, low, close, volume)]. 형식이 다르면 빈 목록."""
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []
    out = []
    for item in root.findall(".//item"):
        d = (item.get("data") or "").split("|")
        if len(d) < 6 or len(d[0]) != 8 or not d[0].isdigit():
            continue
        try:
            o, h, l, c, v = (float(x) for x in d[1:6])
        except ValueError:
            continue
        out.append((f"{d[0][:4]}-{d[0][4:6]}-{d[0][6:8]}", o, h, l, c, v))
    return out


def fetch(code, n, get):
    """(봉 목록, 오류 문자열). `get` 은 requests.get 호환 — 시험에서 바꿔 끼운다."""
    try:
        r = get(FCHART.format(code=code, n=n), timeout=10)
        if getattr(r, "status_code", 200) != 200:
            return [], f"HTTP {r.status_code}"
        bars = parse_fchart(r.text)
        return bars, ("" if bars else "빈 응답")
    except Exception as e:                       # 네트워크 오류는 종목 단위로 기록하고 계속한다
        return [], f"{type(e).__name__}: {e}"[:120]


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


def backfill_universe(snap_dir=SNAP_DIR):
    codes = set()
    for d in snapshot_days(snap_dir):
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{d}_1505.csv.gz"))
        codes |= {x["code"] for x in T.build_A(rows)[0]}
    return sorted(codes)


# ── 저장 (처음 값 보존 · 수정 기록) ───────────────────────────────────
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
    with gzip.open(path, "wt", encoding="utf-8") as fh:   # mtime 고정은 하지 않는다 — 내용이 같으면 커밋 안 함은 git 이 판단
        fh.write(buf.getvalue())


def store(bars_by_code, fetched_at, bars_dir=BARS_DIR, today=None, now=None):
    """새 (date, code) 는 추가, 기존과 값이 다르면 덮어쓰지 않고 수정 기록. (추가 수, 수정 수, 미확정 제외 수)."""
    os.makedirs(bars_dir, exist_ok=True)
    now = now or datetime.datetime.now(KST)
    today = today or now.strftime("%Y-%m-%d")
    unsettled = (now.hour, now.minute) < MARKET_CLOSE_SAFE
    by_month = {}
    skipped = 0
    for code, bars in bars_by_code.items():
        for (d, o, h, l, c, v) in bars:
            if d == today and unsettled:
                skipped += 1
                continue
            by_month.setdefault(d[:7], []).append({"date": d, "code": code, "open": f"{o:g}", "high": f"{h:g}",
                                                   "low": f"{l:g}", "close": f"{c:g}", "volume": f"{v:g}",
                                                   "fetchedAt": fetched_at})
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
            for f in ("open", "high", "low", "close", "volume"):
                if old[k][f] != r[f]:
                    revs.append({"date": r["date"], "code": r["code"], "field": f, "old": old[k][f], "new": r[f],
                                 "oldFetchedAt": old[k]["fetchedAt"], "newFetchedAt": fetched_at})
            if any(old[k][f] != r[f] for f in ("open", "high", "low", "close", "volume")):
                revised += 1
        if changed:
            _write_gz(path, BAR_COLS, sorted(old.values(), key=lambda x: (x["date"], x["code"])))
    if revs:
        rp = os.path.join(bars_dir, "revisions.csv.gz")
        _write_gz(rp, REV_COLS, _read_gz(rp) + revs)
    return added, revised, skipped


def stored_dates(bars_dir=BARS_DIR):
    return sorted({r["date"] for p in glob.glob(os.path.join(bars_dir, "20*.csv.gz")) for r in _read_gz(p)})


def run_bars(day, get, bars_dir=BARS_DIR, snap_dir=SNAP_DIR, now=None, pause=0.05, sleep=time.sleep):
    now = now or datetime.datetime.now(KST)
    backfill = not glob.glob(os.path.join(bars_dir, "20*.csv.gz"))
    codes = backfill_universe(snap_dir) if backfill else universe(day, snap_dir)
    n = BACKFILL_N if backfill else DAILY_N
    got, fails = {}, {}
    for code in list(codes) + list(INDEXES):
        bars, err = fetch(code, n, get)
        sleep(pause)                              # 원천에 부담을 주지 않도록 종목 사이를 띄운다
        if err:
            fails[code] = err
        else:
            got[code] = bars
    added, revised, skipped = store(got, now.isoformat(timespec="seconds"), bars_dir, day, now)
    return {"mode": "백필" if backfill else "일일", "requested": len(codes) + len(INDEXES), "ok": len(got),
            "failed": len(fails), "fail_sample": dict(list(fails.items())[:3]),
            "added": added, "revised": revised, "unsettled_skipped": skipped}


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
    """잠긴 사전등록의 확증 구간 상태 — 수(數)만. 수익률 숫자는 내지 않는다."""
    days, _ = T.collect(snap_dir, with_returns=True)
    valid, bad = T.valid_hold_days(days)
    conf = [d for d in days if d["date"] >= T.CONFIRM_FROM and not d.get("skip")]
    return {"observed": len(conf), "valid": len(valid), "bad": bad}


def report_text(day, bars, struct, locked, bars_dir=BARS_DIR):
    L = [f"{TAG} 테마·대장 경로 연구 일일 보고 · {day}"]
    if bars:
        L.append(f"· 일봉({bars['mode']}): 요청 {bars['requested']} · 성공 {bars['ok']} · 실패 {bars['failed']} · "
                 f"새 행 {bars['added']} · 값 변경 기록 {bars['revised']}")
        if bars["unsettled_skipped"]:
            L.append(f"  (장 마감 전 당일 봉 {bars['unsettled_skipped']}건은 미확정이라 저장 안 함)")
    sd = stored_dates(bars_dir)
    L.append(f"· 저장된 일봉 날짜: {sd[0] if sd else '—'} ~ {sd[-1] if sd else '—'} ({len(sd)}일)")
    if struct:
        L.append(f"· 오늘 대장 후보 {struct['leaders']}개")
        if struct.get("prev"):
            L.append(f"  직전 관측일({struct['prev']}) 대장 {struct['prev_leaders']}개 중 오늘도 대장 {struct['kept_leader']}개 · "
                     f"이어진 테마 {struct['themes_kept']}개 중 대장 교체 {struct['swapped']}개")
    else:
        L.append("· 오늘 15:05 스냅샷 없음 — 구조 집계 생략")
    if locked:
        L.append(f"· 잠긴 사전등록 leader-hold-v1: 확증 관측 {locked['observed']}일 · 유효 비교일 {locked['valid']}/{T.EVAL_VALID_DAYS} (수익률 비공개)")
    L.append("관측 전용 · 자동주문 없음 · 대장 후보의 이후 수익률은 방화벽 때문에 이 보고에 넣지 않는다")
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


def target_day(now):
    """예약 실행이 자정을 넘겨 도착하면(깃허브 cron 지연) 전날 장을 대상으로 한다."""
    if now.hour < 8:
        return (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    return now.strftime("%Y-%m-%d")


def trading_day(day):
    from hyeoks_trading_calendar import scheduled_session
    try:
        return scheduled_session(day), ""
    except ValueError as e:
        return False, f"달력 범위 밖: {e}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--bars", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--send", action="store_true")
    ap.add_argument("--date", default="")
    a = ap.parse_args(argv)
    now = datetime.datetime.now(KST)
    day = a.date or target_day(now)
    ok, why = trading_day(day)
    if not ok:
        print(f"ℹ️ {day} 은 예정 거래일이 아니다{(' — ' + why) if why else ''}. 수집·보고 생략")
        return 0
    bars = None
    if a.bars:
        import requests
        bars = run_bars(day, requests.get, now=now)
        print(f"📈 일봉 {bars}")
        with open(os.path.join(BARS_DIR, "last_run.txt"), "w", encoding="utf-8") as fh:
            fh.write(f"{day} {bars['mode']} requested={bars['requested']} ok={bars['ok']} failed={bars['failed']} "
                     f"added={bars['added']} revised={bars['revised']}\n")
    if a.report:
        text = report_text(day, bars, structure_today(day), locked_status())
        print(text)
        if a.send:
            import requests
            sent, err = send(text, requests.post, os.environ.get("TELEGRAM_BOT_TOKEN"))
            print("✅ [연구] 보고 발송" if sent else f"⚠️ [연구] 보고 발송 실패: {err}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
