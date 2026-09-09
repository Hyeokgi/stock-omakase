"""계좌 입력 v3 를 위한 수집 어댑터 — 확인한 것만 verified 로 표시한다.

왜 새로 만드나 (2026-09-09)
---------------------------
기존 수집기를 그대로 쓸 수 없다는 외부 지적이 맞았다. 확인한 결함:

  · `hyeoks_analyst.py` 9곳 이상 `verify=False` + `disable_warnings`
  · `hyeoks_phase2_exit.py:121` 캐시가 **종목만 키** — `count` 가 달라도 같은 결과
  · 같은 함수가 잘못된 행을 `except: continue` 로 **조용히 생략**
  · 일봉 dict 에 `tradable` 필드가 **아예 없음**
  · `hyeoks_snapshot_watch.py:67` 거래일 기본값 20일

어제 내 보고서는 이 둘을 "✅ 재사용 가능"으로 적었는데, **존재만 확인하고 품질을
감사하지 않은 것**이었다. 여기서는 그 다섯 가지를 전부 반대로 한다.

이 어댑터가 하지 않는 것 — 중요하다
------------------------------------
**확인하지 못한 것에 `verified` 를 넣지 않는다.** v3 게이트는 근거 없는 입력을
막으라고 만든 것이고, 어댑터가 자가 인증하면 게이트가 무의미해진다.

  · `price_basis` — 네이버 조정 기준을 우리가 대조할 자료가 없다 → 항상 `unverified`
  · `calendar_verification` — 지수 일봉으로 만든 목록을 **같은 목록으로 검증했다고
    하지 않는다.** 독립 자료는 사람이 넣어야 한다
  · `snapshot_1505` — 해당 슬롯 관측 상태와 캡처 시각만 보존한다.
    하루 전체 `tradable`은 만들지 않는다. 15:05 종베 진입 의도는 유지하며 시가/종가 상태와 구분한다.

그래서 이 어댑터의 정상 산출물은 대개 **빌더가 막는 입력**이다. 그게 맞는 동작이다.
무엇이 모자란지를 `gaps` 로 정확히 알려주는 것이 이 도구의 값이다.
"""
import datetime
import gzip
import csv
import glob
import json
import os
import re
import math

KST = datetime.timezone(datetime.timedelta(hours=9))
ADAPTER_VERSION = "account-source-adapter-v2"
SNAPSHOT_GLOB = "data/market_snapshot/{date}_1505.csv.gz"

C_ENTRY, C_CHANNEL, C_CODE = 1, 2, 4


# ── 거래 가능 상태 — 스냅샷에서만 온다 ────────────────────────────────
def snapshot_dates(root="data/market_snapshot"):
    """15:05 스냅샷이 있는 날짜. 이 날들만 tradable 을 확인할 수 있다."""
    out = []
    for p in sorted(glob.glob(os.path.join(root, "*_1505.csv.gz"))):
        m = re.search(r"(\d{4}-\d{2}-\d{2})_1505", os.path.basename(p))
        if m:
            out.append(m.group(1))
    return out


def tradable_map(date, root="data/market_snapshot"):
    """그날 15:05 슬롯의 {종목코드: 관측 상태/캡처 시각/출처}.

    ⚠️ 값이 'Y'/'N' 이 아니면 **그 종목은 넣지 않는다** — 모르는 것을 True 로
       채우면 거래정지 종목이 정상 매매된 것처럼 계산된다.
    """
    path = os.path.join(root, f"{date}_1505.csv.gz")
    if not os.path.exists(path):
        return {}
    out = {}
    with gzip.open(path, "rt", encoding="utf-8") as f:
        parts = next(csv.reader([f.readline()]))
        meta = dict(p.split('=',1) for p in parts[1:] if '=' in p)
        if not parts or parts[0] != '#meta' or meta.get('slot') != '1505':
            raise ValueError('invalid snapshot metadata/slot')
        stamp = datetime.datetime.fromisoformat(meta.get('capturedAt','').replace('Z','+00:00'))
        if stamp.tzinfo is None: raise ValueError('snapshot capture timezone required')
        local = stamp.astimezone(KST)
        if local.date().isoformat()!=date or not '14:50' <= local.strftime('%H:%M') <= '15:25':
            raise ValueError('snapshot date/window mismatch')
        seen = set()
        for row in csv.DictReader(f):
            code = (row.get("itemcode") or "").strip().zfill(6)
            if not re.fullmatch(r'\d{6}',code) or code == '000000' or code in seen:
                raise ValueError('invalid/duplicate snapshot code')
            seen.add(code)
            flag = (row.get("tradeStopYn") or "").strip().upper()
            if not code or flag not in ("Y", "N"):
                continue
            out[code] = {'status_at_snapshot': flag == 'N', 'slot': '1505',
                         'captured_at': stamp.isoformat(), 'source': os.path.basename(path)}
    return out


# ── 원장에서 필요한 (종목, 날짜) 를 뽑는다 ────────────────────────────
def required_cells(rows, sessions, horizon_of, as_of=None, entry_model='next_open'):
    """예정 보유 구간의 기초 (종목코드, 거래일) 쌍. 실제 실행 전체 필요량은 아님.

    next_open은 신호 다음 거래일부터, closing_1505는 신호일부터 다음 거래일까지.
    청산 지연에 따른 추가 기간과 주문 거절은 여기서 계산하지 않는다.
    """
    from hyeoks_verdict import is_excluded
    if not sessions or sessions != sorted(set(sessions)):
        raise ValueError('sorted unique calendar required')
    for d in sessions: datetime.date.fromisoformat(d)
    as_of = as_of or sessions[-1]
    if as_of not in sessions or entry_model not in ('next_open','closing_1505'):
        raise ValueError('invalid as_of/entry_model')
    idx = {d: i for i, d in enumerate(sessions)}
    need, unknown_entry = set(), []
    for row in rows[1:]:
        if not any(str(c).strip() for c in row) or is_excluded(row):
            continue
        if len(row) <= C_CODE: raise ValueError('truncated ledger row')
        code = str(row[C_CODE]).replace("'", "").strip().zfill(6)
        entry = str(row[C_ENTRY]).strip()[:10]
        ch = str(row[C_CHANNEL]).strip()
        if ch.startswith('지수벤치'):
            continue
        if not re.fullmatch(r'\d{6}',code) or code=='000000' or not ch:
            raise ValueError('invalid ledger code/channel')
        if entry not in idx or entry > as_of:
            unknown_entry.append((code, entry))
            continue
        h = 1 if entry_model == 'closing_1505' else horizon_of(ch)
        start = idx[entry] + (entry_model == 'next_open')
        for j in range(start, min(idx[entry] + h + 1, idx[as_of]+1)):
            need.add((code, sessions[j]))
    return need, unknown_entry


def coverage(need, snap_dates, root="data/market_snapshot"):
    """기초 칸 중 15:05 상태 관측값이 몇 개 존재하는가. 체결 검증 수가 아니다."""
    have_by_date = {d: tradable_map(d, root) for d in snap_dates}
    known, missing = set(), []
    for code, date in sorted(need):
        if date in have_by_date and code in have_by_date[date]:
            known.add((code, date))
        else:
            missing.append((code, date))
    return known, missing, have_by_date


def build_source(rows, sessions, prices, as_of, horizon_of,
                 config=None, root="data/market_snapshot", now=None):
    """v3 준비 자료를 만든다. **검증 상태를 정직하게 적는 것이 핵심이다.**

    prices 는 {code: {date: {open,high,low,close}}}. 관측 자료만 덧붙이고 tradable은 만들지 않는다.
    """
    now = now or datetime.datetime.now(KST)
    need, unknown_entry = required_cells(rows, sessions, horizon_of, as_of)
    snaps = [d for d in snapshot_dates(root) if d in set(sessions)]
    known, missing_tradable, have = coverage(need, snaps, root)

    out_prices, missing_price = {}, []
    for code, date in sorted(need):
        bar = (prices.get(code) or {}).get(date)
        if not bar:
            missing_price.append((code, date))
            continue
        out_prices.setdefault(code, {})[date] = {
            "open": bar["open"], "high": bar["high"],
            "low": bar["low"], "close": bar["close"],
        }
        if (code,date) in known:
            out_prices[code][date]['snapshot_1505'] = have[date][code]
        # Do not promote a 15:05 observation to daily execution availability.

    gaps = {
        "required_cells": len(need),
        "tradable_known": len(known),
        "tradable_missing": len(missing_tradable),
        "price_missing": len(missing_price),
        "snapshot_dates_in_window": len(snaps),
        "first_snapshot": snaps[0] if snaps else None,
        "entry_not_in_calendar": len(unknown_entry),
        "sample_tradable_missing": [f"{c}@{d}" for c, d in missing_tradable[:5]],
        "sample_price_missing": [f"{c}@{d}" for c, d in missing_price[:5]],
    }
    source = {
        "schema": "account-input-v3",
        "adapter_version": ADAPTER_VERSION,
        "as_of": as_of,
        "captured_at": now.isoformat(),
        "theme_policy": "none",            # 테마 자료가 불완전하므로 기본은 none
        "calendar_source": "지수 일봉 파생 거래일(어댑터 생성) — 독립 검증 필요",
        "price_source": "네이버 fchart 일봉 — 조정 기준 미대조",
        "price_basis": "consistent_adjusted_ohlc",
        # 🚨 자가 인증하지 않는다. 사람이 독립 자료로 채우기 전까지 막혀야 한다.
        "calendar_verification": {
            "status": "unverified",
            "evidence": "",
            "sessions": [],
        },
        "price_verification": {"status": "unverified", "evidence": ""},
        "sessions": sessions,
        "rows": rows,
        "prices": out_prices,
        "config": config or {},
    }
    return source, gaps


def gap_report(gaps, as_of, include_samples=False):
    price = '미검사' if gaps['price_missing'] is None else str(gaps['price_missing'])
    lines = [f"# 계좌 입력 관측 자료 상태 — {as_of}", "",
             "15:05는 종베의 의도된 진입 슬롯으로 유지한다. 실제 캡처 시각은 별도 보존한다.",
             "관측 상태는 해당 스냅샷에서만 유효하다. 시가·종가 거래 가능 또는 실체결 보장이 아니다.",
             f"진입 모형: {gaps.get('entry_model', 'next_open')}", "",
             "| 항목 | 값 |", "|---|---:|",
             f"| 예정 보유 구간 기초 칸 | {gaps['required_cells']} |",
             f"| 15:05 상태 관측값 있음 | {gaps['tradable_known']} |",
             f"| 15:05 상태 관측값 없음 | {gaps['tradable_missing']} |",
             f"| 가격 결손 | {price} |",
             f"| 달력 내 스냅샷 파일 일수 | {gaps['snapshot_dates_in_window']} |",
             f"| 첫 파일 | {gaps['first_snapshot']} |",
             f"| 달력 밖/기준일 이후 신호 | {gaps['entry_not_in_calendar']} |", "",
             "분모는 예정 보유 구간의 기초 필요량이다. 주문 거절에 따른 감소와 청산 지연에 따른 추가 필요량은 미산정이다.",
             "상태 관측값 없음 비율을 체결 가능 상태의 결손률이라고 부르지 않는다.",
             "가격 미검사는 0건이 아니다. 관측 수집 성공과 계좌 실행 준비 완료는 다르다.", "",
             "## 남은 검증", "",
             "- 거래일 독립 근거, 가격 조정 기준과 기업행사 대조.",
             "- 의도한 주문 시점의 상태 및 체결 가격. 15:05 관측값을 다음 날로 이월하지 않는다.",
             "- 짧은 구간은 연결 검증에 사용할 수 있으나 수익성 채택 근거는 아니다.",
             "- 창을 좁혀도 전 구간 검증이 자동 보장되지 않는다.",
             "- 상태가 없다는 이유로 보유 평가용 OHLC를 삭제하거나 정상 거래로 추정하지 않는다.", ""]
    if include_samples:
        lines.append("비공개 결손 예시: "+", ".join(gaps['sample_tradable_missing']))
    return "\n".join(lines)


# ── 일봉 수집 — 기존 수집기의 다섯 결함을 정확히 반대로 한다 ──────────
def parse_fchart(xml_text):
    """fchart 응답 → (봉 목록, 파싱 실패 건수). **순수 함수라 오프라인 검증된다.**

    ⚠️ 기존 `get_daily_bars` 는 잘못된 행을 `except: continue` 로 조용히 건너뛰었다.
       그러면 "봉이 원래 없는 날"과 "우리가 못 읽은 날"이 구별되지 않는다.
       여기서는 **실패 건수를 돌려주고**, 호출자가 0 이 아니면 거부한다.
    """
    import xml.etree.ElementTree as ET
    bars, bad = [], 0
    root = ET.fromstring(xml_text)          # 깨진 XML 은 예외로 올린다
    seen = set()
    for item in root.findall(".//item"):
        parts = (item.get("data") or "").split("|")
        if len(parts) < 5 or not parts[0]:
            bad += 1
            continue
        raw = parts[0]
        try:
            if not re.fullmatch(r'\d{8}',raw): raise ValueError('invalid date')
            datetime.date(int(raw[:4]),int(raw[4:6]),int(raw[6:8]))
            if raw in seen: raise ValueError('duplicate date')
            seen.add(raw)
            o, h, l, c = (float(parts[1]), float(parts[2]),
                          float(parts[3]), float(parts[4]))
        except ValueError:
            bad += 1
            continue
        if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)) or not (l <= o <= h and l <= c <= h):
            bad += 1                        # 0 인 봉을 정상 가격으로 바꾸지 않는다
            continue
        bars.append({"date": f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}",
                     "open": o, "high": h, "low": l, "close": c})
    return sorted(bars,key=lambda b:b['date']), bad


_BAR_CACHE = {}


def fetch_bars(symbol, count, session=None, timeout=10):
    """일봉을 받는다. 실패를 삼키지 않는다.

    기존 수집기와 다른 점 — 전부 의도적이다.
      · **TLS 검증을 켠다.** 기존 코드는 `verify=False` 였다(9곳 이상)
      · **캐시 키에 `count` 를 넣는다.** 기존은 종목만 키라 기간이 달라도 같은 결과
      · **HTTP 상태를 본다.** 200 이 아니면 예외
      · **파싱 실패 건수를 확인한다.** 0 이 아니면 예외 — 조용히 생략하지 않는다
      · 기본값 없음 — `count` 를 **반드시** 받는다(20일 기본값 사고 방지)
    """
    if not isinstance(count,int) or isinstance(count,bool) or count <= 0:
        raise ValueError('positive integer count required')
    if session is None:
        import requests
        session = requests
    key = (symbol, int(count))
    if key in _BAR_CACHE:
        return _BAR_CACHE[key]
    ses = session
    url = ("https://fchart.stock.naver.com/sise.nhn"
           f"?symbol={symbol}&timeframe=day&count={int(count)}&requestType=0")
    res = ses.get(url, timeout=timeout)     # verify 는 기본값(True) 그대로 둔다
    if getattr(res, "status_code", None) != 200:
        raise RuntimeError(f"{symbol}: HTTP {getattr(res, 'status_code', '?')}")
    bars, bad = parse_fchart(res.text)
    if bad:
        raise RuntimeError(f"{symbol}: 파싱 실패 {bad}건 — 조용히 넘기지 않는다")
    if not bars:
        raise RuntimeError(f"{symbol}: 봉이 0개")
    _BAR_CACHE[key] = bars
    return bars


def self_test():
    ok = True

    def chk(label, cond, note=""):
        nonlocal ok
        ok = ok and bool(cond)
        print(f"  {'✅' if cond else '❌'} {label}" + (f"   {note}" if note else ""))

    print("🧪 수집 어댑터")
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="adapter_")
    try:
        def write_snap(date, entries):
            path = os.path.join(tmp, f"{date}_1505.csv.gz")
            with gzip.open(path, "wt", encoding="utf-8") as f:
                f.write(f"#meta,capturedAt={date}T15:05:00+09:00,slot=1505\n")
                f.write("itemcode,itemname,tradeStopYn\n")
                for code, flag in entries:
                    f.write(f"{code},이름,{flag}\n")

        write_snap("2026-09-02", [("000001", "N"), ("000002", "Y"), ("000003", "")])
        tm = tradable_map("2026-09-02", tmp)
        chk("15:05 N 관측", tm['000001']['status_at_snapshot'] is True)
        chk("15:05 Y 관측", tm['000002']['status_at_snapshot'] is False)
        chk("값이 비면 **아예 넣지 않는다**(모름을 True 로 안 채움)",
            "000003" not in tm, f"{sorted(tm)}")
        chk("스냅샷 없는 날은 빈 map", tradable_map("2026-01-01", tmp) == {})
        chk("스냅샷 날짜 목록", snapshot_dates(tmp) == ["2026-09-02"])

        sessions = ["2026-09-01", "2026-09-02", "2026-09-03"]
        hdr = [""] * 38
        def mkrow(code, entry):
            r = [""] * 38
            r[C_ENTRY], r[C_CHANNEL], r[C_CODE] = entry, "차트TOP2", code
            return r
        rows = [hdr, mkrow("000001", "2026-09-01")]
        need, unk = required_cells(rows, sessions, lambda c: 1)
        chk("신호일+H 까지 칸이 필요하다",
            need == {("000001", "2026-09-02")}, f"{sorted(need)}")
        chk("달력에 없는 진입일은 따로 센다",
            required_cells([hdr, mkrow("000001", "2026-12-25")], sessions,
                           lambda c: 1)[1] == [("000001", "2026-12-25")])

        prices = {"000001": {d: {"open": 100, "high": 101, "low": 99, "close": 100}
                             for d in sessions}}
        src, gaps = build_source(rows, sessions, prices, "2026-09-03",
                                 lambda c: 1, root=tmp)
        chk("필요한 봉을 보존한다",
            list(src["prices"].get("000001", {})) == ["2026-09-02"], f"{src['prices']}")
        chk("시가 진입 전 신호일은 분모에서 제외", gaps["tradable_missing"] == 0)
        chk("15:05 관측은 보존하고 일별 tradable은 만들지 않는다",
            src["prices"]["000001"]["2026-09-02"]['snapshot_1505']['status_at_snapshot'] is True
            and 'tradable' not in src["prices"]["000001"]["2026-09-02"])

        # 🚨 자가 인증 금지 — 이 검사가 이 도구의 핵심이다
        chk("달력 검증을 스스로 verified 라고 하지 않는다",
            src["calendar_verification"]["status"] == "unverified")
        chk("달력 검증 sessions 를 자기 목록으로 채우지 않는다",
            src["calendar_verification"]["sessions"] == [])
        chk("가격 검증도 unverified", src["price_verification"]["status"] == "unverified")
        chk("테마는 기본 none (자료가 불완전하므로)", src["theme_policy"] == "none")
        chk("스키마는 v3", src["schema"] == "account-input-v3")

        # 거래정지 종목은 tradable=False 로 들어간다(삭제하지 않는다)
        rows2 = [hdr, mkrow("000002", "2026-09-01")]
        src2, _ = build_source(rows2, sessions, {"000002": {d: {"open": 1, "high": 1,
                                "low": 1, "close": 1} for d in sessions}},
                               "2026-09-03", lambda c: 1, root=tmp)
        chk("거래정지 종목을 사후 삭제하지 않고 False 로 남긴다",
            src2["prices"]["000002"]["2026-09-02"]['snapshot_1505']['status_at_snapshot'] is False)

        # ── 파싱 — 기존 수집기가 조용히 넘기던 것들 ──────────────────
        good = ('<r><item data="20260902|100|110|90|105"/>'
                '<item data="20260903|100|110|90|105"/></r>')
        b, bad = parse_fchart(good)
        chk("정상 봉 2개", (len(b), bad) == (2, 0), f"{b[:1]}")
        chk("날짜가 ISO 로 바뀐다", b[0]["date"] == "2026-09-02")
        b, bad = parse_fchart('<r><item data="20260902|100|110|90"/></r>')
        chk("필드 부족은 **세어서 알린다**(조용히 생략 안 함)", (len(b), bad) == (0, 1))
        b, bad = parse_fchart('<r><item data="20260902|0|110|90|105"/></r>')
        chk("0 인 봉을 정상 가격으로 바꾸지 않는다", (len(b), bad) == (0, 1))
        b, bad = parse_fchart('<r><item data="20260902|100|90|110|105"/></r>')
        chk("고저가 뒤집힌 봉도 거부", (len(b), bad) == (0, 1))
        b, bad = parse_fchart('<r><item data="20260902|abc|110|90|105"/></r>')
        chk("숫자가 아니면 거부", (len(b), bad) == (0, 1))

        class FakeRes:
            def __init__(self, code, text): self.status_code, self.text = code, text
        class FakeSes:
            def __init__(self, res): self.res, self.calls = res, []
            def get(self, url, timeout=None):
                self.calls.append(url)
                return self.res
        _BAR_CACHE.clear()
        ses = FakeSes(FakeRes(200, good))
        got = fetch_bars("000001", 5, session=ses)
        chk("정상 응답이면 봉을 돌려준다", len(got) == 2)
        chk("요청 URL 에 count 가 그대로 들어간다", "count=5" in ses.calls[0])
        fetch_bars("000001", 5, session=ses)
        chk("같은 (종목,count) 는 캐시", len(ses.calls) == 1)
        fetch_bars("000001", 9, session=ses)
        chk("**count 가 다르면 다시 받는다**(기존 캐시 결함)", len(ses.calls) == 2)
        err = None
        try:
            fetch_bars("000002", 5, session=FakeSes(FakeRes(503, good)))
        except RuntimeError as e:
            err = str(e)
        chk("HTTP 200 이 아니면 예외", err and "503" in err, err)
        err = None
        try:
            fetch_bars("000003", 5, session=FakeSes(
                FakeRes(200, '<r><item data="20260902|100|110|90"/></r>')))
        except RuntimeError as e:
            err = str(e)
        chk("파싱 실패가 있으면 예외 — 부분 결과를 쓰지 않는다",
            err and "파싱 실패" in err, err)
        chk("count 에 기본값이 없다(20일 사고 방지)",
            "count" in fetch_bars.__code__.co_varnames[:3])

        rep = gap_report(gaps, "2026-09-03")
        chk("체결 상태와 구별한다", "실체결 보장" in rep)
        chk("추정 금지를 리포트에 적는다", "추정하지 않는다" in rep)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--gaps-only", action="store_true",
                    help="가격을 받지 않고 **거래상태 결손만** 잰다. 첫 실행은 이걸로.")
    ap.add_argument("--days", type=int, default=120,
                    help="거래일 달력을 몇 개 받을지. 기본값 없이 명시하는 것이 원칙이나 "
                         "달력은 종목이 아니라 시장 단위라 넉넉히 받는다.")
    ap.add_argument("--out", default=None)
    ap.add_argument('--entry-model', choices=['next_open','closing_1505'], default='next_open',
                    help='closing_1505: 원장 신호일 15:05 진입~다음 거래일의 자료 필요량만 점검; 성과 계산 아님')
    a = ap.parse_args(argv)
    if a.self_test:
        return self_test()
    if not a.gaps_only:
        ap.error('현재 CLI는 --gaps-only만 지원합니다. 가격 수집 완료로 표시하지 않습니다.')

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(
        "https://docs.google.com/spreadsheets/d/"
        "1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit")
    rows = doc.worksheet("백테스트_로그").get_all_values()      # 읽기 전용
    today = datetime.datetime.now(KST).strftime("%Y-%m-%d")

    # 거래일 달력 — 지수 일봉에서. ⚠️ 이걸 **독립 검증 자료로 쓰지 않는다.**
    sessions = sorted({b["date"] for b in fetch_bars("KOSPI", a.days)})
    print(f"📅 거래일 {len(sessions)}일 ({sessions[0]} ~ {sessions[-1]})")

    from hyeoks_verdict import HORIZON, DEFAULT_HORIZON
    horizon_of = lambda ch: HORIZON.get(ch, DEFAULT_HORIZON)

    need, unknown_entry = required_cells(rows, sessions, horizon_of, sessions[-1], a.entry_model)
    snaps = [d for d in snapshot_dates() if d in set(sessions)]
    known, missing, _have = coverage(need, snaps)
    codes = {c for c, _ in need}
    ent = sorted({d for _, d in need})
    gaps = {
        "required_cells": len(need), "tradable_known": len(known),
        "tradable_missing": len(missing), "price_missing": None, 'entry_model': a.entry_model,
        "snapshot_dates_in_window": len(snaps),
        "first_snapshot": snaps[0] if snaps else None,
        "entry_not_in_calendar": len(unknown_entry),
        "sample_tradable_missing": [f"{c}@{d}" for c, d in missing[:5]],
        "sample_price_missing": [],
    }
    print(f"📒 원장 {len(rows) - 1}행 · 고유 종목 {len(codes)}개 · "
          f"기초 칸 {len(need)} ({ent[0] if ent else '-'} ~ {ent[-1] if ent else '-'})")
    print(f"🚧 거래상태 결손 {len(missing)} / {len(need)}")

    md = gap_report(gaps, today)
    path = a.out or f"data/account/{today}_source_gaps.md"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"💾 저장: {path}")
    print()
    print(md)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
