# -*- coding: utf-8 -*-
"""
생산 영수증(receipt) — 파이프라인이 "무엇을 했는지" 를 기계가 읽을 형태로 남긴다.

2026-09-18 사용자 지시 ②·⑤~⑧.
**사람이 7개의 True/False 를 넣어 Stability Gate 를 통과시키는 방식은 증거가 아니다.**
각 생산 파이프라인이 구조화된 영수증을 떨구고, Evidence Builder 가 그것을 조합한다.

판정 단위는 workflow run 이 아니라 `cycle_id = KRX 거래일` 이다.
한 거래일에 scanner·analyst·earnings 가 여러 번 돌아도 Gate 에는 최대 한 cycle 만 남는다.

설계 원칙
---------
1. **영수증은 주장하지 않는다.** "정상" 이 아니라 숫자를 남긴다.
   판정은 Evidence Builder 가 하고, 그 규칙은 한 곳에만 있다.
2. **쓰기 실패가 생산을 죽이지 않는다.** 관측을 지키려다 수집을 잃지 않는다
   (scanner_census 와 같은 규율). 대신 **조용히 실패하지도 않는다** — 로그에 남긴다.
3. **append-only.** 같은 거래일에 여러 번 떨어져도 덮어쓰지 않는다.
   Evidence Builder 가 그 날의 영수증 전부를 보고 판정한다.
"""
import datetime
import json
import os
import pathlib

KST = datetime.timezone(datetime.timedelta(hours=9))
RECEIPT_DIR = "data/receipts"
RECEIPT_VERSION = "receipt-v2"

# 🔴 2026-09-18 P0-1 — v1 은 거래일마다 **공유 JSONL 한 개**에 append 했다. 두 문제:
#    ① 워크플로는 서로 다른 runner 에서 돈다. 각자 로컬 파일에 쓰고, 그걸 git 에
#       보존하는 것은 main.yml 뿐이었다. analyst·earnings·consensus 영수증은
#       **runner 가 끝나는 순간 사라졌다.** Evidence Builder 가 요구하는 세 종류가
#       저장소에 동시에 존재할 수 없는 구조였다 → Gate 는 영원히 통과 불가.
#    ② 여러 워크플로가 같은 파일을 고치면 git merge conflict 가 난다.
#    그래서 v2 는 **영수증 한 건당 독립 파일**이다. 각 워크플로가 자기 파일만 보존한다.
#        data/receipts/2026-09-21/scanner-<runid>.json
#                                 analyst-<runid>.json  ...

# 어떤 파이프라인이 어떤 영수증을 내는가. Evidence Builder 가 이 이름으로 찾는다.
KINDS = ("scanner", "analyst", "earnings", "consensus")


def cycle_date_now(now=None, back=10):
    """이 실행이 속한 **거래일**. 벽시계 날짜가 아니다.

    🔴 2026-09-19 첫 실제 실행이 드러낸 P0.
       실적 수집기가 23:45 KST(금 9/18)에 시작해 00:17 KST(토 9/19)에 끝났다.
       영수증을 `datetime.now(KST)` 로 찍으니 cycle_date 가 **9/19** 가 됐다.
       9/19 는 토요일 = 비거래일이라 Gate 에서 SKIP 으로 처리되고, 그 영수증은
       영원히 쓰이지 않는다. 더 나쁜 것은 scanner·analyst 는 장중에 돌아 9/18 로
       기록되므로 **required 3종이 같은 날짜에 모이지 않는다** — 그 거래일은
       무슨 일이 있어도 FAIL 이다. Gate 가 3/3 에 도달할 수 없다.

    그래서 벽시계 날짜가 거래일이 아니면 **가장 최근 거래일로 되돌린다.**
    자정을 넘긴 실행도, 주말로 넘어간 실행도 자기 거래일에 묶인다.
    """
    from hyeoks_trading_calendar import scheduled_session
    now = now or datetime.datetime.now(KST)
    day = now.date()
    for _ in range(back + 1):
        iso = day.isoformat()
        if scheduled_session(iso):
            return iso
        day -= datetime.timedelta(days=1)
    return now.date().isoformat()      # 달력이 이상하면 벽시계 날짜라도 남긴다


def previous_trading_day(now=None, back=15):
    """오늘(KST) **직전**의 가장 최근 거래일. 판정 대상 거래일이다.

    🔴 2026-09-19 생산 P0 ② — finalizer run #1 이 `TZ=Asia/Seoul date +%Y-%m-%d`
       로 판정 대상을 정했다. 예약은 23:30 KST(금)였는데 Actions 가 **3시간 19분**
       늦어 02:49 KST(토)에 돌았고, 대상 거래일이 `2026-09-19` — **토요일**이 됐다.
       비거래일이므로 SKIP 만 기록된다. 금요일 9/18 은 아무도 판정하지 않는다.

       이것은 수집기에서 방금 고친 것과 **같은 결함**이다. 벽시계 날짜를 거래일로
       쓰면 cron 지연 한 번에 그 거래일을 통째로 잃는다.

    그래서 "지금" 이 아니라 "**끝난 거래일**" 을 본다. 오늘 직전으로 한 칸 물러선 뒤
    가장 가까운 거래일까지 되돌린다. 이 값은 실행이 몇 시간 늦어도 같은 답을 준다
    (같은 날 안에서는 언제 돌든 불변이다). 이미 기록된 거래일이면 Gate 가 중복을
    세지 않으므로 다시 돌아도 안전하다.
    """
    from hyeoks_trading_calendar import scheduled_session
    now = now or datetime.datetime.now(KST)
    day = now.date() - datetime.timedelta(days=1)
    for _ in range(back + 1):
        iso = day.isoformat()
        if scheduled_session(iso):
            return iso
        day -= datetime.timedelta(days=1)
    return (now.date() - datetime.timedelta(days=1)).isoformat()


def dir_for(day, root=RECEIPT_DIR):
    return os.path.join(root, str(day))


def path_for(day, kind, run_id="", root=RECEIPT_DIR, seq=""):
    """영수증 한 건의 경로. 파일 이름이 겹치지 않아야 충돌이 없다."""
    tag = str(run_id or "norun").replace("/", "_")[:40]
    return os.path.join(dir_for(day, root), f"{kind}-{tag}{seq}.json")


def emit(day, kind, payload, root=RECEIPT_DIR, run_id="", sha="", fingerprint=""):
    """영수증 한 줄. 실패해도 예외를 밖으로 내지 않는다.

    반환 (성공, 메시지) — 호출부는 메시지를 **반드시 로그에 찍는다.**
    """
    if kind not in KINDS:
        return False, f"알 수 없는 영수증 종류 {kind}"
    try:
        rec = {
            "cycle_date": str(day),
            "kind": kind,
            "emitted_at": datetime.datetime.now(KST).isoformat(timespec="microseconds"),
            "run_id": str(run_id or os.environ.get("GITHUB_RUN_ID", "")),
            "code_sha": str(sha or (os.environ.get("GITHUB_SHA") or "")[:8]),
            "fingerprint": str(fingerprint),
            "receipt_version": RECEIPT_VERSION,
            "payload": payload,
        }
        os.makedirs(dir_for(day, root), exist_ok=True)
        blob = json.dumps(rec, ensure_ascii=False, sort_keys=True, indent=1)
        target = path_for(day, kind, rec["run_id"], root)
        # 같은 run 이 두 번 떨구면(재시도 등) 덮어쓰지 않고 옆에 쌓는다 — append-only
        n = 1
        while os.path.exists(target):
            target = path_for(day, kind, rec["run_id"], root, seq=f"-{n}")
            n += 1
        with open(target, "w", encoding="utf-8") as fh:
            fh.write(blob)
        return True, f"영수증 {kind} → {target}"
    except Exception as e:                       # noqa: BLE001 — 생산을 죽이지 않는다
        return False, f"영수증 {kind} 기록 실패: {type(e).__name__}: {e}"


def load(day, root=RECEIPT_DIR):
    """그 거래일의 영수증 전부. 깨진 파일은 세되 버린다(조용히 넘기지 않는다).

    `emitted_at` 순으로 돌려준다 — Builder 가 '마지막 것' 을 고를 수 있어야 한다.
    """
    d = dir_for(day, root)
    out, broken = [], 0
    if not os.path.isdir(d):
        return out, broken
    for name in sorted(os.listdir(d)):
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(d, name), encoding="utf-8") as fh:
                rec = json.load(fh)
            if not isinstance(rec, dict) or "kind" not in rec:
                raise ValueError("스키마 아님")
            rec["_file"] = name
            out.append(rec)
        except (ValueError, OSError):
            broken += 1
    # 같은 순간이면 파일 이름으로 가른다 — 정렬이 흔들리면 "마지막" 이 흔들린다
    out.sort(key=lambda r: (str(r.get("emitted_at", "")), str(r.get("_file", ""))))
    return out, broken


def latest(day, kind, root=RECEIPT_DIR, fingerprint=None):
    """그 날 그 종류의 **마지막** 영수증. 지문을 주면 그 지문의 것만 본다."""
    rows, _ = load(day, root)
    hits = [r for r in rows if r.get("kind") == kind
            and (fingerprint is None or r.get("fingerprint") == fingerprint)]
    return hits[-1] if hits else None


def _selftest():
    import tempfile
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    print("🧪 생산 영수증 (v2 — 건당 독립 파일)")
    # 🔴 자정을 넘긴 실행이 다음 날로 새지 않는가
    KST9 = datetime.timezone(datetime.timedelta(hours=9))
    midnight = datetime.datetime(2026, 9, 19, 0, 17, tzinfo=KST9)   # 토 00:17
    chk("자정을 넘겨도 그 거래일(금 9/18)에 묶인다",
        cycle_date_now(midnight) == "2026-09-18", cycle_date_now(midnight))
    chk("장중 실행은 그날 그대로",
        cycle_date_now(datetime.datetime(2026, 9, 18, 14, 44, tzinfo=KST9)) == "2026-09-18")
    chk("주말 실행도 직전 거래일로",
        cycle_date_now(datetime.datetime(2026, 9, 20, 10, 0, tzinfo=KST9)) == "2026-09-18")
    chk("월요일은 월요일",
        cycle_date_now(datetime.datetime(2026, 9, 21, 9, 0, tzinfo=KST9)) == "2026-09-21")

    # 🔴 판정 대상 거래일 — finalizer 가 쓰는 값(2026-09-19 생산 P0 ②)
    #    실제로 겪은 상황: 예약 23:30 금 → 실제 02:49 토. 그래도 금요일을 판정해야 한다.
    chk("토 02:49 에 늦게 돌아도 판정 대상은 금 9/18",
        previous_trading_day(datetime.datetime(2026, 9, 19, 2, 49, tzinfo=KST9)) == "2026-09-18")
    chk("토 06:30 정시도 같은 답",
        previous_trading_day(datetime.datetime(2026, 9, 19, 6, 30, tzinfo=KST9)) == "2026-09-18")
    chk("화 06:30 은 월요일을 판정한다",
        previous_trading_day(datetime.datetime(2026, 9, 22, 6, 30, tzinfo=KST9)) == "2026-09-21")
    chk("월 06:30 은 주말을 건너뛰고 금요일로",
        previous_trading_day(datetime.datetime(2026, 9, 21, 6, 30, tzinfo=KST9)) == "2026-09-18")
    chk("연휴(9/24·25 휴장) 다음 월요일은 9/23 을 판정한다",
        previous_trading_day(datetime.datetime(2026, 9, 28, 6, 30, tzinfo=KST9)) == "2026-09-23",
        previous_trading_day(datetime.datetime(2026, 9, 28, 6, 30, tzinfo=KST9)))
    chk("판정 대상은 결코 오늘이 아니다(진행 중인 거래일을 판정하지 않는다)",
        all(previous_trading_day(datetime.datetime(2026, 9, d, 6, 30, tzinfo=KST9))
            < f"2026-09-{d:02d}" for d in range(15, 30)))

    with tempfile.TemporaryDirectory() as d:
        okk, msg = emit("2026-09-18", "scanner", {"scanned": 718}, root=d,
                        run_id="r1", sha="abc", fingerprint="fp1")
        chk("영수증이 파일 하나로 떨어진다", okk and msg.endswith("scanner-r1.json"), msg)

        rows, broken = load("2026-09-18", root=d)
        chk("읽힌다", len(rows) == 1 and broken == 0)
        chk("payload 가 보존된다", rows[0]["payload"]["scanned"] == 718)
        chk("실행 식별자가 붙는다",
            rows[0]["run_id"] == "r1" and rows[0]["fingerprint"] == "fp1")

        # 🔴 P0-1 핵심 — 워크플로마다 **다른 파일**이라 서로 덮지 않는다
        emit("2026-09-18", "analyst", {"x": 1}, root=d, run_id="r2", fingerprint="fp1")
        emit("2026-09-18", "earnings", {"y": 2}, root=d, run_id="r3", fingerprint="fp1")
        files = sorted(os.listdir(dir_for("2026-09-18", d)))
        chk("종류마다 독립 파일", files == ["analyst-r2.json", "earnings-r3.json",
                                            "scanner-r1.json"], files)
        chk("세 종류가 동시에 존재한다", len(load("2026-09-18", root=d)[0]) == 3)

        emit("2026-09-18", "scanner", {"scanned": 720}, root=d, run_id="r1", fingerprint="fp1")
        chk("같은 run 이 또 떨궈도 덮어쓰지 않는다(append-only)",
            os.path.exists(os.path.join(dir_for("2026-09-18", d), "scanner-r1-1.json")))
        chk("마지막 것을 고를 수 있다",
            latest("2026-09-18", "scanner", root=d)["payload"]["scanned"] == 720)

        emit("2026-09-18", "consensus", {"state": "OK"}, root=d, run_id="r4", fingerprint="fp2")
        chk("지문으로 거를 수 있다",
            latest("2026-09-18", "consensus", root=d, fingerprint="fp1") is None)
        chk("다른 지문은 찾힌다",
            latest("2026-09-18", "consensus", root=d, fingerprint="fp2") is not None)

        chk("모르는 종류는 거부", emit("2026-09-18", "엉뚱", {}, root=d)[0] is False)
        chk("없는 날은 빈 목록", load("2026-01-01", root=d) == ([], 0))

        pathlib.Path(dir_for("2026-09-18", d), "scanner-깨짐.json").write_text(
            "{깨진", encoding="utf-8")
        rows, broken = load("2026-09-18", root=d)
        chk("깨진 파일은 세고 버린다", broken == 1 and len(rows) == 5, (broken, len(rows)))

        bad_ok, bad_msg = emit("2026-09-18", "scanner", {"x": object()}, root=d)
        chk("직렬화 불가여도 예외를 밖으로 내지 않는다", bad_ok is False)
        chk("그리고 조용하지도 않다", "실패" in bad_msg)

        chk("쓰기 실패가 생산을 죽이지 않는다",
            emit("2026-09-18", "scanner", {}, root="/proc/없는경로")[0] is False)
    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    if "--previous-trading-day" in sys.argv:
        # finalizer 가 판정 대상 거래일을 여기서 받는다(쉘의 벽시계 날짜가 아니라)
        print(previous_trading_day())
        sys.exit(0)
    sys.exit(0 if _selftest() else 1)
