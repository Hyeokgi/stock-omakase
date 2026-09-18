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

KST = datetime.timezone(datetime.timedelta(hours=9))
RECEIPT_DIR = "data/receipts"
RECEIPT_VERSION = "receipt-v1"

# 어떤 파이프라인이 어떤 영수증을 내는가. Evidence Builder 가 이 이름으로 찾는다.
KINDS = ("scanner", "analyst", "earnings", "consensus")


def path_for(day, root=RECEIPT_DIR):
    return os.path.join(root, f"{day}.jsonl")


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
            "emitted_at": datetime.datetime.now(KST).isoformat(timespec="seconds"),
            "run_id": str(run_id or os.environ.get("GITHUB_RUN_ID", "")),
            "code_sha": str(sha or (os.environ.get("GITHUB_SHA") or "")[:8]),
            "fingerprint": str(fingerprint),
            "receipt_version": RECEIPT_VERSION,
            "payload": payload,
        }
        os.makedirs(root, exist_ok=True)
        with open(path_for(day, root), "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
        return True, f"영수증 {kind} 기록"
    except Exception as e:                       # noqa: BLE001 — 생산을 죽이지 않는다
        return False, f"영수증 {kind} 기록 실패: {type(e).__name__}: {e}"


def load(day, root=RECEIPT_DIR):
    """그 거래일의 영수증 전부. 깨진 줄은 세되 버린다(조용히 넘기지 않는다)."""
    p = path_for(day, root)
    out, broken = [], 0
    if not os.path.exists(p):
        return out, broken
    with open(p, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except ValueError:
                broken += 1
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

    print("🧪 생산 영수증")
    with tempfile.TemporaryDirectory() as d:
        okk, msg = emit("2026-09-18", "scanner", {"scanned": 718}, root=d,
                        run_id="r1", sha="abc", fingerprint="fp1")
        chk("영수증이 기록된다", okk and "scanner" in msg)

        rows, broken = load("2026-09-18", root=d)
        chk("읽힌다", len(rows) == 1 and broken == 0)
        chk("payload 가 보존된다", rows[0]["payload"]["scanned"] == 718)
        chk("실행 식별자가 붙는다",
            rows[0]["run_id"] == "r1" and rows[0]["fingerprint"] == "fp1")

        emit("2026-09-18", "scanner", {"scanned": 720}, root=d, fingerprint="fp1")
        rows, _ = load("2026-09-18", root=d)
        chk("append-only — 덮어쓰지 않는다", len(rows) == 2)
        chk("마지막 것을 고를 수 있다",
            latest("2026-09-18", "scanner", root=d)["payload"]["scanned"] == 720)

        emit("2026-09-18", "earnings", {"targets": 143}, root=d, fingerprint="fp2")
        chk("지문으로 거를 수 있다",
            latest("2026-09-18", "earnings", root=d, fingerprint="fp1") is None)
        chk("다른 지문은 찾힌다",
            latest("2026-09-18", "earnings", root=d, fingerprint="fp2") is not None)

        chk("모르는 종류는 거부", emit("2026-09-18", "엉뚱", {}, root=d)[0] is False)
        chk("없는 날은 빈 목록", load("2026-01-01", root=d) == ([], 0))

        with open(path_for("2026-09-18", d), "a", encoding="utf-8") as fh:
            fh.write("{깨진 줄\n")
        rows, broken = load("2026-09-18", root=d)
        chk("깨진 줄은 세고 버린다", broken == 1 and len(rows) == 3)

        bad_ok, bad_msg = emit("2026-09-18", "scanner", {"x": object()}, root=d)
        chk("직렬화 불가여도 예외를 밖으로 내지 않는다", bad_ok is False)
        chk("그리고 조용하지도 않다", "실패" in bad_msg)

        chk("쓰기 실패가 생산을 죽이지 않는다",
            emit("2026-09-18", "scanner", {}, root="/proc/없는경로")[0] is False)
    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
