# -*- coding: utf-8 -*-
"""
운영 안정화 종료선 — 감사 무한루프를 끊는다.

2026-09-18 GPT 제안 §7 채택. 사용자 승인 후 문서 버전 갱신 대상이다.

왜 필요한가: 오류를 찾을 때마다 검사도구가 생기고 그 도구가 또 오류를 찾는
구조가 되고 있다. 이번 주에만 A~E 다섯 건 + GPT P0 네 건을 고쳤고, 고칠 때마다
새 시험이 늘었다. 이대로면 **본래 목적인 수익 조합 발견이 계속 뒤로 밀린다.**

그래서 선을 긋는다. 다만 선언이 아니라 **기록으로** 긋는다 —
"이제 안정된 것 같다" 는 말은 증거가 아니다. 실제 예정 실행 3회가 7개 기준을
연속으로 통과해야 한다. 한 번이라도 떨어지면 연속 카운트는 **0 으로 돌아간다.**

이 문턱은 "버그가 0" 이라는 뜻이 아니다.
**"수익 연구를 재개해도 입력을 믿을 수 있다"** 는 운영 기준이다.
"""
import csv
import hashlib
import os

GATE_VERSION = "stability-gate-v2"

# ═══════════════════════════════════════════════════════════════════════
# ③ 안정화 1회 = 하나의 **KRX 거래일 end-to-end 생산 사이클**
#    개별 workflow run 이 아니다. 하루에 스캐너가 144회 도는데 그 중 하나가
#    통과했다고 "1회" 로 세면, 같은 날 세 번 돌려 문턱을 통과할 수 있다.
#    비거래일은 SKIP — streak 를 늘리지도 끊지도 않는다.
# ═══════════════════════════════════════════════════════════════════════
SKIP = "SKIP"
PASS = "PASS"
FAIL = "FAIL"

# ═══════════════════════════════════════════════════════════════════════
# ④ 3회 연속은 **같은 파이프라인**이어야 한다
#    핵심 코드·워크플로·스키마가 바뀌면 그 3회는 서로 다른 시스템의 기록이다.
#    지문이 바뀌면 streak 를 0 으로 되돌린다.
# ═══════════════════════════════════════════════════════════════════════
#    ⚠️ 데이터 자동 커밋으로 매일 달라지는 저장소 전체 SHA 를 쓰지 않는다.
#       **핵심 생산 코드·워크플로·스키마/계약 모듈**만 본다(사용자 지시 ⑥).
FINGERPRINT_FILES = [
    # 생산 코드
    "omakase.py", "hyeoks_analyst.py", "hyeoks_earnings_collector.py",
    "scanner_census.py", "hyeoks_tajeom.py",
    # 스키마·계약
    "earnings_schema.py", "feature_store.py", "rank_pool.py",
    # 안정화 판정 자체
    "stability_gate.py", "evidence_builder.py", "production_receipt.py",
    "feature_telemetry.py",
    # 워크플로
    ".github/workflows/main.yml",
    ".github/workflows/earnings_collector.yml",
    ".github/workflows/consensus_aux.yml",
    ".github/workflows/ai_report.yml",
    ".github/workflows/review_regressions.yml",
]


def fingerprint(root=".", files=None):
    """핵심 코드·워크플로·스키마의 지문. 하나라도 바뀌면 값이 바뀐다.

    없는 파일은 이름만으로도 지문에 들어간다 — 파일이 사라진 것도 변경이다.
    """
    h = hashlib.sha256()
    for name in sorted(files or FINGERPRINT_FILES):
        path = os.path.join(root, name)
        h.update(name.encode("utf-8"))
        try:
            with open(path, "rb") as fh:
                h.update(fh.read())
        except OSError:
            h.update(b"<missing>")
    return h.hexdigest()[:12]
GATE_DIR = "data/stability_gate"
GATE_LOG = os.path.join(GATE_DIR, "runs.csv")
REQUIRED_STREAK = 3

# 기준 7개. 키는 증거 딕셔너리의 이름이고 값은 사람이 읽을 설명이다.
# 순서를 바꾸지 않는다 — 기록된 행과 대조해야 한다.
CRITERIA = [
    ("ci_green", "핵심 CI 전부 green"),
    ("earnings_schema_ok", "DB_실적 schema contract 정상(V3 열을 이름으로 찾고 범위 통과)"),
    ("dart_complete", "DART primary 전체 대상 완료 또는 **명시적** 결측"),
    ("no_silent_parse_error", "V1/V2/V3 조용한 파싱오류 0"),
    ("store_written", "feature_store / rank_pool 정상 기록"),
    ("ledger_written", "백테스트 원장 정상 적재"),
    ("no_unexplained_failure", "핵심 workflow 에 설명되지 않은 실패 없음"),
]
KEYS = [k for k, _ in CRITERIA]

HEADER = (["cycle_date", "run_id", "source", "verdict", "fingerprint"]
          + KEYS + ["aux_state", "note", "gate_version"])


class EvidenceMissing(ValueError):
    """증거가 빠진 항목을 통과로 치지 않는다. 모르는 것은 통과가 아니다."""


def evaluate(evidence):
    """증거 딕셔너리 → (통과 여부, 떨어진 기준 목록).

    값은 True/False 만 받는다. 빠졌거나 None 이면 EvidenceMissing 이다 —
    "확인 못 했다" 를 "괜찮다" 로 바꾸는 순간 이 문턱은 의미가 없다.
    """
    missing = [k for k in KEYS if evidence.get(k) is None]
    if missing:
        raise EvidenceMissing(f"증거 없음: {missing}")
    bad = [k for k in KEYS if evidence[k] is not True]
    return (not bad), bad


def load(path=GATE_LOG):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def is_trading_day(date):
    """등록 달력으로 거래일 여부를 본다. 비거래일은 사이클이 아니다."""
    from hyeoks_trading_calendar import scheduled_session
    return bool(scheduled_session(date))


def record(cycle_date, run_id, source, evidence, note="", path=GATE_LOG,
           fp=None, aux_state="", root="."):
    """**거래일 사이클 하나**를 append-only 로 남긴다.

    ③ 같은 거래일을 두 번 세지 않는다. 비거래일은 SKIP 으로만 기록된다.
    ④ 지문을 같이 남긴다 — 나중에 "이 3회가 같은 시스템이었나" 를 물을 수 있다.
    """
    cycle_date = str(cycle_date).strip()
    run_id = str(run_id).strip()
    if not cycle_date:
        raise ValueError("cycle_date 가 없으면 사이클을 셀 수 없다")
    if not run_id:
        raise ValueError("run_id 가 없으면 증거를 되짚을 수 없다")
    rows = load(path)
    if any(r["cycle_date"] == cycle_date for r in rows):
        return False, f"이미 기록된 거래일 {cycle_date} — 같은 날을 두 번 세지 않는다"

    if not is_trading_day(cycle_date):
        verdict, bad = SKIP, []
    else:
        ok, bad = evaluate(evidence)
        verdict = PASS if ok else FAIL

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fresh = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        if fresh:
            writer.writerow(HEADER)
        writer.writerow(
            [cycle_date, run_id, source, verdict, fp or fingerprint(root)]
            + [("-" if verdict == SKIP else ("Y" if evidence.get(k) else "N")) for k in KEYS]
            + [aux_state, note, GATE_VERSION])
    if verdict == SKIP:
        return True, "비거래일 — SKIP(연속을 늘리지도 끊지도 않는다)"
    return True, ("통과" if verdict == PASS else f"미달: {bad}")


def streak(rows=None, path=GATE_LOG, fp=None, root="."):
    """뒤에서부터 연속 PASS 수.

    ③ SKIP(비거래일)은 **건너뛴다** — 늘리지도 끊지도 않는다.
    ④ 현재 지문과 다른 행을 만나면 거기서 끊는다. 다른 시스템의 기록이기 때문이다.
    """
    rows = load(path) if rows is None else rows
    current = fp or fingerprint(root)
    n = 0
    for r in reversed(rows):
        v = r.get("verdict")
        if v == SKIP:
            continue
        if v != PASS:
            break
        if r.get("fingerprint") and r["fingerprint"] != current:
            break            # 파이프라인이 바뀌었다 — 여기부터는 다른 시스템이다
        n += 1
    return n


def state(rows=None, path=GATE_LOG, fp=None, root="."):
    """(통과 여부, 사람이 읽을 한 줄)."""
    rows = load(path) if rows is None else rows
    current = fp or fingerprint(root)
    n = streak(rows, fp=current)
    if n >= REQUIRED_STREAK:
        return True, (f"✅ 안정화 종료선 통과 — 연속 {n}거래일 (기준 {REQUIRED_STREAK}) "
                      f"· 지문 {current}. 운영 감사 Phase 를 닫고 알파 연구로 복귀한다")
    graded = [r for r in rows if r.get("verdict") != SKIP]
    last = graded[-1]["verdict"] if graded else "기록 없음"
    why = ""
    if graded and graded[-1].get("fingerprint") not in ("", None, current):
        why = " · ⚠️ 지문이 바뀌었다(파이프라인 변경) — 연속을 처음부터 다시 센다"
    return False, (f"⏳ 연속 {n}/{REQUIRED_STREAK}거래일 — 마지막 판정 {last} "
                   f"· 지문 {current}.{why} 아직 감사 Phase 다")


def report(rows=None, path=GATE_LOG, root="."):
    rows = load(path) if rows is None else rows
    current = fingerprint(root)
    lines = [f"🚦 {GATE_VERSION} — 기록 {len(rows)}일", state(rows, fp=current)[1], ""]
    for r in rows[-REQUIRED_STREAK - 2:]:
        marks = " ".join(f"{k}={r.get(k, '?')}" for k in KEYS)
        same = "" if r.get("fingerprint") == current else "  ⚠️지문다름"
        lines.append(f"  {r['cycle_date']} {r['source']} [{r['verdict']}] {marks}"
                     f"  aux={r.get('aux_state', '-')}{same}")
    if not rows:
        lines.append("  (아직 거래일 사이클을 한 번도 기록하지 않았다)")
    return "\n".join(lines)


def _selftest():
    import tempfile
    ok_count = 0

    def chk(name, cond, extra=""):
        nonlocal ok_count
        assert cond, f"{name} {extra}"
        ok_count += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    print("🧪 안정화 종료선")
    good = {k: True for k in KEYS}
    chk("전부 참이면 통과", evaluate(good) == (True, []))
    bad = dict(good, dart_complete=False)
    chk("하나만 떨어져도 미달", evaluate(bad) == (False, ["dart_complete"]))

    try:
        evaluate({k: True for k in KEYS[:-1]})
        raise AssertionError("빠진 증거를 통과시켰다")
    except EvidenceMissing:
        chk("빠진 증거는 통과가 아니다 — 모르는 것을 괜찮다로 바꾸지 않는다", True)

    try:
        evaluate(dict(good, ci_green=None))
        raise AssertionError("None 을 통과시켰다")
    except EvidenceMissing:
        chk("None 도 증거가 아니다", True)

    chk("기준은 7개", len(CRITERIA) == 7)
    chk("연속 요구는 3회", REQUIRED_STREAK == 3)

    with tempfile.TemporaryDirectory() as d:
        pth = os.path.join(d, "runs.csv")
        FP = "aaaaaaaaaaaa"
        chk("기록 없으면 미통과", state(path=pth, fp=FP)[0] is False)

        # ③ 거래일 사이클 단위로 센다
        for day in ("2026-09-16", "2026-09-17", "2026-09-18"):
            done, _ = record(day, f"run-{day}", "cycle", good, path=pth, fp=FP)
            assert done, day
        chk("거래일 3회 연속이면 통과", state(path=pth, fp=FP)[0] is True, streak(path=pth, fp=FP))

        again, why = record("2026-09-18", "다른런", "cycle", good, path=pth, fp=FP)
        chk("같은 거래일을 두 번 세지 않는다", again is False and "이미" in why)

        # ③ 비거래일은 SKIP — 늘리지도 끊지도 않는다
        before = streak(path=pth, fp=FP)
        record("2026-09-19", "run-토", "cycle", {}, path=pth, fp=FP)   # 토요일
        chk("비거래일은 SKIP 이다", load(pth)[-1]["verdict"] == SKIP)
        chk("SKIP 은 연속을 늘리지 않는다", streak(path=pth, fp=FP) == before, streak(path=pth, fp=FP))
        record("2026-09-21", "run-월", "cycle", good, path=pth, fp=FP)
        chk("SKIP 이 연속을 끊지도 않는다", streak(path=pth, fp=FP) == before + 1)
        chk("SKIP 행은 증거 없이도 기록된다(빈 증거로 죽지 않는다)",
            load(pth)[-2]["ci_green"] == "-")

        # ④ 지문이 바뀌면 처음부터 다시
        chk("지문이 바뀌면 연속이 0 이다", streak(path=pth, fp="bbbbbbbbbbbb") == 0)
        chk("지문이 바뀌면 통과가 풀린다", state(path=pth, fp="bbbbbbbbbbbb")[0] is False)
        chk("그 이유를 말한다", "지문이 바뀌었다" in state(path=pth, fp="bbbbbbbbbbbb")[1])
        record("2026-09-22", "run-새지문", "cycle", good, path=pth, fp="bbbbbbbbbbbb")
        chk("새 지문에서 1 부터 센다", streak(path=pth, fp="bbbbbbbbbbbb") == 1)

        # 미달은 여전히 연속을 끊는다
        record("2026-09-23", "run-미달", "cycle", bad, path=pth, fp="bbbbbbbbbbbb")
        chk("미달이면 연속이 0", streak(path=pth, fp="bbbbbbbbbbbb") == 0)

        chk("append-only", len(load(pth)) == 7, len(load(pth)))
        chk("지문이 행마다 남는다", all(r["fingerprint"] for r in load(pth)))

    # ② 증거는 사람이 넣지 않는다 — Evidence Builder 와 실제로 물리는가
    import evidence_builder as _eb
    chk("gate 기준과 builder 기준 이름이 같다", set(KEYS) == set(_eb.build("2026-09-19", "x")[0]))
    chk("builder 는 bool 만 낸다",
        all(isinstance(v, bool) for v in _eb.build("2026-09-19", "x")[0].values()))
    chk("증거가 없으면 builder 도 통과를 내지 않는다",
        evaluate(_eb.build("2026-09-19", "x")[0])[0] is False)

    # ④ 지문 자체
    chk("지문은 12자리", len(fingerprint()) == 12)
    chk("같은 입력이면 같은 지문", fingerprint() == fingerprint())
    chk("파일 목록이 바뀌면 지문이 바뀐다",
        fingerprint(files=["omakase.py"]) != fingerprint(files=["omakase.py", "rank_pool.py"]))
    chk("없는 파일도 지문에 반영된다(사라진 것도 변경이다)",
        fingerprint(files=["없는파일.py"]) != fingerprint(files=["다른없는파일.py"]))
    chk("핵심 파일이 지문에 들어 있다",
        all(f in FINGERPRINT_FILES for f in (
            "omakase.py", "earnings_schema.py", "feature_store.py", "rank_pool.py",
            "evidence_builder.py", "production_receipt.py", "feature_telemetry.py",
            ".github/workflows/main.yml")))
    chk("데이터 디렉터리는 지문에 없다(매일 달라지면 streak 가 매일 0 이 된다)",
        not any(f.startswith("data/") for f in FINGERPRINT_FILES))

    print("\n" + f"✅ 전부 통과 ({ok_count}건)")
    return ok_count


def record_cycle(cycle_date, source="cycle", root=".", path=GATE_LOG,
                 receipts_root=None):
    """🔴 2026-09-18 지시 ② — **증거를 사람이 넣지 않는다.**

    Evidence Builder 가 영수증에서 7개 기준을 기계적으로 만들고, 그걸 그대로 기록한다.
    이 함수 밖에서 evidence 를 손으로 만들어 넣는 경로는 문서에서 증거로 인정되지 않는다.
    """
    import evidence_builder
    import production_receipt
    fp = fingerprint(root)
    if not is_trading_day(cycle_date):
        return record(cycle_date, f"skip-{cycle_date}", source, {},
                      path=path, fp=fp, root=root)
    ev, detail = evidence_builder.build(
        cycle_date, fp, root=receipts_root or production_receipt.RECEIPT_DIR)
    run_id = "+".join(sorted({
        r.get("run_id", "") for r in production_receipt.load(
            cycle_date, receipts_root or production_receipt.RECEIPT_DIR)[0]
        if r.get("run_id")})) or f"norun-{cycle_date}"
    note = "; ".join(f"{k}:{v}" for k, v in detail["reasons"].items() if not ev[k])
    print(evidence_builder.render(ev, detail))
    return record(cycle_date, run_id[:200], source, ev, note=note[:900],
                  path=path, fp=fp, root=root, aux_state=detail["aux_state"])


if __name__ == "__main__":
    import sys
    if "--self-test" in sys.argv:
        sys.exit(0 if _selftest() else 1)
    if "--record" in sys.argv:
        i = sys.argv.index("--record")
        if i + 1 >= len(sys.argv):
            print("❌ --record 다음에 거래일(YYYY-MM-DD)이 필요하다")
            sys.exit(2)
        done, why = record_cycle(sys.argv[i + 1])
        print(f"\n{'기록' if done else '무시'}: {why}")
        print(report())
        sys.exit(0)
    print(report())
