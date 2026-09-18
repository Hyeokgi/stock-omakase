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
import os

GATE_VERSION = "stability-gate-v1"
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

HEADER = ["run_id", "date", "source", "verdict"] + KEYS + ["note", "gate_version"]


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


def record(run_id, date, source, evidence, note="", path=GATE_LOG):
    """실행 하나를 append-only 로 남긴다. 같은 run_id 는 두 번 세지 않는다."""
    run_id = str(run_id).strip()
    if not run_id:
        raise ValueError("run_id 가 없으면 중복을 막을 수 없다")
    rows = load(path)
    if any(r["run_id"] == run_id for r in rows):
        return False, f"이미 기록된 실행 {run_id} — 같은 실행을 두 번 세지 않는다"
    ok, bad = evaluate(evidence)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fresh = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        if fresh:
            writer.writerow(HEADER)
        writer.writerow([run_id, date, source, "PASS" if ok else "FAIL"]
                        + ["Y" if evidence[k] else "N" for k in KEYS]
                        + [note, GATE_VERSION])
    return True, ("통과" if ok else f"미달: {bad}")


def streak(rows=None, path=GATE_LOG):
    """뒤에서부터 연속 PASS 수. FAIL 을 만나면 거기서 끊는다."""
    rows = load(path) if rows is None else rows
    n = 0
    for r in reversed(rows):
        if r.get("verdict") != "PASS":
            break
        n += 1
    return n


def state(rows=None, path=GATE_LOG):
    """(통과 여부, 사람이 읽을 한 줄)."""
    rows = load(path) if rows is None else rows
    n = streak(rows)
    if n >= REQUIRED_STREAK:
        return True, (f"✅ 안정화 종료선 통과 — 연속 {n}회 (기준 {REQUIRED_STREAK}). "
                      "운영 감사 Phase 를 닫고 알파 연구로 복귀한다")
    last = rows[-1]["verdict"] if rows else "기록 없음"
    return False, (f"⏳ 연속 {n}/{REQUIRED_STREAK}회 — 마지막 실행 {last}. "
                   "아직 감사 Phase 다")


def report(rows=None, path=GATE_LOG):
    rows = load(path) if rows is None else rows
    lines = [f"🚦 {GATE_VERSION} — 기록 {len(rows)}회", state(rows)[1], ""]
    for r in rows[-REQUIRED_STREAK - 2:]:
        marks = " ".join(f"{k}={r.get(k, '?')}" for k in KEYS)
        lines.append(f"  {r['date']} {r['source']} [{r['verdict']}] {marks}")
    if not rows:
        lines.append("  (아직 실제 생산 실행을 한 번도 기록하지 않았다)")
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
        p = os.path.join(d, "runs.csv")
        chk("기록 없으면 미통과", state(path=p)[0] is False)

        for i in range(3):
            done, _ = record(f"run{i}", f"2026-09-{18+i}", "earnings", good, path=p)
            assert done
        chk("3회 연속이면 통과", state(path=p)[0] is True, streak(path=p))

        again, why = record("run0", "2026-09-18", "earnings", good, path=p)
        chk("같은 실행을 두 번 세지 않는다", again is False and "이미" in why)

        record("run3", "2026-09-21", "earnings", bad, path=p)
        chk("한 번 떨어지면 통과가 풀린다", state(path=p)[0] is False)
        chk("연속 카운트가 0 으로 돌아간다", streak(path=p) == 0)

        record("run4", "2026-09-22", "earnings", good, path=p)
        chk("다시 1 부터 센다", streak(path=p) == 1)
        chk("리포트가 상태를 말한다", "1/3" in report(path=p))

        rows = load(p)
        chk("append-only — 지운 행이 없다", len(rows) == 5, len(rows))
        chk("기준 열이 행마다 남는다", all(all(k in r for k in KEYS) for r in rows))

    print("\n" + f"✅ 전부 통과 ({ok_count}건)")
    return ok_count


if __name__ == "__main__":
    import sys
    if "--self-test" in sys.argv:
        sys.exit(0 if _selftest() else 1)
    print(report())
