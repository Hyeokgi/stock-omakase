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
import datetime

GATE_VERSION = "stability-gate-v3"

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
    "badge_observations.py",
    # 🔴 2026-09-20 — naver_sources 는 종목코드 규칙을 krx_code 에 위임한다.
    #    위임한 모듈이 지문에 없으면 규칙이 바뀌어도 지문이 그대로다(3차 교차검증과 같은 계열).
    "krx_code.py", "krx_amount.py",
    "naver_sources.py", "after_market_quotes.py", "telegram_target.py",
    "hyeoks_performance_memory.py", "hyeoks_data_quality.py",
    "hyeoks_run_freeze.py",
    "data/market_snapshot/nontrading.txt", "data/market_snapshot/calendar_scope.json",
    # 생산 코드
    "omakase.py", "hyeoks_analyst.py", "hyeoks_earnings_collector.py",
    "scanner_census.py", "hyeoks_tajeom.py",
    # 스키마·계약
    "earnings_schema.py", "feature_store.py", "rank_pool.py",
    # P1-4 — is_trading_day() 가 이걸 쓴다. 거래일 판정이 바뀌면 사이클 정의가 바뀐다.
    "hyeoks_trading_calendar.py",
    # 안정화 판정 자체
    "stability_gate.py", "evidence_builder.py", "production_receipt.py",
    "feature_telemetry.py",
    # 워크플로
    ".github/workflows/main.yml",
    ".github/workflows/earnings_collector.yml",
    ".github/workflows/consensus_aux.yml",
    ".github/workflows/ai_report.yml",
    ".github/workflows/review_regressions.yml",
    # 🔴 2026-09-18 — Gate **자신을 실행하는 코드**가 지문에 빠져 있었다.
    #    이 셋이 바뀌면 "증거를 어떻게 모으고 누가 판정하는가" 가 바뀐다.
    #    빠져 있으면 변경 전 PASS 와 변경 후 PASS 를 같은 3회로 셀 수 있다 —
    #    지문을 만든 목적과 정면으로 충돌한다.
    ".github/workflows/stability_finalizer.yml",
    ".github/receipt_commit.sh",
    ".github/workflow_states.py",
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

# 🔴 2026-09-20 — 운영 연속 기록(`runs.csv`)에 **역순 행을 끼워 넣지 않는다.**
#    v3 의 streak() 는 이력이 단조 증가라고 가정한다. 그런데 finalizer 의
#    `workflow_dispatch` 로 빠진 과거 거래일을 뒤늦게 판정하면 역순 행이 생기고,
#    그것이 append-only 라 **복구 경로 없이** 연속 계산을 망가뜨렸다(재현 확인).
#    그래서 뒤늦은 과거 판정은 버리지도, 본 기록에 섞지도 않고 여기 남긴다.
#    이 파일은 감사용이며 **연속 카운트의 근거가 아니다.**
AUDIT_LOG = os.path.join(GATE_DIR, "backfill_audit.csv")
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


def _audit(cycle_date, run_id, source, evidence, note="", path=AUDIT_LOG,
           fp="", aux_state=""):
    """뒤늦은 과거 판정을 **감사 기록**으로만 남긴다. 연속 카운트에 쓰지 않는다.

    streak() 는 이 파일을 읽지 않는다. 사람이 "그날은 어땠나" 를 물을 때 쓴다.
    """
    try:
        ok, bad = (evaluate(evidence) if is_trading_day(cycle_date) else (None, []))
    except EvidenceMissing as e:
        ok, bad = False, [str(e)]
    verdict = SKIP if ok is None else (PASS if ok else FAIL)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fresh = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        if fresh:
            w.writerow(HEADER)
        w.writerow([cycle_date, run_id, source, verdict, fp]
                   + [("-" if verdict == SKIP else ("Y" if evidence.get(k) else "N"))
                      for k in KEYS]
                   + [aux_state, note, GATE_VERSION])
    return verdict, bad


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

    # 🔴 2026-09-20 — 운영 기록은 **단조 증가**여야 한다. 역순은 거부하고,
    #    판정 자체는 감사 기록으로 보존한다(사용자 지시 ①). 조용히 버리지 않는다.
    prior = [r["cycle_date"] for r in rows if r.get("cycle_date")]
    if prior and cycle_date < max(prior):
        why = (f"역순 기록 거부: {cycle_date} 는 마지막 기록 {max(prior)} 보다 이르다. "
               f"운영 연속 기록에 끼워 넣지 않는다")
        # 🔴 거부가 본질이고 감사 기록은 부수적이다. 감사 쓰기가 실패해도 **거부는
        #    성립해야** 한다 — 안 그러면 쓰기 오류가 거부를 예외로 바꿔 finalizer 를
        #    죽인다(내 첫 구현이 그랬다). 대신 조용히 넘기지도 않는다.
        try:
            _audit(cycle_date, run_id, source, evidence, note=why,
                   path=AUDIT_LOG, fp=fp or fingerprint(root), aux_state=aux_state)
            why += f" — 감사 기록 {AUDIT_LOG} 에 남겼다"
        except Exception as e:                    # noqa: BLE001 — 사유를 싣고 계속한다
            why += f" — ⚠️ 감사 기록 실패({type(e).__name__}) 이 판정은 보존되지 않았다"
        return False, why

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
    # 🔴 2026-09-20 — v3 는 이력에 이상 행이 하나라도 있으면 `return 0` 이었다.
    #    `runs.csv` 는 append-only 이고 과거 행을 고치지 않으므로 **영구 0** 이었다.
    #    3/3 을 달성한 뒤 뒤늦은 과거 판정 한 번으로 영구히 0 이 됐다(재현 확인).
    #
    #    그래서 **앞에서부터** 걸어 마지막 이상 지점을 찾고, **그보다 뒤의 행만** 센다.
    #    · 이상 행을 무시해 앞뒤를 잇지 않는다 — 연속성은 그 지점에서 끊긴다
    #    · 그보다 뒤에 정상 거래일이 쌓이면 다시 셀 수 있다 — 회복 경로가 있다
    #    · 이상 행이 가장 최근 끝에 닿아 있으면 셀 꼬리가 없다 → 0
    #      (이력 전체가 역순이거나 최신 행이 중복이면 그 자체가 신뢰 불가다)
    #    과거 행은 지우지 않는다. 읽는 방식만 바꾼다.
    resume = 0                     # 이 인덱스부터가 신뢰할 수 있는 구간이다
    seen = set()
    prev = None
    for i, r in enumerate(rows):
        try:
            day = datetime.date.fromisoformat(r["cycle_date"])
        except (KeyError, TypeError, ValueError):
            resume, seen, prev = i + 1, set(), None
            continue               # 날짜를 못 읽으면 순서를 보증할 수 없다
        if day in seen or (prev is not None and day <= prev):
            resume, seen, prev = i + 1, {day}, day
            continue               # 중복·역순 — 여기까지는 신뢰하지 않는다
        seen.add(day)
        prev = day

    n = 0
    newer = None
    for r in reversed(rows[resume:]):
        day = datetime.date.fromisoformat(r["cycle_date"])
        v = r.get("verdict")
        if v == SKIP:
            if is_trading_day(day.isoformat()):
                break              # 거래일을 SKIP 으로 적은 것은 성공 근거가 아니다
            continue               # 비거래일 — 연속을 늘리지도 끊지도 않는다
        if v != PASS:
            break
        if r.get("fingerprint") != current or not is_trading_day(day.isoformat()):
            break                  # 파이프라인이 바뀌었다 — 여기부터는 다른 시스템이다
        if newer is not None:
            expected = newer - datetime.timedelta(days=1)
            while not is_trading_day(expected.isoformat()):
                expected -= datetime.timedelta(days=1)
            if day != expected:
                break              # 평일 결측 — 연속이 아니다
        n += 1
        newer = day
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

    # P1-2 — cutoff 전에는 보류한다
    import tempfile as _tf
    import datetime as _dt
    with _tf.TemporaryDirectory() as _d:
        early = _dt.datetime(2026, 9, 21, 15, 0,
                             tzinfo=_dt.timezone(_dt.timedelta(hours=9)))
        late = early.replace(hour=23, minute=30)
        r_ok, r_why = ready("2026-09-21", _d, now=early)
        chk("영수증이 없고 cutoff 전이면 보류", r_ok is False and "보류" in r_why)
        chk("cutoff 을 지나면 판정한다", ready("2026-09-21", _d, now=late)[0] is True)
        import production_receipt as _pr
        for _k in ("scanner", "analyst", "earnings"):
            _pr.emit("2026-09-21", _k, {}, root=_d, run_id="r", fingerprint="fp")
        chk("required 가 다 모이면 cutoff 전에도 판정한다",
            ready("2026-09-21", _d, now=early)[0] is True)

    # ② 증거는 사람이 넣지 않는다 — Evidence Builder 와 실제로 물리는가
    import evidence_builder as _eb
    chk("gate 기준과 builder 기준 이름이 같다", set(KEYS) == set(_eb.build("2026-09-19", "x")[0]))
    chk("builder 는 bool 만 낸다",
        all(isinstance(v, bool) for v in _eb.build("2026-09-19", "x")[0].values()))
    chk("증거가 없으면 builder 도 통과를 내지 않는다",
        evaluate(_eb.build("2026-09-19", "x")[0])[0] is False)

    # P1-1 — CI 증거를 지문으로 찾는다
    def nochange(_sha):
        return ["data/feature_store/2026-09-18.csv.gz", "docs/메모.md"]

    def codechange(_sha):
        return ["omakase.py"]

    ok, why = ci_evidence_for_fingerprint(runs=[("abc1234def", "success")],
                                          changed_since=nochange)
    chk("데이터만 바뀌었으면 이전 CI 성공을 증거로 인정한다", ok, why)
    ok2, why2 = ci_evidence_for_fingerprint(runs=[("abc1234def", "success")],
                                            changed_since=codechange)
    chk("핵심 파일이 바뀌었으면 인정하지 않는다", ok2 is False and "omakase.py" in why2)
    chk("실패한 run 은 증거가 아니다",
        ci_evidence_for_fingerprint(runs=[("a", "failure")], changed_since=nochange)[0] is False)
    chk("run 이 없으면 증거가 없다", ci_evidence_for_fingerprint(runs=[])[0] is False)
    chk("변경 목록을 못 보면 인정하지 않는다",
        ci_evidence_for_fingerprint(runs=[("a", "success")], changed_since=None)[0] is False)

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
            "hyeoks_trading_calendar.py", ".github/workflows/main.yml",
            ".github/workflows/stability_finalizer.yml",
            ".github/receipt_commit.sh", ".github/workflow_states.py")))
    chk("관측 데이터는 제외하되 달력 설정은 지문에 포함한다",
        {f for f in FINGERPRINT_FILES if f.startswith('data/')} ==
        {'data/market_snapshot/nontrading.txt', 'data/market_snapshot/calendar_scope.json'})

    print("\n" + f"✅ 전부 통과 ({ok_count}건)")
    return ok_count


def ci_evidence_for_fingerprint(root=".", runs=None, changed_since=None):
    """🔴 2026-09-18 P1-1 — CI 증거를 **HEAD SHA** 로 찾으면 안 된다.

    데이터 전용 커밋(`[skip ci]`)에는 audit CI 가 돌지 않는다. 그래서 정상 코드인데도
    `conclusion=""` 이 되어 `ci_green` 이 거짓이 된다 — 매일 데이터가 커밋되므로
    사실상 항상 거짓이다.

    증명해야 하는 것은 "HEAD 가 CI 를 통과했나" 가 아니라
    **"지금 지문이 CI 를 통과했나"** 다. 그래서:
      ① 최근 성공한 audit run 의 SHA 를 받아
      ② 그 SHA 와 현재 사이에 FINGERPRINT_FILES 가 바뀌지 않았는지 보고
      ③ 안 바뀌었으면 그 성공을 현재 지문의 증거로 인정한다.

    `runs` 는 [(sha, conclusion), ...] 를 최신순으로 받는다(호출부가 API 로 채운다).
    `changed_since(sha)` 는 그 SHA 이후 바뀐 파일 목록을 준다.
    """
    core = set(FINGERPRINT_FILES)
    for sha, conclusion in (runs or []):
        if conclusion != "success":
            continue
        try:
            changed = set(changed_since(sha)) if changed_since else None
        except Exception as e:                       # noqa: BLE001
            return False, f"변경 목록 조회 실패({type(e).__name__})"
        if changed is None:
            return False, "변경 목록을 볼 수 없다"
        touched = sorted(core & changed)
        if touched:
            return False, (f"{sha[:8]} 이후 핵심 파일이 바뀌었다: {touched} — "
                           "그 성공은 지금 지문의 증거가 아니다")
        return True, f"{sha[:8]} audit 성공 · 이후 핵심 파일 변경 없음"
    return False, "성공한 audit run 을 찾지 못했다"


# P1-2 — 너무 일찍 판정하면 영수증이 아직 없다. cutoff 전에는 **보류**한다.
#    (영수증이 다 모이기 전의 FAIL 을 영구 기록으로 남기지 않는다)
CUTOFF_KST_HOUR = 23


def ready(cycle_date, root=None, now=None):
    """(준비됨, 사유) — required 영수증이 다 모였는가, 아니면 cutoff 을 지났는가."""
    import datetime as _dt
    import evidence_builder
    import production_receipt
    root = root or production_receipt.RECEIPT_DIR
    rows, _ = production_receipt.load(cycle_date, root)
    kinds = {r.get("kind") for r in rows}
    missing = [k for k in evidence_builder.REQUIRED_KINDS if k not in kinds]
    if not missing:
        return True, "required 영수증 전부 도착"
    now = now or _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=9)))
    try:
        day = _dt.date.fromisoformat(str(cycle_date))
    except ValueError:
        return False, f"거래일 형식 오류 {cycle_date}"
    cutoff = _dt.datetime.combine(day, _dt.time(CUTOFF_KST_HOUR), tzinfo=now.tzinfo)
    if now < cutoff:
        return False, (f"아직 {missing} 영수증이 없고 cutoff({CUTOFF_KST_HOUR}시 KST) 전이다 "
                       "— 보류한다(영구 FAIL 로 기록하지 않는다)")
    return True, f"cutoff 경과 — {missing} 없이 판정한다"


def record_cycle(cycle_date, source="cycle", root=".", path=GATE_LOG,
                 receipts_root=None, workflow_states=None, now=None, fp=None):
    """🔴 2026-09-18 지시 ② — **증거를 사람이 넣지 않는다.**

    Evidence Builder 가 영수증에서 7개 기준을 기계적으로 만들고, 그걸 그대로 기록한다.
    이 함수 밖에서 evidence 를 손으로 만들어 넣는 경로는 문서에서 증거로 인정되지 않는다.
    """
    import evidence_builder
    import production_receipt
    fp = fp or fingerprint(root)
    if not is_trading_day(cycle_date):
        return record(cycle_date, f"skip-{cycle_date}", source, {},
                      path=path, fp=fp, root=root)
    ok_ready, why_ready = ready(cycle_date, receipts_root, now=now)
    if not ok_ready:
        return False, why_ready          # 기록하지 않는다 — 다음 실행에서 다시 본다
    ev, detail = evidence_builder.build(
        cycle_date, fp, root=receipts_root or production_receipt.RECEIPT_DIR,
        workflow_states=workflow_states)
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
    if "--ci-evidence" in sys.argv:
        # 워크플로가 넘긴 runs(JSON) 와 변경 파일 목록으로 판정해 한 줄 출력한다
        import json
        import subprocess
        raw = sys.argv[sys.argv.index("--ci-evidence") + 1]
        runs = [(r.get("head_sha", ""), r.get("conclusion", ""))
                for r in json.loads(raw or "[]")]

        def changed(sha):
            out = subprocess.run(["git", "diff", "--name-only", f"{sha}..HEAD"],
                                 capture_output=True, text=True, timeout=60)
            if out.returncode != 0:
                raise RuntimeError(out.stderr.strip()[:120])
            return [l for l in out.stdout.splitlines() if l.strip()]

        ok, why = ci_evidence_for_fingerprint(runs=runs, changed_since=changed)
        print(f"CI_EVIDENCE={'success' if ok else ''}")
        print(f"근거: {why}")
        sys.exit(0)
    if "--record" in sys.argv:
        import json
        i = sys.argv.index("--record")
        if i + 1 >= len(sys.argv):
            print("❌ --record 다음에 거래일(YYYY-MM-DD)이 필요하다")
            sys.exit(2)
        wf = None
        if "--workflows" in sys.argv:
            wf = json.loads(sys.argv[sys.argv.index("--workflows") + 1] or "{}")
        done, why = record_cycle(sys.argv[i + 1], workflow_states=wf)
        print(f"\n{'기록' if done else '무시'}: {why}")
        print(report())
        sys.exit(0)
    print(report())
