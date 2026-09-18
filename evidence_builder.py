# -*- coding: utf-8 -*-
"""
Evidence Builder — 영수증에서 Stability Gate 증거를 **기계적으로** 만든다.

2026-09-18 사용자 지시 ②·④·⑤·⑥·⑦·⑧.

무엇을 바꾸는가
---------------
이전에는 `stability_gate.evaluate()` 에 사람이 True/False 7개를 넣었다.
그건 판정이지 증거가 아니다. 이제 각 파이프라인이 영수증을 떨구고, 이 모듈이
그것을 읽어 기준별 참·거짓과 **그 근거**를 같이 만든다.

규칙 하나
---------
**모르는 것은 통과가 아니다.** 영수증이 없으면 그 기준은 False 이고 사유가 붙는다.
`stability_gate.evaluate()` 가 None 을 거부하므로, 여기서도 None 을 내지 않고
False + 사유를 낸다. 침묵이 통과로 바뀌는 경로를 만들지 않는다.

기준별 판정 근거 (⑤~⑧)
-----------------------
④ no_silent_parse_error 는 **V1·V2·V3 각각**을 본다. V3 요약만으로 True 로 만들지 않는다.
⑤ store_written 은 존재만 보지 않는다 — 거래일·run_id·지문 일치와 유효 행수를 본다.
⑥ ledger_written 은 append 호출 성공이 아니라 **기대한 trade_id 가 실재하는지**(read-after-write).
⑦ no_unexplained_failure 는 사람의 해석이 아니라 **required workflow 의 expected state** 충족.
   AUX(컨센서스)는 기준이 아니라 별도로 기록한다.
"""
import os

import production_receipt as R

BUILDER_VERSION = "evidence-builder-v1"

# ⑦ 거래일마다 반드시 기대 상태에 도달해야 하는 생산 파이프라인
REQUIRED_KINDS = ("scanner", "analyst", "earnings")
# 보조 — 판정에 넣지 않고 따로 기록한다
AUX_KINDS = ("consensus",)

MIN_STORE_ROWS = 50          # 스캔 풀은 700대다. 50 미만이면 정상 수집이 아니다
MIN_POOL_ROWS = 1            # rank_pool 은 채널당 상위 N — 0 이면 기록이 안 된 것이다


def _miss(kind):
    return f"{kind} 영수증 없음"


def build(cycle_date, fingerprint, root=R.RECEIPT_DIR):
    """(evidence, detail) — evidence 는 stability_gate 가 먹는 7개 bool."""
    rows, broken = R.load(cycle_date, root)
    got = {k: R.latest(cycle_date, k, root, fingerprint=fingerprint)
           for k in REQUIRED_KINDS + AUX_KINDS}
    ev, why = {}, {}

    def put(key, value, reason):
        ev[key] = bool(value)
        why[key] = reason

    # ① 핵심 CI green — 그 지문의 CI 결과를 스캐너 영수증이 싣는다
    sc = got["scanner"]
    if not sc:
        put("ci_green", False, _miss("scanner"))
    else:
        ci = sc["payload"].get("ci_conclusion")
        put("ci_green", ci == "success", f"ci_conclusion={ci!r}")

    # ② DB_실적 schema contract
    ea = got["earnings"]
    if not ea:
        put("earnings_schema_ok", False, _miss("earnings"))
    else:
        p = ea["payload"]
        ok = (p.get("schema_version") == "earnings-v2"
              and p.get("v3_out_of_range", 1) == 0
              and not p.get("schema_reason"))
        put("earnings_schema_ok", ok,
            f"schema={p.get('schema_version')} 범위밖={p.get('v3_out_of_range')} "
            f"사유={p.get('schema_reason') or '-'}")

    # ③ DART primary — 전체 완료 **또는 명시적 결측**
    if not ea:
        put("dart_complete", False, _miss("earnings"))
    else:
        p = ea["payload"]
        targets = p.get("targets")
        done = p.get("dart_success")
        skipped = p.get("dart_skipped_explicit")
        accounted = (isinstance(targets, int) and isinstance(done, int)
                     and isinstance(skipped, int) and done + skipped == targets)
        put("dart_complete", accounted and not p.get("write_blocked"),
            f"대상={targets} 성공={done} 명시결측={skipped} "
            f"본표차단={p.get('write_blocked') or '-'}")

    # ④ V1·V2·V3 **각각의** 파싱 오류 0 (⑤)
    if not sc:
        put("no_silent_parse_error", False, _miss("scanner"))
    else:
        p = sc["payload"]
        errs = {f"{n}_parse_errors": p.get(f"{n}_parse_errors") for n in ("v1", "v2", "v3")}
        missing = [k for k, v in errs.items() if not isinstance(v, int)]
        if missing:
            put("no_silent_parse_error", False, f"telemetry 없음: {missing}")
        else:
            v3_oor = (ea or {}).get("payload", {}).get("v3_out_of_range", 0) if ea else 0
            put("no_silent_parse_error",
                all(v == 0 for v in errs.values()) and v3_oor == 0,
                f"{errs} v3_range_err={v3_oor}")

    # ⑤ store_written — 존재가 아니라 **일치와 유효 행수** (⑥)
    if not sc:
        put("store_written", False, _miss("scanner"))
    else:
        p = sc["payload"]
        fs, rp = p.get("feature_store") or {}, p.get("rank_pool") or {}
        ok = (fs.get("date") == cycle_date and rp.get("date") == cycle_date
              and fs.get("run_id") and fs.get("run_id") == sc.get("run_id")
              and fs.get("fingerprint") == fingerprint
              and rp.get("fingerprint") == fingerprint
              and isinstance(fs.get("rows"), int) and fs["rows"] >= MIN_STORE_ROWS
              and isinstance(rp.get("rows"), int) and rp["rows"] >= MIN_POOL_ROWS)
        put("store_written", ok, f"feature_store={fs} rank_pool={rp}")

    # ⑥ ledger_written — read-after-write (⑦)
    if not sc:
        put("ledger_written", False, _miss("scanner"))
    else:
        led = sc["payload"].get("ledger") or {}
        expected = led.get("expected_trade_ids")
        found = led.get("found_trade_ids")
        ok = (isinstance(expected, list) and isinstance(found, list)
              and len(expected) > 0 and set(expected) <= set(found))
        put("ledger_written", ok,
            f"기대 {len(expected) if isinstance(expected, list) else '?'}건 · "
            f"확인 {len(found) if isinstance(found, list) else '?'}건 · "
            f"누락 {sorted(set(expected) - set(found)) if ok is False and isinstance(expected, list) and isinstance(found, list) else '-'}")

    # ⑦ no_unexplained_failure — 기계 기준 (⑧)
    absent = [k for k in REQUIRED_KINDS if not got[k]]
    bad_state = [k for k in REQUIRED_KINDS
                 if got[k] and got[k]["payload"].get("expected_state") != "reached"]
    put("no_unexplained_failure",
        not absent and not bad_state and broken == 0,
        f"영수증없음={absent} 기대상태미달={bad_state} 깨진줄={broken}")

    aux = got["consensus"]
    detail = {
        "cycle_date": cycle_date,
        "fingerprint": fingerprint,
        "builder_version": BUILDER_VERSION,
        "receipts": len(rows),
        "broken_lines": broken,
        "reasons": why,
        # AUX 는 **판정에 넣지 않는다** — 보조 자료의 실패가 주 판정을 흐리지 않게
        "aux_state": (aux["payload"].get("state") if aux else "영수증없음"),
    }
    return ev, detail


def render(ev, detail):
    lines = [f"🧾 {BUILDER_VERSION} — {detail['cycle_date']} · 지문 {detail['fingerprint']}",
             f"   영수증 {detail['receipts']}건 · 깨진 줄 {detail['broken_lines']} · "
             f"AUX(컨센서스)={detail['aux_state']}  ← 판정 대상 아님", ""]
    for k, v in ev.items():
        lines.append(f"   {'✅' if v else '❌'} {k:<24} {detail['reasons'][k]}")
    return "\n".join(lines)


def _selftest():
    import tempfile
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    FP, DAY = "fp-test", "2026-09-18"

    def good_scanner():
        return {
            "ci_conclusion": "success", "expected_state": "reached",
            "v1_parse_errors": 0, "v2_parse_errors": 0, "v3_parse_errors": 0,
            "feature_store": {"date": DAY, "run_id": "R1", "fingerprint": FP, "rows": 718},
            "rank_pool": {"date": DAY, "fingerprint": FP, "rows": 20},
            "ledger": {"expected_trade_ids": ["t1", "t2"], "found_trade_ids": ["t1", "t2", "t0"]},
        }

    def good_earnings():
        return {"expected_state": "reached", "schema_version": "earnings-v2",
                "v3_out_of_range": 0, "schema_reason": "",
                "targets": 143, "dart_success": 140, "dart_skipped_explicit": 3,
                "write_blocked": ""}

    def seed(d, scanner=None, earnings=None, analyst=True, consensus=None):
        if scanner is not None:
            R.emit(DAY, "scanner", scanner, root=d, run_id="R1", fingerprint=FP)
        if analyst:
            R.emit(DAY, "analyst", {"expected_state": "reached"}, root=d,
                   run_id="R1", fingerprint=FP)
        if earnings is not None:
            R.emit(DAY, "earnings", earnings, root=d, run_id="R2", fingerprint=FP)
        if consensus is not None:
            R.emit(DAY, "consensus", consensus, root=d, run_id="R3", fingerprint=FP)

    print("🧪 Evidence Builder")
    with tempfile.TemporaryDirectory() as d:
        seed(d, good_scanner(), good_earnings(), consensus={"state": "DEGRADED"})
        ev, det = build(DAY, FP, root=d)
        chk("전부 갖추면 7개 모두 참", all(ev.values()), det["reasons"])
        chk("기준이 7개다", len(ev) == 7)
        chk("AUX 는 판정에 없다", "aux" not in " ".join(ev))
        chk("AUX 상태는 따로 기록된다", det["aux_state"] == "DEGRADED")
        chk("AUX 가 DEGRADED 여도 주 판정은 통과", all(ev.values()))

    with tempfile.TemporaryDirectory() as d:
        ev, det = build(DAY, FP, root=d)
        chk("영수증이 없으면 전부 거짓", not any(ev.values()))
        chk("그리고 사유가 붙는다", "영수증 없음" in det["reasons"]["ci_green"])
        chk("None 을 내지 않는다(모르는 것은 통과가 아니다)",
            all(isinstance(v, bool) for v in ev.values()))

    with tempfile.TemporaryDirectory() as d:          # ⑤ V1/V2/V3 각각
        p = good_scanner(); p["v2_parse_errors"] = 3
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d)
        chk("V2 만 오류여도 잡힌다", ev["no_silent_parse_error"] is False, det["reasons"]["no_silent_parse_error"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p.pop("v1_parse_errors")
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d)
        chk("V3 만 있고 V1 telemetry 가 없으면 참으로 만들지 않는다",
            ev["no_silent_parse_error"] is False and "telemetry 없음" in det["reasons"]["no_silent_parse_error"])

    with tempfile.TemporaryDirectory() as d:          # ⑥ store 심화
        p = good_scanner(); p["feature_store"]["fingerprint"] = "다른지문"
        seed(d, p, good_earnings())
        chk("지문이 다른 feature_store 는 인정하지 않는다",
            build(DAY, FP, root=d)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["feature_store"]["rows"] = 3
        seed(d, p, good_earnings())
        chk("행 수가 모자라면 인정하지 않는다",
            build(DAY, FP, root=d)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["feature_store"]["date"] = "2026-09-17"
        seed(d, p, good_earnings())
        chk("다른 거래일 파일은 인정하지 않는다",
            build(DAY, FP, root=d)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:          # ⑦ 원장 read-after-write
        p = good_scanner(); p["ledger"]["found_trade_ids"] = ["t1"]
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d)
        chk("기대한 trade_id 가 시트에 없으면 거짓", ev["ledger_written"] is False)
        chk("누락된 id 를 말한다", "t2" in det["reasons"]["ledger_written"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["ledger"] = {"expected_trade_ids": [], "found_trade_ids": []}
        seed(d, p, good_earnings())
        chk("기대가 0건이면 적재를 증명한 것이 아니다",
            build(DAY, FP, root=d)[0]["ledger_written"] is False)

    with tempfile.TemporaryDirectory() as d:          # ⑧ 기계 기준
        seed(d, good_scanner(), good_earnings(), analyst=False)
        ev, det = build(DAY, FP, root=d)
        chk("required 파이프라인이 빠지면 거짓", ev["no_unexplained_failure"] is False)
        chk("무엇이 빠졌는지 말한다", "analyst" in det["reasons"]["no_unexplained_failure"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["expected_state"] = "degraded"
        seed(d, p, good_earnings())
        chk("기대 상태에 도달 못하면 거짓",
            build(DAY, FP, root=d)[0]["no_unexplained_failure"] is False)

    with tempfile.TemporaryDirectory() as d:          # ③ DART
        e = good_earnings(); e["dart_skipped_explicit"] = 0     # 143 != 140+0
        seed(d, good_scanner(), e)
        chk("대상 수가 안 맞으면 거짓(결측이 명시되지 않았다)",
            build(DAY, FP, root=d)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:
        e = good_earnings(); e["write_blocked"] = "스키마 불일치"
        seed(d, good_scanner(), e)
        chk("본표 쓰기가 막혔으면 거짓", build(DAY, FP, root=d)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:          # ② 스키마
        e = good_earnings(); e["v3_out_of_range"] = 2
        seed(d, good_scanner(), e)
        ev = build(DAY, FP, root=d)[0]
        chk("범위밖이 있으면 스키마 기준이 거짓", ev["earnings_schema_ok"] is False)
        chk("그리고 파싱오류 기준도 같이 거짓", ev["no_silent_parse_error"] is False)

    with tempfile.TemporaryDirectory() as d:          # ① CI
        p = good_scanner(); p["ci_conclusion"] = "failure"
        seed(d, p, good_earnings())
        chk("CI 가 실패면 거짓", build(DAY, FP, root=d)[0]["ci_green"] is False)

    with tempfile.TemporaryDirectory() as d:          # 지문 격리
        seed(d, good_scanner(), good_earnings())
        chk("다른 지문으로 조회하면 증거가 없다", not any(build(DAY, "다른지문", root=d)[0].values()))

    with tempfile.TemporaryDirectory() as d:          # gate 와 실제로 물린다
        import stability_gate as G
        seed(d, good_scanner(), good_earnings())
        ev, _ = build(DAY, FP, root=d)
        chk("stability_gate 가 이 증거를 그대로 먹는다", G.evaluate(ev) == (True, []))
        chk("기준 이름이 gate 와 일치한다", set(ev) == set(G.KEYS))

    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
