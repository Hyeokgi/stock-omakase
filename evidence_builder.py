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
# P1-3 — 거래일마다 성공해야 하는 실제 워크플로
REQUIRED_WORKFLOWS = ("main.yml", "ai_report.yml", "earnings_collector.yml")

MIN_STORE_ROWS = 50          # 스캔 풀은 700대다. 50 미만이면 정상 수집이 아니다
MIN_POOL_ROWS = 1            # rank_pool 은 채널당 상위 N — 0 이면 기록이 안 된 것이다


def _miss(kind):
    return f"{kind} 영수증 없음"


def build(cycle_date, fingerprint, root=R.RECEIPT_DIR, workflow_states=None):
    """(evidence, detail) — stability_gate 가 먹는 7개 bool.

    workflow_states: {"main.yml": "success", ...} — P1-3.
    영수증의 `expected_state` 만 보면 영수증 **이후**의 실패를 못 본다
    (scanner 영수증 뒤에도 품질검사·git push 가 남아 있다). 실제 Actions 결론을
    같이 본다. 주지 않으면 그 자체로 `no_unexplained_failure` 가 거짓이다.
    """
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
        # 🔴 v3_unparsable 도 본다. scanner·analyst 는 20시 실적 갱신 **이전**의
        #    DB_실적을 읽으므로, 그날 새로 쓰인 행의 해석불가는 그쪽 telemetry 에
        #    잡히지 않는다. 수집기 영수증이 유일한 증거다.
        ok = (p.get("schema_version") == "earnings-v2"
              and p.get("v3_out_of_range", 1) == 0
              and p.get("v3_unparsable", 1) == 0
              and not p.get("schema_reason"))
        put("earnings_schema_ok", ok,
            f"schema={p.get('schema_version')} 범위밖={p.get('v3_out_of_range')} "
            f"해석불가={p.get('v3_unparsable')} 사유={p.get('schema_reason') or '-'}")

    # ③ DART primary — 사유별 계수로 본다 (P0-2)
    #    `targets - success` 를 결측으로 역산하면 항등식이 되어 아무것도 검증하지 못한다.
    if not ea:
        put("dart_complete", False, _miss("earnings"))
    else:
        p = ea["payload"]
        oc = p.get("outcomes")
        targets = p.get("targets")
        if not isinstance(oc, dict) or not isinstance(targets, int):
            put("dart_complete", False, "사유별 계수(outcomes)가 없다 — 역산은 인정하지 않는다")
        else:
            allowed = oc.get("success", 0) + oc.get("allowed_missing_corp_code", 0) \
                + oc.get("allowed_insufficient_data", 0)
            blockers = {k: oc.get(k, 0) for k in
                        ("hard_error", "circuit_breaker_unprocessed",
                         "time_budget_unprocessed")}
            ok = (allowed == targets
                  and all(v == 0 for v in blockers.values())
                  and p.get("unaccounted") == 0
                  and not p.get("write_blocked")
                  and p.get("target_source_health") == "ok")
            put("dart_complete", ok,
                f"대상={targets} 허용합={allowed} {blockers} "
                f"미분류={p.get('unaccounted')} 본표차단={p.get('write_blocked') or '-'} "
                f"입력={p.get('target_source_health')}")

    # ④ V1·V2·V3 **각각** — "계측되지 않음" 과 "실패 0" 을 가른다 (P0-5)
    #    이전에는 계측 지점이 없는 피처도 receipt 가 정수 0 을 만들어 참이 됐다.
    sources = [("scanner", sc), ("analyst", got["analyst"])]
    if not sc or not got["analyst"]:
        put("no_silent_parse_error", False,
            _miss("scanner") if not sc else _miss("analyst"))
    else:
        bad, detail_bits = [], []
        for name, rec in sources:
            tel = (rec["payload"] or {}).get("features")
            if not isinstance(tel, dict):
                bad.append(f"{name}: features 블록 없음")
                continue
            for feat in ("v1", "v2", "v3"):
                f = tel.get(feat)
                if not isinstance(f, dict) or f.get("measured") is not True:
                    bad.append(f"{name}.{feat}: 계측 안 됨")
                    continue
                errs = int(f.get("errors", 0) or 0)
                extra = int(f.get("unparsable", 0) or 0) + int(f.get("out_of_range", 0) or 0)
                if f.get("schema_reason"):
                    bad.append(f"{name}.{feat}: 스키마 {f['schema_reason']}")
                if errs or extra:
                    bad.append(f"{name}.{feat}: 오류 {errs}+{extra}")
                detail_bits.append(f"{name}.{feat}(e{errs}/x{extra})")
        put("no_silent_parse_error", not bad,
            (", ".join(bad) if bad else " ".join(detail_bits)))

    # ⑤ store_written — 존재가 아니라 **일치와 유효 행수** (⑥)
    if not sc:
        put("store_written", False, _miss("scanner"))
    else:
        p = sc["payload"]
        fs, rp = p.get("feature_store") or {}, p.get("rank_pool") or {}
        # 🔴 P0-4 — 문서가 "OBSERVE → cycle FAIL" 이라고 적었으면 코드도 그래야 한다.
        #    버린 행(dropped)이 있으면 연구 표본에서 종목이 사라진 것이다.
        ok = (fs.get("date") == cycle_date and rp.get("date") == cycle_date
              and fs.get("run_id") and fs.get("run_id") == sc.get("run_id")
              and fs.get("fingerprint") == fingerprint
              and rp.get("fingerprint") == fingerprint
              and isinstance(fs.get("rows"), int) and fs["rows"] >= MIN_STORE_ROWS
              and isinstance(rp.get("rows"), int) and rp["rows"] >= MIN_POOL_ROWS
              and fs.get("dropped") == 0
              and rp.get("total_dropped") == 0
              # 🔴 2026-09-19 — 영수증 날짜와 저장 날짜가 갈리면 증거가 아니다
              and sc["payload"].get("cycle_date_matches") is not False)
        put("store_written", ok,
            f"feature_store(행={fs.get('rows')} 버림={fs.get('dropped')}) "
            f"rank_pool(행={rp.get('rows')} 버림={rp.get('total_dropped')}) "
            f"날짜={fs.get('date')}/{rp.get('date')} "
            f"사이클일치={sc['payload'].get('cycle_date_matches')} 지문일치="
            f"{fs.get('fingerprint') == fingerprint and rp.get('fingerprint') == fingerprint}")

    # ⑥ ledger_written — read-after-write. **scanner 원장과 리포트 원장 둘 다** (P0-3)
    an = got["analyst"]
    if not sc or not an:
        put("ledger_written", False, _miss("scanner") if not sc else _miss("analyst"))
    else:
        bits, bad = [], []
        for name, rec in (("scanner", sc), ("report", an)):
            led = (rec["payload"] or {}).get("ledger") or {}
            expected, found = led.get("expected_trade_ids"), led.get("found_trade_ids")
            if not isinstance(expected, list) or not isinstance(found, list):
                bad.append(f"{name}: 원장 확인 기록 없음")
                continue
            missing = sorted(set(expected) - set(found))
            # 기대가 0건이면 "적재를 증명한 것" 이 아니다. 다만 그날 픽이 0 일 수 있으므로
            # 영수증이 그 사실을 명시(`expected_zero_reason`)했는지를 본다.
            if not expected and not led.get("expected_zero_reason"):
                bad.append(f"{name}: 기대 0건인데 사유가 없다")
            if missing:
                bad.append(f"{name}: 누락 {missing}")
            bits.append(f"{name}({len(found)}/{len(expected)})")
        put("ledger_written", not bad, ", ".join(bad) if bad else " ".join(bits))

    # ⑦ no_unexplained_failure — 기계 기준 (⑧)
    absent = [k for k in REQUIRED_KINDS if not got[k]]
    bad_state = [k for k in REQUIRED_KINDS
                 if got[k] and got[k]["payload"].get("expected_state") != "reached"]
    # analyst 는 **최종** 영수증만 인정한다 (P0-3)
    not_final = [k for k in ("analyst",)
                 if got[k] and got[k]["payload"].get("stage") != "final"]
    # P1-3 — 실제 Actions 결론
    if workflow_states is None:
        wf_bad = ["workflow 결론 미조회"]
    else:
        wf_bad = [f"{w}={c}" for w, c in sorted(workflow_states.items())
                  if c != "success"]
        missing_wf = [w for w in REQUIRED_WORKFLOWS if w not in workflow_states]
        wf_bad += [f"{w}=결론없음" for w in missing_wf]
    put("no_unexplained_failure",
        not absent and not bad_state and not not_final and not wf_bad and broken == 0,
        f"영수증없음={absent} 기대상태미달={bad_state} 최종아님={not_final} "
        f"workflow={wf_bad or 'ok'} 깨진파일={broken}")

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
        "workflow_states": dict(workflow_states or {}),
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

    FEATS = {"v1": {"measured": True, "parsed": 700, "errors": 0},
             "v2": {"measured": True, "parsed": 700, "errors": 0},
             "v3": {"measured": True, "used": 140, "errors": 0,
                    "unparsable": 0, "out_of_range": 0, "schema_reason": ""}}
    WF = {"main.yml": "success", "ai_report.yml": "success",
          "earnings_collector.yml": "success"}

    def good_scanner():
        import copy
        return {
            "ci_conclusion": "success", "expected_state": "reached",
            "features": copy.deepcopy(FEATS),
            "feature_store": {"date": DAY, "run_id": "R1", "fingerprint": FP,
                              "rows": 718, "dropped": 0},
            "rank_pool": {"date": DAY, "fingerprint": FP, "rows": 20,
                          "total_dropped": 0},
            "ledger": {"expected_trade_ids": ["t1", "t2"],
                       "found_trade_ids": ["t1", "t2", "t0"]},
        }

    def good_analyst():
        import copy
        return {"expected_state": "reached", "stage": "final",
                "features": copy.deepcopy(FEATS),
                "ledger": {"expected_trade_ids": ["r1"], "found_trade_ids": ["r1"]}}

    def good_earnings():
        return {"expected_state": "reached", "schema_version": "earnings-v2",
                "v3_out_of_range": 0, "v3_unparsable": 0, "schema_reason": "",
                "targets": 143, "dart_success": 140,
                "outcomes": {"success": 140, "allowed_missing_corp_code": 2,
                             "allowed_insufficient_data": 1, "hard_error": 0,
                             "circuit_breaker_unprocessed": 0,
                             "time_budget_unprocessed": 0},
                "unaccounted": 0, "target_source_health": "ok",
                "write_blocked": ""}

    def seed(d, scanner=None, earnings=None, analyst=True, consensus=None):
        if scanner is not None:
            R.emit(DAY, "scanner", scanner, root=d, run_id="R1", fingerprint=FP)
        if analyst:
            R.emit(DAY, "analyst", good_analyst() if analyst is True else analyst,
                   root=d, run_id="R1", fingerprint=FP)
        if earnings is not None:
            R.emit(DAY, "earnings", earnings, root=d, run_id="R2", fingerprint=FP)
        if consensus is not None:
            R.emit(DAY, "consensus", consensus, root=d, run_id="R3", fingerprint=FP)

    print("🧪 Evidence Builder")
    with tempfile.TemporaryDirectory() as d:
        seed(d, good_scanner(), good_earnings(), consensus={"state": "DEGRADED"})
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("전부 갖추면 7개 모두 참", all(ev.values()), det["reasons"])
        chk("기준이 7개다", len(ev) == 7)
        chk("AUX 는 판정에 없다", "aux" not in " ".join(ev))
        chk("AUX 상태는 따로 기록된다", det["aux_state"] == "DEGRADED")
        chk("AUX 가 DEGRADED 여도 주 판정은 통과", all(ev.values()))

    with tempfile.TemporaryDirectory() as d:
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("영수증이 없으면 전부 거짓", not any(ev.values()))
        chk("그리고 사유가 붙는다", "영수증 없음" in det["reasons"]["ci_green"])
        chk("None 을 내지 않는다(모르는 것은 통과가 아니다)",
            all(isinstance(v, bool) for v in ev.values()))

    with tempfile.TemporaryDirectory() as d:          # ⑤ V1/V2/V3 각각
        p = good_scanner(); p["features"]["v2"]["errors"] = 3
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("V2 만 오류여도 잡힌다", ev["no_silent_parse_error"] is False, det["reasons"]["no_silent_parse_error"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["features"]["v1"]["measured"] = False
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("계측되지 않은 피처는 '실패 0' 이 아니다 (P0-5)",
            ev["no_silent_parse_error"] is False
            and "계측 안 됨" in det["reasons"]["no_silent_parse_error"])

    with tempfile.TemporaryDirectory() as d:          # ⑥ store 심화
        p = good_scanner(); p["feature_store"]["fingerprint"] = "다른지문"
        seed(d, p, good_earnings())
        chk("지문이 다른 feature_store 는 인정하지 않는다",
            build(DAY, FP, root=d, workflow_states=WF)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["feature_store"]["rows"] = 3
        seed(d, p, good_earnings())
        chk("행 수가 모자라면 인정하지 않는다",
            build(DAY, FP, root=d, workflow_states=WF)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["feature_store"]["date"] = "2026-09-17"
        seed(d, p, good_earnings())
        chk("다른 거래일 파일은 인정하지 않는다",
            build(DAY, FP, root=d, workflow_states=WF)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:          # ⑦ 원장 read-after-write
        p = good_scanner(); p["ledger"]["found_trade_ids"] = ["t1"]
        seed(d, p, good_earnings())
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("기대한 trade_id 가 시트에 없으면 거짓", ev["ledger_written"] is False)
        chk("누락된 id 를 말한다", "t2" in det["reasons"]["ledger_written"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["ledger"] = {"expected_trade_ids": [], "found_trade_ids": []}
        seed(d, p, good_earnings())
        chk("기대가 0건이면 적재를 증명한 것이 아니다",
            build(DAY, FP, root=d, workflow_states=WF)[0]["ledger_written"] is False)

    with tempfile.TemporaryDirectory() as d:          # ⑧ 기계 기준
        seed(d, good_scanner(), good_earnings(), analyst=False)
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("required 파이프라인이 빠지면 거짓", ev["no_unexplained_failure"] is False)
        chk("무엇이 빠졌는지 말한다", "analyst" in det["reasons"]["no_unexplained_failure"])

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["expected_state"] = "degraded"
        seed(d, p, good_earnings())
        chk("기대 상태에 도달 못하면 거짓",
            build(DAY, FP, root=d, workflow_states=WF)[0]["no_unexplained_failure"] is False)

    with tempfile.TemporaryDirectory() as d:          # ③ DART
        e = good_earnings(); e["outcomes"]["allowed_missing_corp_code"] = 0  # 합이 안 맞는다
        seed(d, good_scanner(), e)
        chk("사유별 합이 대상과 다르면 거짓 (P0-2 — 역산이 아니다)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:
        e = good_earnings(); e["write_blocked"] = "스키마 불일치"
        seed(d, good_scanner(), e)
        chk("본표 쓰기가 막혔으면 거짓", build(DAY, FP, root=d, workflow_states=WF)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:          # ② 스키마
        e = good_earnings(); e["v3_out_of_range"] = 2
        sc2 = good_scanner(); sc2["features"]["v3"]["out_of_range"] = 2
        seed(d, sc2, e)
        ev = build(DAY, FP, root=d, workflow_states=WF)[0]
        chk("범위밖이 있으면 스키마 기준이 거짓", ev["earnings_schema_ok"] is False)
        chk("그리고 파싱오류 기준도 같이 거짓 (P0-5)", ev["no_silent_parse_error"] is False)

    with tempfile.TemporaryDirectory() as d:
        sc3 = good_scanner(); sc3["features"]["v3"]["unparsable"] = 1
        seed(d, sc3, good_earnings())
        e4 = good_earnings(); e4["v3_unparsable"] = 3
        seed(d, good_scanner(), e4)
        chk("수집기의 v3_unparsable 도 스키마 기준이다",
            build(DAY, FP, root=d, workflow_states=WF)[0]["earnings_schema_ok"] is False)

    with tempfile.TemporaryDirectory() as d:
        sc3 = good_scanner(); sc3["features"]["v3"]["unparsable"] = 1
        seed(d, sc3, good_earnings())
        chk("scanner 의 v3_unparsable 도 Gate 기준이다 (P0-5)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["no_silent_parse_error"] is False)

    with tempfile.TemporaryDirectory() as d:          # ① CI
        p = good_scanner(); p["ci_conclusion"] = "failure"
        seed(d, p, good_earnings())
        chk("CI 가 실패면 거짓", build(DAY, FP, root=d, workflow_states=WF)[0]["ci_green"] is False)

    with tempfile.TemporaryDirectory() as d:          # P0-2 항등식 회귀
        e = good_earnings(); e.pop("outcomes")
        seed(d, good_scanner(), e)
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("outcomes 가 없으면 역산을 인정하지 않는다 (P0-2)",
            ev["dart_complete"] is False and "역산" in det["reasons"]["dart_complete"])

    with tempfile.TemporaryDirectory() as d:
        e = good_earnings(); e["outcomes"]["hard_error"] = 1
        e["outcomes"]["allowed_insufficient_data"] = 0
        seed(d, good_scanner(), e)
        chk("hard_error 는 허용되는 결측이 아니다 (P0-2)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:
        e = good_earnings(); e["outcomes"]["time_budget_unprocessed"] = 16
        seed(d, good_scanner(), e)
        chk("시간예산 초과분이 있으면 거짓 (P0-2)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:
        e = good_earnings(); e["target_source_health"] = "target 수 비정상(3)"
        seed(d, good_scanner(), e)
        chk("입력 시트가 줄었으면 거짓 (P0-2)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["dart_complete"] is False)

    with tempfile.TemporaryDirectory() as d:          # P0-4 drop
        p = good_scanner(); p["feature_store"]["dropped"] = 2
        seed(d, p, good_earnings())
        chk("feature_store 가 행을 버렸으면 store_written 이 거짓 (P0-4)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:
        p = good_scanner(); p["rank_pool"]["total_dropped"] = 1
        seed(d, p, good_earnings())
        chk("rank_pool 이 행을 버렸으면 거짓 (P0-4)",
            build(DAY, FP, root=d, workflow_states=WF)[0]["store_written"] is False)

    with tempfile.TemporaryDirectory() as d:          # P0-3 리포트 원장
        seed(d, good_scanner(), good_earnings(),
             analyst={**good_analyst(),
                      "ledger": {"expected_trade_ids": ["r1"], "found_trade_ids": []}})
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("리포트 원장 누락도 ledger_written 을 떨어뜨린다 (P0-3)",
            ev["ledger_written"] is False and "report" in det["reasons"]["ledger_written"])

    with tempfile.TemporaryDirectory() as d:
        seed(d, good_scanner(), good_earnings(),
             analyst={**good_analyst(), "stage": "start"})
        ev, det = build(DAY, FP, root=d, workflow_states=WF)
        chk("analyst 중간 영수증은 인정하지 않는다 (P0-3)",
            ev["no_unexplained_failure"] is False
            and "최종아님" in det["reasons"]["no_unexplained_failure"])

    with tempfile.TemporaryDirectory() as d:          # P1-3 실제 workflow 결론
        seed(d, good_scanner(), good_earnings())
        chk("workflow 결론을 안 주면 거짓 (P1-3)",
            build(DAY, FP, root=d)[0]["no_unexplained_failure"] is False)
        chk("workflow 하나가 실패면 거짓 (P1-3)",
            build(DAY, FP, root=d,
                  workflow_states={**WF, "ai_report.yml": "failure"})[0]["no_unexplained_failure"] is False)
        chk("required workflow 결론이 빠져도 거짓 (P1-3)",
            build(DAY, FP, root=d,
                  workflow_states={"main.yml": "success"})[0]["no_unexplained_failure"] is False)

    with tempfile.TemporaryDirectory() as d:          # 지문 격리
        seed(d, good_scanner(), good_earnings())
        chk("다른 지문으로 조회하면 증거가 없다", not any(build(DAY, "다른지문", root=d, workflow_states=WF)[0].values()))

    with tempfile.TemporaryDirectory() as d:          # gate 와 실제로 물린다
        import stability_gate as G
        seed(d, good_scanner(), good_earnings())
        ev, _ = build(DAY, FP, root=d, workflow_states=WF)
        chk("stability_gate 가 이 증거를 그대로 먹는다", G.evaluate(ev) == (True, []))
        chk("기준 이름이 gate 와 일치한다", set(ev) == set(G.KEYS))

    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
