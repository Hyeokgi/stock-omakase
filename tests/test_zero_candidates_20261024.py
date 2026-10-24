# 급락장 '후보 0' 판정(허브 #20) 시험 — 두 채널 후보가 모두 0 이고 사유가 있을 때만 순위 풀 0행을 정상으로 받는지 확인한다
"""2026-10-24 허브 #20 — docs/collaboration/2026-10-16_Claude_10-14·15판정FAIL_순위풀0.md 안 A.

10/14·10/15 는 스캐너 후보(candidate_pool)가 정말 0 이라 순위 풀도 0행이었는데, 판정기가 그 0 을
'기록 실패' 로 읽어 store_written · no_unexplained_failure 가 떨어졌다.
"""
import ast
import pathlib
import tempfile
import unittest

import evidence_builder as E
import production_receipt as R

ROOT = pathlib.Path(__file__).resolve().parent.parent
DAY, FP = "2026-10-14", "fp"
WF = {"main.yml": "success", "ai_report.yml": "success", "earnings_collector.yml": "success"}
FEATS = {"v1": {"measured": True, "parsed": 0, "errors": 0}, "v2": {"measured": True, "parsed": 0, "errors": 0},
         "v3": {"measured": True, "used": 0, "errors": 0, "unparsable": 0, "out_of_range": 0, "schema_reason": ""}}


def omakase_fragment():
    """omakase.py 의 `_pool_cands` · `_pool_zero_ok` 계산 두 줄을 그대로 꺼낸다."""
    tree = ast.parse((ROOT / "omakase.py").read_text(encoding="utf-8"))
    want = {"_pool_cands", "_pool_zero_ok"}
    found = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
             and any(getattr(t, "id", "") in want for t in n.targets)]
    assert {t.id for n in found for t in n.targets} == want, "계산 줄을 찾지 못했다 — 시험을 갱신하라"
    code = compile(ast.Module(body=sorted(found, key=lambda n: n.lineno), type_ignores=[]), "omakase.py", "exec")

    def run(plan):
        ns = {"_pool_plan": plan}
        exec(code, ns)
        return ns["_pool_cands"], ns["_pool_zero_ok"]
    return run


def scanner(rows, cands, reason):
    return {"cycle_date_matches": True, "ci_conclusion": "success",
            "expected_state": "reached" if (rows or reason) else "degraded", "features": FEATS,
            "feature_store": {"date": DAY, "run_id": "R1", "fingerprint": FP, "rows": 750, "dropped": 0},
            "rank_pool": {"date": DAY, "fingerprint": FP, "rows": rows, "candidates": cands,
                          "expected_zero_reason": reason, "total_dropped": 0},
            "ledger": {"expected_trade_ids": [], "found_trade_ids": [], "expected_zero_reason": "신규 진입 행 없음"}}


def build(payload):
    with tempfile.TemporaryDirectory() as d:
        R.emit(DAY, "scanner", payload, root=d, run_id="R1", fingerprint=FP)
        R.emit(DAY, "analyst", {"expected_state": "reached", "stage": "final", "features": FEATS,
                                "ledger": {"expected_trade_ids": ["r1"], "found_trade_ids": ["r1"]}},
               root=d, run_id="R1", fingerprint=FP)
        R.emit(DAY, "earnings", {"expected_state": "reached", "schema_version": "earnings-v2",
                                 "v3_out_of_range": 0, "v3_unparsable": 0, "schema_reason": "",
                                 "targets": 10, "dart_success": 10,
                                 "outcomes": {"success": 10, "allowed_missing_corp_code": 0,
                                              "allowed_insufficient_data": 0, "hard_error": 0,
                                              "circuit_breaker_unprocessed": 0, "time_budget_unprocessed": 0},
                                 "unaccounted": 0, "target_source_health": "ok", "write_blocked": ""},
               root=d, run_id="R2", fingerprint=FP)
        return E.build(DAY, FP, root=d, workflow_states=WF)


class ScannerSide(unittest.TestCase):
    def setUp(self):
        self.run = omakase_fragment()

    def test_both_channels_empty_is_zero_ok(self):
        self.assertEqual(self.run([("차트TOP2", [], 29, None, "", None), ("수급TOP2", [], 31, None, "", None)]),
                         ({"차트TOP2": 0, "수급TOP2": 0}, True))

    def test_any_candidate_or_no_plan_is_not_zero_ok(self):
        self.assertFalse(self.run([("차트TOP2", [1, 2], 29, None, "", None), ("수급TOP2", [], 31, None, "", None)])[1])
        self.assertEqual(self.run([]), ({}, False))

    def test_receipt_carries_counts_reason_and_state(self):
        src = (ROOT / "omakase.py").read_text(encoding="utf-8")
        self.assertIn('"candidates": _pool_cands', src)
        self.assertIn('"expected_zero_reason": ("후보 없음" if _pool_zero_ok else "")', src)
        self.assertIn("(_pool_rows or _pool_zero_ok)", src)


class GateSide(unittest.TestCase):
    def test_1014_case_now_passes(self):
        ev, det = build(scanner(0, {"차트TOP2": 0, "수급TOP2": 0}, "후보 없음"))
        self.assertTrue(ev["store_written"], det["reasons"]["store_written"])
        self.assertTrue(ev["no_unexplained_failure"], det["reasons"]["no_unexplained_failure"])
        self.assertIn("사유=후보 없음", det["reasons"]["store_written"])

    def test_zero_rows_with_candidates_still_fails(self):
        ev, _ = build(scanner(0, {"차트TOP2": 4, "수급TOP2": 1}, ""))
        self.assertFalse(ev["store_written"])
        self.assertFalse(ev["no_unexplained_failure"])

    def test_pool_rows_ok_edges(self):
        ok = E.pool_rows_ok
        self.assertTrue(ok({"rows": 3}))
        self.assertTrue(ok({"rows": 0, "candidates": {"차트TOP2": 0, "수급TOP2": 0}, "expected_zero_reason": "후보 없음"}))
        self.assertFalse(ok({"rows": 0, "candidates": {"차트TOP2": 0, "수급TOP2": 0}, "expected_zero_reason": " "}))
        self.assertFalse(ok({"rows": 0, "candidates": {"차트TOP2": False, "수급TOP2": 0}, "expected_zero_reason": "x"}))
        self.assertFalse(ok({"rows": True}))
        self.assertFalse(ok({"rows": 0}))
        # 2026-10-25 Codex 검토(P3) — 형식이 정확해야 한다
        Z = {"candidates": {"차트TOP2": 0, "수급TOP2": 0}, "expected_zero_reason": "후보 없음"}
        self.assertTrue(ok(dict(Z, rows=0)))
        self.assertFalse(ok(dict(Z, rows=False)))
        self.assertFalse(ok(dict(Z, rows=0.0)))
        self.assertFalse(ok(dict(Z, rows=0, expected_zero_reason=7)))
        self.assertFalse(ok(dict(Z, rows=0, candidates={"차트TOP2": 0.0, "수급TOP2": 0})))


if __name__ == "__main__":
    unittest.main()
