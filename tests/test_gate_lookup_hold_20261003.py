"""2026-10-03 — 워크플로 결론 조회가 일시 오류로 끝내 확정되지 않으면 **기록을 보류**한다.

10/2 에는 외부 창구의 일시 오류 하나가 그날을 FAIL 로 박제했다. runs.csv 는 append-only 이고 같은 거래일을
두 번 기록하지 않으므로 고칠 길이 없었다. 모르는 것은 FAIL 도 PASS 도 아니다.

지킬 것
  · 불확정이 있고 확정된 실패가 없으면 → 기록하지 않는다(runs.csv 불변). PASS 로도 세지 않는다.
  · 확정된 실패가 이미 있으면 → 보류할 이유가 없다. FAIL 을 기록하고 불확정이었던 것은 note 에 남긴다.
  · 비거래일 → 조회와 무관하게 SKIP 이다.
  · 보류 뒤 다음 실행에서 결론을 얻으면 정상 기록된다(두 번째 슬롯이 하는 일).
  · CLI 는 보류일 때만 종료 코드 75 로 끝나 잡을 빨갛게 한다.
  · 보류가 연속 기록을 몰래 잇지 않는다(빠진 평일은 연속을 끊는다).
"""
import contextlib
import datetime
import io
import pathlib
import tempfile
import unittest
from unittest import mock

import stability_gate as G

ROOT = pathlib.Path(__file__).resolve().parents[1]


class LookupHoldTests(unittest.TestCase):
    FP = "hold00000000"
    WF = {"main.yml": "success", "ai_report.yml": "success", "earnings_collector.yml": "success"}
    DAY = "2026-09-21"                      # 월요일 — 거래일

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.rec = str(pathlib.Path(self.tmp.name) / "receipts")
        self.log = str(pathlib.Path(self.tmp.name) / "runs.csv")
        self.seed(self.DAY)

    def feats(self):
        return {"v1": {"measured": True, "parsed": 700, "errors": 0},
                "v2": {"measured": True, "parsed": 700, "errors": 0},
                "v3": {"measured": True, "used": 140, "errors": 0,
                       "unparsable": 0, "out_of_range": 0, "schema_reason": ""}}

    def seed(self, day):
        import production_receipt as R
        sc = {"expected_state": "reached", "ci_conclusion": "success", "cycle_date_matches": True,
              "features": self.feats(),
              "feature_store": {"date": day, "run_id": "R1", "fingerprint": self.FP,
                                "rows": 718, "dropped": 0},
              "rank_pool": {"date": day, "fingerprint": self.FP, "rows": 8, "total_dropped": 0},
              "ledger": {"expected_trade_ids": ["t1"], "found_trade_ids": ["t1"]}}
        an = {"expected_state": "reached", "stage": "final", "features": self.feats(),
              "ledger": {"expected_trade_ids": ["r1"], "found_trade_ids": ["r1"]}}
        ea = {"expected_state": "reached", "schema_version": "earnings-v2",
              "v3_out_of_range": 0, "v3_unparsable": 0, "schema_reason": "",
              "targets": 143, "dart_success": 143,
              "outcomes": {"success": 143, "allowed_missing_corp_code": 0,
                           "allowed_insufficient_data": 0, "hard_error": 0,
                           "circuit_breaker_unprocessed": 0, "time_budget_unprocessed": 0},
              "unaccounted": 0, "target_source_health": "ok", "write_blocked": ""}
        R.emit(day, "scanner", sc, root=self.rec, run_id="R1", fingerprint=self.FP)
        R.emit(day, "analyst", an, root=self.rec, run_id="R1", fingerprint=self.FP)
        R.emit(day, "earnings", ea, root=self.rec, run_id="R2", fingerprint=self.FP)
        R.emit(day, "consensus", {"state": "DEGRADED", "expected_state": "reached"},
               root=self.rec, run_id="R3", fingerprint=self.FP)

    def record(self, day=None, **kw):
        kw.setdefault("workflow_states", self.WF)
        with contextlib.redirect_stdout(io.StringIO()):
            return G.record_cycle(day or self.DAY, path=self.log, receipts_root=self.rec,
                                  fp=self.FP, **kw)

    # ── 핵심: 보류 ────────────────────────────────────────────────────
    def test_inconclusive_lookup_holds_instead_of_recording_fail(self):
        resolved = {k: v for k, v in self.WF.items() if k != "ai_report.yml"}
        done, why = self.record(workflow_states=resolved, workflow_pending=["ai_report.yml"])
        self.assertFalse(done)
        self.assertTrue(why.startswith(G.LOOKUP_HOLD_TAG), why)
        self.assertIn("ai_report.yml", why)
        self.assertEqual(G.load(self.log), [], "보류인데 runs.csv 에 무언가 남았다")

    def test_a_hold_is_never_counted_as_a_pass(self):
        self.record(workflow_pending=["main.yml"], workflow_states={})
        self.assertEqual(G.streak(path=self.log, fp=self.FP), 0)

    def test_without_pending_the_same_inputs_still_pass(self):
        """대조군 — 보류 장치가 정상 기록을 막지 않는다."""
        done, why = self.record()
        self.assertTrue(done, why)
        self.assertEqual(G.load(self.log)[0]["verdict"], "PASS")

    def test_empty_or_none_pending_changes_nothing(self):
        for pending in (None, [], ()):
            with self.subTest(pending=pending):
                self.setUp()
                done, _ = self.record(workflow_pending=pending)
                self.assertTrue(done)
                self.assertEqual(G.load(self.log)[0]["verdict"], "PASS")

    def test_second_slot_records_normally_after_a_hold(self):
        """08:30 슬롯이 하는 일 — 보류했던 같은 거래일에 이번엔 결론을 얻었다."""
        self.record(workflow_pending=["main.yml"], workflow_states={})
        self.assertEqual(G.load(self.log), [])
        done, why = self.record()                       # 결론을 얻은 두 번째 시도
        self.assertTrue(done, why)
        rows = G.load(self.log)
        self.assertEqual((len(rows), rows[0]["cycle_date"], rows[0]["verdict"]),
                         (1, self.DAY, "PASS"))

    # ── 확정된 실패가 있으면 보류하지 않는다 ───────────────────────────
    def test_a_decided_failure_is_recorded_even_if_another_lookup_is_pending(self):
        states = {"main.yml": "failure", "earnings_collector.yml": "success"}
        done, why = self.record(workflow_states=states, workflow_pending=["ai_report.yml"])
        self.assertTrue(done, "이미 FAIL 이 확정된 날을 보류할 이유가 없다")
        row = G.load(self.log)[0]
        self.assertEqual(row["verdict"], "FAIL")
        self.assertIn("조회불확정=['ai_report.yml']", row["note"])
        self.assertIn("main.yml=failure", row["note"])

    # ── 비거래일·역순은 그대로 ────────────────────────────────────────
    def test_non_trading_day_is_skipped_regardless_of_lookup(self):
        done, _ = self.record(day="2026-09-19", workflow_pending=["main.yml"], workflow_states={})
        self.assertTrue(done)
        self.assertEqual(G.load(self.log)[0]["verdict"], "SKIP")

    def test_receipts_not_ready_still_hold_with_their_own_reason(self):
        done, why = self.record(day="2026-09-22", workflow_pending=["main.yml"],
                                now=datetime.datetime(2026, 9, 22, 15, 0, tzinfo=datetime.timezone(
                                    datetime.timedelta(hours=9))))
        self.assertFalse(done)
        self.assertNotIn(G.LOOKUP_HOLD_TAG, why)        # 영수증 보류가 먼저 말한다
        self.assertEqual(G.load(self.log), [])

    # ── 보류가 연속을 몰래 잇지 않는다 ────────────────────────────────
    def test_an_unrecorded_weekday_breaks_the_streak(self):
        """D 를 보류한 채 D+1 이 PASS 로 기록돼도 D-1 과 D+1 이 이어지지 않는다."""
        for d in ("2026-09-18", "2026-09-22"):         # 9/21 은 건너뛴다(보류)
            self.seed(d)
            done, why = self.record(day=d)
            self.assertTrue(done, why)
        self.assertEqual(G.streak(path=self.log, fp=self.FP), 1)


class CliExitCodeTests(unittest.TestCase):
    def run_cli(self, argv, record_result):
        out = io.StringIO()
        with mock.patch.object(G, "record_cycle", return_value=record_result) as rc, \
                mock.patch.object(G, "report", return_value="REPORT"), \
                contextlib.redirect_stdout(out):
            code = G._cli_record(argv)
        return code, rc, out.getvalue()

    def test_hold_exits_75_and_annotates_an_error(self):
        why = f"{G.LOOKUP_HOLD_TAG} — ['main.yml'] 실행 결론을 확정하지 못했다"
        code, rc, out = self.run_cli(["x", "--record", "2026-09-28", "--workflows", "{}",
                                      "--workflows-pending", '["main.yml"]'], (False, why))
        self.assertEqual(code, G.HOLD_EXIT_CODE)
        self.assertEqual(G.HOLD_EXIT_CODE, 75)
        self.assertIn("::error::", out)
        rc.assert_called_once_with("2026-09-28", workflow_states={}, workflow_pending=["main.yml"])

    def test_normal_outcomes_exit_zero(self):
        for result in ((True, "통과"), (False, "이미 기록된 거래일 2026-09-28 — 같은 날을 두 번 세지 않는다"),
                       (False, "아직 ['earnings'] 영수증이 없고 cutoff(23시 KST) 전이다 — 보류한다")):
            with self.subTest(result=result):
                code, _, out = self.run_cli(["x", "--record", "2026-09-28"], result)
                self.assertEqual(code, 0)
                self.assertNotIn("::error::", out)

    def test_missing_pending_argument_means_none(self):
        _, rc, _ = self.run_cli(["x", "--record", "2026-09-28", "--workflows", '{"a": "success"}'],
                                (True, "통과"))
        rc.assert_called_once_with("2026-09-28", workflow_states={"a": "success"},
                                   workflow_pending=None)

    def test_blank_pending_argument_means_empty(self):
        """워크플로가 환경 변수를 못 받아 빈 문자열이 와도 죽지 않는다."""
        _, rc, _ = self.run_cli(["x", "--record", "2026-09-28", "--workflows-pending", ""],
                                (True, "통과"))
        self.assertEqual(rc.call_args.kwargs["workflow_pending"], [])

    def test_record_without_a_date_is_a_usage_error(self):
        code, rc, _ = self.run_cli(["x", "--record"], (True, ""))
        self.assertEqual(code, 2)
        rc.assert_not_called()

    def test_workflows_option_is_not_confused_with_workflows_pending(self):
        """`--workflows` 는 정확히 일치하는 인자만 본다 — `--workflows-pending` 을 JSON 으로 읽지 않는다."""
        _, rc, _ = self.run_cli(["x", "--record", "2026-09-28", "--workflows-pending", '["a"]'],
                                (True, ""))
        self.assertIsNone(rc.call_args.kwargs["workflow_states"])


if __name__ == "__main__":
    unittest.main()
