# -*- coding: utf-8 -*-
"""
안정화 종료선 — 2026-09-18 GPT 제안 §7.

지금 가장 큰 위험은 오류 자체가 아니라, 오류를 찾을 때마다 새 감사 도구가 생기고
그 도구가 또 오류를 찾으면서 **본래 목적(수익 조합 발견)이 계속 뒤로 밀리는 것**이다.

그래서 선을 긋되 말이 아니라 기록으로 긋는다. 이 파일은 그 선이
"이제 된 것 같다" 로 넘어가지 못하게 못을 박는다.
"""
import pathlib
import tempfile
import unittest

import stability_gate as G

ROOT = pathlib.Path(__file__).resolve().parent.parent


class CriteriaTests(unittest.TestCase):
    def test_seven_criteria_in_fixed_order(self):
        """순서를 바꾸면 이미 기록된 행과 대조가 안 된다."""
        self.assertEqual(len(G.CRITERIA), 7)
        self.assertEqual(G.KEYS[0], "ci_green")
        self.assertEqual(G.KEYS[-1], "no_unexplained_failure")

    def test_missing_evidence_is_not_a_pass(self):
        """'확인 못 했다' 를 '괜찮다' 로 바꾸는 순간 이 문턱은 의미가 없다."""
        with self.assertRaises(G.EvidenceMissing):
            G.evaluate({k: True for k in G.KEYS[:-1]})

    def test_none_is_not_a_pass(self):
        with self.assertRaises(G.EvidenceMissing):
            G.evaluate({**{k: True for k in G.KEYS}, "ci_green": None})

    def test_truthy_but_not_true_is_not_a_pass(self):
        """'Y' 나 1 을 통과로 받으면 증거가 흐려진다."""
        ok, bad = G.evaluate({**{k: True for k in G.KEYS}, "ci_green": "Y"})
        self.assertFalse(ok)
        self.assertEqual(bad, ["ci_green"])

    def test_one_failure_fails_the_run(self):
        ok, bad = G.evaluate({**{k: True for k in G.KEYS}, "dart_complete": False})
        self.assertFalse(ok)
        self.assertEqual(bad, ["dart_complete"])


class CycleTests(unittest.TestCase):
    """③ 안정화 1회 = 개별 workflow run 이 아니라 **거래일 end-to-end 사이클**."""

    FP = "aaaaaaaaaaaa"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name) / "runs.csv")
        self.good = {k: True for k in G.KEYS}
        self.bad = {**self.good, "store_written": False}

    def tearDown(self):
        self.tmp.cleanup()

    def add(self, day, evidence, run_id=None, fp=None):
        return G.record(day, run_id or f"run-{day}", "cycle", evidence,
                        path=self.path, fp=fp or self.FP)

    def test_three_consecutive_trading_days_open_the_gate(self):
        for day in ("2026-09-16", "2026-09-17", "2026-09-18"):
            self.add(day, self.good)
        self.assertTrue(G.state(path=self.path, fp=self.FP)[0])

    def test_same_trading_day_counted_once(self):
        """하루에 스캐너가 144회 돈다 — run 단위로 세면 하루에 문턱을 넘을 수 있다."""
        self.add("2026-09-18", self.good)
        done, why = self.add("2026-09-18", self.good, run_id="다른런")
        self.assertFalse(done)
        self.assertIn("이미", why)
        self.assertEqual(G.streak(path=self.path, fp=self.FP), 1)

    def test_non_trading_day_is_skip(self):
        self.add("2026-09-19", {})          # 토요일
        self.assertEqual(G.load(self.path)[-1]["verdict"], G.SKIP)

    def test_skip_neither_extends_nor_breaks(self):
        self.add("2026-09-17", self.good)
        self.add("2026-09-18", self.good)
        n = G.streak(path=self.path, fp=self.FP)
        self.add("2026-09-19", {})          # 토
        self.add("2026-09-20", {})          # 일
        self.assertEqual(G.streak(path=self.path, fp=self.FP), n)
        self.add("2026-09-21", self.good)   # 월
        self.assertEqual(G.streak(path=self.path, fp=self.FP), n + 1)

    def test_skip_does_not_need_evidence(self):
        """비거래일에 증거를 요구하면 주말마다 EvidenceMissing 이 난다."""
        done, why = self.add("2026-09-19", {})
        self.assertTrue(done)
        self.assertIn("SKIP", why)

    def test_a_failure_still_resets(self):
        for day in ("2026-09-16", "2026-09-17"):
            self.add(day, self.good)
        self.add("2026-09-18", self.bad)
        self.assertEqual(G.streak(path=self.path, fp=self.FP), 0)

    def test_cycle_date_is_required(self):
        with self.assertRaises(ValueError):
            G.record("", "r", "cycle", self.good, path=self.path)

    def test_run_id_is_still_required(self):
        with self.assertRaises(ValueError):
            G.record("2026-09-18", "", "cycle", self.good, path=self.path)

    def test_every_criterion_recorded(self):
        self.add("2026-09-18", self.bad)
        row = G.load(self.path)[0]
        self.assertEqual(row["store_written"], "N")
        self.assertEqual(row["verdict"], "FAIL")
        self.assertTrue(row["fingerprint"])

    def test_log_is_append_only(self):
        for day in ("2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"):
            self.add(day, self.good if day != "2026-09-16" else self.bad)
        self.assertEqual(len(G.load(self.path)), 4)

    def test_empty_log_is_not_a_pass(self):
        self.assertFalse(G.state(path=self.path, fp=self.FP)[0])
        self.assertIn("한 번도", G.report(path=self.path))


class FingerprintTests(unittest.TestCase):
    """④ 3회 연속은 **같은 파이프라인**이어야 한다."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name) / "runs.csv")
        self.good = {k: True for k in G.KEYS}

    def tearDown(self):
        self.tmp.cleanup()

    def fill(self, fp):
        for day in ("2026-09-16", "2026-09-17", "2026-09-18"):
            G.record(day, f"r-{day}", "cycle", self.good, path=self.path, fp=fp)

    def test_changing_the_pipeline_resets_the_streak(self):
        """silent exception 정리와 feature_store 확장이 끝난 뒤부터 3회를 센다."""
        self.fill("old000000000")
        self.assertEqual(G.streak(path=self.path, fp="old000000000"), 3)
        self.assertEqual(G.streak(path=self.path, fp="new000000000"), 0)
        self.assertFalse(G.state(path=self.path, fp="new000000000")[0])

    def test_reset_reason_is_stated(self):
        self.fill("old000000000")
        self.assertIn("지문이 바뀌었다", G.state(path=self.path, fp="new000000000")[1])

    def test_core_files_are_in_the_fingerprint(self):
        for f in ("omakase.py", "hyeoks_analyst.py", "earnings_schema.py",
                  "feature_store.py", "rank_pool.py",
                  "evidence_builder.py", "production_receipt.py", "feature_telemetry.py",
                  ".github/workflows/main.yml",
                  ".github/workflows/review_regressions.yml",
                  ".github/workflows/earnings_collector.yml"):
            with self.subTest(f):
                self.assertIn(f, G.FINGERPRINT_FILES)

    def test_fingerprint_changes_with_content(self):
        import tempfile as T
        with T.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "x.py"
            p.write_text("a", encoding="utf-8")
            one = G.fingerprint(root=d, files=["x.py"])
            p.write_text("b", encoding="utf-8")
            self.assertNotEqual(one, G.fingerprint(root=d, files=["x.py"]))

    def test_data_directories_are_not_in_the_fingerprint(self):
        """⑥ — 데이터 자동 커밋으로 매일 달라지면 streak 가 매일 0 이 된다."""
        self.assertFalse([f for f in G.FINGERPRINT_FILES if f.startswith("data/")])

    def test_missing_file_is_a_change(self):
        self.assertNotEqual(G.fingerprint(files=["없는1.py"]),
                            G.fingerprint(files=["없는2.py"]))

    def test_fingerprint_is_recorded_per_row(self):
        self.fill("abc123abc123")
        self.assertTrue(all(r["fingerprint"] == "abc123abc123" for r in G.load(self.path)))


class EndToEndGateTests(unittest.TestCase):
    """영수증 → Builder → Gate 전 경로. 참/거짓 시나리오를 모두 본다.

    사용자 지시 "Gate false/true 시나리오 확인" — 판정이 **양쪽으로** 움직여야 한다.
    한쪽으로만 움직이면 그 기준은 아무것도 검증하지 못한다(P0-2 가 그랬다).
    """

    FP = "e2e000000000"
    WF = {"main.yml": "success", "ai_report.yml": "success",
          "earnings_collector.yml": "success"}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rec = str(pathlib.Path(self.tmp.name) / "receipts")

    def tearDown(self):
        self.tmp.cleanup()

    def feats(self):
        return {"v1": {"measured": True, "parsed": 700, "errors": 0},
                "v2": {"measured": True, "parsed": 700, "errors": 0},
                "v3": {"measured": True, "used": 140, "errors": 0,
                       "unparsable": 0, "out_of_range": 0, "schema_reason": ""}}

    def seed(self, day="2026-09-21", **over):
        import production_receipt as R
        sc = {"expected_state": "reached", "ci_conclusion": "success",
              "features": self.feats(),
              "feature_store": {"date": day, "run_id": "R1", "fingerprint": self.FP,
                                "rows": 718, "dropped": 0},
              "rank_pool": {"date": day, "fingerprint": self.FP, "rows": 8,
                            "total_dropped": 0},
              "ledger": {"expected_trade_ids": ["t1"], "found_trade_ids": ["t1"]}}
        an = {"expected_state": "reached", "stage": "final", "features": self.feats(),
              "ledger": {"expected_trade_ids": ["r1"], "found_trade_ids": ["r1"]}}
        ea = {"expected_state": "reached", "schema_version": "earnings-v2",
              "v3_out_of_range": 0, "schema_reason": "",
              "targets": 143, "dart_success": 143,
              "outcomes": {"success": 143, "allowed_missing_corp_code": 0,
                           "allowed_insufficient_data": 0, "hard_error": 0,
                           "circuit_breaker_unprocessed": 0,
                           "time_budget_unprocessed": 0},
              "unaccounted": 0, "target_source_health": "ok", "write_blocked": ""}
        sc.update(over.get("scanner") or {})
        an.update(over.get("analyst") or {})
        ea.update(over.get("earnings") or {})
        R.emit(day, "scanner", sc, root=self.rec, run_id="R1", fingerprint=self.FP)
        R.emit(day, "analyst", an, root=self.rec, run_id="R1", fingerprint=self.FP)
        R.emit(day, "earnings", ea, root=self.rec, run_id="R2", fingerprint=self.FP)
        R.emit(day, "consensus", {"state": "DEGRADED", "expected_state": "reached"},
               root=self.rec, run_id="R3", fingerprint=self.FP)

    def build(self, day="2026-09-21", wf=None):
        import evidence_builder as E
        return E.build(day, self.FP, root=self.rec,
                       workflow_states=self.WF if wf is None else wf)

    def test_true_scenario_passes_every_criterion(self):
        self.seed()
        ev, det = self.build()
        self.assertTrue(all(ev.values()), det["reasons"])
        self.assertEqual(G.evaluate(ev), (True, []))

    def test_aux_degraded_does_not_block(self):
        """보조 자료의 저하가 주 판정을 흐리지 않는다."""
        self.seed()
        ev, det = self.build()
        self.assertTrue(all(ev.values()))
        self.assertEqual(det["aux_state"], "DEGRADED")

    def test_false_scenarios(self):
        cases = {
            "dart 시간예산": {"earnings": {"outcomes": {
                "success": 127, "allowed_missing_corp_code": 0,
                "allowed_insufficient_data": 0, "hard_error": 0,
                "circuit_breaker_unprocessed": 0, "time_budget_unprocessed": 16}}},
            "feature_store 버림": {"scanner": {"feature_store": {
                "date": "2026-09-21", "run_id": "R1", "fingerprint": FP_OK,
                "rows": 718, "dropped": 3}}},
            "v1 미계측": {"scanner": {"features": {
                "v1": {"measured": False}, "v2": {"measured": True, "errors": 0},
                "v3": {"measured": True, "errors": 0}}}},
            "리포트 원장 누락": {"analyst": {"ledger": {
                "expected_trade_ids": ["r1"], "found_trade_ids": []}}},
            "analyst 중간 영수증": {"analyst": {"stage": "start"}},
            "본표 차단": {"earnings": {"write_blocked": "스키마 불일치"}},
        }
        for name, over in cases.items():
            with self.subTest(name):
                self.tearDown(); self.setUp()
                self.seed(**over)
                ev, det = self.build()
                self.assertFalse(all(ev.values()), f"{name} 이 통과해 버렸다: {det['reasons']}")
                self.assertFalse(G.evaluate(ev)[0])

    def test_workflow_failure_blocks_even_when_receipts_look_fine(self):
        """P1-3 — 영수증은 멀쩡한데 워크플로가 실패한 경우."""
        self.seed()
        ev, _ = self.build(wf={**self.WF, "main.yml": "failure"})
        self.assertFalse(ev["no_unexplained_failure"])

    def test_record_cycle_uses_only_receipts(self):
        """사람이 bool 을 넣지 않는다 — 영수증만으로 한 사이클이 만들어지는가."""
        self.seed()
        log = str(pathlib.Path(self.tmp.name) / "runs.csv")
        done, why = G.record_cycle("2026-09-21", path=log, receipts_root=self.rec,
                                   workflow_states=self.WF, fp=self.FP)
        self.assertTrue(done, why)
        self.assertEqual(G.load(log)[0]["verdict"], "PASS")
        self.assertEqual(G.load(log)[0]["aux_state"], "DEGRADED")

    def test_holds_before_cutoff_instead_of_recording_fail(self):
        """P1-2 — 영수증이 아직 없을 때 영구 FAIL 을 남기지 않는다."""
        log = str(pathlib.Path(self.tmp.name) / "runs.csv")
        done, why = G.record_cycle("2026-09-21", path=log, receipts_root=self.rec,
                                   workflow_states=self.WF, fp=self.FP,
                                   now=__import__("datetime").datetime(
                                       2026, 9, 21, 15, 0,
                                       tzinfo=__import__("datetime").timezone(
                                           __import__("datetime").timedelta(hours=9))))
        self.assertFalse(done)
        self.assertIn("보류", why)
        self.assertEqual(G.load(log), [])


FP_OK = "e2e000000000"


class ProductionEvidenceTests(unittest.TestCase):
    """문턱이 읽을 증거를 생산 코드가 실제로 찍는가 (GPT §1)."""

    def test_collector_prints_the_fixed_summary(self):
        src = (ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        for token in ("[DB_실적]", "[Consensus]", "V3 정상", "최저 갱신일시",
                      "schema=", "preflight ", "state="):
            with self.subTest(token):
                self.assertIn(token, src)

    def test_analyst_prints_the_v3_input_summary(self):
        src = (ROOT / "hyeoks_analyst.py").read_text(encoding="utf-8")
        for token in ("[V3 INPUT]", "V3 사용가능", "범위밖"):
            with self.subTest(token):
                self.assertIn(token, src)

    def test_schema_version_has_a_single_source(self):
        """버전 문자열의 사본을 두지 않는다 — 어긋나면 어느 쪽이 거짓말인지 모른다."""
        import earnings_schema
        self.assertEqual(earnings_schema.SCHEMA_VERSION, "earnings-v2")
        src = (ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn("EARNINGS_SCHEMA_VERSION = _es.SCHEMA_VERSION", src)
        self.assertNotIn('EARNINGS_SCHEMA_VERSION = "', src)

    def test_selftest(self):
        self.assertGreaterEqual(G._selftest(), 15)


if __name__ == "__main__":
    unittest.main()
