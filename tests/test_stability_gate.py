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


class StreakTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name) / "runs.csv")
        self.good = {k: True for k in G.KEYS}
        self.bad = {**self.good, "store_written": False}

    def tearDown(self):
        self.tmp.cleanup()

    def add(self, run_id, evidence, date="2026-09-18"):
        return G.record(run_id, date, "earnings", evidence, path=self.path)

    def test_three_consecutive_passes_open_the_gate(self):
        for i in range(3):
            self.add(f"r{i}", self.good)
        self.assertTrue(G.state(path=self.path)[0])

    def test_two_is_not_enough(self):
        for i in range(2):
            self.add(f"r{i}", self.good)
        self.assertFalse(G.state(path=self.path)[0])

    def test_a_single_failure_resets_the_streak_to_zero(self):
        """'거의 3회' 를 3회로 반올림하지 않는다."""
        for i in range(2):
            self.add(f"r{i}", self.good)
        self.add("bad", self.bad)
        self.assertEqual(G.streak(path=self.path), 0)
        self.assertFalse(G.state(path=self.path)[0])
        self.add("r3", self.good)
        self.assertEqual(G.streak(path=self.path), 1)

    def test_same_run_is_not_counted_twice(self):
        """같은 실행을 세 번 기록해 문턱을 통과하는 길을 막는다."""
        self.add("same", self.good)
        for _ in range(2):
            done, why = self.add("same", self.good)
            self.assertFalse(done)
            self.assertIn("이미", why)
        self.assertEqual(G.streak(path=self.path), 1)

    def test_log_is_append_only(self):
        for i in range(4):
            self.add(f"r{i}", self.good if i != 1 else self.bad)
        self.assertEqual(len(G.load(self.path)), 4)

    def test_every_criterion_is_recorded_per_run(self):
        """나중에 '어느 기준이 떨어졌나' 를 다시 물을 수 있어야 한다."""
        self.add("r0", self.bad)
        row = G.load(self.path)[0]
        for k in G.KEYS:
            self.assertIn(k, row)
        self.assertEqual(row["store_written"], "N")
        self.assertEqual(row["verdict"], "FAIL")

    def test_run_id_is_required(self):
        with self.assertRaises(ValueError):
            G.record("", "2026-09-18", "earnings", self.good, path=self.path)

    def test_empty_log_is_not_a_pass(self):
        self.assertFalse(G.state(path=self.path)[0])
        self.assertIn("한 번도", G.report(path=self.path))


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

    def test_schema_version_is_declared(self):
        src = (ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn('EARNINGS_SCHEMA_VERSION = "earnings-v2"', src)

    def test_selftest(self):
        self.assertGreaterEqual(G._selftest(), 15)


if __name__ == "__main__":
    unittest.main()
