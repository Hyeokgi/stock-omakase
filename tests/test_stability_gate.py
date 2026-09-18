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
