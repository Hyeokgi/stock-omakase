"""테마·대장 경로 자료 사전 — 방화벽(확증 구간 미열람)과 읽기 전용."""
import inspect
import unittest

import hyeoks_theme_abc as T
import hyeoks_theme_path_dict as D


class ThemePathDictTests(unittest.TestCase):
    def test_reads_only_days_before_the_confirmation_window(self):
        days = D.research_days()
        self.assertTrue(days)
        self.assertTrue(all(d < T.CONFIRM_FROM for d in days))

    def test_cutoff_is_enforced_even_if_later_files_exist(self):
        self.assertEqual(D.research_days(cutoff="2026-09-01"), ["2026-08-28", "2026-08-31"])

    def test_no_return_or_next_day_price_path(self):
        src = inspect.getsource(D)
        for banned in ("exit_open", "next_trading", "layer_returns", "openPrice", "with_returns=True"):
            self.assertNotIn(banned, src)

    def test_report_runs(self):
        out = D.report()
        self.assertIn("구조 집계", out)
        self.assertIn("방화벽", out)


if __name__ == "__main__":
    unittest.main()
