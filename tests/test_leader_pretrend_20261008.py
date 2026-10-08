"""방향 탐색 3회차 — t0 당일·이후 봉을 쓰지 않는다, 확증 구간 봉은 거부, 계열 불일치 제외."""
import gzip
import os
import tempfile
import unittest

import hyeoks_leader_pretrend as P


def series(n, last="2026-09-15", close=100.0):
    import datetime
    d = datetime.date.fromisoformat(last)
    out = []
    for i in range(n):
        out.append(((d - datetime.timedelta(days=n - 1 - i)).isoformat(), close * 1.01, close))
    return out


class PreTrend(unittest.TestCase):
    def test_uses_only_bars_before_t0(self):
        s = series(300, last="2026-09-15") + [("2026-09-16", 999.0, 500.0), ("2026-09-17", 999.0, 900.0)]
        f, bad = P.features(s, "2026-09-16", 110.0, 10.0)
        self.assertFalse(bad)
        self.assertAlmostEqual(f["pos60"], 110 / 101 - 1)
        self.assertAlmostEqual(f["run5"], 0.0)
        self.assertIn("pos52w", f)

    def test_series_mismatch_is_excluded(self):
        f, bad = P.features(series(100), "2026-09-16", 150.0, 0.0)     # 스냅샷 전일 종가 150 vs 봉 100
        self.assertTrue(bad)
        self.assertEqual(f, {})

    def test_short_history_gives_missing_not_zero(self):
        f, _ = P.features(series(30), "2026-09-16", 100.0, 0.0)
        self.assertNotIn("pos60", f)
        self.assertIn("run20", f)

    def test_bar_file_after_cutoff_is_refused(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "b.csv.gz")
        with gzip.open(p, "wt", encoding="utf-8") as fh:
            fh.write("date,code,open,high,low,close,volume,fetchedAt\n2026-10-06,000001,1,1,1,1,1,x\n")
        with self.assertRaises(AssertionError):
            P.load_bars([p])

    def test_bins_are_the_preregistered_ones(self):
        self.assertEqual([P.bin_of("run5", x) for x in (-0.01, 0.0, 0.1, 0.3)], ["<0", "0~10%", "10~30%", "≥30%"])
        self.assertEqual([P.bin_of("pos60", x) for x in (0.0, -0.10, -0.20, -0.31)], ["≥−5%", "−5~−15%", "−15~−30%", "<−30%"])


if __name__ == "__main__":
    unittest.main()
