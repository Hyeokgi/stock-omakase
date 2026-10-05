"""대장 이후 방향 예측 탐색 v1 — 방화벽·단위 정의·통계 도구 시험."""
import os
import shutil
import tempfile
import unittest

import hyeoks_leader_direction as L


class Firewall(unittest.TestCase):
    def test_cutoff_is_the_locked_confirmation_start(self):
        self.assertEqual(L.CUTOFF, "2026-10-06")

    def test_future_snapshots_are_never_read(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        src = sorted(f for f in os.listdir(L.SNAP_DIR) if f.endswith("_1505.csv.gz"))[-1]
        shutil.copy(os.path.join(L.SNAP_DIR, src), os.path.join(d, "2026-10-02_1505.csv.gz"))
        shutil.copy(os.path.join(L.SNAP_DIR, src), os.path.join(d, "2026-10-06_1505.csv.gz"))
        days = L.load_days(d)
        self.assertEqual([x["date"] for x in days], ["2026-10-02"])

    def test_real_run_uses_only_exploration_dates(self):
        days = L.load_days()
        self.assertTrue(days and all(x["date"] < L.CUTOFF for x in days))
        rows = L.state_rows(days, L.episodes(days))
        for r in rows:
            self.assertLess(r["date"], L.CUTOFF)
            for k in L.KS:
                self.assertLess(r.get(f"r{k}_date", ""), L.CUTOFF)


class Units(unittest.TestCase):
    def test_episode_is_first_appearance_only(self):
        days = [{"date": "d1", "B": {"A": {"theme": "1"}}}, {"date": "d2", "B": {"A": {"theme": "2"}, "B": {"theme": "3"}}}]
        eps = L.episodes(days)
        self.assertEqual([(e["code"], e["t0"], e["theme"]) for e in eps], [("A", "d1", "1"), ("B", "d2", "3")])

    def test_path_classes(self):
        self.assertEqual(L.path_class([0.04, 0.05, 0.05, 0.06, 0.07]), "즉시상승")
        self.assertEqual(L.path_class([0.01, -0.01, 0.0, 0.02, 0.05]), "횡보후상승")
        self.assertEqual(L.path_class([-0.05, -0.02, 0.0, 0.01, 0.01]), "조정후회복")
        self.assertEqual(L.path_class([-0.01, -0.02, -0.03, -0.04, -0.05]), "지속약화")
        self.assertIsNone(L.path_class([0.01, None, 0, 0, 0]))

    def test_alive_rule_is_the_preregistered_one(self):
        self.assertTrue(L.alive({"f_theme_val_vs_t0": 0.8, "f_theme_breadth": 0.5}))
        self.assertFalse(L.alive({"f_theme_val_vs_t0": 0.79, "f_theme_breadth": 0.9}))
        self.assertFalse(L.alive({"f_theme_val_vs_t0": 1.2, "f_theme_breadth": 0.49}))
        self.assertEqual((L.DD_REST, L.FLAT_3, L.PRIMARY_K, L.DISCOVERY_END), (-0.03, 0.03, 3, "2026-09-16"))


class Stats(unittest.TestCase):
    def test_auc_and_spearman(self):
        self.assertEqual(L.auc([1, 2, 3, 4], [0, 0, 1, 1]), 1.0)
        self.assertEqual(L.auc([4, 3, 2, 1], [0, 0, 1, 1]), 0.0)
        self.assertAlmostEqual(L.spearman([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]), 1.0)

    def test_holm(self):
        out = L.holm([("a", 0.01), ("b", 0.04), ("c", 0.5)])
        self.assertEqual(out, {"a": True, "b": False, "c": False})

    def test_boot_diff_is_by_date_and_deterministic(self):
        g1 = [{"date": f"d{i}", "x": 0.1} for i in range(10)]
        g2 = [{"date": f"d{i}", "x": -0.1} for i in range(10)]
        a = L.boot_diff(g1, g2, "x", n=200)
        self.assertEqual(a, L.boot_diff(g1, g2, "x", n=200))
        self.assertEqual(a[0], 1.0)
        self.assertEqual(a[3], 0.0)


if __name__ == "__main__":
    unittest.main()
