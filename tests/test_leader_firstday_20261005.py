"""사전등록 leader-firstday-v1 — 잠긴 정의 고정·열람 통제·탐색 구간 분리 시험."""
import contextlib
import io
import unittest
from unittest import mock

import hyeoks_leader_firstday as F


def day(date, A, B, prices, breadth=0.5):
    rows = {c: {"nowPrice": str(p)} for c, p in prices.items()}
    return {"date": date, "rows": rows, "A": A, "B": set(B), "breadth": breadth}


def it(code, rate, amt):
    return {"code": code, "rate": rate, "amt": amt}


class Locked(unittest.TestCase):
    def test_preregistered_constants(self):
        self.assertEqual((F.CONFIRM_FROM, F.LOOKBACK, F.HORIZON, F.SAMPLE_DAYS, F.RATE_CAP),
                         ("2026-10-06", 20, 3, 60, 29.5))
        self.assertEqual((F.BOOT_N, F.BOOT_BLOCK, F.SEED), (10_000, 3, 20261005))
        self.assertEqual([b[0] for b in F.RATE_BINS], ["≤0", "0~5%", "5~10%", "10~20%", "20~29.5%"])

    def test_rate_bins(self):
        self.assertEqual([F.rate_bin(x) for x in (-3, 0, 0.1, 5, 9.9, 10, 20, 29.4, 29.5)],
                         ["≤0", "≤0", "0~5%", "5~10%", "5~10%", "10~20%", "20~29.5%", "20~29.5%", None])


class Registry(unittest.TestCase):
    def test_new_leader_needs_20_clean_days_and_upper_limit_is_excluded(self):
        A = [it("L", 8, 300), it("O", 8, 200), it("C", 8, 100), it("U", 29.9, 50)]
        days = [day("2026-10-01", A, ["O"], {}), day("2026-10-02", A, ["L", "O", "U"], {})]
        reg = F.registry(days)
        d2 = {r["code"]: r["group"] for r in reg if r["i"] == 1}
        self.assertEqual(d2.get("L"), "T", "처음 대장 → 처리군")
        self.assertNotIn("O", d2, "직전 20 관측일 안에 대장 → 처리도 대조도 아님")
        self.assertEqual(d2.get("C"), "C")
        self.assertNotIn("U", d2, "상한가 근처는 공통 제외")


class Statistic(unittest.TestCase):
    def test_stratified_difference_and_dropped(self):
        rows = [{"stratum": "a", "group": "T", "y": 0}, {"stratum": "a", "group": "C", "y": 1},
                {"stratum": "a", "group": "C", "y": 0}, {"stratum": "b", "group": "T", "y": 1}]
        D, N, dropped = F.stratified_D(rows)
        self.assertEqual((D, N, dropped), (-0.5, 1, 1))

    def test_bootstrap_is_deterministic(self):
        rows = [{"date": f"d{i}", "stratum": (f"d{i}", "x", 0), "group": g, "y": y}
                for i in range(9) for g, y in (("T", 0), ("C", 1))]
        a = F.block_boot(rows, n=200)
        self.assertEqual(a, F.block_boot(rows, n=200))
        self.assertEqual(a[0], -1.0)


class ViewingControl(unittest.TestCase):
    def setUp(self):
        self.days = F.load_obs()

    def test_evaluate_refuses_before_gate(self):
        with self.assertRaises(PermissionError):
            F.evaluate(self.days, hold_done=False)
        with self.assertRaises(PermissionError):
            F.evaluate(self.days, hold_done=True)       # 표본이 아직 없다

    def test_status_never_computes_outcomes_or_prints_rates(self):
        out = io.StringIO()
        with mock.patch.object(F, "outcome", side_effect=AssertionError("상태 점검이 결과를 계산하면 안 된다")), \
                mock.patch.object(F, "leader_hold_concluded", return_value=False), contextlib.redirect_stdout(out):
            F.main([])
        self.assertNotIn("%", out.getvalue())
        self.assertIn("성과 비공개", out.getvalue())

    def test_explore_uses_only_pre_confirmation_dates(self):
        seen = []
        real = F.outcome
        def spy(days, i, code, k=F.HORIZON):
            seen.append((days[i]["date"], days[i + k]["date"] if i + k < len(days) else ""))
            return real(days, i, code, k)
        with mock.patch.object(F, "outcome", side_effect=spy):
            F.explore_past(self.days)
        self.assertTrue(seen)
        self.assertTrue(all(a < F.CONFIRM_FROM and b < F.CONFIRM_FROM for a, b in seen))


if __name__ == "__main__":
    unittest.main()
