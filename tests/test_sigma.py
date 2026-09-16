"""설계별 σ 실측 — 규율과 자유도 손해가 실제로 반영되는가."""
import unittest
import hyeoks_sigma as S
from hyeoks_verdict import C_ENTRY_DATE, C_CHANNEL, STOCK_COL, INDEX_COL


def mk(ch, day, stock, index, n=32):
    r = [""] * n
    r[C_ENTRY_DATE], r[C_CHANNEL] = day, ch
    r[STOCK_COL[5]], r[INDEX_COL[5]] = str(stock), str(index)
    return r


class SigmaTests(unittest.TestCase):
    def test_module_self_test_passes(self):
        self.assertEqual(S.self_test(), 0)

    def test_need_n_uses_t_not_normal(self):
        """자유도 손해가 빠지면 날짜짝짓기를 과대평가하게 된다."""
        import math
        t_based = S.need_n(4.5, 1.5)
        z_based = math.ceil(((1.6449 + 0.8416) * 4.5 / 1.5) ** 2)
        self.assertGreater(t_based, z_based)

    def test_pairing_can_be_worse_than_daily(self):
        """🔴 계획 §0 이 ④를 최선으로 **가정**했는데 그렇지 않을 수 있다.

        짝짓기는 공통요인을 없애지만 **대조군 자신의 잡음을 더한다.**
        대조군이 하루 1픽이면 그 잡음이 크다. 이 성질을 코드가 갖고 있어야
        실측이 의미를 갖는다 — 늘 ④가 이기는 구현이면 잴 이유가 없다.
        """
        rows = [[""] * 32]
        # 시장 공통요인은 0, 대조군만 크게 흔들리는 상황
        for d, noise in enumerate([20.0, -20.0, 18.0, -18.0, 22.0, -22.0], start=1):
            day = f"2026-09-{d:02d}"
            rows.append(mk("차트TOP2", day, 1.0, 0.0))     # 알파 일정 = 잡음 0
            rows.append(mk("랜덤2", day, noise, 0.0))      # 대조군만 요동
        p = S.picks(rows, horizon_of=lambda ch: 5)
        daily = S.sigma_of(S.series_daily(p, "차트TOP2"))
        paired = S.sigma_of(S.series_paired(p, "차트TOP2"))
        self.assertLess(daily, paired, "짝짓기가 잡음을 더하는 경우를 못 잡는다")

    def test_pairing_helps_when_common_factor_dominates(self):
        """반대로 공통요인이 크고 대조군이 조용하면 짝짓기가 이긴다."""
        rows = [[""] * 32]
        for d, mkt in enumerate([15.0, -15.0, 12.0, -12.0, 18.0, -18.0], start=1):
            day = f"2026-09-{d:02d}"
            # 알파에 시장 몫이 남아 있는 상황(지수 차감이 불완전)
            rows.append(mk("차트TOP2", day, mkt + 1.0, 0.0))
            rows.append(mk("랜덤2", day, mkt, 0.0))
        p = S.picks(rows, horizon_of=lambda ch: 5)
        self.assertGreater(S.sigma_of(S.series_daily(p, "차트TOP2")),
                           S.sigma_of(S.series_paired(p, "차트TOP2")))

    def test_report_refuses_to_pick_delta_or_judge(self):
        rows = [[""] * 32]
        for d in range(1, 11):
            day = f"2026-09-{d:02d}"
            rows.append(mk("차트TOP2", day, d, 1.0))
            rows.append(mk("랜덤2", day, 1.0, 1.0))
        md = S.report(S.picks(rows, horizon_of=lambda ch: 5), channels=["차트TOP2"])
        self.assertIn("판정이 아니다", md)
        self.assertIn("p-해킹", md)
        for d in S.DELTA_GRID:
            self.assertIn(f"δ={d}", md)


if __name__ == "__main__":
    unittest.main()
