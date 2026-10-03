"""`leader-hold-v1` 잠금 상수 고정 — 사전등록 문서(LOCKED)의 값이 코드에서 조용히 바뀌지 않게 한다.

이 시험이 실패하면 **정의·문턱·평가 규칙이 바뀐 것**이다. 바꾸려면 이 시험을 고치는 것이 아니라
`leader-hold-v2` 를 새로 사전등록한다(잠금 후 변경 규칙). 이미 본 확증 날짜는 v2 의 탐색 구간으로 넘어간다.
오류 수정은 허용되지만 상수는 그대로여야 하고, 고친 날짜와 이유를 사전등록 §11 에 남긴다.
"""
import unittest

import hyeoks_theme_abc as T
from hyeoks_closing_bet import COST


class LockedConstants(unittest.TestCase):
    def test_study_identity_and_windows(self):
        self.assertEqual(T.STUDY_ID, "leader-hold-v1")
        self.assertEqual(T.CONFIRM_FROM, "2026-10-06")
        self.assertEqual(T.EVAL_VALID_DAYS, 60)
        self.assertEqual(T.CAP_TRADING_DAYS, 120)

    def test_definitions(self):
        self.assertEqual(T.HOLD_PCT, 98)
        self.assertEqual(T.RISK_RATE_CAP, 0.295)
        self.assertEqual(T.MIN_BREAKOUT_TV, 10_000_000_000)
        self.assertEqual(T.TOPK, 2)
        self.assertEqual(T.RISK_REASONS, ("경보미확인", "시장경보", "상한가근접"))
        self.assertEqual(COST, 0.0035)

    def test_bootstrap(self):
        self.assertEqual((T.BOOT_BLOCK, T.BOOT_N, T.BOOT_SEED), (5, 10_000, 20261006))

    def test_quality_hold_thresholds(self):
        """무자금 보조 연구에 한해 승인된 초과 보류 기준 — 시가 결측 5% · 가드 제외 10% · 진입 자료 결측 거래일 5%."""
        self.assertEqual(T.QUALITY_MISSING_MAX, 0.05)
        self.assertEqual(T.QUALITY_GUARD_MAX, 0.10)
        self.assertEqual(T.MATURED_MISSING_NOTES, ("익일달력결측", "익일 스냅샷 없음"))

    def test_nothing_here_enables_orders(self):
        """사전등록 승인 범위 밖: 선정 조건 변경 · 실전 투자 · 자동주문."""
        import inspect
        src = inspect.getsource(T)
        for banned in ("place_order", "submit_order", "requests.post", "gspread"):
            self.assertNotIn(banned, src)


if __name__ == "__main__":
    unittest.main()
