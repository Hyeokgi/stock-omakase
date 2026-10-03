"""호가 필드 감사(`hyeoks_quote_audit.py`) — 읽기 전용이고 수익률·익일 가격 경로가 없다."""
import contextlib
import io
import unittest

import hyeoks_quote_audit as Q


class QuoteAuditTests(unittest.TestCase):
    def test_self_test_passes(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(Q.self_test(), 0)

    def test_report_runs_on_repo_snapshots_without_error(self):
        out = Q.report()
        self.assertIn("호가 필드 감사", out)
        self.assertIn("수익률을 계산하지 않고", out)

    def test_total_depth_is_never_described_as_best_quote_quantity(self):
        out = Q.report()
        self.assertIn("총잔량은 최우선 호가의 수량이 아니다", out)

    def test_the_audit_never_builds_a_price_when_the_quote_is_unusable(self):
        row = {"askBuy": "0", "askSell": "101", "nowPrice": "100", "totalBuyVolume": "0", "totalSellVolume": "5"}
        st = Q.quote_state(row)
        self.assertIsNone(st["spread_pct"])
        self.assertIsNone(st["where"])


if __name__ == "__main__":
    unittest.main()
