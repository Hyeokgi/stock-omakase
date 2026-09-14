import ast
import datetime as dt
from pathlib import Path
import unittest

from after_market_quotes import scanner_after_quote, KST
from nightly_quotes import naver_quote


class AfterMarketQuotes(unittest.TestCase):
    now = dt.datetime(2026, 9, 14, 17, 36, tzinfo=KST)

    def payload(self, **extra):
        return {'closePrice': '672,000', 'overMarketPriceInfo': {
            'tradingSessionType': 'AFTER_MARKET', 'overPrice': '672,000',
            'localTradedAt': '2026-09-14T17:35:41+09:00',
            'compareToPreviousClosePrice': '-14,000', 'fluctuationsRatio': '-2.04',
            **extra}}

    def test_generic_is_not_nxt_or_krx(self):
        a, b, market = scanner_after_quote(self.payload(), self.now)
        self.assertEqual(b, '')
        self.assertEqual(market, '시간외(시장 미확인)')
        self.assertIn('전일종가 대비 -2.04%', a)
        self.assertIn('정규장종가 대비 미확인', a)
        self.assertNotIn('0.00%', a)
        self.assertIn('2026-09-14 17:35:41 KST', a)

    def test_invalid_and_old_quotes_are_missing(self):
        for fields in ({'overPrice': 0}, {'overPrice': 'nan'},
                       {'localTradedAt': '2026-09-13T17:35:00+09:00'},
                       {'localTradedAt': '2026-09-14T18:35:00+09:00'},
                       {'localTradedAt': '2026-09-14T17:35:00'},
                       {'tradingSessionType': 'PRE_MARKET'}):
            with self.subTest(fields=fields):
                self.assertEqual(scanner_after_quote(self.payload(**fields), self.now)[2], '시간외 미확인')

    def test_missing_rate_never_becomes_zero(self):
        for fields in ({'fluctuationsRatio': None}, {'fluctuationsRatio': '1'},
                       {'compareToPreviousClosePrice': None}):
            a, _, _ = scanner_after_quote(self.payload(**fields), self.now)
            self.assertIn('전일대비 미확인', a)
            self.assertNotIn('%', a)

    def test_genuine_zero_and_staleness(self):
        a, _, _ = scanner_after_quote(self.payload(fluctuationsRatio='0',
            compareToPreviousClosePrice='0', localTradedAt='2026-09-14T17:00:00+09:00'), self.now)
        self.assertIn('전일종가 대비 +0.00%', a)
        self.assertIn('지연 관측', a)

    def test_no_legacy_current_price_denominator(self):
        self.assertEqual(naver_quote({'closePrice': 100, 'nxtClosePrice': 100}, 'NXT'), (None, None))

    def test_scanner_uses_one_response_without_old_cross_fill(self):
        path = Path(__file__).resolve().parents[1] / 'omakase.py'
        tree = ast.parse(path.read_text(encoding='utf-8'))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'analyze_single_stock')
        calls = [n.func.id for n in ast.walk(fn) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)]
        self.assertIn('scanner_after_quote', calls)
        self.assertNotIn('fetch_extra_closing_prices_from_kis', calls)


if __name__ == '__main__':
    unittest.main()
