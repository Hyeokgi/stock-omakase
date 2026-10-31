"""계좌 계산기 결함 I(2026-10-25 전체 검토, Codex) — 15:05 종베 매수의 종목 · 테마 한도를 그날 종가로 재던 것.

15:05 에는 그날 종가를 모른다. 한도 판단은 15:05 에 알 수 있는 값으로만 한다 — 보유 종목은 그 종목의 15:05 관측 가격,
없으면 그날 시가(15:05 전에 이미 아는 값), 새로 사는 종목은 15:05 가격. 같은 날 종가만 바꿔도 판단이 같아야 한다.
"""
import unittest

import hyeoks_account as a
from test_closing_bet_fill import D, px

CFG = a.Config(initial_cash=1_000_000, ticket_cash=100_000,
               max_stock_weight=1.0, max_theme_weight=0.25,
               buy_fee=0.0, sell_fee=0.0, slippage=0.0)


def swing(code='000002'):
    return dict(id='s1', channel='단기', group='strategy', code=code, signal=D[0], entry=D[0], exit=D[2],
                horizon=2, themes=['T'])


def bet(code='000001'):
    return dict(id='b1', channel='종베', group='strategy', code=code, signal=D[0], entry=D[0], exit=D[1],
                horizon=1, themes=['T'], fill_model=a.FILL_1505_NEXT_OPEN)


def prices(close_swing, close_bet, swing_1505=1000):
    """시가 매수한 스윙 100주(시가 1000) + 같은 테마 종베 15:05 1000. 종가만 인자로 바꾼다."""
    sw = px(1000, close_swing, p1505=swing_1505) if swing_1505 else px(1000, close_swing, tradable=True)
    if not swing_1505:
        sw.pop('snapshot_1505', None)
    nxt = dict(open=1000, high=1000, low=1000, close=1000, tradable=True,
               snapshot_1505=dict(price_1505=1000, open_price=1000, prev_close_derived=close_bet,
                                  status_at_snapshot=True))
    return {'000002': {D[0]: sw, D[1]: dict(nxt, snapshot_1505=dict(nxt['snapshot_1505'], prev_close_derived=close_swing)),
                       D[2]: nxt},
            '000001': {D[0]: px(950, close_bet, p1505=1000), D[1]: nxt, D[2]: nxt}}


def decided(r):
    return sorted((x['id'], x['reason']) for x in r['rejected']), \
        sorted(p['id'] for p in r['open_positions']) + sorted(t['id'] for t in r['closed_trades'])


class CapAt1505(unittest.TestCase):
    def test_cap_uses_1505_values_not_the_close(self):
        # 15:05 — 테마 T = 스윙 100주 × 1000 + 종베 100주 × 1000 = 20만 / 100만 = 20% ≤ 25% → 산다.
        # 종가로 재면 스윙 1,700 · 종베 1,100 → 28만 / 108만 = 26% > 25% 로 거절됐었다.
        r = a.simulate([swing(), bet()], D, prices(1700, 1100), CFG)
        self.assertEqual(decided(r)[0], [])
        self.assertIn('b1', decided(r)[1])

    def test_decision_does_not_change_with_the_close(self):
        base = decided(a.simulate([swing(), bet()], D, prices(1000, 1000), CFG))
        for cs, cb in ((1700, 1100), (500, 1290), (1290, 700)):
            with self.subTest(close_swing=cs, close_bet=cb):
                self.assertEqual(decided(a.simulate([swing(), bet()], D, prices(cs, cb), CFG)), base)

    def test_held_stock_without_1505_observation_is_valued_at_the_open(self):
        # 스윙 종목에 15:05 관측이 없으면 그날 시가(1000)로 잰다 — 종가(1700)를 쓰지 않는다.
        r = a.simulate([swing(), bet()], D, prices(1700, 1100, swing_1505=None), CFG)
        self.assertEqual(decided(r)[0], [])

    def test_cap_still_binds_at_1505(self):
        # 15:05 값으로도 넘으면 거절한다 — 고친 뒤에도 한도는 살아 있다.
        cfg = a.Config(**{**CFG.__dict__, 'max_theme_weight': 0.15})
        r = a.simulate([swing(), bet()], D, prices(1000, 1000), cfg)
        self.assertEqual(decided(r)[0], [('b1', 'theme_cap')])


if __name__ == '__main__':
    unittest.main()
