"""종베 시가 청산 경로 — `docs/사전등록_2026-09-16_종베_시가청산경로.md` §4 검증 계획.

문턱은 `hyeoks_closing_bet.py` 의 연구 정의를 그대로 옮긴 것이다.
여기서 거는 것은 "계좌 계산기가 그 정의대로 체결하는가"이지 종베의 수익성이 아니다.
"""
import unittest
import hyeoks_account as a

D = ['2026-09-01', '2026-09-02', '2026-09-03']
CFG = a.Config(initial_cash=1_000_000, ticket_cash=100_000,
               max_stock_weight=1.0, max_theme_weight=1.0,
               buy_fee=0.0, sell_fee=0.0, slippage=0.0)


def px(open_, close, *, p1505=None, open_price=None, prev_close=None,
       tradable=True, high=None, low=None):
    """하루치 가격. snapshot_1505 는 어댑터가 넣어 주는 모양 그대로."""
    hi = high if high is not None else max(open_, close, p1505 or 0, open_price or 0)
    lo = low if low is not None else min(x for x in (open_, close, p1505, open_price) if x)
    row = dict(open=open_, high=hi, low=lo, close=close, tradable=tradable)
    snap = {}
    if p1505 is not None:
        snap['price_1505'] = p1505
    if open_price is not None:
        snap['open_price'] = open_price
    if prev_close is not None:
        snap['prev_close_derived'] = prev_close
    if snap or tradable is not None:
        snap.setdefault('status_at_snapshot', tradable)
        row['snapshot_1505'] = snap
    return row


def bet_order(code='000001', entry=D[0], exit_=D[1]):
    return dict(id='b1', channel='종베', group='strategy', code=code,
                signal=entry, entry=entry, exit=exit_, horizon=1, themes=[],
                fill_model=a.FILL_1505_NEXT_OPEN)


class ClosingBetFillTests(unittest.TestCase):

    def prices(self, **day2):
        """진입일 15:05=1000, 청산일은 인자로."""
        return {'000001': {
            D[0]: px(900, 1100, p1505=1000),
            D[1]: px(day2.get('open_', 1100), day2.get('close', 1100),
                     p1505=day2.get('p1505', 1100),
                     open_price=day2.get('open_price'),
                     prev_close=day2.get('prev_close')),
            D[2]: px(1100, 1100, p1505=1100, open_price=1100, prev_close=1100)}}

    # ── 정상 경로 ────────────────────────────────────────────────────
    def test_entry_at_1505_exit_at_next_open(self):
        p = self.prices(open_price=1100, prev_close=1000)
        r = a.simulate([bet_order()], D, p, CFG)
        t = r['closed_trades']
        self.assertEqual(len(t), 1)
        self.assertEqual(t[0]['entry_fill'], 1000)      # 15:05 nowPrice
        self.assertEqual(t[0]['exit_fill'], 1100)       # 익일 시가
        self.assertEqual(t[0]['exit_actual'], D[1])

    def test_no_double_counting_of_1505_to_close_move(self):
        """운영대전제 §4 — 15:05→당일 종가 이동분을 다시 비용으로 빼지 않는다.

        진입가 1000, 그날 종가 1100(이동 +10%), 익일 시가 1100.
        수익률은 익일시가/진입가 − 1 = +10% 여야 한다. 종가 이동분이 또 빠지면 0% 가 된다.
        """
        p = self.prices(open_price=1100, prev_close=1000)
        r = a.simulate([bet_order()], D, p, CFG)
        self.assertAlmostEqual(r['closed_trades'][0]['return_pct'], 10.0, places=9)

    # ── 가드 넷 — 걸리면 청산하지 않고 **남는다** ──────────────────────
    def guard(self, **day2):
        r = a.simulate([bet_order()], D, self.prices(**day2), CFG)
        return r

    def test_guard_no_open_price_keeps_position(self):
        r = self.guard(open_price=None, prev_close=1000)     # 익일시가없음
        self.assertEqual(r['closed_trades'], [])
        self.assertEqual(len(r['open_positions']), 1)
        self.assertEqual(r['open_positions'][0]['exit_blocked'], 'no_open_price')

    def test_guard_prev_close_unavailable_keeps_position(self):
        r = self.guard(open_price=1100, prev_close=None)
        self.assertEqual(r['open_positions'][0]['exit_blocked'], 'prev_close_unavailable')

    def test_guard_price_limit_breach_keeps_position(self):
        # 시가 1400 / 전일종가 1000 = +40% > 30%+0.5%
        r = self.guard(open_price=1400, prev_close=1000)
        self.assertEqual(r['open_positions'][0]['exit_blocked'], 'price_limit_breach')

    def test_guard_corporate_action_keeps_position(self):
        # 액면분할류 — 전일종가 200 vs 우리 진입가 1000 → |200/1000−1| = 80% > 35%
        r = self.guard(open_price=210, prev_close=200)
        self.assertEqual(r['open_positions'][0]['exit_blocked'], 'corporate_action_suspected')

    def test_price_limit_edge_inside_tolerance_still_exits(self):
        # +30.4% 는 30%+0.5% 안이라 청산된다 (문턱을 임의로 좁히지 않았는지)
        r = self.guard(open_price=1304, prev_close=1000)
        self.assertEqual(len(r['closed_trades']), 1)

    # ── 진입 쪽 ─────────────────────────────────────────────────────
    def test_halted_at_1505_is_rejected_not_filled(self):
        p = self.prices(open_price=1100, prev_close=1000)
        p['000001'][D[0]]['snapshot_1505']['status_at_snapshot'] = False
        r = a.simulate([bet_order()], D, p, CFG)
        self.assertEqual(r['rejected'][0]['reason'], 'halted_at_1505')
        self.assertEqual(r['closed_trades'], [])

    def test_no_1505_observation_is_rejected(self):
        p = self.prices(open_price=1100, prev_close=1000)
        del p['000001'][D[0]]['snapshot_1505']
        r = a.simulate([bet_order()], D, p, CFG)
        self.assertEqual(r['rejected'][0]['reason'], 'no_1505_observation')

    def test_daily_tradable_does_not_substitute_for_1505(self):
        """일별 tradable=True 라도 15:05 관측이 없으면 채우지 않는다."""
        p = self.prices(open_price=1100, prev_close=1000)
        p['000001'][D[0]]['tradable'] = True
        del p['000001'][D[0]]['snapshot_1505']['price_1505']
        r = a.simulate([bet_order()], D, p, CFG)
        self.assertEqual(r['rejected'][0]['reason'], 'no_1505_price')

    # ── 하루 안의 순서 ────────────────────────────────────────────────
    def test_open_exit_cash_funds_same_day_1505_buy(self):
        """시가 청산 현금이 같은 날 15:05 매수에 쓰인다(사전등록 §2-2 ①→③)."""
        cfg = a.Config(initial_cash=100_000, ticket_cash=100_000,
                       max_stock_weight=1.0, max_theme_weight=1.0,
                       buy_fee=0.0, sell_fee=0.0, slippage=0.0)
        p = {'000001': {D[0]: px(900, 1000, p1505=1000),
                        D[1]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000),
                        D[2]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000)},
             '000002': {D[0]: px(1000, 1000, p1505=1000),
                        D[1]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000),
                        D[2]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000)}}
        orders = [bet_order('000001', D[0], D[1]),
                  dict(bet_order('000002', D[1], D[2]), id='b2')]
        r = a.simulate(orders, D, p, cfg)
        # 첫 주문이 D[1] 시가에 청산되어 현금이 돌아왔으므로 둘째가 같은 날 체결된다
        self.assertEqual(len(r['closed_trades']), 2)
        self.assertEqual([t['id'] for t in r['closed_trades']], ['b1', 'b2'])

    def test_insufficient_cash_is_recorded_as_rejected(self):
        """현금이 모자라 못 산 것은 **거절로 남는다**(조용히 사라지지 않는다)."""
        cfg = a.Config(initial_cash=100_000, ticket_cash=60_000,
                       max_stock_weight=1.0, max_theme_weight=1.0,
                       buy_fee=0.0, sell_fee=0.0, slippage=0.0)
        day = {D[0]: px(1000, 1000, p1505=1000),
               D[1]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000),
               D[2]: px(1000, 1000, p1505=1000, open_price=1000, prev_close=1000)}
        p = {'000001': dict(day), '000002': dict(day)}
        orders = [bet_order('000001'), dict(bet_order('000002'), id='b2')]
        r = a.simulate(orders, D, p, cfg)
        self.assertEqual(len(r['closed_trades']), 1)        # 첫 건만 체결
        self.assertEqual(r['rejected'][0]['reason'], 'insufficient_cash')
        self.assertEqual(r['rejected'][0]['id'], 'b2')

    # ── 기존 경로 불변 ────────────────────────────────────────────────
    def test_unknown_fill_model_is_fatal_not_defaulted(self):
        o = dict(bet_order(), fill_model='시가청산')
        with self.assertRaises(a.InputError):
            a.simulate([o], D, self.prices(open_price=1100, prev_close=1000), CFG)

    def test_ledger_order_unaffected_by_snapshot_fields(self):
        """원장 주문은 15:05 필드가 있어도 기존대로 시가매수/종가매도."""
        o = dict(bet_order(), fill_model=a.FILL_NEXT_OPEN_CLOSE, entry=D[1], exit=D[2])
        p = self.prices(open_price=1100, prev_close=1000)
        t = a.simulate([o], D, p, CFG)['closed_trades'][0]
        self.assertEqual(t['entry_fill'], p['000001'][D[1]]['open'])
        self.assertEqual(t['exit_fill'], p['000001'][D[2]]['close'])


class LedgerPathUnchangedTests(unittest.TestCase):
    """종베 경로를 넣기 전(`f6f2199`)의 원장 결과와 값이 같은가.

    ⚠️ 정확히 말하면 **비트 단위로 같지는 않다** — 체결 기록에 `fill_model` 과
       `entry_price_raw` 두 칸이 **추가**됐다. 계산된 값은 하나도 바뀌지 않았다.
       사전등록 §4 는 "비트 단위"라고 적었는데 그건 내 표현이 부정확했다(§6 정정).
       여기서는 실제로 지켜야 하는 것을 건다: **기존 칸의 값이 하나도 안 바뀐다.**
    """

    BEFORE = {"channel": "차트TOP2", "code": "000001", "cost_basis": 1000.0,
              "entry": "2026-09-02", "entry_fill": 100.0, "exit": "2026-09-08",
              "exit_actual": "2026-09-08", "exit_fill": 100.0, "group": "strategy",
              "horizon": 5, "id": "one", "pnl": 0.0, "qty": 10, "return_pct": 0.0,
              "signal": "2026-09-01", "themes": ["theme"]}
    ADDED = {"fill_model", "entry_price_raw"}

    def test_existing_fields_unchanged(self):
        import sys
        sys.path.insert(0, 'tests')
        from test_account_nav import fixture
        t = a.calculate(fixture())['accounts']['strategy']['closed_trades'][0]
        for k, v in self.BEFORE.items():
            self.assertEqual(t[k], v, f'{k} 이 바뀌었다')

    def test_only_additive(self):
        import sys
        sys.path.insert(0, 'tests')
        from test_account_nav import fixture
        t = a.calculate(fixture())['accounts']['strategy']['closed_trades'][0]
        self.assertEqual(set(t) - set(self.BEFORE), self.ADDED,
                         '예상 밖의 칸이 늘거나 줄었다')

    def test_ledger_orders_are_marked_explicitly(self):
        import sys
        sys.path.insert(0, 'tests')
        from test_account_nav import fixture
        b = fixture()
        orders, _ = a.orders_from_ledger(b, b['sessions'], b['as_of'])
        self.assertTrue(orders)
        for o in orders:
            self.assertEqual(o['fill_model'], a.FILL_NEXT_OPEN_CLOSE)


if __name__ == '__main__':
    unittest.main()
