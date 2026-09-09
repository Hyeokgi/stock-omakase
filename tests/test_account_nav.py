import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import hyeoks_account as a
from hyeoks_verdict import EXPECTED_HEADER


def fixture():
    days = ['2026-09-01','2026-09-02','2026-09-03','2026-09-04','2026-09-07','2026-09-08']
    header = ['']*38
    for k,v in EXPECTED_HEADER.items(): header[k]=v
    for k,v in {3:'종목명',4:'종목코드',5:'주도테마',16:'진입가(T+1시가)'}.items():header[k]=v
    row = ['']*38
    row[:6]=['one',days[0],'차트TOP2','A','000001','theme']
    return dict(schema='account-input-v2',as_of=days[-1],captured_at='2026-09-08T18:00:00+09:00',
                calendar_source='SYNTHETIC',price_source='SYNTHETIC',price_basis='consistent_adjusted_ohlc',
                sessions=days,rows=[header,row],theme_map={days[0]:{'000001':['theme']}},
                config=dict(initial_cash=1000,ticket_cash=1000,max_stock_weight=1,max_theme_weight=1,buy_fee=0,sell_fee=0),
                prices={'000001':{d:dict(open=100,high=100,low=100,close=100,tradable=True) for d in days}})


def order(ident='one',code='000001',entry='2026-09-02',exit='2026-09-08',themes=None):
    return dict(id=ident,channel='strategy',code=code,entry=entry,exit=exit,themes=themes or ['theme'])


class AccountTests(unittest.TestCase):
    def test_flat_cash_reconciles(self):
        r=a.calculate(fixture())['accounts']['strategy']
        self.assertEqual(r['return_pct'],0)
        self.assertEqual(len(r['closed_trades']),1)
        for x in r['series']:
            self.assertEqual(x['cash']+x['market_value'],x['nav'])

    def test_loss_is_loss_not_positive_alpha(self):
        b=fixture();b['prices']['000001'][b['as_of']].update(low=95,close=95)
        b['rows'][1][19]='-5';b['rows'][1][23]='-10'
        self.assertAlmostEqual(a.calculate(b)['accounts']['strategy']['return_pct'],-5)

    def test_raw_marks_do_not_change_nav(self):
        b=fixture();r=a.calculate(b)['accounts']
        b['rows'][1][19]='99';b['rows'][1][23]='-99'
        self.assertEqual(r,a.calculate(b)['accounts'])

    def test_open_long_kept_until_asof(self):
        b=fixture();b['rows'][1][2]='리포트TOP2_장기'
        r=a.calculate(b)['accounts']['strategy']
        self.assertEqual(len(r['open_positions']),1)
        self.assertEqual(r['closed_trades'],[])
        self.assertIsNone(r['closed_win_rate_pct'])
        self.assertEqual(r['series'][-1]['open_positions'],1)

    def test_no_sparse_mark_still_holds_position(self):
        b=fixture();b['as_of']=b['sessions'][2]
        self.assertEqual(len(a.calculate(b)['accounts']['strategy']['open_positions']),1)

    def test_signal_day_not_entry_day(self):
        r=a.calculate(fixture())['accounts']['strategy']
        self.assertEqual(r['series'][0]['open_positions'],0)
        self.assertEqual(r['closed_trades'][0]['entry'],'2026-09-02')

    def test_pending_entry_is_not_missing_position(self):
        b=fixture();b['rows'][1][1]=b['as_of']
        r=a.calculate(b)
        self.assertEqual(r['diagnostics'][0]['status'],'pending_entry')

    def test_calendar_holiday_not_weekday_approximation(self):
        b=fixture();b['sessions'].remove('2026-09-03')
        r=a.calculate(b)['accounts']['strategy']
        self.assertEqual(len(r['open_positions']),1)

    def test_correct_high_water_nav_drawdown(self):
        s=[dict(date=str(i),nav=v) for i,v in enumerate([100,200,150])]
        self.assertEqual(a.nav_metrics(s,100)['mdd_pct'],25)

    def test_initial_cash_is_peak_before_first_loss(self):
        self.assertAlmostEqual(a.nav_metrics([dict(date='x',nav=90)],100)['mdd_pct'],10)

    def test_synchronized_hedged_prices_no_phantom_drawdown(self):
        b=fixture();days=b['sessions'];prices=b['prices']
        prices['000002']=copy.deepcopy(prices['000001'])
        prices['000001'][days[1]].update(high=110,close=110)
        prices['000002'][days[1]].update(low=90,close=90)
        r=a.simulate([order(),order('two','000002')],days,prices,a.Config(1000,500,1,1,0,0))
        self.assertEqual(r['mdd_pct'],0)

    def test_missing_price_aborts_no_stale_fill(self):
        b=fixture();del b['prices']['000001']['2026-09-03']
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_invalid_ohlc_aborts(self):
        b=fixture();b['prices']['000001']['2026-09-03']['close']=200
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_nonfinite_price_aborts(self):
        b=fixture();b['prices']['000001']['2026-09-03']['close']=float('nan')
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_future_row_aborts(self):
        b=fixture();b['rows'][1][1]='2099-01-01'
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_duplicate_id_aborts(self):
        b=fixture();b['rows'].append(b['rows'][1][:])
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_schema_swap_aborts(self):
        b=fixture();b['rows'][0][19],b['rows'][0][23]=b['rows'][0][23],b['rows'][0][19]
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_missing_calendar_provenance_aborts(self):
        b=fixture();del b['calendar_source']
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_missing_theme_history_aborts(self):
        b=fixture();b['theme_map']={'2026-09-02':{'000001':['theme']}}
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_no_cash_reuse_from_evening_sale(self):
        b=fixture();r=a.simulate([order(exit='2026-09-03'),order('two',entry='2026-09-03')],
                                b['sessions'],b['prices'],a.Config(1000,1000,1,1,0,0))
        self.assertEqual(r['rejected'][0]['reason'],'insufficient_cash')
        self.assertEqual(len(r['closed_trades']),1)

    def test_stock_cap_combines_channels_and_dates(self):
        b=fixture();r=a.simulate([order(),order('two',entry='2026-09-03')],b['sessions'],b['prices'],a.Config(1000,400,.6,1,0,0))
        self.assertEqual(r['rejected'][0]['reason'],'stock_cap')

    def test_theme_cap_aggregates_distinct_codes(self):
        b=fixture();b['prices']['000002']=copy.deepcopy(b['prices']['000001'])
        r=a.simulate([order(),order('two','000002')],b['sessions'],b['prices'],a.Config(1000,400,1,.6,0,0))
        self.assertEqual(r['rejected'][0]['reason'],'theme_cap')

    def test_caps_checked_after_fees(self):
        b=fixture();r=a.simulate([order()],b['sessions'],b['prices'],a.Config(1000,600,.5,1,.01,.01))
        self.assertEqual(r['rejected'][0]['reason'],'stock_cap')

    def test_fees_and_slippage_reduce_cash_once_each_leg(self):
        b=fixture();r=a.simulate([order()],b['sessions'],b['prices'],a.Config(1000,1000,1,1,.01,.02,.01))
        # 9 shares: buy 101*1.01, sell 99*.98
        expected=1000-9*101*1.01+9*99*.98
        self.assertAlmostEqual(r['final_nav'],expected)
        self.assertAlmostEqual(r['fees'],9*101*.01+9*99*.02)

    def test_halted_exit_remains_open(self):
        b=fixture();b['prices']['000001'][b['as_of']]['tradable']=False
        r=a.calculate(b)['accounts']['strategy']
        self.assertEqual(len(r['open_positions']),1)
        self.assertEqual(r['closed_trades'],[])

    def test_nontradable_entry_rejected(self):
        b=fixture();b['prices']['000001']['2026-09-02']['tradable']=False
        self.assertEqual(a.calculate(b)['accounts']['strategy']['rejected'][0]['reason'],'not_tradable')

    def test_random_badge_and_indices_are_separate(self):
        b=fixture()
        for n,ch in enumerate(['랜덤2','랜덤2_배지','지수벤치_KOSPI']):
            row=b['rows'][1][:];row[0]=str(n);row[2]=ch;b['rows'].append(row)
        b['indices']={'KOSPI':copy.deepcopy(b['prices']['000001'])}
        r=a.calculate(b)
        for name in ['strategy','random','random_badge']:
            self.assertEqual(len(r['accounts'][name]['closed_trades']),1)
        self.assertEqual(list(r['benchmarks']),['KOSPI'])

    def test_order_is_deterministic(self):
        b=fixture();o=[order(),order('two')];c=a.Config(1000,700,1,1,0,0)
        self.assertEqual(a.simulate(o,b['sessions'],b['prices'],c),a.simulate(o[::-1],b['sessions'],b['prices'],c))

    def test_atomic_save_unique_and_readback(self):
        b=fixture();r=a.calculate(b)
        with tempfile.TemporaryDirectory() as d:
            p=a.save_bundle(b,r,d);q=a.save_bundle(b,r,d)
            self.assertNotEqual(p,q)
            self.assertTrue((p/'COMPLETE').exists())
            m=json.loads((p/'manifest.json').read_text())
            for f,h in m['files'].items():self.assertEqual(hashlib.sha256((p/f).read_bytes()).hexdigest(),h)

    def test_failed_save_has_no_complete_run(self):
        b=fixture();r=a.calculate(b)
        with tempfile.TemporaryDirectory() as d:
            with patch.object(a.os,'fsync',side_effect=OSError('disk')):
                with self.assertRaises(OSError):a.save_bundle(b,r,d)
            self.assertEqual(list(Path(d).glob('*/COMPLETE')),[])

    def test_cli_requires_frozen_input(self):
        with self.assertRaises(SystemExit) as e:a.main([])
        self.assertEqual(e.exception.code,2)

    def test_raw_prices_not_silently_mixed_with_adjusted(self):
        b=fixture();b['price_basis']='raw'
        with self.assertRaises(a.InputError):a.calculate(b)


if __name__=='__main__':unittest.main()
