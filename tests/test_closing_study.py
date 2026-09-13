import ast
import copy
import datetime
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import xml.etree.ElementTree as ET

import hyeoks_closing_bet as cb
from closing_study import split_windows


def rows(n):
    return [{'date': (datetime.date(2026, 1, 1) + datetime.timedelta(days=i)).isoformat(),
             'rel': (-1)**i * .001} for i in range(n)]


class FrozenWindows(unittest.TestCase):
    def setUp(self):
        self.state = {}
        self.calibrate = Mock(return_value={'sigma': .001, 'need': 40, 'window': 40, 'over': False})

    def split(self, data):
        return split_windows(data, self.state, 20, self.calibrate, {'test': True})

    def test_only_pilot_sets_length(self):
        self.split(rows(25))
        changed = rows(70)
        for r in changed[25:]:
            r['rel'] = 99
        _, pilot, main = self.split(changed)
        self.calibrate.assert_called_once_with([r['rel'] for r in rows(20)])
        self.assertEqual(len(pilot), 20)
        self.assertEqual(len(main), 40)
        self.assertEqual(main[0]['date'], changed[20]['date'])
        self.assertEqual(main[-1]['date'], changed[59]['date'])

    def test_pilot_correction_rejected(self):
        self.split(rows(25))
        data = rows(25)
        data[0]['rel'] += 1
        with self.assertRaises(ValueError):
            self.split(data)

    def test_partial_main_correction_rejected(self):
        self.split(rows(25))
        data = rows(30)
        data[22]['rel'] += 1
        with self.assertRaises(ValueError):
            self.split(data)

    def test_future_rows_do_not_change_final(self):
        _, _, first = self.split(rows(60))
        before = copy.deepcopy(self.state)
        _, _, second = self.split(rows(100))
        self.assertEqual(first, second)
        self.assertEqual(before, self.state)

    def test_missing_frozen_day_rejected(self):
        self.split(rows(60))
        with self.assertRaises(ValueError):
            self.split(rows(70)[1:])

    def test_no_calibration_before_20(self):
        self.assertIsNone(self.split(rows(19))[0])
        self.calibrate.assert_not_called()

    def test_policy_change_rejected(self):
        self.split(rows(25))
        with self.assertRaises(ValueError):
            split_windows(rows(30), self.state, 20, self.calibrate, {'test': False})

    def test_report_replays_after_future_data_and_json_roundtrip(self):
        import json
        with tempfile.TemporaryDirectory() as tmp:
            dates = cb._bizdays('2026-01-05', 80)
            cb._synth(tmp, dates)
            state = {}
            first, stage = cb.report(tmp, '2026-06-01', state)
            self.assertEqual(stage, 2)
            self.assertIn('파일럿 20일 제외', first)
            frozen = json.loads(json.dumps(state))
            cb._synth(tmp, cb._bizdays('2026-01-05', 100))
            second, stage = cb.report(tmp, '2026-07-01', frozen)
            self.assertEqual(stage, 2)
            self.assertEqual(first, second)
            self.assertEqual(state, frozen)

    def test_zero_pilot_variance_is_hold_not_crash(self):
        data = [{'date': r['date'], 'ok': True, 'matured': True, 'rel': 0.0, 'drop': {}}
                for r in rows(25)]
        with patch.object(cb, 'collect', return_value=(data, [r['date'] for r in data])):
            text, stage = cb.report('synthetic', state={})
        self.assertEqual(stage, 1)
        self.assertIn('분산 추정 불가/0', text)


class HolidayTests(unittest.TestCase):
    def setUp(self):
        self.cal = cb.load_nontrading(str(Path(__file__).resolve().parents[1] / cb.SNAP_DIR))

    def test_chuseok_exit(self):
        self.assertEqual(cb.next_trading_day('2026-09-23', self.cal), '2026-09-28')
        self.assertEqual(cb.next_trading_day('2026-09-28', self.cal), '2026-09-29')

    def test_october_holidays(self):
        self.assertEqual(cb.next_trading_day('2026-10-02', self.cal), '2026-10-06')
        self.assertEqual(cb.next_trading_day('2026-10-08', self.cal), '2026-10-12')

    def test_unknown_year_fails(self):
        with self.assertRaises(ValueError):
            cb.next_trading_day('2026-12-30', self.cal)

    def test_malformed_calendar_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / 'nontrading.txt').write_text('2026-99-01', encoding='utf-8')
            with self.assertRaises(ValueError):
                cb.load_nontrading(tmp)

    def test_missing_open_is_not_mature_even_quality_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            dates = ['2026-09-23', '2026-09-28', '2026-09-29']
            cb._synth(tmp, dates)
            # No holiday override: Sep 24 absent is a gap, not inferred holiday.
            ds, _ = cb.collect(tmp, with_returns=False)
            self.assertFalse(ds[0]['ok'])
            self.assertEqual(ds[0]['why'], '익일달력결측')

    def test_nonfinite_price_rejected(self):
        self.assertEqual(cb._f('nan'), 0)
        self.assertEqual(cb._f('inf'), 0)

    def test_open_missing_quality_does_not_count_mature(self):
        with tempfile.TemporaryDirectory() as tmp:
            dates = ['2026-09-10', '2026-09-11']
            cb._synth(tmp, dates)
            bad = [cb._row(f'{i:06d}', 1000, 10**10, op=0) for i in range(60)]
            for slot in ('1300', '1505'):
                cb._write(tmp, dates[1], slot, bad)
            ds, _ = cb.collect(tmp, with_returns=False)
            self.assertFalse(ds[0]['ok'])
            self.assertNotIn('rel', ds[0])


class CollectorCalendarTests(unittest.TestCase):
    def test_analyst_never_disables_tls(self):
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / 'hyeoks_analyst.py').read_text(encoding='utf-8'))
        bad = [n for n in ast.walk(tree) if isinstance(n, ast.keyword)
               and n.arg == 'verify' and isinstance(n.value, ast.Constant) and n.value.value is False]
        self.assertEqual(bad, [])

    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        tree = ast.parse((root / 'hyeoks_market_snapshot.py').read_text(encoding='utf-8'))
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'is_trading_day')
        self.session = Mock()
        scope = {'SESSION': self.session, 'ET': ET}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), '<collector-calendar>', 'exec'), scope)
        self.call = scope['is_trading_day']

    def test_holiday_never_calls_network(self):
        self.assertFalse(self.call('2026-09-24'))
        self.session.get.assert_not_called()

    def test_missing_today_is_failure_not_holiday(self):
        self.session.get.return_value.text = '<protocol></protocol>'
        with self.assertRaises(RuntimeError):
            self.call('2026-09-28')

    def test_verified_tls_and_http_status(self):
        self.session.get.return_value.text = '<protocol><item data="20260928|1|2|3"/></protocol>'
        self.assertTrue(self.call('2026-09-28'))
        self.session.get.return_value.raise_for_status.assert_called_once()
        self.assertNotIn('verify', self.session.get.call_args.kwargs)

    def test_http_failure_not_silenced(self):
        self.session.get.return_value.raise_for_status.side_effect = ValueError('HTTP 503')
        with self.assertRaises(RuntimeError):
            self.call('2026-09-28')


if __name__ == '__main__':
    unittest.main()
