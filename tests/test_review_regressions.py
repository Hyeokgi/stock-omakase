"""Sept 13 audit regressions. Synthetic only; no network or account credentials."""
import ast
import contextlib
import datetime
import io
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

import hyeoks_verdict as v
from nightly_quotes import after_hours_header, naver_quote, quote_label


def dated(values, start):
    return {(start + datetime.timedelta(days=i)).isoformat(): [x]
            for i, x in enumerate(values)}


class VerdictRegressions(unittest.TestCase):
    def sample(self, channel='차트TOP2', offset=0, constant=False):
        a = [5 + i / 100 for i in range(30)]
        b = [i / 100 if constant else (-1) ** i * .1 for i in range(30)]
        start = datetime.date(2026, 6, 1)
        data = {channel: {5: a}, v.CONTROL: {5: b}}
        bydate = {channel: {5: dated(a, start)}, v.CONTROL: {5: dated(b, start + datetime.timedelta(days=offset))}}
        return data, {channel: 30, v.CONTROL: 30}, bydate

    def report(self, sample):
        data, raw, bydate = sample
        return v.build_report(data, raw, {}, '2026-09-13', bydate)

    def test_one_common_day_never_falls_back_to_welch(self):
        text, rows, conf, passed = self.report(self.sample(offset=29))
        row = next(r for r in rows if r['ch'] == '차트TOP2')
        self.assertGreater(row['wt'], 2)
        self.assertEqual(row['pk'], 1)
        self.assertIsNone(row['t'])
        self.assertEqual(conf, [])
        self.assertFalse(passed)
        self.assertIn('짝 검정 미성립', row['v'])

    def test_missing_dates_never_approve(self):
        data, raw, _ = self.sample()
        _, rows, conf, passed = v.build_report(data, raw, {}, '2026-09-13')
        self.assertFalse(conf or passed)
        self.assertIsNone(next(r for r in rows if r['ch'] == '차트TOP2')['t'])

    def test_zero_difference_variance_never_falls_back(self):
        data, raw, bydate = self.sample()
        # Integers give exact constant differences, avoiding float roundoff.
        data['차트TOP2'][5] = list(range(5, 35))
        data[v.CONTROL][5] = list(range(30))
        for ch in data:
            bydate[ch][5] = dated(data[ch][5], datetime.date(2026, 6, 1))
        _, rows, conf, passed = self.report((data, raw, bydate))
        self.assertIsNone(next(r for r in rows if r['ch'] == '차트TOP2')['t'])
        self.assertFalse(conf or passed)

    def test_nominal_holm_cannot_approve_until_protocol_fixed(self):
        _, rows, conf, passed = self.report(self.sample())
        row = next(r for r in rows if r['ch'] == '차트TOP2')
        self.assertTrue(row['nominal_holm_pass'])
        self.assertTrue(conf)
        self.assertFalse(passed)
        self.assertEqual(row['v'], v.INFERENCE_HOLD)

    def test_milestone_no_pair_no_fallback(self):
        data, raw, bd = self.sample('리포트TOP2_단기', offset=29)
        row = v.milestone_rows(data, raw, bd, '2026-09-13')[0]
        self.assertIsNone(row['t'])
        self.assertFalse(row['hit'] or row['threshold_met'])

    def test_milestone_nominal_threshold_not_replication_start(self):
        data, raw, bd = self.sample('리포트TOP2_단기')
        rows = v.milestone_rows(data, raw, bd, '2026-09-13')
        self.assertTrue(rows[0]['threshold_met'])
        self.assertFalse(rows[0]['hit'])
        self.assertGreaterEqual(rows[0]['need'], v.MIN_N)
        for scheduled in (False, True):
            text = v.milestone_report(rows, '2026-09-13', scheduled)
            self.assertIn('시작일을 확정하지 않는다', text)
            self.assertNotIn('수집 실패는 아니다', text)

    def ledger_row(self, day, ch='리포트TOP2_단기'):
        row = [''] * 38
        row[v.C_ENTRY_DATE], row[v.C_CHANNEL] = day, ch
        row[v.STOCK_COL[5]], row[v.INDEX_COL[5]] = '3', '1'
        return row

    def test_policy_cutoff_reconciles_without_deleting_history(self):
        rows = [[''] * 38, self.ledger_row('2026-09-06'), self.ledger_row('2026-09-07'),
                self.ledger_row('bad-date'), self.ledger_row('2026-09-06', v.CONTROL)]
        original = [list(r) for r in rows]
        data, raw, skipped, bd = v.collect(rows, datetime.date(2026, 10, 1), current_policy=True)
        self.assertEqual(raw['리포트TOP2_단기'], 1)
        self.assertEqual(raw[v.CONTROL], 1)
        self.assertEqual(skipped['구정책·정책일미상(판정 제외)'], 2)
        self.assertEqual(set(bd['리포트TOP2_단기'][5]), {'2026-09-07'})
        self.assertTrue(v.reconcile(rows, skipped)[2])
        self.assertEqual(rows, original)
        self.assertEqual(v.collect(rows, datetime.date(2026, 10, 1))[1]['리포트TOP2_단기'], 3)

    def test_regime_boundary_uses_paired_signal_dates(self):
        bd = {'차트TOP2': {5: {'2026-09-13': [1, 3], '2026-09-14': [5], '2026-09-15': [99]}},
              v.CONTROL: {5: {'2026-09-13': [1], '2026-09-14': [2]}}}
        self.assertEqual(v.regime_summary(bd, '차트TOP2', 5),
                         {'개편 전': {'days': 1, 'mean': 1}, '개편 후': {'days': 1, 'mean': 3}})


class QuoteRegressions(unittest.TestCase):
    def test_empty_data_not_flat(self):
        for venue in ('NXT', '시외'):
            self.assertEqual(naver_quote({}, venue), (None, None))

    def test_genuine_flat_preserved(self):
        label, venue = naver_quote({'closePrice': '100', 'nxtClosePrice': '100',
                                    'nxtFluctuationsRatio': '0'}, 'NXT')
        self.assertEqual(venue, 'NXT')
        self.assertIn('0.00%', label)

    def test_venues_never_cross_fill(self):
        ext = {'closePrice': '100', 'timeExtraClosePrice': '110'}
        self.assertEqual(naver_quote(ext, 'NXT'), (None, None))
        self.assertEqual(naver_quote(ext, '시외')[1], '시외')
        self.assertEqual(naver_quote({'nxtClosePrice': 100, 'nxtFluctuationsRatio': 1}, '시외'), (None, None))

    def test_zero_regular_without_rate_not_divided(self):
        self.assertEqual(naver_quote({'closePrice': 0, 'nxtClosePrice': 100}, 'NXT'), (None, None))

    def test_missing_and_nonfinite_values_not_valid(self):
        for price, rate in ((0, 0), (100, None), (100, 'nan'), ('inf', 1), (True, 0), (-1, 0)):
            self.assertIsNone(quote_label(price, rate, 'test'))

    def test_headers_preserve_unrelated_columns(self):
        before = [f'column{i}' for i in range(38)]
        after = after_hours_header(before, {'label': 'after', 'window': '16-20'})
        self.assertEqual(before[20], after[20])
        self.assertEqual(before[22], after[22])
        self.assertEqual(after[28:], before[28:])
        self.assertEqual(len(after_hours_header([], {'label': 'x', 'window': 'y'})), 28)


class NightlyIntegration(unittest.TestCase):
    def setUp(self):
        # Execute actual production function bodies, without importing optional
        # API SDKs or top-level network setup. All external objects are mocked.
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'hyeoks_nightly.py').read_text(encoding='utf-8'))
        tree.body = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
        self.env = dict(datetime=datetime, quote_label=quote_label, naver_quote=naver_quote,
                        after_hours_header=after_hours_header, KST=v.KST,
                        REFORM_DATE=datetime.date(2026, 9, 14))
        exec(compile(tree, 'hyeoks_nightly.py', 'exec'), self.env)

    def req(self, body, status=200):
        response = Mock(status_code=status)
        response.json.return_value = body
        req = Mock()
        req.get.return_value = response
        return req

    def test_kis_empty_and_error_are_not_flat_or_reset(self):
        call = self.env['get_after_hours_price']
        self.assertIn('미확인', call('000001', {}, self.req({'rt_cd': '0', 'output': {}})))
        self.assertIn('API오류', call('000001', {}, self.req({}, 503)))

    def test_kis_missing_nxt_rate_not_zero(self):
        self.assertIsNone(self.env['get_nxt_kis_price']('000001', {}, self.req({'rt_cd': '0', 'output': {'stck_prpr': 100}})))

    def test_actual_naver_wrapper_respects_venue(self):
        body = {'closePrice': 100, 'timeExtraClosePrice': 110}
        call = self.env['get_naver_after_price']
        self.assertEqual(call('000001', self.req(body), 'NXT'), (None, None))
        self.assertEqual(call('000001', self.req(body), '시외')[1], '시외')

    def test_regime_boundary(self):
        call = self.env['after_hours_regime']
        self.assertEqual(call(datetime.date(2026, 9, 13))['label'], '시간외단일가')
        self.assertEqual(call(datetime.date(2026, 9, 14))['label'], '애프터마켓')

    def setup_main(self, hour=18):
        class FixedClock(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 14, hour, 17, tzinfo=tz)
        clock = SimpleNamespace(datetime=FixedClock, date=datetime.date, timedelta=datetime.timedelta)
        row = ['value'] * 38
        row[0], row[1] = 'synthetic', '000001'
        sheet = Mock()
        sheet.get_all_values.return_value = [[f'h{i}' for i in range(38)], row]
        settings = Mock()
        settings.get_all_values.return_value = [['KIS_TOKEN', 'fake-test-token']]
        doc = Mock()
        doc.worksheet.side_effect = lambda name: settings if name == '⚙️설정' else sheet
        client = Mock()
        client.open_by_url.return_value = doc
        gspread = Mock()
        gspread.authorize.return_value = client
        self.env.update(datetime=clock, os=SimpleNamespace(environ={'GCP_CREDENTIALS': '{}'}),
                        json=json, gspread=gspread, ServiceAccountCredentials=Mock(), requests=Mock(),
                        SHEET_URL='test-only', KIS_APP_KEY='test', KIS_APP_SECRET='test',
                        time=Mock(), send_telegram=Mock(),
                        get_after_hours_price=Mock(return_value='미확인'),
                        get_nxt_kis_price=Mock(return_value=None),
                        get_naver_after_price=Mock(side_effect=lambda code, req, venue: ('+1.00% [시외]', '시외') if venue == '시외' else (None, None)),
                        get_chart_data=Mock(return_value=('ma', 'high')))
        return sheet

    def test_main_writes_only_owned_columns_and_keeps_unknown_nxt(self):
        sheet = self.setup_main()
        with contextlib.redirect_stdout(io.StringIO()):
            self.env['main']()
        updates = {x['range']: x['values'] for x in sheet.batch_update.call_args.args[0]}
        self.assertEqual(set(updates), {'AA1:AB1', 'AA2:AA2', 'AB2:AB2', 'F2:F2', 'M2:M2'})
        self.assertIn('미검증', updates['AA2:AA2'][0][0])
        self.assertIn('조회 2026-09-14', updates['AA2:AA2'][0][0])
        self.assertIn('미확인(NXT', updates['AB2:AB2'][0][0])
        self.assertEqual(sheet.batch_update.call_args.kwargs['value_input_option'], 'RAW')

    def test_phase1_does_not_rewrite_phase2_columns(self):
        sheet = self.setup_main(hour=17)
        with contextlib.redirect_stdout(io.StringIO()):
            self.env['main']()
        self.assertEqual({u['range'] for u in sheet.batch_update.call_args.args[0]}, {'AA1:AB1', 'AA2:AA2'})

    def test_save_failure_nonzero(self):
        sheet = self.setup_main()
        sheet.batch_update.side_effect = RuntimeError('synthetic write failure')
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(self.env['main'](), 1)


if __name__ == '__main__':
    unittest.main()
