import datetime as dt
import tempfile
import unittest
from unittest.mock import patch
import feature_store as F
import stability_gate as G
import production_receipt as R
import evidence_builder as E
import rank_pool as P


class AuditRegressions(unittest.TestCase):
    def test_real_static_mapping(self):
        rows = []
        for i in range(1, 4):
            row = [''] * 35
            row[1] = str(i)
            rows.append(row)
        out = F.build_rows('2026-09-21', rows, static_db={
            '000001': {'is_junk': False}, '000002': {'is_junk': True}})
        self.assertEqual([r[F.HEADER.index('is_junk')] for r in out], ['N', 'Y', 'UNKNOWN'])

    def test_legacy_membership_is_unknown(self):
        self.assertEqual(F.junk_flag(None), 'UNKNOWN')
        self.assertEqual(F.junk_flag({'is_junk': 'False'}), 'UNKNOWN')

    def test_missing_weekday_breaks_streak(self):
        rows = [dict(cycle_date=d, verdict='PASS', fingerprint='fp')
                for d in ['2026-09-15', '2026-09-17', '2026-09-18']]
        self.assertEqual(G.streak(rows, fp='fp'), 2)
        self.assertFalse(G.state(rows, fp='fp')[0])
        self.assertEqual(G.streak(rows + [rows[-1]], fp='fp'), 0)
        self.assertEqual(G.streak(list(reversed(rows)), fp='fp'), 0)

    def test_real_holidays_do_not_break_streak(self):
        rows = [dict(cycle_date=d, verdict='PASS', fingerprint='fp')
                for d in ['2026-09-22', '2026-09-23', '2026-09-28']]
        self.assertEqual(G.streak(rows, fp='fp'), 3)

    def test_ci_rejects_helper_or_calendar_changes(self):
        for name in ['naver_sources.py', 'data/market_snapshot/nontrading.txt',
                     'data/market_snapshot/calendar_scope.json']:
            self.assertFalse(G.ci_evidence_for_fingerprint(
                runs=[('old', 'success')], changed_since=lambda _: [name])[0])

    def test_ambiguous_weekday_delay_requires_cycle(self):
        now = dt.datetime(2026, 9, 22, 0, 17, tzinfo=R.KST)
        with self.assertRaises(ValueError):
            R.collector_cycle(now)
        self.assertEqual(R.collector_cycle(now, '2026-09-21'), '2026-09-21')
        with self.assertRaises(ValueError):
            R.collector_cycle(now, '2026-09-23')

    def test_missing_cycle_flag_is_not_proof(self):
        sc = {'run_id': 'r', 'payload': {
            'feature_store': {'date': '2026-09-21', 'run_id': 'r', 'fingerprint': 'fp',
                              'rows': 700, 'dropped': 0},
            'rank_pool': {'date': '2026-09-21', 'fingerprint': 'fp', 'rows': 20, 'total_dropped': 0}}}
        with patch.object(R, 'load', return_value=([sc], 0)), patch.object(
            R, 'latest', side_effect=lambda day, kind, *a, **k: sc if kind == 'scanner' else None):
            self.assertFalse(E.build('2026-09-21', 'fp')[0]['store_written'])
            sc['payload']['cycle_date_matches'] = True
            self.assertTrue(E.build('2026-09-21', 'fp')[0]['store_written'])

    def test_rank_count_is_rows_not_channels(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = tmp + '/pool.csv'
            rows = []
            for code in ['000001', '000002']:
                r = [''] * 35
                r[0], r[1], r[29] = 'name', code, 80
                rows.append(r)
            ok, _ = P.record('2026-09-21', '차트TOP2', rows, [], 29, run_id='r', path=path)
            self.assertTrue(ok)
            self.assertEqual(P.recorded_count('2026-09-21', '차트TOP2', 'r', path), 2)
