import datetime as dt
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import badge_observations as B
import feature_store as F


class BadgeEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.start = dt.datetime(2026, 9, 23, 14, 51, tzinfo=B.KST)
        self.end = self.start + dt.timedelta(minutes=3)
        self.row = [''] * len(F.RESULT_FIELDS)
        self.row[0:3] = ['Test', "'0015N0", 100]
        self.row[8] = '💎(외인집중)'
        self.features = {'0015N0': {'conditions': {
            'is_foreigner_active_buy': True, 'is_long_term_pick': True}}}

    def build(self, **kwargs):
        args = dict(targets={'Test': '0015N0'}, results=[self.row],
                    features=self.features, started=self.start, available=self.end)
        args.update(kwargs)
        return B.build(**args)

    def test_hidden_condition_combination_preserved(self):
        r = self.build()['rows'][0]
        self.assertTrue(r['independent_conditions']['conditions']['is_long_term_pick'])
        self.assertNotIn('코어픽', r['display_tajeom'])

    def test_no_winner_filter_and_all_targets_accounted(self):
        p = self.build(targets={'Test': '0015N0', 'Missing': '000660'})
        self.assertEqual(len(p['rows']), 2)
        self.assertEqual(p['missing_or_failed'], 1)
        self.assertIsNone(p['rows'][0]['independent_conditions'])

    def test_fallback_is_unknown_not_negative_badge(self):
        self.row[2] = 0
        r = self.build()['rows'][0]
        self.assertEqual(r['status'], 'MISSING_OR_FAILED')
        self.assertIsNone(r['display_tajeom'])
        self.assertFalse(r['timely_computed'])

    def test_capture_absence_is_not_a_valid_row(self):
        self.assertEqual(self.build(features={})['missing_or_failed'], 1)

    def test_after_cutoff_not_eligible(self):
        self.assertFalse(self.build(available=self.end.replace(hour=15, minute=6))['timing_ok'])

    def test_old_scan_not_eligible(self):
        self.assertFalse(self.build(started=self.start.replace(minute=30))['timing_ok'])

    def test_predecision_capture_does_not_certify_execution(self):
        p = self.build()
        self.assertTrue(p['timing_ok'])
        self.assertFalse(p['rows'][0]['execution_verified'])
        self.assertEqual(p['label_contract']['status'], 'NOT_JOINED')

    def test_holiday_exit(self):
        self.assertEqual(self.build()['label_contract']['scheduled_exit_date'], '2026-09-28')

    def test_naive_time_rejected(self):
        with self.assertRaises(ValueError):
            self.build(started=self.start.replace(tzinfo=None))

    def test_duplicate_result_refused(self):
        with self.assertRaises(ValueError):
            self.build(results=[self.row, self.row])

    def test_inputs_not_mutated(self):
        before = json.dumps(self.row)
        self.build()
        self.assertEqual(before, json.dumps(self.row))

    def test_closed_day_and_outside_window(self):
        self.assertFalse(B.in_window(self.start.replace(day=24)))
        self.assertFalse(B.in_window(self.start.replace(hour=19)))

    def test_atomic_immutable_file(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / 'e.json.gz'
            B.save_once(p, {'test': 1})
            with self.assertRaises(FileExistsError):
                B.save_once(p, {'test': 2})
            self.assertEqual(json.loads(gzip.decompress(p.read_bytes())), {'test': 1})

    def test_real_entrypoints_and_whitelist(self):
        with tempfile.TemporaryDirectory() as td:
            B.begin({'Test': '0015N0'}, now=self.start, root=td)
            B.capture('0015N0', {'is_long_term_pick': True, 'secret': 'NEVER_STORE',
                                'is_foreigner_active_buy': False, 'trading_value': 10})
            B.finish({'Test': '0015N0'}, [self.row], now=self.end)
            files = list(Path(td).rglob('*.gz'))
            self.assertEqual(len(files), 3)
            for p in files:
                self.assertNotIn('NEVER_STORE', gzip.decompress(p.read_bytes()).decode())
            out = json.loads(gzip.decompress(next(Path(td).rglob('observations.json.gz')).read_bytes()))
            self.assertEqual(out['missing_or_failed'], 0)
            self.assertIsNone(out['rows'][0]['independent_conditions']['conditions']['is_junk'])

    def test_write_failure_does_not_stop_scanner(self):
        with patch.object(B, 'save_once', side_effect=OSError('disk')):
            B.begin({}, now=self.start)
            B.capture('0015N0', {})
            B.finish({}, [], now=self.end)

    def test_wiring_before_selection(self):
        src = Path('omakase.py').read_text(encoding='utf-8')
        self.assertLess(src.index('badge_observations.finish('), src.index('candidate_pool ='))
        workflow = Path('.github/workflows/main.yml').read_text(encoding='utf-8')
        self.assertIn('python badge_observations.py --upload', workflow)
        self.assertNotIn('path: data/research_private/badge_observations/', workflow)
        import stability_gate
        self.assertIn('badge_observations.py', stability_gate.FINGERPRINT_FILES)

    def test_archive_acknowledged_and_not_repeated(self):
        with tempfile.TemporaryDirectory() as td:
            B.begin({'Test': '0015N0'}, now=self.start, root=td)
            B.capture('0015N0', {})
            B.finish({'Test': '0015N0'}, [self.row], now=self.end)
            from unittest.mock import Mock
            uploader = Mock(return_value='test-drive-id')
            self.assertEqual(B.archive(td, uploader), 0)
            self.assertEqual(B.archive(td, uploader), 0)
            self.assertEqual(uploader.call_count, 1)

    def test_archive_uncertain_is_not_retried(self):
        with tempfile.TemporaryDirectory() as td:
            B.begin({}, now=self.start, root=td)
            from unittest.mock import Mock
            uploader = Mock(side_effect=TimeoutError())
            self.assertEqual(B.archive(td, uploader), 1)
            self.assertEqual(B.archive(td, uploader), 1)
            self.assertEqual(uploader.call_count, 1)

    def test_incomplete_capture_remains_a_failure(self):
        with tempfile.TemporaryDirectory() as td:
            B.begin({}, now=self.start, root=td)
            self.assertEqual(B.archive(td, lambda n, d: 'test-id'), 1)

    def test_latest_before_decision_not_best_coverage(self):
        a = self.build()
        b = self.build(available=self.end + dt.timedelta(minutes=3), features={})
        late = self.build(available=self.end.replace(hour=15, minute=6))
        chosen = B.select_predecision([late, a, b], '2026-09-23')
        self.assertIs(chosen, b)
        self.assertEqual(chosen['missing_or_failed'], 1)

    def test_no_timely_batch_does_not_use_postdecision(self):
        late = self.build(available=self.end.replace(hour=15, minute=6))
        self.assertIsNone(B.select_predecision([late], '2026-09-23'))
