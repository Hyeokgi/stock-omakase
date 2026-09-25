"""Actual NumPy scalars through capture -> gzip -> archive, never real upload."""
import contextlib
import datetime as dt
import gzip
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

import numpy as np
import badge_observations as B
import feature_store as F


class BadgeBooleanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(setattr, B, '_active', False)
        self.start = dt.datetime(2026, 9, 28, 14, 52, tzinfo=B.KST)
        self.end = self.start.replace(minute=55)
        self.targets = {'Test': '005930'}
        self.row = [''] * len(F.RESULT_FIELDS)
        self.row[:3] = ['Test', '005930', 100]
        B.begin(self.targets, now=self.start, root=self.tmp.name)

    def capture(self, value):
        context = {key: value for key in B.FLAGS}
        context['minervini_diag'] = {'has_data': np.bool_(True), 'c1': value}
        B.capture('005930', context)
        return B._features['005930']

    def finish(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            B.finish(self.targets, [self.row], now=self.end)
        path = next(Path(self.tmp.name).rglob('observations.json.gz'))
        return json.loads(gzip.decompress(path.read_bytes())), output.getvalue()

    def test_numpy_true_and_false_for_every_flag(self):
        for value in (np.bool_(True), np.bool_(False), True, False):
            with self.subTest(value=repr(value), kind=type(value).__name__):
                evidence = self.capture(value)
                for key in B.FLAGS:
                    self.assertIs(evidence['conditions'][key], bool(value))
                self.assertEqual(evidence['condition_missing_reasons'], {})

    def test_scanner_short_circuit_expression_stays_false(self):
        # pandas mean produces this NumPy scalar. Python `and` returns the
        # first false operand, not necessarily a builtin bool.
        smi_ratio = 50 / np.mean([100, 200])
        value = (smi_ratio >= 3.0) and (True)
        self.assertIsInstance(value, np.bool_)
        self.assertIs(self.capture(value)['conditions']['is_foreigner_active_buy'], False)

    def test_invalid_values_are_not_truth_coerced(self):
        for value in (0, 1, 'False', 'True', '', [], {}, float('nan'),
                      np.int64(1), np.float64(0), np.array(True),
                      np.array([True, False])):
            with self.subTest(kind=type(value).__name__, value=repr(value)):
                evidence = self.capture(value)
                self.assertIsNone(evidence['conditions']['is_foreigner_active_buy'])
                self.assertEqual(evidence['condition_missing_reasons']['is_foreigner_active_buy'],
                                 'invalid_boolean_type')

    def test_arbitrary_truth_method_is_never_called(self):
        class NotABoolean:
            def __bool__(self):
                raise AssertionError('must not coerce arbitrary object')
        self.assertIsNone(self.capture(NotABoolean())['conditions']['is_jongbe_cand'])

    def test_absent_and_explicit_null_are_distinct(self):
        B.capture('005930', {'is_jongbe_cand': None})
        reasons = B._features['005930']['condition_missing_reasons']
        self.assertEqual(reasons['is_jongbe_cand'], 'null_input')
        self.assertEqual(reasons['is_foreigner_active_buy'], 'missing_input')

    def test_history_absence_remains_unknown(self):
        B.capture('005930', {'is_minervini_template': np.bool_(True),
                            'minervini_diag': {'has_data': np.bool_(False)}})
        evidence = B._features['005930']
        self.assertIsNone(evidence['conditions']['is_minervini_template'])
        self.assertEqual(evidence['condition_missing_reasons']['is_minervini_template'],
                         'history_not_verified')

    def test_numpy_diagnostics_are_json_booleans_not_strings(self):
        evidence = self.capture(np.bool_(True))
        self.assertIs(evidence['minervini_diagnostic']['has_data'], True)
        self.assertIs(evidence['minervini_diagnostic']['c1'], True)
        self.assertEqual(evidence['condition_schema'], B.CONDITION_SCHEMA)

    def test_real_gzip_roundtrip_and_quality_denominator(self):
        self.capture(np.bool_(False))
        payload, output = self.finish()
        self.assertEqual(payload['missing_or_failed'], 0)
        self.assertTrue(payload['timing_ok'])
        for counts in payload['condition_quality'].values():
            self.assertEqual(counts, {'true': 0, 'false': 1, 'missing': 0, 'invalid_type': 0})
        self.assertIs(payload['rows'][0]['independent_conditions']['conditions']['is_jongbe_cand'], False)
        self.assertEqual(payload['label_contract']['status'], 'NOT_JOINED')
        self.assertIn('invalid_type=0', output)
        completed = json.loads(gzip.decompress(next(Path(self.tmp.name).rglob('completed.json.gz')).read_bytes()))
        self.assertEqual(completed['condition_quality'], payload['condition_quality'])

    def test_missing_fields_do_not_become_missing_rows(self):
        self.capture(None)
        payload, _ = self.finish()
        self.assertEqual(payload['missing_or_failed'], 0)
        self.assertEqual(payload['condition_quality']['is_jongbe_cand']['missing'], 1)
        self.assertEqual(payload['condition_quality']['is_jongbe_cand']['false'], 0)

    def test_failed_row_is_in_every_field_denominator(self):
        self.capture(True)
        self.row[2] = 0
        payload, _ = self.finish()
        self.assertEqual(payload['missing_or_failed'], 1)
        for counts in payload['condition_quality'].values():
            self.assertEqual(counts['missing'], 1)
            self.assertEqual(counts['true'], 0)

    def test_bad_types_are_counted_and_warned_without_stopping(self):
        self.capture('False')
        payload, output = self.finish()
        self.assertEqual(payload['condition_quality']['is_jongbe_cand']['invalid_type'], 1)
        self.assertIn('::warning::badge conditions', output)

    def test_archive_preserves_boolean_types_without_real_network(self):
        self.capture(np.bool_(True))
        self.finish()
        uploader = Mock(return_value='fake-drive-id')
        self.assertEqual(B.archive(self.tmp.name, uploader=uploader), 0)
        bundle = json.loads(gzip.decompress(uploader.call_args.args[1]))
        payload = bundle['parts']['observations.json.gz']
        self.assertIs(payload['rows'][0]['independent_conditions']['conditions']['is_jongbe_cand'], True)
        self.assertEqual(payload['condition_schema'], B.CONDITION_SCHEMA)

    def test_helper_still_imports_without_site_packages(self):
        result = subprocess.run([sys.executable, '-S', '-c',
                                 'import badge_observations as b; assert b.BOOLEAN_TYPES == (bool,)'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ci_installs_real_numpy_for_this_regression(self):
        workflow = Path('.github/workflows/review_regressions.yml').read_text(encoding='utf-8')
        self.assertTrue(any('pip install' in line and 'numpy' in line for line in workflow.splitlines()))
