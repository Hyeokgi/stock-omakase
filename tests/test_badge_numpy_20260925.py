"""2026-09-25 — NumPy 참·거짓이 배지 증거에서 결측(None)으로 사라지던 결함.

`capture()` 는 `isinstance(v, bool)` 만 참·거짓으로 인정했다. 스캐너의 배지 다수는
pandas 값과의 비교(`current_price >= ma20` 등)라 `numpy.bool_` 이 되고, 이것은
파이썬 `bool` 이 아니다. 그래서 계산된 참·거짓이 "계산 안 됨" 과 구분 없이 None 이 됐다.

모의 객체가 아니라 **실제 NumPy 값**으로 시험한다. numpy 가 없으면 건너뛰지 않고 실패한다
(건너뛰면 이 결함이 다시 숨는다).
"""
import datetime as dt
import gzip
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

import badge_observations as B
import feature_store as F


def _context(**overrides):
    ctx = {k: False for k in B.FLAGS}
    ctx['minervini_diag'] = {'has_data': True}
    ctx.update(overrides)
    return ctx


class ConditionValueTests(unittest.TestCase):
    def test_numpy_true_and_false_are_preserved(self):
        self.assertEqual(B.condition(np.bool_(True)), (True, None))
        self.assertEqual(B.condition(np.bool_(False)), (False, None))

    def test_numpy_comparison_result_is_preserved(self):
        price, ma20 = np.float64(10500.0), np.float64(10000.0)
        value = price >= ma20
        self.assertIsInstance(value, np.bool_)
        self.assertNotIsInstance(value, bool)       # 결함의 원인 자체를 고정한다
        got, why = B.condition(value)
        self.assertIs(got, True)
        self.assertIsNone(why)

    def test_python_bool_unchanged(self):
        self.assertEqual(B.condition(True), (True, None))
        self.assertEqual(B.condition(False), (False, None))

    def test_none_is_missing_not_false(self):
        self.assertEqual(B.condition(None), (None, 'missing'))

    def test_wrong_types_are_not_coerced(self):
        for raw in (0, 1, np.int64(0), np.int64(1), 'True', 'False', 0.0, np.float64(1.0)):
            got, why = B.condition(raw)
            self.assertIsNone(got, raw)
            self.assertTrue(why.startswith('bad_type:'), (raw, why))

    def test_clean_converts_numpy_scalars(self):
        out = B.clean({'b': np.bool_(True), 'i': np.int64(7), 'f': np.float64(1.5),
                       'nan': np.float64('nan'), 'l': [np.bool_(False)]})
        self.assertEqual(out, {'b': True, 'i': 7, 'f': 1.5, 'nan': None, 'l': [False]})
        self.assertIs(type(out['b']), bool)
        self.assertIs(type(out['i']), int)
        json.dumps(out, allow_nan=False)             # 직렬화 가능해야 한다


class CaptureTests(unittest.TestCase):
    def setUp(self):
        B._active, B._features = True, {}

    def tearDown(self):
        B._active, B._features = False, {}

    def captured(self, **ctx):
        B.capture('005930', _context(**ctx))
        return B._features['005930']

    def test_capture_keeps_numpy_badges(self):
        ev = self.captured(is_jongbe_cand=np.bool_(True), is_platform_breakout=np.bool_(False))
        self.assertIs(ev['conditions']['is_jongbe_cand'], True)
        self.assertIs(ev['conditions']['is_platform_breakout'], False)
        self.assertNotIn('is_jongbe_cand', ev['condition_issues'])
        self.assertEqual(ev['conditions_version'], 'badge-conditions-v2')

    def test_capture_marks_missing_and_bad_type(self):
        ctx = _context(is_super_leader=np.int64(1))
        del ctx['is_junk']
        B.capture('005930', ctx)
        ev = B._features['005930']
        self.assertIsNone(ev['conditions']['is_super_leader'])
        self.assertEqual(ev['condition_issues']['is_super_leader'], 'bad_type:int64')
        self.assertIsNone(ev['conditions']['is_junk'])
        self.assertEqual(ev['condition_issues']['is_junk'], 'missing')

    def test_minervini_without_data_is_missing_even_if_numpy(self):
        ev = self.captured(is_minervini_template=np.bool_(False),
                           minervini_diag={'has_data': np.bool_(False)})
        self.assertIsNone(ev['conditions']['is_minervini_template'])
        self.assertEqual(ev['condition_issues']['is_minervini_template'], 'no_data')

    def test_minervini_diagnostic_numpy_values_are_bools(self):
        ev = self.captured(minervini_diag={'has_data': np.bool_(True), 'c1': np.bool_(True),
                                           'c2': np.bool_(False)})
        diag = ev['minervini_diagnostic']
        self.assertIs(diag['c1'], True)
        self.assertIs(diag['c2'], False)

    def test_numpy_metrics_are_numbers_not_strings(self):
        ev = self.captured(trading_value=np.int64(12_000_000_000), ma20=np.float64(10000.5))
        self.assertEqual(ev['metrics']['trading_value'], 12_000_000_000)
        self.assertIs(type(ev['metrics']['trading_value']), int)
        self.assertEqual(ev['metrics']['ma20'], 10000.5)


class CountsAndFilesTests(unittest.TestCase):
    def setUp(self):
        self.start = dt.datetime(2026, 9, 28, 14, 51, tzinfo=B.KST)
        self.end = self.start + dt.timedelta(minutes=3)

    def _row(self, name, code):
        row = [''] * len(F.RESULT_FIELDS)
        row[0:3] = [name, "'" + code, 100]
        return row

    def _features(self):
        B._active, B._features = True, {}
        try:
            B.capture('005930', _context(is_jongbe_cand=np.bool_(True)))
            B.capture('000660', _context(is_jongbe_cand=np.bool_(False),
                                         is_super_leader=np.int64(1)))
            return dict(B._features)
        finally:
            B._active, B._features = False, {}

    def test_counts_per_badge(self):
        payload = B.build(targets={'A': '005930', 'B': '000660', 'C': '035720'},
                          results=[self._row('A', '005930'), self._row('B', '000660')],
                          features=self._features(), started=self.start, available=self.end)
        self.assertEqual(payload['conditions_version'], 'badge-conditions-v2')
        c = payload['condition_counts']
        self.assertEqual(c['is_jongbe_cand'], {'true': 1, 'false': 1, 'missing': 0, 'bad_type': 0})
        self.assertEqual(c['is_super_leader'], {'true': 0, 'false': 1, 'missing': 1, 'bad_type': 1})
        # 수집 실패 행(C)은 배지 건수에 섞지 않는다
        self.assertEqual(payload['missing_or_failed'], 1)
        self.assertEqual(sum(c['is_junk'].values()) - c['is_junk']['bad_type'], 2)

    def test_finish_writes_counts_to_files_and_log(self):
        features = self._features()
        with tempfile.TemporaryDirectory() as d:
            folder = Path(d) / 'run'
            with patch.object(B, '_active', True), patch.object(B, '_features', features), \
                 patch.object(B, '_start', self.start), patch.object(B, '_folder', folder), \
                 patch('builtins.print') as out:
                B.finish({'A': '005930', 'B': '000660'},
                         [self._row('A', '005930'), self._row('B', '000660')], now=self.end)
            obs = json.loads(gzip.decompress((folder / 'observations.json.gz').read_bytes()))
            done = json.loads(gzip.decompress((folder / 'completed.json.gz').read_bytes()))
        self.assertIs(obs['rows'][0]['independent_conditions']['conditions']['is_jongbe_cand'], True)
        self.assertEqual(done['conditions_version'], 'badge-conditions-v2')
        self.assertEqual(done['condition_counts']['is_jongbe_cand']['true'], 1)
        logged = '\n'.join(str(c.args[0]) for c in out.call_args_list)
        self.assertIn('badge-conditions-v2', logged)
        self.assertIn('is_jongbe_cand: T=1 F=1 결측=0', logged)
        self.assertIn('타입이상 1', logged)


if __name__ == '__main__':
    unittest.main()
