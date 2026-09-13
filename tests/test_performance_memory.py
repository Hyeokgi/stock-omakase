import ast
import contextlib
import datetime
import io
from pathlib import Path
from types import SimpleNamespace
import unittest

from hyeoks_performance_memory import performance_summary, MEMORY_VERSION

ROOT = Path(__file__).resolve().parents[1]
TREE = ast.parse((ROOT / 'omakase.py').read_text(encoding='utf-8'))
HEADER = ast.literal_eval(next(n.value for n in TREE.body if isinstance(n, ast.Assign)
                              and any(isinstance(t, ast.Name) and t.id == 'BT_HEADER' for t in n.targets)))
TODAY = datetime.date(2026, 10, 30)


def row(**fields):
    data = {'trade_id': 'test-1', '채널': '리포트TOP2_단기', '진입일': '2026-09-07',
            '진입가(T+1시가)': '10000'}
    data.update(fields)
    return [data.get(name, '') for name in HEADER]


def summary(*rows):
    return performance_summary([HEADER, *rows], today=TODAY)


class PerformanceMemoryTests(unittest.TestCase):
    def test_entry_price_never_becomes_return(self):
        self.assertEqual(summary(row()), '')

    def test_short_uses_t5_not_t3_or_t1(self):
        text = summary(row(**{'종목T+1': '90', '종목T+3': '80', '종목T+5': '-2'}))
        self.assertIn('단기 T+5', text)
        self.assertIn('평균 종목수익률 -2.0%', text)
        self.assertIn('양수 수익 비율 0%', text)
        self.assertNotIn('10000', text)

    def test_mid_uses_t10(self):
        text = summary(row(**{'채널': '리포트TOP2_중기', '종목T+5': '40', '종목T+10': '3.5%'}))
        self.assertIn('중기 T+10', text)
        self.assertIn('+3.5%', text)

    def test_no_horizon_fallback(self):
        self.assertEqual(summary(row(**{'종목T+1': '90', '종목T+3': '80'})), '')
        self.assertEqual(summary(row(**{'채널': '리포트TOP2_중기', '종목T+5': '40'})), '')

    def test_renumbered_columns_follow_header(self):
        r = row(**{'종목T+5': '2'})
        self.assertEqual(performance_summary([HEADER[::-1], r[::-1]], TODAY), summary(r))

    def test_missing_or_duplicate_headers_rejected(self):
        for header in (['broken'], HEADER + ['종목T+5']):
            with self.assertRaises(ValueError):
                performance_summary([header, row()], TODAY)

    def test_excluded_old_policy_future_and_early_rows_omitted(self):
        for fields in ({'실제캡처거래일': '거래정지 제외'}, {'진입일': '2026-09-06'},
                       {'진입일': '2026-11-01'}, {'진입일': '2026-10-29'}, {'진입일': 'bad'}):
            self.assertEqual(summary(row(**{'종목T+5': '9', **fields})), '')

    def test_nonfinite_or_invalid_return_omitted(self):
        for value in ('NaN', 'inf', '-inf', 'error', '', '1%2'):
            self.assertEqual(summary(row(**{'종목T+5': value})), '')

    def test_zero_return_and_numeric_formats(self):
        text = summary(row(**{'종목T+5': '0%'}), row(**{'trade_id': 'test-2', '종목T+5': ' 1,000.0% '}))
        self.assertIn('+500.0%', text)
        self.assertIn('양수 수익 비율 50%', text)
        self.assertIn('비용·지수 미차감', text)
        self.assertIn(MEMORY_VERSION, text)

    def test_duplicate_trade_ids_fail_closed(self):
        with self.assertRaises(ValueError):
            summary(row(**{'종목T+5': '2'}), row(**{'종목T+5': '3'}))

    def test_latest_twenty_signals_by_date_not_sheet_order(self):
        records = []
        for i in range(21):
            day = (datetime.date(2026, 9, 7) + datetime.timedelta(days=i)).isoformat()
            records.append(row(**{'trade_id': str(i), '진입일': day, '종목T+5': '999' if i == 0 else '1'}))
        text = summary(*reversed(records))
        self.assertIn('최근 20개 신호 중 유효 기록 20건', text)
        self.assertIn('+1.0%', text)

    def test_production_wrapper_calls_helper_and_fails_closed(self):
        tree = ast.parse((ROOT / 'hyeoks_analyst.py').read_text(encoding='utf-8'))
        fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
                  and n.name == 'get_recent_performance_summary')
        env = {'performance_summary': lambda rows: performance_summary(rows, TODAY)}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), 'analyst-wrapper', 'exec'), env)
        def doc(rows):
            return SimpleNamespace(worksheet=lambda _: SimpleNamespace(get_all_values=lambda: rows))
        call = env['get_recent_performance_summary']
        self.assertEqual(call(doc([HEADER, row()])), '')
        self.assertIn('+2.0%', call(doc([HEADER, row(**{'종목T+5': '2'})])))
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(call(doc([['broken'], row()])), '')


if __name__ == '__main__':
    unittest.main()
