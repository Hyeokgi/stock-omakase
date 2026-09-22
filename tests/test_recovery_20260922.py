"""Offline recovery checks: no credentials, API calls, or repository data writes."""
import ast
import datetime
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

import production_receipt as receipt
import stability_gate

ROOT = Path(__file__).resolve().parents[1]


class AnalystInitialization(unittest.TestCase):
    def prefix(self):
        tree = ast.parse((ROOT / 'hyeoks_analyst.py').read_text(encoding='utf-8'))
        nodes = []
        for node in tree.body:
            # External imports deliberately excluded. Test real initializer expressions,
            # not dependencies, API startup, or the entire production script.
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                nodes.append(node)
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == 'RECEIPT_STATE' for t in node.targets
            ):
                return nodes
        self.fail('RECEIPT_STATE not found')

    def execute(self, nodes):
        namespace = {'datetime': datetime,
                     'feature_telemetry': types.SimpleNamespace(Telemetry=lambda: object())}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), 'analyst-initializers', 'exec'), namespace)
        return namespace

    def test_real_initializer_order_executes(self):
        state = self.execute(self.prefix())['RECEIPT_STATE']
        self.assertEqual(state['started_at'].utcoffset(), datetime.timedelta(hours=9))
        self.assertEqual(state['stage'], 'start')

    def test_historical_order_actually_raises(self):
        nodes = [n for n in self.prefix() if not (
            isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'KST' for t in n.targets))]
        with self.assertRaises(NameError):
            self.execute(nodes)


class RejectionEvidence(unittest.TestCase):
    def test_rejection_remains_fail_closed(self):
        at = datetime.datetime(2026, 9, 22, 1, 46, tzinfo=receipt.KST)
        with self.assertRaises(ValueError):
            receipt.collector_cycle(at)
        self.assertEqual(receipt.collector_cycle(at, '2026-09-21'), '2026-09-21')

    def test_incident_is_not_a_receipt_and_does_not_overwrite(self):
        at = datetime.datetime(2026, 9, 22, 1, 46, tzinfo=receipt.KST)
        with tempfile.TemporaryDirectory() as root:
            ok, filename = receipt.record_cycle_rejection(at, 'primary', root)
            self.assertTrue(ok)
            data = json.loads(Path(filename).read_text(encoding='utf-8'))
            self.assertIsNone(data['cycle_date'])
            self.assertFalse(data['sheet_access_started'])
            self.assertNotIn('kind', data)
            self.assertEqual(receipt.load('2026-09-21', root)[0], [])
            self.assertEqual(receipt.load('2026-09-22', root)[0], [])
            _, second = receipt.record_cycle_rejection(at, 'primary', root)
            self.assertNotEqual(filename, second)

    def test_write_failure_is_reported(self):
        with patch.object(Path, 'mkdir', side_effect=OSError('unavailable')):
            self.assertEqual(receipt.record_cycle_rejection(
                datetime.datetime.now(receipt.KST), 'aux'), (False, 'OSError'))

    def test_backup_keeps_same_cycle_and_no_double_count(self):
        for hour in (6, 8):
            at = datetime.datetime(2026, 9, 22, hour, 30, tzinfo=receipt.KST)
            self.assertEqual(receipt.previous_trading_day(at), '2026-09-21')
        with tempfile.TemporaryDirectory() as root:
            path = str(Path(root) / 'runs.csv')
            ev = {key: True for key in stability_gate.KEYS}
            first = stability_gate.record('2026-09-21', 'one', 'test', ev, path=path, fp='test')
            second = stability_gate.record('2026-09-21', 'two', 'test', ev, path=path, fp='test')
            self.assertTrue(first[0])
            self.assertFalse(second[0])

    def test_workflow_backup_is_wired(self):
        source = (ROOT / '.github/workflows/stability_finalizer.yml').read_text(encoding='utf-8')
        self.assertIn("cron: '30 23 * * 1-5'", source)
        self.assertIn('--previous-trading-day', source)


if __name__ == '__main__':
    unittest.main()
