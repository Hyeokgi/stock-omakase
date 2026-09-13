"""Exercise actual function bodies without importing credential-initializing top level."""
import ast
import datetime
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock


class TrendSafetyTests(unittest.TestCase):
    def setUp(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / "hyeoks_trend.py").read_text(encoding="utf-8"))
        code = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)], type_ignores=[])
        self.ns = dict(os=os, json=json, datetime=datetime, tempfile=tempfile,
                       KST=datetime.timezone(datetime.timedelta(hours=9)),
                       MAX_REPORTS=8, DRIVE_FOLDER_NAME="unused", DRIVE_FOLDER_ID="fixed",
                       time=SimpleNamespace(monotonic=lambda: 0, sleep=lambda _: None), print=lambda *a: None)
        exec(compile(code, "hyeoks_trend.py", "exec"), self.ns)
        self.headers = ["분석일자", "섹터/테마명", "핵심 상승 논리", "Top Pick 1", "Top Pick 2", "추세추종 진입 전략", "리포트 출처(파일명)"]
        self.sheet = Mock()
        self.sheet.get_all_values.return_value = [self.headers]
        self.ns["db_trend_sheet"] = self.sheet
        self.client = Mock()
        self.client.files.upload.return_value = SimpleNamespace(name="temporary")
        self.data = dict(industry="업종", core_logic="하향", top_pick_1="", top_pick_2="", strategy="위험", published_date="unknown")
        self.client.models.generate_content.return_value = SimpleNamespace(text=json.dumps(self.data))
        self.ns["client"] = self.client
        self.ns["get_pdfs_from_drive"] = lambda _: [{"id": str(i), "name": str(i)+".pdf", "md5Checksum": str(i)} for i in range(12)]
        self.paths = []
        def download(*_):
            fd, path = tempfile.mkstemp(suffix=".pdf")
            os.close(fd)
            self.paths.append(path)
            return path
        self.ns["download_file"] = download

    def tearDown(self):
        for path in self.paths:
            Path(path).unlink(missing_ok=True)

    def test_budget_append_raw_no_clear(self):
        self.ns["main"]()
        self.assertEqual(self.client.models.generate_content.call_count, 8)
        self.assertEqual(self.sheet.append_rows.call_count, 8)
        self.assertEqual(self.sheet.append_rows.call_args.kwargs["value_input_option"], "RAW")
        self.sheet.batch_clear.assert_not_called()
        self.sheet.update.assert_not_called()
        self.assertTrue(all(not Path(p).exists() for p in self.paths))

    def test_header_mismatch_fails_before_paid_call(self):
        self.sheet.get_all_values.return_value = [["different"]]
        with self.assertRaises(ValueError):
            self.ns["main"]()
        self.client.models.generate_content.assert_not_called()

    def test_content_duplicate_skipped(self):
        self.sheet.get_all_values.return_value = [self.headers, ["", "", "", "", "", "", "old.pdf"]]
        self.ns["get_pdfs_from_drive"] = lambda _: [
            dict(id="a", name="old.pdf", md5Checksum="same"),
            dict(id="b", name="renamed.pdf", md5Checksum="same")]
        self.ns["main"]()
        self.client.models.generate_content.assert_not_called()

    def test_evidence_required_and_temp_cleaned(self):
        self.data["top_pick_1"] = "추천"
        self.client.models.generate_content.return_value.text = json.dumps(self.data)
        with self.assertRaises(ValueError):
            self.ns["main"]()
        self.sheet.append_rows.assert_not_called()
        self.client.files.delete.assert_called_once()
        self.assertTrue(all(not Path(p).exists() for p in self.paths))

    def test_uncertain_append_not_retried(self):
        self.sheet.append_rows.side_effect = TimeoutError("uncertain")
        with self.assertRaises(TimeoutError):
            self.ns["main"]()
        self.assertEqual(self.sheet.append_rows.call_count, 1)
        self.assertEqual(self.client.models.generate_content.call_count, 1)


if __name__ == "__main__":
    unittest.main()
