import unittest
from unittest.mock import Mock

import hyeoks_data_quality as q


class QualityTests(unittest.TestCase):
    def test_dimensions(self):
        rows = q.build_rows()
        self.assertEqual(len(rows), 149)
        self.assertEqual(len(rows[29]), 15)
        self.assertEqual(rows[28][0], "날짜(달력일)")

    def test_rolling_dates_and_units(self):
        rows = q.build_rows()
        self.assertEqual(rows[29][0], "=TODAY()-0")
        self.assertEqual(rows[-1][0], "=TODAY()-119")
        self.assertIn("*100", rows[19][2])
        self.assertNotIn("*100", rows[29][6])

    def test_pairs_require_entry_stock_and_benchmark(self):
        formula = q.pair_count('"test"', 5, "A30")
        for col in ("Q", "T", "X"):
            self.assertIn(f"ISNUMBER({q.ref(col)})", formula)
        self.assertIn('SEARCH("제외"', formula)
        self.assertIn(">0", formula)

    def test_empty_not_zero_return(self):
        self.assertIn('=0,"",', q.pair_mean('"test"', 10, "A30"))
        self.assertIn("ISNUMBER", q.build_rows()[29][13])

    def test_no_claimed_1505_execution(self):
        self.assertEqual(q.build_rows()[8][2], "미연결")
        self.assertIn("대체하지 않음", q.build_rows()[8][8])

    def test_source_never_written(self):
        for req in q.sheet_requests(123, True):
            self.assertNotIn("delete", next(iter(req)).lower())
        cells = q.sheet_requests(123, True)[1]["updateCells"]
        self.assertEqual(cells["start"]["sheetId"], 123)
        self.assertEqual(cells["fields"], "userEnteredValue")

    def test_bad_header_rejected(self):
        with self.assertRaises(ValueError):
            q.validate_header(["trade_id", "wrong"])

    def make_doc(self):
        doc = Mock()
        sheet = doc.worksheet.return_value
        sheet.get.return_value = q.build_rows()
        values = [["ok"] * 15 for _ in range(149)]
        values[10][2] = q.VERSION
        values[13][1] = "일치(내용 정확성은 미검증)"
        sheet.get_all_values.return_value = values
        return doc

    def test_monitor_rejects_error(self):
        doc = self.make_doc()
        doc.worksheet.return_value.get_all_values.return_value[29][3] = "#DIV/0!"
        from unittest.mock import patch
        with patch.object(q, "validate_header"), self.assertRaises(ValueError):
            q.check_document(doc)

    def test_monitor_rejects_replaced_formula(self):
        doc = self.make_doc()
        doc.worksheet.return_value.get.return_value[29][0] = "2026-09-10"
        from unittest.mock import patch
        with patch.object(q, "validate_header"), self.assertRaises(ValueError):
            q.check_document(doc)

    def test_monitor_passes_intact_view(self):
        from unittest.mock import patch
        with patch.object(q, "validate_header"):
            q.check_document(self.make_doc())


if __name__ == "__main__":
    unittest.main()
