import json
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


class BenchmarkAdjustment(unittest.TestCase):
    """v2 — 채널 간 비교에서 시장 구성 차이를 빼낸다.

    원장은 행마다 그 종목의 벤치(코스피/코스닥) 지수 수익률을 채운다. 그걸 안 빼면
    코스닥을 많이 담은 채널과 코스피를 많이 담은 채널의 차이에 실력이 아닌 것이 섞인다.
    """

    def test_daily_mean_subtracts_the_rows_own_index(self):
        for horizon in (5, 10):
            stock, index = q.COLS[horizon]
            f = q.pair_mean('"차트TOP2"', horizon, "$A30")
            self.assertIn(q.ref(stock), f)
            self.assertIn(q.ref(index), f, f"T+{horizon} 에서 지수 열이 빠졌다")
            self.assertIn(f"-IFERROR({q.ref(index)}*1,0)", f)

    def test_cost_is_not_subtracted_here(self):
        # 상수라 채널 간 차이에서 정확히 상쇄된다. 빼면 '초과'가 '순알파'로 오인된다.
        self.assertNotIn("0.35", q.pair_mean('"차트TOP2"', 5, "$A30"))

    def test_version_bumped_so_stale_view_fails_loudly(self):
        self.assertEqual(q.VERSION, "quality-v2")

    def test_layout_unchanged_by_the_new_note(self):
        rows = q.build_rows()
        self.assertEqual(len(rows), q.DAILY_START + q.DAYS - 1)
        self.assertEqual(rows[24][0], "원장 최근 추천일")
        self.assertEqual(rows[18][0], "동일 추천일 비교")
        self.assertEqual(rows[19][0], "단기 − 차트 T+5")
        self.assertEqual(rows[q.DAILY_START - 2][0], "날짜(달력일)")

    def test_daily_headers_say_excess_not_raw_return(self):
        head = q.build_rows()[q.DAILY_START - 2]
        self.assertIn("단기 초과T+5", head)
        self.assertIn("중기 초과T+10", head)


class ExistingTabOverwrite(unittest.TestCase):
    """이미 설치된 탭을 덮어쓸 때는 만들기 요청이 들어가면 안 된다."""

    def _kinds(self, add):
        return [k for req in q.sheet_requests(4242, add=add) for k in req]

    def test_new_install_creates_sheet_and_table(self):
        kinds = self._kinds(True)
        self.assertEqual(kinds.count("addSheet"), 1)
        self.assertEqual(kinds.count("addTable"), 1)

    def test_overwrite_creates_nothing(self):
        kinds = self._kinds(False)
        self.assertNotIn("addSheet", kinds)
        self.assertNotIn("addTable", kinds)

    def test_overwrite_still_writes_every_cell(self):
        cells = [r for r in q.sheet_requests(4242, add=False) if "updateCells" in r]
        self.assertEqual(len(cells), 1)
        rows = cells[0]["updateCells"]["rows"]
        self.assertEqual(len(rows), q.DAILY_START + q.DAYS - 1)

    def test_overwrite_targets_the_given_sheet_id(self):
        for req in q.sheet_requests(4242, add=False):
            blob = json.dumps(req)
            self.assertNotIn('"sheetId": 0', blob)
            self.assertIn("4242", blob)
