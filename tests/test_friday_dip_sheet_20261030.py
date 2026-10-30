"""금요일 밤 하락 알림 — 웹앱 C단계 시트 기록(후보 · 19:40 · 20:00 변화, 같은 날 다시 쓰면 한 벌, 실패해도 알림 영향 없음)."""
import sys
import tempfile
import types
import unittest
from unittest import mock

import hyeoks_friday_dip_alert as A
from test_friday_dip_alert_20261023 import ENV, Clock, FakeKis, Post, fri, world


class Sheet:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def __call__(self, day, rows):
        if self.fail:
            raise RuntimeError("시트 없음")
        self.calls.append((day, rows))


class Main(unittest.TestCase):
    def run_main(self, start, argv=(), stocks=None, sheet=None):
        k, post, c = FakeKis(stocks or world()), Post(), Clock(start)
        sheet = sheet or Sheet()
        rc = A.main(list(argv), env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep, runs_dir=tempfile.mkdtemp(),
                    sheet=sheet)
        return rc, post, c, sheet

    def test_hit_records_both_picks_after_2000(self):
        rc, post, c, sheet = self.run_main(fri("19:05"))
        self.assertEqual((rc, len(post.sent)), (0, 2))
        self.assertGreaterEqual(c.t, fri("20:02"), "20:00 마감값은 20:00 이후에 읽는다")
        (day, rows), = sheet.calls
        self.assertEqual(day, "2026-10-23")
        self.assertEqual([r[1] for r in rows], [1, 2])
        top = dict(zip(A.SHEET_HEADER, rows[0]))
        self.assertEqual((top["코드"], top["KRX종가"], top["당일등락률(%)"]), ("111110", 1150, 15.0))
        self.assertEqual((top["19:40 NXT(%)"], top["20:00 NXT(%)"], top["20:00 NXT −3%↓"]), (-3.48, -3.48, "해당"))
        self.assertEqual(top["상태"], "발송성공")
        second = dict(zip(A.SHEET_HEADER, rows[1]))
        self.assertEqual((second["20:00 KRX애프터(%)"], second["20:00 NXT −3%↓"]), ("", "-"))
        self.assertEqual(len(rows[0]), len(A.SHEET_HEADER))

    def test_no_hit_still_records_candidates(self):
        s = world()
        s["111110"].update(nx=1150, after=1149)
        rc, post, _, sheet = self.run_main(fri("19:41"), stocks=s)
        self.assertEqual((rc, len(post.sent)), (0, 1))
        self.assertEqual([r[-2] for r in sheet.calls[0][1]], ["해당없음", "해당없음"])

    def test_no_candidates_records_one_status_row(self):
        s = world()
        for v in s.values():
            v.update(vol=1)
        _, _, _, sheet = self.run_main(fri("19:41"), stocks=s)
        (_, rows), = sheet.calls
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0][0], rows[0][-2], len(rows[0])), ("2026-10-23", "대상없음", len(A.SHEET_HEADER)))

    def test_dry_run_does_not_touch_sheet(self):
        _, _, _, sheet = self.run_main(fri("19:05"), ["--day", "2026-10-23", "--dry-run"])
        self.assertEqual(sheet.calls, [])

    def test_sheet_failure_does_not_change_alert_result(self):
        rc, post, _, _ = self.run_main(fri("19:05"), sheet=Sheet(fail=True))
        self.assertEqual((rc, len(post.sent)), (0, 2))

    def test_test_day_does_not_wait(self):
        _, _, c, sheet = self.run_main(fri("10:00"), ["--day", "2026-10-23"])
        self.assertLess(c.t, fri("19:00"))
        self.assertEqual(len(sheet.calls), 1)


class FakeWs:
    def __init__(self, values):
        self.values = values

    def get_all_values(self):
        return [list(r) for r in self.values]

    def delete_rows(self, i):
        del self.values[i - 1]

    def append_rows(self, rows, value_input_option=None):
        self.values += rows

    def append_row(self, row, value_input_option=None):
        self.values.append(row)


class WriteSheet(unittest.TestCase):
    def fake_modules(self, book):
        class NotFound(Exception):
            pass
        gs = types.SimpleNamespace(WorksheetNotFound=NotFound,
                                   authorize=lambda cred: types.SimpleNamespace(open_by_url=lambda url: book(NotFound)))
        sa = types.SimpleNamespace(ServiceAccountCredentials=types.SimpleNamespace(
            from_json_keyfile_name=lambda f, s: object()))
        return {"gspread": gs, "oauth2client": types.SimpleNamespace(service_account=sa),
                "oauth2client.service_account": sa}

    def test_same_day_rows_are_replaced(self):
        ws = FakeWs([A.SHEET_HEADER, ["2026-10-16", 1], ["2026-10-23", 1], ["2026-10-23", 2]])
        book = lambda nf: types.SimpleNamespace(worksheet=lambda t: ws)
        with mock.patch.dict(sys.modules, self.fake_modules(book)):
            A.write_sheet("2026-10-23", [["2026-10-23", 9]])
        self.assertEqual(ws.values, [A.SHEET_HEADER, ["2026-10-16", 1], ["2026-10-23", 9]])

    def test_missing_tab_is_created_with_header(self):
        made = {}

        def book(nf):
            def worksheet(t):
                raise nf()

            def add_worksheet(title, rows, cols):
                made["ws"] = FakeWs([])
                made["title"] = title
                return made["ws"]
            return types.SimpleNamespace(worksheet=worksheet, add_worksheet=add_worksheet)
        with mock.patch.dict(sys.modules, self.fake_modules(book)):
            A.write_sheet("2026-10-30", [["2026-10-30", "", "대상없음"]])
        self.assertEqual(made["title"], A.SHEET_NAME)
        self.assertEqual(made["ws"].values, [A.SHEET_HEADER, ["2026-10-30", "", "대상없음"]])


if __name__ == "__main__":
    unittest.main()
