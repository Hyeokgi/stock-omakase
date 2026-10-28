"""기업정보 신규 상장 동기화 — KIND 파싱 · 대조 · 안전장치 · 공개 로그에 종목명 없음."""
import contextlib
import io
import unittest

import hyeoks_company_info as C


def kind_html(rows):
    head = "".join(f"<th>{h}</th>" for h in C.HEADER)
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{head}</tr>{body}</table>".encode("cp949")


def row(code, name="회사", date="2020-01-01"):
    return [name, "코스닥", code, "업종", "제품", date, "12월", "대표", "http://x", "서울"]


def kind_rows(n=C.MIN_KIND_ROWS, extra=()):
    return [row(f"{i:06d}", f"기존{i}") for i in range(1, n + 1)] + list(extra)


class Sheet:
    def __init__(self, values):
        self.values = values
        self.appended = []
        self.updated = []
        self.calls = []

    def get_all_values(self):
        return self.values

    def append_rows(self, rows, **kw):
        self.appended.append((rows, kw))
        self.calls.append("append")

    def batch_update(self, data, **kw):
        self.updated.append((data, kw))
        self.calls.append("update")


def sheet_with(rows):
    return Sheet([list(C.HEADER)] + rows)


def run(raw, sheet, dry=False):
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        rc = C.run(lambda: raw, sheet, dry)
    return rc, out.getvalue()


class CompanyInfoTests(unittest.TestCase):
    def test_parse_normalizes_codes_and_keeps_ten_columns(self):
        rows = C.parse_kind(kind_html(kind_rows(extra=[row("0220W0", "신규", "2026-10-27")])))
        self.assertEqual(rows[-1][2], "0220W0")
        self.assertEqual(rows[0][2], "000001")
        self.assertTrue(all(len(r) == 10 for r in rows))

    def test_appends_only_missing_codes_in_listing_order_as_raw(self):
        existing = [row(f"{i:06d}", f"기존{i}") for i in range(1, C.MIN_KIND_ROWS + 1)]
        existing[4][2] = "5"                       # 시트에 앞자리 0 이 잘려 들어간 코드도 같은 종목
        new = [row("0220W0", "비밀신규나", "2026-10-27"), row("999998", "비밀신규가", "2026-09-01")]
        sheet = sheet_with(existing)
        rc, log = run(kind_html(kind_rows(extra=new)), sheet)
        self.assertEqual(rc, 0)
        (rows, kw), = sheet.appended
        self.assertEqual([r[2] for r in rows], ["999998", "0220W0"], "상장일 순")
        self.assertEqual(kw["value_input_option"], "RAW", "코드 앞자리 0 을 지킨다")
        self.assertNotIn("비밀신규", log, "공개 로그에 종목명이 없다")
        self.assertIn("신규 2개", log)

    def test_nothing_to_add_writes_nothing(self):
        sheet = sheet_with(kind_rows())
        rc, log = run(kind_html(kind_rows()), sheet)
        self.assertEqual((rc, sheet.appended), (0, []))
        self.assertIn("신규 0개", log)

    def test_renamed_rows_get_kind_name_in_column_a_only(self):
        existing = kind_rows()
        existing[0][0] = "비밀옛이름"
        existing[2][0] = "비밀옛이름2"
        existing[2][1] = "시트의다른값"                 # A열 밖은 건드리지 않는다
        existing.append(row("000001", "비밀옛이름3"))     # 같은 코드가 두 번 — 둘 다 본다
        sheet = sheet_with(existing)
        rc, log = run(kind_html(kind_rows(extra=[row("999999", "신규")])), sheet)
        self.assertEqual(rc, 0)
        (data, kw), = sheet.updated
        self.assertEqual(data, [{"range": "A2", "values": [["기존1"]]}, {"range": "A4", "values": [["기존3"]]},
                                {"range": f"A{len(existing) + 1}", "values": [["기존1"]]}])
        self.assertEqual(kw["value_input_option"], "RAW")
        self.assertEqual(sheet.calls, ["update", "append"], "고친 뒤에 덧붙인다(행 번호가 그대로)")
        self.assertIn("상호가 KIND 와 다른 행 3개", log)
        self.assertNotIn("비밀옛이름", log, "공개 로그에 옛 이름이 없다")

    def test_too_many_renames_writes_nothing(self):
        existing = kind_rows()
        for r in existing[:C.MAX_RENAME + 1]:
            r[0] = "다름"
        sheet = sheet_with(existing)
        rc, log = run(kind_html(kind_rows(extra=[row("999999", "신규")])), sheet)
        self.assertEqual((rc, sheet.updated, sheet.appended), (1, [], []))
        self.assertIn("상호 갱신", log)

    def test_guards_stop_before_writing(self):
        cases = {
            "잘린 응답": (kind_html(kind_rows(n=C.MIN_KIND_ROWS - 1)), sheet_with([])),
            "KIND 머리글": (b"<table><tr><th>x</th></tr></table>", sheet_with([])),
            "시트 머리글": (kind_html(kind_rows()), Sheet([["회사명", "다른열"]])),
            "너무 많음": (kind_html(kind_rows(extra=[row(f"9{i:05d}") for i in range(C.MAX_APPEND + 1)])),
                       sheet_with(kind_rows())),
            "중복 코드": (kind_html(kind_rows(extra=[row("000001")])), sheet_with([])),
        }
        for name, (raw, sheet) in cases.items():
            with self.subTest(name):
                rc, log = run(raw, sheet)
                self.assertEqual(rc, 1)
                self.assertEqual(sheet.appended, [])
                self.assertIn("::error::", log)

    def test_dry_run_reports_but_does_not_write(self):
        sheet = sheet_with(kind_rows())
        sheet.values[1][0] = "옛이름"
        rc, log = run(kind_html(kind_rows(extra=[row("999999", "신규")])), sheet, dry=True)
        self.assertEqual((rc, sheet.appended, sheet.updated), (0, [], []))
        self.assertIn("시험 실행", log)

    def test_workflow_is_weekday_morning_and_cleans_secret(self):
        from pathlib import Path
        wf = (Path(__file__).resolve().parents[1] / ".github/workflows/company_info.yml").read_text(encoding="utf-8")
        self.assertIn("cron: '20 22 * * 0-4'", wf)
        self.assertIn("unlink(missing_ok=True)", wf)
        self.assertIn("if: always()", wf)


if __name__ == "__main__":
    unittest.main()
