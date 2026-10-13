# 테마 현황 표시 전용 탭 시험 — 표시 내용, 시트 실패 무해, 선정 경로(수급_실시간·all_theme_map)와 분리됐는지 확인한다
"""2026-10-13 사용자 요청 — '보안 테마가 왜 안 뜨나'. theme_status.py 와 omakase.py 의 수집 정보 연결부 시험."""
import ast
import pathlib
import unittest

import stability_gate as G
import theme_status as T

ROOT = pathlib.Path(__file__).resolve().parent.parent

# 2026-10-13 10:58 네이버 조회 모양(표시 시험용 합성값)
DOM = {"theme": "딥페이크(deepfake) / 보안주(정보)",
       "top": {"name": "가", "code": "'000001", "rate": 16.45, "value": 196800},
       "second": {"name": "나", "code": "'000002", "rate": 11.03, "value": 26100}}
ROWS = [{"날짜": "2026-10-13", "시간": "10:51", "순위": 1, "테마명": "시멘트", "종목명": "다", "종목코드": "'000003",
         "등락률(%)": 5.1, "거래대금(억원)": 300},
        {"날짜": "2026-10-13", "시간": "10:51", "순위": 1, "테마명": "시멘트", "종목명": "라", "종목코드": "'000004",
         "등락률(%)": 4.0, "거래대금(억원)": 120}]


class FakeSheet:
    def __init__(self, fail=False):
        self.calls, self.fail = [], fail

    def update(self, range_name, values, value_input_option=None):
        if self.fail:
            raise RuntimeError("APIError 503")
        self.calls.append(("update", range_name, values))

    def batch_clear(self, ranges):
        self.calls.append(("clear", ranges))


class FakeDoc:
    def __init__(self, exists=True, fail=False):
        self.sheet, self.exists, self.added = FakeSheet(fail), exists, []

    def worksheet(self, name):
        if not self.exists:
            raise LookupError(name)
        return self.sheet

    def add_worksheet(self, title, rows, cols):
        self.added.append(title)
        return self.sheet


class Build(unittest.TestCase):
    def test_no_passing_theme_is_said_plainly_and_dominated_groups_are_shown(self):
        head, body = T.build({"dominated": [DOM], "note": "조건을 넘은 묶음 1개가 모두 1등 독식으로 빠짐"}, [], "2026-10-13 10:51")
        self.assertIn("지금 통과 테마 없음", head[2][1])
        self.assertIn("앞선 결과가 남아 있다", head[2][1])
        self.assertIn("1등 독식", head[2][1])
        self.assertEqual(body[0], T.HEADER)
        self.assertEqual([r[0] for r in body[1:]], ["1등 독식 — 대장", "1등 독식 — 2등"])
        self.assertEqual(body[1][6], 1968, "백만원 → 억원")
        self.assertEqual(body[1][7], "1등/2등 거래대금 7.5배")

    def test_passing_themes_are_listed_as_written_to_the_input_tab(self):
        head, body = T.build({"dominated": [], "note": ""}, ROWS, "2026-10-13 10:51")
        self.assertIn("통과 테마 1개", head[2][1])
        self.assertEqual([r[3] for r in body[1:]], ["다", "라"])


class Update(unittest.TestCase):
    def test_writes_and_creates_the_tab_when_missing(self):
        doc = FakeDoc(exists=False)
        self.assertTrue(T.update(doc, ROWS, {"dominated": [DOM], "note": ""}, "2026-10-13 10:51"))
        self.assertEqual(doc.added, ["테마_현황"])
        ranges = [c[1] for c in doc.sheet.calls if c[0] == "update"]
        self.assertEqual(ranges, ["A1", "A5", "A7"])

    def test_last_input_update_time_is_kept_when_nothing_passed(self):
        doc = FakeDoc()
        T.update(doc, [], {"dominated": [DOM], "note": ""}, "2026-10-13 10:51")
        ranges = [c[1] for c in doc.sheet.calls if c[0] == "update"]
        self.assertNotIn("A5", ranges, "통과가 없으면 '수급_실시간 마지막 갱신' 칸을 덮지 않는다")

    def test_sheet_failure_never_raises(self):
        self.assertFalse(T.update(FakeDoc(fail=True), ROWS, {"dominated": [], "note": ""}, "x"))


class Isolation(unittest.TestCase):
    """표시 정보가 선정 경로로 새어 들어가면 정책 변경이다(블록 A ~11/06)."""

    def setUp(self):
        self.src = (ROOT / "omakase.py").read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)

    def test_nobody_reads_the_display_tab(self):
        """주석이 아니라 **코드의 문자열**로 탭 이름을 쓰는 곳은 theme_status.py 뿐이어야 한다."""
        for p in ROOT.glob("*.py"):
            if p.name == "theme_status.py":
                continue
            with self.subTest(p.name):
                tree = ast.parse(p.read_text(encoding="utf-8", errors="ignore"))
                hits = [n.lineno for n in ast.walk(tree)
                        if isinstance(n, ast.Constant) and isinstance(n.value, str) and "테마_현황" in n.value
                        and not (isinstance(getattr(n, "parent", None), ast.Expr))]
                docstrings = {d.body[0].value.lineno for d in ast.walk(tree)
                              if isinstance(d, (ast.Module, ast.FunctionDef, ast.ClassDef)) and d.body
                              and isinstance(d.body[0], ast.Expr) and isinstance(d.body[0].value, ast.Constant)}
                self.assertEqual([h for h in hits if h not in docstrings], [])

    def test_status_is_only_touched_by_the_collector_and_the_display_call(self):
        users = set()
        for fn in ast.walk(self.tree):
            if isinstance(fn, ast.FunctionDef):
                if any(isinstance(n, ast.Name) and n.id == "THEME_STATUS" for n in ast.walk(fn)):
                    users.add(fn.name)
        self.assertEqual(users, {"get_real_money_themes"}, "선정 함수가 THEME_STATUS 를 읽으면 안 된다")
        main = next(n for n in self.tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test))
        calls = [ast.unparse(c.func) for c in ast.walk(main) if isinstance(c, ast.Call)]
        self.assertIn("theme_status.update", calls)
        self.assertLess(calls.index("update_google_sheet"), calls.index("theme_status.update"))

    def test_dominance_branch_still_only_drops_the_group(self):
        fn = next(n for n in ast.walk(self.tree) if isinstance(n, ast.FunctionDef) and n.name == "get_real_money_themes")
        branch = next(n for n in ast.walk(fn) if isinstance(n, ast.If) and "* 5" in ast.unparse(n.test))
        kinds = [type(s).__name__ for s in branch.body]
        self.assertEqual(kinds, ["Expr", "Expr", "Continue"], "출력·표시용 기록·continue 만 — 선정 값은 건드리지 않는다")
        self.assertIn("THEME_STATUS", ast.unparse(branch.body[1]))

    def test_return_shape_of_the_collector_is_unchanged(self):
        fn = next(n for n in ast.walk(self.tree) if isinstance(n, ast.FunctionDef) and n.name == "get_real_money_themes")
        rets = [r for r in ast.walk(fn) if isinstance(r, ast.Return)]
        self.assertTrue(rets)
        self.assertTrue(all(isinstance(r.value, ast.Tuple) and len(r.value.elts) == 3 for r in rets))

    def test_module_is_in_the_fingerprint(self):
        self.assertIn("theme_status.py", G.FINGERPRINT_FILES)


if __name__ == "__main__":
    unittest.main()
