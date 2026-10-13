# 테마 현황 표시 전용 탭 시험 — 표시 내용, 시트 실패 무해, 선정 경로(수급_실시간·all_theme_map)와 분리, 시간 상한을 확인한다
"""2026-10-13 사용자 요청 — '보안 테마가 왜 안 뜨나'. theme_status.py 와 omakase.py 의 수집 정보 연결부 시험.
Codex 10/13 확인 반영 — 기록 실패 격리, 스캐너 뒤·시간 상한 안 표시, 실제 쓰기 성공일 때만 '갱신함'."""
import ast
import datetime as _dt
import pathlib
import threading
import time
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
        head, body = T.build({"dominated": [], "note": "", "input_written": "10:51"}, ROWS, "2026-10-13 10:51")
        self.assertIn("통과 테마 1개 — 수급_실시간 갱신함(10:51)", head[2][1])
        self.assertEqual([r[3] for r in body[1:]], ["다", "라"])

    def test_does_not_claim_the_input_tab_was_updated_when_the_write_failed(self):
        """Codex 10/13 — 원본 탭 쓰기가 실패했는데 '갱신함' 이라고 쓰면 안 된다."""
        head, _ = T.build({"dominated": [], "note": "", "input_written": ""}, ROWS, "2026-10-13 10:51")
        self.assertNotIn("갱신함", head[2][1])
        self.assertIn("쓰기 실패 또는 미확인", head[2][1])


class Update(unittest.TestCase):
    def test_writes_and_creates_the_tab_when_missing(self):
        doc = FakeDoc(exists=False)
        self.assertTrue(T.update(doc, ROWS, {"dominated": [DOM], "note": "", "input_written": "10:51"},
                                 "2026-10-13 10:51"))
        self.assertEqual(doc.added, ["테마_현황"])
        ranges = [c[1] for c in doc.sheet.calls if c[0] == "update"]
        self.assertEqual(ranges, ["A1", "A5", "A7"])

    def test_last_input_update_time_is_kept_when_nothing_passed_or_the_write_failed(self):
        for rows, written in (([], "10:51"), (ROWS, "")):
            doc = FakeDoc()
            T.update(doc, rows, {"dominated": [DOM], "note": "", "input_written": written}, "2026-10-13 10:51")
            ranges = [c[1] for c in doc.sheet.calls if c[0] == "update"]
            with self.subTest(rows=len(rows), written=written):
                self.assertNotIn("A5", ranges, "실제로 갱신된 때만 '수급_실시간 마지막 갱신' 칸을 바꾼다")

    def test_sheet_failure_never_raises(self):
        self.assertFalse(T.update(FakeDoc(fail=True), ROWS, {"dominated": [], "note": ""}, "x"))


def _snippet(src, node, indent="    "):
    seg = ast.get_source_segment(src, node, padded=True)
    lines = seg.splitlines()
    ind = min(len(l) - len(l.lstrip()) for l in lines if l.strip())
    return "\n".join(indent + l[ind:] for l in lines)


class Isolation(unittest.TestCase):
    """표시 정보가 선정 경로로 새어 들어가거나, 표시 쪽 실패가 수집·스캐너를 흔들면 정책 변경이다(블록 A ~11/06)."""

    ALLOWED = {"get_real_money_themes", "update_google_sheet", "update_theme_display"}

    def setUp(self):
        self.src = (ROOT / "omakase.py").read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)
        self.fn = {n.name: n for n in ast.walk(self.tree) if isinstance(n, ast.FunctionDef)}

    def test_nobody_reads_the_display_tab(self):
        """탭을 여는 호출(`worksheet(…)`·`add_worksheet(…)`)에 이 탭 이름이나 `theme_status.TAB` 을 쓰는 곳은
        theme_status.py 뿐이어야 한다. 경고 문구 속 이름은 여는 것이 아니다."""
        for p in ROOT.glob("*.py"):
            if p.name == "theme_status.py":
                continue
            with self.subTest(p.name):
                tree = ast.parse(p.read_text(encoding="utf-8", errors="ignore"))
                opens = [ast.unparse(c) for c in ast.walk(tree)
                         if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                         and c.func.attr in ("worksheet", "add_worksheet")
                         and ("테마_현황" in ast.unparse(c) or "TAB" in ast.unparse(c))]
                self.assertEqual(opens, [])

    def test_status_is_touched_only_by_the_collector_the_sheet_writer_and_the_display(self):
        users = {name for name, f in self.fn.items()
                 if any(isinstance(n, ast.Name) and n.id == "THEME_STATUS" for n in ast.walk(f))}
        self.assertEqual(users, self.ALLOWED, "선정 함수(update_technical_data·analyze_single_stock 등)가 읽으면 안 된다")
        writes = [ast.unparse(n) for n in ast.walk(self.fn["update_google_sheet"])
                  if isinstance(n, ast.Assign) and "THEME_STATUS" in ast.unparse(n)]
        self.assertEqual(len(writes), 1)
        self.assertTrue(writes[0].startswith("THEME_STATUS['input_written'] ="), "쓰기 성공 표시 한 칸만 쓴다")

    def test_display_runs_after_the_scanner_and_only_through_the_time_limited_wrapper(self):
        main = next(n for n in self.tree.body if isinstance(n, ast.If) and "__main__" in ast.unparse(n.test))
        calls = [ast.unparse(c.func) for c in ast.walk(main) if isinstance(c, ast.Call)]
        self.assertNotIn("theme_status.update", calls, "직접 부르지 않는다 — 시간 상한 감싸개로만")
        self.assertLess(calls.index("update_technical_data"), calls.index("update_theme_display"),
                        "스캐너·영수증이 끝난 뒤에 표시한다")
        self.assertLess(calls.index("manage_schedule_sheet"), calls.index("update_theme_display"))

    def test_dominance_branch_still_only_drops_the_group_even_if_recording_fails(self):
        fn = self.fn["get_real_money_themes"]
        branch = next(n for n in ast.walk(fn) if isinstance(n, ast.If) and "* 5" in ast.unparse(n.test))
        self.assertEqual([type(x).__name__ for x in branch.body], ["Expr", "Try", "Continue"])
        rec = branch.body[1]
        self.assertTrue(all(isinstance(h.type, ast.Name) and h.type.id == "Exception"
                            and not any(isinstance(x, ast.Raise) for x in ast.walk(h)) for h in rec.handlers))
        # 실제 문장 실행 — 기록(append)이 터져도 continue 로 그 묶음만 빠지고 예외가 새지 않는다
        code = ("def _probe():\n    trail = []\n    for _once in [0]:\n" + _snippet(self.src, branch, "        ")
                + "\n        trail.append('fell-through')\n    trail.append('after')\n    return trail\n")

        class Boom(list):
            def append(self, x):
                raise KeyError("rate")
        g = {"merged_stocks_val": [{"name": "가", "code": "1", "rate": 1, "value": 500},
                                   {"name": "나", "code": "2", "rate": 1, "value": 10}],
             "merged_name": "t", "theme_names": ["t"], "THEME_STATUS": {"dominated": Boom()},
             "print": lambda *a, **k: None}
        exec(compile(code, "<omakase 조각>", "exec"), g)
        self.assertEqual(g["_probe"](), ["after"])

    def test_collector_returns_exactly_what_it_returned_before(self):
        rets = sorted({ast.unparse(r.value) for r in ast.walk(self.fn["get_real_money_themes"])
                       if isinstance(r, ast.Return)})
        self.assertEqual(rets, ["(pd.DataFrame(), False, {})", "(pd.DataFrame(), is_market_closed, {})",
                                "(pd.DataFrame(final_rows), is_market_closed, all_theme_map)"])

    def test_display_wrapper_does_not_wait_past_its_limit(self):
        seg = ast.get_source_segment(self.src, self.fn["update_theme_display"])
        seen = []

        class Slow:
            @staticmethod
            def update(doc, rows, status, now_str):
                time.sleep(2)

        class Fast:
            @staticmethod
            def update(doc, rows, status, now_str):
                seen.append((rows, status))

        class DF:
            empty = False

            @staticmethod
            def to_dict(kind):
                return [{"순위": 1}]
        for stub, limit in ((Slow, 0.1), (Fast, 5)):
            g = {"datetime": _dt, "KST": _dt.timezone(_dt.timedelta(hours=9)), "THEME_STATUS": {"note": "n"},
                 "theme_status": stub, "print": lambda *a, **k: None}
            exec(compile(seg, "<omakase 조각>", "exec"), g)
            t0 = time.monotonic()
            g["update_theme_display"](None, DF(), timeout=limit)
            with self.subTest(stub.__name__):
                self.assertLess(time.monotonic() - t0, 1.5, "시트가 멈춰도 기다리지 않는다")
        self.assertEqual(seen, [([{"순위": 1}], {"note": "n"})], "상태는 사본을 넘긴다")

    def test_module_is_in_the_fingerprint(self):
        self.assertIn("theme_status.py", G.FINGERPRINT_FILES)


if __name__ == "__main__":
    unittest.main()
