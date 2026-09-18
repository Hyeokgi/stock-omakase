# -*- coding: utf-8 -*-
"""V3 사고범위 계측 — 읽기 전용이고, 셀 수 없는 것을 세었다고 말하지 않는가."""
import pathlib
import unittest

import earnings_schema as S
import hyeoks_v3_probe as P

ROOT = pathlib.Path(__file__).resolve().parent.parent


def row(code, v3="", qoq=""):
    r = [""] * len(S.HEADER)
    r[S.CODE_COL] = code
    r[P.OLD_COL] = str(qoq)
    r[S.V3_COL] = str(v3)
    return r


class ClassifyTests(unittest.TestCase):
    def test_split_is_exhaustive(self):
        res = P.classify([list(S.HEADER), row("A", 72, -8), row("B", 15, "12.3"),
                          row("C", 72, 30)])
        self.assertEqual(res["확정영향"] + res["잠재영향"] + res["영향없음"], res["총행"])

    def test_verdict_flip_is_confirmed_impact(self):
        res = P.classify([list(S.HEADER), row("A", 72, -8)])
        self.assertEqual(res["확정영향"], 1)
        self.assertEqual(res["cases"][0]["옛판정"], "제외")
        self.assertEqual(res["cases"][0]["새판정"], "통과")

    def test_decimal_qoq_is_no_impact(self):
        """옛 경로가 int() 에 실패하면 V3 없음으로 처리됐다 — 필터가 꺼진다."""
        self.assertEqual(P.classify([list(S.HEADER), row("A", 15, "12.3")])["영향없음"], 1)

    def test_same_verdict_is_still_potential_impact(self):
        """결과가 같아도 근거가 틀렸다면 '영향 없음' 이 아니다."""
        self.assertEqual(P.classify([list(S.HEADER), row("A", 72, 30)])["잠재영향"], 1)

    def test_refuses_on_bad_header(self):
        self.assertNotEqual(P.classify([["종목코드", "엉뚱"], ["A", "1"]])["reason"], "")


class HonestyTests(unittest.TestCase):
    """이 도구가 과장하지 않는가 — GPT 가 특히 경계한 지점."""

    def test_report_states_what_cannot_be_counted(self):
        text = P.render(P.classify([list(S.HEADER), row("A", 72, -8)]))
        self.assertIn("재구성 불가", text)
        self.assertIn("과거 탈락 종목 수는 셀 수 없다", text)

    def test_docstring_separates_knowable_from_unknowable(self):
        self.assertIn("말할 수 없다", P.__doc__)

    def test_threshold_matches_the_real_filter(self):
        analyst = (ROOT / "hyeoks_analyst.py").read_text(encoding="utf-8")
        self.assertIn(f"v3_score < {P.THRESHOLD}", analyst)


class ReadOnlyTests(unittest.TestCase):
    def test_no_sheet_write_calls(self):
        import ast
        tree = ast.parse((ROOT / "hyeoks_v3_probe.py").read_text(encoding="utf-8"))
        writes = [n.func.attr for n in ast.walk(tree)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                  and n.func.attr in {"update", "clear", "batch_clear", "append_row",
                                      "append_rows", "add_worksheet", "delete_rows"}]
        self.assertEqual(writes, [])

    def test_workflow_is_manual_only(self):
        import yaml
        d = yaml.safe_load((ROOT / ".github/workflows/v3_probe.yml").read_text(encoding="utf-8"))
        self.assertEqual(list(d[True]), ["workflow_dispatch"])

    def test_workflow_runs_the_selftest_first(self):
        """깨진 도구로 사고범위를 재지 않는다."""
        import yaml
        steps = yaml.safe_load(
            (ROOT / ".github/workflows/v3_probe.yml").read_text(encoding="utf-8"))["jobs"]["probe"]["steps"]
        runs = [s.get("run", "") for s in steps]
        self.assertLess(next(i for i, r in enumerate(runs) if "--self-test" in r),
                        next(i for i, r in enumerate(runs) if "--json" in r))

    def test_selftest(self):
        self.assertGreaterEqual(P._selftest(), 12)


if __name__ == "__main__":
    unittest.main()
