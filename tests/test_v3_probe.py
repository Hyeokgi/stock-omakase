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
                          row("C", 30, 30)])
        self.assertEqual(res["판정불일치"] + res["근거불일치"] + res["동일"], res["총행"])

    def test_verdict_flip_is_reported(self):
        res = P.classify([list(S.HEADER), row("A", 72, -8)])
        self.assertEqual(res["판정불일치"], 1)
        self.assertEqual(res["cases"][0]["옛판정"], "탈락")
        self.assertEqual(res["cases"][0]["새판정"], "통과")

    def test_unparsable_old_value_is_a_pass_not_an_absence(self):
        """🔴 2026-09-18 ⑨ 회귀 — 처음엔 이 경우를 '영향 없음' 으로 묻었다.

        QoQ="12.3" 이면 옛 경로는 int() 실패로 V3 없음(fail-open) → **통과**,
        진짜 V3=15 는 20 미만이라 **탈락**. 판정이 뒤집힌다.
        """
        res = P.classify([list(S.HEADER), row("A", 15, "12.3")])
        self.assertEqual(res["판정불일치"], 1)
        self.assertEqual(res["동일"], 0)
        self.assertEqual(res["cases"][0]["옛판정"], "통과")
        self.assertEqual(res["cases"][0]["새판정"], "탈락")

    def test_same_verdict_different_value_is_reason_mismatch(self):
        """결과가 같아도 근거가 틀렸다면 '동일' 이 아니다."""
        self.assertEqual(P.classify([list(S.HEADER), row("A", 72, 30)])["근거불일치"], 1)

    def test_identical_value_is_identical(self):
        self.assertEqual(P.classify([list(S.HEADER), row("A", 30, 30)])["동일"], 1)

    def test_verdict_helper_matches_the_analyst_fail_open(self):
        """analyst 는 `v3_score is not None and v3_score < 20` 이다 — 없으면 통과."""
        self.assertEqual(P.verdict(None), "통과")
        self.assertEqual(P.verdict(19), "탈락")
        self.assertEqual(P.verdict(20), "통과")

    def test_refuses_on_bad_header(self):
        self.assertNotEqual(P.classify([["종목코드", "엉뚱"], ["A", "1"]])["reason"], "")


class HonestyTests(unittest.TestCase):
    """이 도구가 과장하지 않는가 — GPT 가 특히 경계한 지점."""

    def test_report_states_what_cannot_be_counted(self):
        text = P.render(P.classify([list(S.HEADER), row("A", 72, -8)]))
        self.assertIn("재구성 불가", text)
        self.assertIn("과거 탈락 종목 수는 셀 수 없다", text)

    def test_no_bucket_name_claims_historical_impact(self):
        """🔴 ⑨ — '확정 영향' 은 과거에 일어난 일처럼 읽힌다. 쓰지 않는다."""
        res = P.classify([list(S.HEADER), row("A", 72, -8)])
        self.assertNotIn("확정영향", res)
        self.assertNotIn("확정 영향", P.render(res))
        self.assertIn("오늘 시트 기준", P.render(res))

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
        self.assertGreaterEqual(P._selftest(), 15)


if __name__ == "__main__":
    unittest.main()
