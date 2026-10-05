# -*- coding: utf-8 -*-
"""목표가·손절가 검증 — 2026-10-04.

사고 ① 10/2 장기 리포트: 7자리 현재가를 1/10 로 읽고 목표·손절을 1/10 크기로 냈다.
사고 ② 간단 브리핑 JSON 예시값 150000/135000 을 그대로 베낀 행.
수치는 합성값이다(비공개 시트 행을 옮기지 않는다).
"""
import pathlib
import re
import unittest

import price_levels as P

ROOT = pathlib.Path(__file__).resolve().parent.parent


class ValidateLevelsTests(unittest.TestCase):
    def test_tenfold_misread_is_corrected_when_unambiguous(self):
        lv = P.validate_levels(1500000, "200,000", "135000", "long")
        self.assertTrue(lv["ok"])
        self.assertEqual((lv["target"], lv["stop"]), (2000000, 1350000))
        self.assertEqual(lv["scale"], (10, 10))
        self.assertTrue(any("단위 보정" in w for w in lv["warnings"]))

    def test_inverted_levels_are_rejected(self):
        lv = P.validate_levels(50000, 48000, 52000, "short")
        self.assertFalse(lv["ok"])
        self.assertIsNone(lv["target"])
        self.assertTrue(lv["issues"])

    def test_copied_example_is_rejected(self):
        lv = P.validate_levels(30000, 150000, 135000, None)
        self.assertFalse(lv["ok"])

    def test_in_band_values_pass_unchanged(self):
        lv = P.validate_levels(100000, 110000, 93000, "short")
        self.assertTrue(lv["ok"])
        self.assertEqual((lv["target"], lv["stop"]), (110000, 93000))
        self.assertGreaterEqual(lv["rr"], 1.2)
        self.assertEqual(lv["warnings"], [])

    def test_out_of_band_but_sane_is_kept_with_warning(self):
        lv = P.validate_levels(100000, 125000, 93000, "short")
        self.assertTrue(lv["ok"])
        self.assertEqual(lv["target"], 125000)
        self.assertTrue(any("밴드 이탈" in w for w in lv["warnings"]))

    def test_low_reward_risk_is_invalid(self):
        """사용자 결정 2026-10-05 — 손익비 미달은 무효(가상계좌·백테스트 목표/손절에 쓰지 않는다)."""
        # 밴드 안의 조합(+7% / -8%)이지만 손익비 0.88 < 1.2
        lv = P.validate_levels(100000, 107000, 92000, "short")
        self.assertFalse(lv["ok"])
        self.assertIsNone(lv["target"])
        self.assertTrue(any("손익비" in i for i in lv["issues"]))
        self.assertIn("쓰지 않았습니다", P.report_note(lv))
        # 중기 +12% / -11% = 1.09 < 1.8, 장기 +20% / -18% = 1.11 < 2.2
        self.assertFalse(P.validate_levels(100000, 112000, 89000, "mid")["ok"])
        self.assertFalse(P.validate_levels(100000, 120000, 82000, "long")["ok"])

    def test_reward_risk_at_the_minimum_passes_after_rounding(self):
        # 단기 +9% / -7.5% = 1.2 정확히, 호가 반올림으로 1.196 이 돼도 소수 둘째 자리 기준 1.20
        self.assertTrue(P.validate_levels(100000, 109000, 92500, "short")["ok"])
        self.assertTrue(P.validate_levels(61000, 66500, 56400, "short")["ok"])
        self.assertFalse(P.validate_levels(61000, 66400, 56400, "short")["ok"])   # 1.17

    def test_briefing_has_no_reward_risk_floor(self):
        """시스템 채널(간단 브리핑)은 손절이 좁든 손익비가 낮든 AI 값을 그대로 쓴다 — 사용자 결정 2026-10-05."""
        self.assertEqual(P.brief_cells("100000", 103000, 97000)[:2], ("103,000원", "97,000원"))

    def test_missing_values(self):
        self.assertFalse(P.validate_levels(100000, None, 93000, "short")["ok"])
        self.assertFalse(P.validate_levels(0, 110000, 93000, "short")["ok"])
        self.assertFalse(P.validate_levels(100000, "00000", "00000", "short")["ok"])

    def test_scale_fix_must_land_in_band(self):
        # ×10 하면 구조는 맞지만 단기 밴드(+7~12%) 밖 → 보정하지 않는다
        lv = P.validate_levels(100000, 14500, 9000, "short")
        self.assertFalse(lv["ok"])


class BriefCellsTests(unittest.TestCase):
    def test_explicit_zero_is_veto(self):
        self.assertEqual(P.brief_cells("61,000", 0, 0)[:2], ("관망", "관망"))

    def test_valid_values_are_formatted(self):
        self.assertEqual(P.brief_cells("61000", "66000", 57500)[:2], ("66,000원", "57,500원"))

    def test_unreadable_or_copied_values_are_not_written(self):
        self.assertEqual(P.brief_cells("30,000", 150000, 135000)[:2], (None, None))
        self.assertEqual(P.brief_cells("30,000", None, None)[:2], (None, None))
        self.assertEqual(P.brief_cells("30,000", "목표가(원 단위 정수)", "x")[:2], (None, None))
        self.assertEqual(P.brief_cells("30,000", 0, 28000)[:2], (None, None))
        self.assertEqual(P.brief_cells("", 33000, 28000)[:2], (None, None))


class SanityHelpersTests(unittest.TestCase):
    def test_to_int(self):
        self.assertEqual(P.to_int("1,350,000원"), 1350000)
        self.assertEqual(P.to_int(61000.0), 61000)
        self.assertEqual(P.to_int("61000.0"), 61000)
        self.assertEqual(P.to_int(""), 0)
        self.assertEqual(P.to_int("관망"), 0)

    def test_sane_elements(self):
        self.assertTrue(P.sane_target(30000, 33000))
        self.assertFalse(P.sane_target(30000, 150000))
        self.assertFalse(P.sane_target(30000, 29000))
        self.assertTrue(P.sane_stop(30000, 28000))
        self.assertFalse(P.sane_stop(30000, 13500))


class PromptAndWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.analyst = (ROOT / "hyeoks_analyst.py").read_text(encoding="utf-8")
        cls.omakase = (ROOT / "omakase.py").read_text(encoding="utf-8")

    def test_band_numbers_are_unchanged(self):
        """밴드 수치는 사용자 승인 없이 바꾸지 않는다 — 10/4 이전 프롬프트 수치 그대로."""
        self.assertEqual(P.PRICE_BANDS, {
            "short": ((7, 12), (6, 8), 1.2),
            "mid": ((12, 20), (7, 11), 1.8),
            "long": ((20, 40), (10, 18), 2.2),
        })

    def test_band_prompt_gives_won_ranges_with_commas(self):
        txt = P.band_prompt_lines("long", 1500000)
        self.assertIn("1,500,000원", txt)
        self.assertIn("1,800,000~2,100,000원", txt)
        self.assertIn("최소 2.2", txt)
        self.assertIn("무효로 처리", txt)
        self.assertEqual(P.band_prompt_lines("other", 1500000), "")

    def test_deep_report_prompt_formats_price_with_commas(self):
        self.assertIn("★확정 현재가: {best_cand['curr_p']:,}원", self.analyst)
        self.assertNotIn("★확정 현재가: {best_cand['curr_p']}원", self.analyst)
        self.assertIn("price_levels.band_prompt_lines(st_type", self.analyst)

    def test_deep_report_levels_are_validated(self):
        self.assertIn("price_levels.validate_levels(best_cand['curr_p']", self.analyst)
        self.assertIn("price_levels.report_note(lv)", self.analyst)

    def test_briefing_example_numbers_are_gone(self):
        self.assertNotRegex(self.analyst, r'"target_price":\s*150000')
        self.assertNotRegex(self.analyst, r'"stop_loss":\s*135000')
        self.assertEqual(self.analyst.count("price_levels.brief_cells(curr_p"), 2)
        # 무효 값은 칸을 건드리지 않는다
        self.assertGreaterEqual(self.analyst.count("if target_val is not None:"), 4)

    def test_portfolio_does_not_auto_close_on_inconsistent_levels(self):
        self.assertIn("levels_ok = 0 < s_p < buy_p < t_p", self.analyst)
        self.assertIn('if levels_ok and curr_p >= t_p: reason = "목표가 도달"', self.analyst)

    def test_omakase_guards(self):
        self.assertIn("price_levels.is_sane(base_p, target_p, stop_p)", self.omakase)
        self.assertIn("price_levels.sane_target(_base, _pt)", self.omakase)
        self.assertIn("price_levels.sane_stop(_base, _ps)", self.omakase)

    def test_module_is_fingerprinted(self):
        import stability_gate as G
        self.assertIn("price_levels.py", G.FINGERPRINT_FILES)

    def test_data_line_regex_accepts_commas_and_takes_last(self):
        m = re.search(r"_matches = list\(re\.finditer\(r'(.+?)', report_txt\)\)", self.analyst)
        self.assertIsNotNone(m)
        pat = m.group(1)
        txt = ("양식: [DATA] 목표가:00000, 손절가:00000, 분할매수:O\n본문\n"
               "[DATA] 목표가:2,000,000, 손절가:1,350,000원, 분할매수:O")
        last = list(re.finditer(pat, txt))[-1]
        self.assertEqual((last.group(1), last.group(2), last.group(3)), ("2,000,000", "1,350,000", "O"))


class ReportNoteTests(unittest.TestCase):
    def test_note_for_invalid_and_corrected(self):
        bad = P.validate_levels(50000, 48000, 52000, "short")
        self.assertIn("쓰지 않았습니다", P.report_note(bad))
        fixed = P.validate_levels(1500000, 200000, 135000, "long")
        note = P.report_note(fixed)
        self.assertIn("2,000,000원", note)
        self.assertEqual(P.report_note(P.validate_levels(100000, 110000, 93000, "short")), "")


if __name__ == "__main__":
    unittest.main()
