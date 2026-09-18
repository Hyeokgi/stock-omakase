# -*- coding: utf-8 -*-
"""
조용한 예외 12곳 — 2026-09-18 사용자 지시 5번.

**일괄 `raise` 로 바꾸지 않았다.** 12곳을 raise 로 바꾸면 종목 하나가 이상할 때
그날 수집 전체를 잃는다. "조용히 틀림" 이 "요란하게 아무것도 못함" 이 될 뿐이다.
바뀐 것은 **보이느냐**다 — 세고, 사유를 남기고, 분류에 따라 Gate 를 떨어뜨린다.

분류표: docs/silent_exception_분류_2026-09-18.md
"""
import ast
import pathlib
import unittest

import feature_telemetry as FT

ROOT = pathlib.Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "silent_exception_분류_2026-09-18.md"

# 분류표가 정한 것 — 코드와 문서가 어긋나면 하나가 거짓말이다
EXPECTED = {
    "surge_scan": FT.CRITICAL,        # ① 후보 발굴
    "entry_price": FT.CRITICAL,       # ② 트레일링 손절 발동가
    "code_lookup": FT.DISPLAY,        # ③ 다중 원천 폴백
    "past_theme": FT.RESEARCH,        # ④ 과거 테마 매핑
    "theme_money": FT.CRITICAL,       # ⑤ 대장 판정 근거
    "curr_price_fmt": FT.DISPLAY,     # ⑥ 표시
    "fundamental": FT.DISPLAY,        # ⑦ PER/PBR
    "ma20_high60": FT.DISPLAY,        # ⑧ 시간외 표시
    "backfill_target": FT.RESEARCH,   # ⑨ 1회성 유틸
}
# 12곳 표 밖에서 **추가로** 건 계측 — 지시 ⑤(V1·V2·V3 각각)와 ⑦(원장) 때문에 필요하다.
EXTRA = {
    "v1_score": FT.CRITICAL,          # P0-5 — scanner·analyst 양쪽
    "v2_score": FT.CRITICAL,          # P0-5 — V2 게이트 판정 실패(fail-closed 유지)
    "v3_score": FT.CRITICAL,          # P0-5 — DB_실적 읽기 실패도 v3 오류다
    "ledger": FT.CRITICAL,            # ⑦ scanner 원장 read-after-write 누락
    "report_ledger": FT.CRITICAL,     # P0-3 리포트 원장 누락
    "pool_row": FT.OBSERVE,           # ⑪ 순위 풀 기록 실패
}
INSTRUMENTED_FILES = ("omakase.py", "hyeoks_analyst.py", "hyeoks_morning.py",
                      "hyeoks_nightly.py", "hyeoks_backfill_targets.py")


def telemetry_calls():
    """소스에서 `TELEMETRY.note('feature', ..., feature_telemetry.KLASS)` 를 걷는다."""
    found = {}
    for name in INSTRUMENTED_FILES:
        tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "note"
                    and isinstance(node.args[0], ast.Constant)):
                # 분류는 위치가 아니라 `feature_telemetry.X` 형태로 찾는다
                #    (code 인자가 뒤에 붙으면 args[-1] 은 분류가 아니다)
                klass = next((a.attr for a in node.args
                              if isinstance(a, ast.Attribute)
                              and getattr(a.value, "id", "") == "feature_telemetry"), "?")
                found[node.args[0].value] = klass
    return found


class ClassificationTests(unittest.TestCase):
    def test_every_site_is_classified_as_the_table_says(self):
        self.assertEqual(telemetry_calls(), {**EXPECTED, **EXTRA})

    def test_instrumented_handlers_are_not_bare(self):
        """⑦ 은 `except: pass` 였다. bare except 는 KeyboardInterrupt 까지 삼킨다.

        계측한 12곳의 핸들러가 bare 가 아닌지 본다(계측하려면 `as _e` 가 필요하므로
        구조적으로 보장되지만, 나중에 누가 되돌리는 것을 막는다).
        """
        bad = []
        for name in INSTRUMENTED_FILES:
            src = (ROOT / name).read_text(encoding="utf-8")
            tree = ast.parse(src)
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler):
                    continue
                body = "\n".join(ast.unparse(x) for x in node.body)
                if "note(" in body and node.type is None:
                    bad.append(f"{name}:{node.lineno}")
        self.assertEqual(bad, [], f"계측 지점에 bare except 가 남아 있다: {bad}")

    def test_remaining_bare_excepts_do_not_grow(self):
        """🔖 백로그 — 이번 범위 밖의 bare except.

        사용자가 이번 작업을 안정화의 **마지막 구현 묶음**으로 고정했으므로
        범위를 넓히지 않는다. 다만 **늘어나지는 못하게** 못을 박는다.
        (전부 hyeoks_morning.py 안이고 전부 브리핑 표시 경로다)
        """
        BACKLOG = 5
        found = []
        for path in sorted(ROOT.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            found += [f"{path.name}:{n.lineno}" for n in ast.walk(tree)
                      if isinstance(n, ast.ExceptHandler) and n.type is None]
        self.assertLessEqual(len(found), BACKLOG,
                             f"bare except 가 늘었다({len(found)} > {BACKLOG}): {found}")
        self.assertTrue(all(f.startswith("hyeoks_morning.py") for f in found),
                        f"백로그 밖 파일에 bare except 가 생겼다: {found}")

    def test_doc_and_code_agree_on_the_count(self):
        text = DOC.read_text(encoding="utf-8")
        self.assertIn("12곳", text)
        rows = [l for l in text.splitlines() if l.startswith("| ") and l.count("|") >= 9]
        self.assertEqual(len(rows) - 1, 12, "분류표의 행 수가 12가 아니다")

    def test_critical_sites_are_the_ones_that_touch_selection(self):
        crit = {k for k, v in EXPECTED.items() if v == FT.CRITICAL}
        self.assertEqual(crit, {"surge_scan", "entry_price", "theme_money"})

    def test_extra_telemetry_covers_v1_v2_v3_and_ledger(self):
        """⑤ 는 V3 요약만으로 참이 되면 안 되고, ⑦ 은 append 성공으로 참이 되면 안 된다."""
        self.assertIn("v2_score", EXTRA)
        self.assertIn("ledger", EXTRA)
        src = (ROOT / "omakase.py").read_text(encoding="utf-8")
        for key in ("v1_parse_errors", "v2_parse_errors", "v3_parse_errors"):
            with self.subTest(key):
                self.assertIn(key, src)
        self.assertIn("read-after-write", src)


class GateLinkTests(unittest.TestCase):
    """분류가 Gate 로 이어지는가 — 세기만 하고 끝나면 의미가 없다."""

    def test_critical_and_observe_block_the_gate(self):
        t = FT.Telemetry()
        t.note("x", "y", FT.CRITICAL)
        t.note("z", "y", FT.OBSERVE)
        self.assertEqual(t.total(), 2)

    def test_display_and_research_do_not_block(self):
        t = FT.Telemetry()
        t.note("x", "y", FT.DISPLAY)
        t.note("z", "y", FT.RESEARCH)
        self.assertEqual(t.total(), 0)

    def test_note_returns_none_so_the_value_is_not_used(self):
        """잘못된 기본값 금지 — note 는 값을 주지 않는다."""
        self.assertIsNone(FT.Telemetry().note("x", "y"))


class ObserveStoreTests(unittest.TestCase):
    """⑩⑪ — 버린 행이 보이는가."""

    def test_feature_store_counts_dropped_rows(self):
        import feature_store as FS
        rows = FS.build_rows("2026-09-18", [["이름", "005930"] + [""] * 40, None])
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(FS.DROPPED), 1)

    def test_rank_pool_counts_dropped_rows(self):
        import rank_pool as RP
        RP.build_rows("2026-09-18", "차트TOP2", [None], [], 29)
        self.assertEqual(len(RP.DROPPED), 1)

    def test_counters_reset_per_call(self):
        import feature_store as FS
        FS.build_rows("2026-09-18", [None])
        FS.build_rows("2026-09-18", [["이름", "005930"] + [""] * 40])
        self.assertEqual(FS.DROPPED, [])


class ResearchTrialsFailClosedTests(unittest.TestCase):
    """⑫ — 여기만 동작을 바꿨다. 상한을 세는 함수는 못 세면 거부해야 한다."""

    def test_broken_row_refuses_the_count(self):
        import csv
        import tempfile
        import research_trials as RT
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "trials.csv"
            with open(p, "w", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["stage", "features", "note"])
                w.writerow(["1", "a", ""])
                w.writerow(["깨짐", "b", ""])
            with self.assertRaises(ValueError) as cm:
                RT.counts(str(p))
            self.assertIn("집계를 거부", str(cm.exception))

    def test_clean_rows_still_count(self):
        import csv
        import tempfile
        import research_trials as RT
        with tempfile.TemporaryDirectory() as d:
            p = pathlib.Path(d) / "trials.csv"
            with open(p, "w", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh)
                w.writerow(["stage", "features", "note"])
                w.writerow(["1", "a", ""])
                w.writerow(["1", "b", ""])
            self.assertEqual(RT.counts(str(p))[1], 2)

    def test_why_this_one_is_different(self):
        """상한을 세는 함수가 조용히 건너뛰면 상한이 무의미해진다."""
        src = (ROOT / "research_trials.py").read_text(encoding="utf-8")
        self.assertIn("상한을 셀 수 없으므로", src)


if __name__ == "__main__":
    unittest.main()
