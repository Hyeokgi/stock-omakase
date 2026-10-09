# -*- coding: utf-8 -*-
"""스캐너 수정 묶음 2026-10-09 (사용자 승인) — 지문 변경.

① 그날 스캐너 진입은 첫 적재 한 번으로 고정 — 10/8 15:11 실행이 대조군 빈자리를 늦게 채워
   두 번째 영수증(degraded)을 남겼고 그날 판정이 FAIL 이었다.
② 프로그램 조회 실패를 실제 0.0억과 구분 — 표시만, 점수 계산은 그대로.
③ 09시 전 재스캔이 전날 저녁 시간외 관측을 지우지 않는다.

omakase.py 는 무거운 의존성 때문에 import 하지 않는다. 순수 함수는 AST 로 떼어 실행하고,
본문 연결은 소스 구조로 본다(test_after_market_quotes 와 같은 방식).
"""
import ast
import datetime
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = (ROOT / "omakase.py").read_text(encoding="utf-8")
TREE = ast.parse(SRC)
KST = datetime.timezone(datetime.timedelta(hours=9))

_WANT = {"SCANNER_ENTRY_CHANNELS", "scanner_entries_today", "PROGRAM_UNKNOWN",
         "AFTER_QUOTE_MISSING", "_AFTER_STAMP", "keep_prior_after_quote"}


def _load():
    ns = {"re": re}
    for node in TREE.body:
        names = ({t.id for t in node.targets if isinstance(t, ast.Name)} if isinstance(node, ast.Assign)
                 else {node.name} if isinstance(node, ast.FunctionDef) else set())
        if names & _WANT:
            exec(compile(ast.Module([node], []), "omakase.py", "exec"), ns)
    missing = _WANT - set(ns)
    assert not missing, missing
    return ns


NS = _load()


def _fn(name):
    return next(n for n in ast.walk(TREE) if isinstance(n, ast.FunctionDef) and n.name == name)


class FirstEntryOnlyTests(unittest.TestCase):
    def test_counts_only_todays_scanner_channels(self):
        rows = [
            ["2026-10-08_차트TOP2_A", "2026-10-08", "차트TOP2"],
            ["2026-10-08_랜덤2_배지_B", "2026-10-08", "랜덤2_배지"],
            ["2026-10-08_지수벤치_KOSPI", "2026-10-08", "지수벤치_KOSPI"],
            ["2026-10-08_리포트TOP2_단기_C", "2026-10-08", "리포트TOP2_단기"],   # analyst 가 쓴다 — 세지 않음
            ["2026-10-07_차트TOP2_D", "2026-10-07", "차트TOP2"],                  # 다른 날
            [],
        ]
        self.assertEqual(NS["scanner_entries_today"](rows, "2026-10-08"), 3)
        self.assertEqual(NS["scanner_entries_today"](rows[3:], "2026-10-08"), 0)

    def test_report_rows_alone_do_not_block_the_first_scanner_entry(self):
        rows = [["x", "2026-10-12", "리포트TOP2_중기"], ["y", "2026-10-12", "리포트TOP2_단기"]]
        self.assertEqual(NS["scanner_entries_today"](rows, "2026-10-12"), 0)

    def test_add_channel_and_index_rows_stop_when_entered(self):
        fn = _fn("add_channel")
        first = fn.body[0]
        self.assertIsInstance(first, ast.If)
        self.assertEqual(ast.unparse(first.test), "_entered_today")
        self.assertIsInstance(first.body[0], ast.Return)
        self.assertIn("_entered_today = scanner_entries_today(bt_data[1:], today_str)", SRC)
        self.assertIn("if not _entered_today and tid_idx not in existing_ids:", SRC)
        self.assertIn("if not _entered_today and tid_idx_kq not in existing_ids:", SRC)


class ProgramUnknownTests(unittest.TestCase):
    def test_failure_flag_set_only_after_a_parsed_answer(self):
        fn = _fn("analyze_single_stock")
        src = ast.get_source_segment(SRC, fn)
        self.assertIn("pgtr_ok = False", src)
        ok_at = src.index("pgtr_ok = True")
        self.assertLess(src.index('if kis_res.get("rt_cd") == "0":'), ok_at)
        self.assertLess(src.index("pgtr_ntby_eok = (pgtr_qty * current_price) / 100_000_000"), ok_at)

    def test_label_kept_for_v2_score_number_replaced(self):
        text = "⚪ [수급강도 평년] 1.0배 / 프로그램:0.0억"
        out = text.rsplit(" / 프로그램:", 1)[0] + " / " + NS["PROGRAM_UNKNOWN"]
        self.assertEqual(out, "⚪ [수급강도 평년] 1.0배 / 프로그램:미확인(조회 실패)")
        self.assertIn("[수급강도 평년]", out)          # V2 점수가 읽는 라벨
        self.assertNotIn("0.0억", out)
        # 점수 계산은 숫자 0 을 그대로 쓴다 — 실패 때 pgtr_ntby_eok 를 바꾸지 않는다
        self.assertIn("if pgtr_ntby_eok >= 30: v2_base += 15", SRC)


class KeepPriorAfterQuoteTests(unittest.TestCase):
    OLD = "전일종가 대비 -1.74% (164,100원) / 정규장종가 대비 미확인 [시장 미확인 / 2026-10-08 19:54:12 KST]"
    MISSING = "미확인(당일 시간외 시세 없음)"

    def keep(self, new, old, hh, day=12):
        return NS["keep_prior_after_quote"](new, old, datetime.datetime(2026, 10, day, hh, 1, tzinfo=KST))

    def test_premarket_rescan_keeps_last_evening(self):
        self.assertTrue(self.keep(self.MISSING, self.OLD, 7))
        self.assertTrue(self.keep("", self.OLD, 8))
        nightly = "+1.20% (10,000원) [조회 2026-10-08T19:54:12+09:00; 체결시각 미확인]"
        self.assertTrue(self.keep(self.MISSING, nightly, 7))

    def test_session_and_evening_scans_overwrite(self):
        self.assertFalse(self.keep(self.MISSING, self.OLD, 9))
        self.assertFalse(self.keep(self.MISSING, self.OLD, 19))

    def test_fresh_value_or_no_prior_observation_is_not_kept(self):
        fresh = "전일종가 대비 +0.50% (1,000원) / 정규장종가 대비 미확인 [시장 미확인 / 2026-10-12 08:30:00 KST]"
        self.assertFalse(self.keep(fresh, self.OLD, 8))
        self.assertFalse(self.keep(self.MISSING, self.MISSING, 7))
        self.assertFalse(self.keep(self.MISSING, "", 7))
        self.assertFalse(self.keep(self.MISSING, "관측 (1,000원)", 7))         # 날짜 도장 없음

    def test_same_day_stamp_is_not_kept(self):
        same = "관측 (1,000원) [시장 미확인 / 2026-10-12 06:30:00 KST]"
        self.assertFalse(self.keep(self.MISSING, same, 7))

    def test_inline_block_restores_columns_from_the_sheet(self):
        fn = _fn("update_technical_data")
        block = next(n for n in ast.walk(fn) if isinstance(n, ast.If)
                     and ast.unparse(n.test) == "now_time.hour < 9")

        class Sheet:
            def get_all_values(self_inner):
                head = ["h"] * 34
                kept = ["가", "005930"] + [""] * 24 + [self.OLD, "", "시간외(시장 미확인)"] + [""] * 5
                none = ["나", "000660"] + [""] * 24 + [self.MISSING, "", "시간외 미확인"] + [""] * 5
                return [head, kept, none]

        def row(code):
            return ["x", f"'{code}"] + [""] * 24 + [self.MISSING, "", "시간외 미확인"] + [""] * 6
        results = [row("005930"), row("000660"), row("035420")]
        ns = dict(NS, helper_sheet=Sheet(), results=results,
                  now_time=datetime.datetime(2026, 10, 12, 7, 1, tzinfo=KST))
        exec(compile(ast.Module([block], []), "omakase.py", "exec"), ns)
        self.assertEqual(results[0][26:29], [self.OLD, "", "시간외(시장 미확인)"])
        self.assertEqual(results[1][26], self.MISSING)          # 전날에도 관측 없음
        self.assertEqual(results[2][26], self.MISSING)          # 시트에 없던 종목

    def test_wired_before_the_helper_sheet_write(self):
        src = ast.get_source_segment(SRC, _fn("update_technical_data"))
        at = src.index("keep_prior_after_quote(r[26], _old[26], now_time)")
        self.assertLess(src.index("if now_time.hour < 9:"), at)
        self.assertLess(at, src.index('helper_sheet.update(range_name="A1", values=helper_sheet_data'))


if __name__ == "__main__":
    unittest.main()
