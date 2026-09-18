# -*- coding: utf-8 -*-
"""
DB_실적 보존 — 2026-09-18.

9/17 실적 수집기: 시간 예산 55분에 걸려 143종목 중 127개만 처리했다.
로그는 "나머지는 다음 실행에서 이어서 수집됩니다" 라고 말했지만 실제로는

  ① 쓰기 경로가 `clear()` 후 통째로 다시 써서 **나머지 16종목의 이전 행이 사라졌고**
  ② 대상 순서가 시트 순서 고정이라 다음 실행도 **같은 앞쪽 127개**를 돌았다

즉 꼬리는 이어받기는커녕 매일 지워지고 영영 안 채워졌다.
바로 옆 DB_컨센서스 경로는 이미 merge 로 올바르게 보존하고 있었다 —
같은 파일 안에서 두 시트가 다르게 동작했다.
"""
import pathlib
import sys
import types
import unittest

# 무거운 의존성은 스텁으로 막고 순수 함수만 본다
for name, attrs in (("gspread", {}), ("oauth2client", {}),
                    ("oauth2client.service_account", {"ServiceAccountCredentials": object}),
                    ("bs4", {"BeautifulSoup": object})):
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
sys.modules["oauth2client"].service_account = sys.modules["oauth2client.service_account"]

import hyeoks_earnings_collector as E      # noqa: E402

H = E.EARNINGS_HEADER


def row(code, stamp="2026-09-01 20:00:00", name="종목"):
    r = [code, name] + [""] * (len(H) - 3) + [stamp]
    assert len(r) == len(H)
    return r


class MergeTests(unittest.TestCase):
    def test_unprocessed_rows_survive(self):
        """시간 예산에 잘린 종목이 시트에서 사라지면 안 된다 — 이번 사고의 핵심."""
        old = [list(H), row("000660", "2026-09-17 20:00:00"), row("005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        codes = [E._code_of(r) for r in merged[1:]]
        self.assertEqual(sorted(codes), ["000660", "005930"])

    def test_refreshed_row_wins_and_is_not_duplicated(self):
        old = [list(H), row("005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[1][E.STAMP_COL], "2026-09-18 20:00:00")

    def test_preserved_row_keeps_its_own_timestamp(self):
        """보존된 행에 오늘 시각을 찍으면 '오늘 갱신됐다'는 거짓말이 된다."""
        old = [list(H), row("000660", "2026-09-10 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        kept = next(r for r in merged[1:] if E._code_of(r) == "000660")
        self.assertEqual(kept[E.STAMP_COL], "2026-09-10 20:00:00")

    def test_quoted_code_matches(self):
        old = [list(H), row("'005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        self.assertEqual(len(E.merge_earnings_rows(old, new)), 2)

    def test_empty_sheet(self):
        new = [list(H), row("005930")]
        self.assertEqual(E.merge_earnings_rows([], new), new)

    def test_short_old_row_is_padded_not_dropped(self):
        old = [list(H), ["000660", "하이닉스"]]
        merged = E.merge_earnings_rows(old, [list(H), row("005930")])
        kept = next(r for r in merged[1:] if E._code_of(r) == "000660")
        self.assertEqual(len(kept), len(H))

    def test_header_mismatch_refuses(self):
        old = [["종목코드", "옛스키마"], row("000660")]
        with self.assertRaises(ValueError):
            E.merge_earnings_rows(old, [list(H), row("005930")])

    def test_bad_output_schema_refuses(self):
        with self.assertRaises(ValueError):
            E.merge_earnings_rows([], [["종목코드"], ["005930"]])


class StaleFirstTests(unittest.TestCase):
    def test_oldest_first(self):
        old = [list(H), row("A", "2026-09-17"), row("B", "2026-09-10"), row("C", "2026-09-18")]
        self.assertEqual(E.stale_first(["A", "B", "C"], old), ["B", "A", "C"])

    def test_never_collected_goes_first(self):
        """한 번도 못 받은 종목이 맨 앞에 와야 언젠가 받는다."""
        old = [list(H), row("A", "2026-09-17")]
        self.assertEqual(E.stale_first(["A", "Z"], old)[0], "Z")

    def test_yesterdays_tail_is_todays_head(self):
        """어제 예산에 잘린 꼬리가 오늘 맨 앞에 오는가 — ②의 회귀."""
        codes = [f"{i:06d}" for i in range(5)]
        old = [list(H)] + [row(c, "2026-09-17 20:00:00") for c in codes[:3]]
        self.assertEqual(E.stale_first(codes, old)[:2], ["000003", "000004"])

    def test_no_old_sheet_keeps_a_stable_order(self):
        self.assertEqual(E.stale_first(["B", "A"], []), ["A", "B"])

    def test_header_row_is_not_a_stock(self):
        self.assertNotIn("종목코드", E.stale_first(["A"], [list(H), row("A")]))


class SchemaTests(unittest.TestCase):
    def test_stamp_column_is_last(self):
        self.assertEqual(E.STAMP_COL, len(H) - 1)
        self.assertEqual(H[E.STAMP_COL], "갱신일시")

    def test_header_is_written_from_one_place(self):
        """지역 사본을 두면 둘이 어긋나고 병합이 헛돈다."""
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertEqual(src.count('"종목코드", "종목명", "최신분기"'), 1)


if __name__ == "__main__":
    unittest.main()
