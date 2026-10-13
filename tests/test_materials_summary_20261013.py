# [연구] 보고의 재료 한 줄 — 하루 아침·오후 두 실행(2026-10-10 GAS 아침 발사)을 함께 세는지 확인하는 시험
#   10/12 보고는 조회 수가 많은 아침 실행만 골라 'DART 15:05 범위 불완전'·'한투 1505건' 으로 썼다(오후 실행 누락).
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import hyeoks_research_daily as R  # noqa: E402

DART_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "polls", "pollFailures", "disclosures", "withStockCode",
             "seenBy1505", "lastPollBefore1505", "coverage1505", "capped", "status", "stopReason", "privateFile", "privateStatus"]
NEWS_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "polls", "pollFailures", "news", "withStockCode",
             "publishedBy1505", "overflowPolls", "overflowWindows", "status", "stopReason", "privateFile", "privateStatus"]


def write(d, name, cols, rows):
    with open(os.path.join(d, name), "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def dart(run, start, polls, n, cov, status="완료"):
    return {"day": "2026-10-12", "runId": run, "startedAt": f"2026-10-12T{start}:00+09:00", "polls": polls,
            "disclosures": n, "withStockCode": n, "seenBy1505": n, "coverage1505": cov, "status": status}


def news(run, start, polls, n, code, over=0, status="완료"):
    return {"day": "2026-10-12", "runId": run, "startedAt": f"2026-10-12T{start}:00+09:00", "polls": polls,
            "news": n, "withStockCode": code, "publishedBy1505": n, "overflowPolls": over, "status": status}


class TwoRunDayTests(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def test_1012_shape_uses_afternoon_coverage_and_sums_news(self):
        write(self.d, "dart_runs.csv", DART_COLS, [dart("a", "09:03", 20, 559, "불완전"), dart("b", "12:19", 17, 637, "완전"),
                                                   dart("c", "14:59", 1, 637, "완전")])
        # 일부러 시작 순서를 섞어 둔다 — 실행별 건수는 시작 시각 순으로 쓴다
        write(self.d, "kis_news_runs.csv", NEWS_COLS, [news("b", "12:19", 34, 1111, 324, 18), news("a", "09:03", 39, 1505, 485, 34),
                                                       news("c", "15:04", 1, 40, 19)])
        line = R.materials_summary("2026-10-12", self.d)
        self.assertIn("DART 공시 637건(15:05까지 637 · 15:05 범위 완전 · 완료) · 실행 3", line)
        self.assertNotIn("불완전", line)
        self.assertIn("한투 뉴스 2656건(실행별 1505+1111+40, 겹침 포함 가능 · 15:05까지 2656 · 종목코드 828 · 완료) · 실행 3", line)
        self.assertIn("넘침 의심 52회", line)
        self.assertNotIn("%", line)

    def test_single_run_line_unchanged_shape(self):
        write(self.d, "dart_runs.csv", DART_COLS, [dart("a", "14:54", 2, 621, "완전")])
        write(self.d, "kis_news_runs.csv", NEWS_COLS, [news("a", "14:53", 2, 80, 20, 1)])
        line = R.materials_summary("2026-10-12", self.d)
        self.assertIn("DART 공시 621건(15:05까지 621 · 15:05 범위 완전 · 완료)", line)
        self.assertNotIn("실행 ", line.replace("실행별", ""))
        self.assertIn("한투 뉴스 80건(15:05까지 80 · 종목코드 20 · 완료) ⚠️ 넘침 의심 1회", line)

    def test_failed_run_is_flagged(self):
        write(self.d, "dart_runs.csv", DART_COLS, [dart("a", "09:03", 20, 559, "불완전"), dart("b", "12:19", 3, 600, "완전", "중단")])
        line = R.materials_summary("2026-10-12", self.d)
        self.assertIn("미완료 실행 1(중단)", line)

    def test_no_rows(self):
        self.assertEqual(R.materials_summary("2026-10-12", self.d), "")


if __name__ == "__main__":
    unittest.main()
