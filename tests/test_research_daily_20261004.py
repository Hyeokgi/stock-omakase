"""연구 일일 수집·보고 — 처음 값 보존, 미확정 당일 봉 제외, 배지 사라진 대장 계속 추적, 보고에 수익률 없음."""
import datetime
import gzip
import csv
import os
import shutil
import tempfile
import unittest
from unittest import mock

import hyeoks_research_daily as R
from hyeoks_closing_bet import KST

XML = ('<?xml version="1.0" encoding="EUC-KR" ?><protocol><chartdata symbol="005930">'
       '<item data="20261001|100|110|90|105|1000" /><item data="20261002|105|120|100|118|2000" />'
       '</chartdata></protocol>')


class Resp:
    def __init__(self, text="", code=200):
        self.text, self.status_code = text, code


def rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class ParseAndFetch(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(R.parse_fchart(XML), [("2026-10-01", 100, 110, 90, 105, 1000), ("2026-10-02", 105, 120, 100, 118, 2000)])

    def test_parse_rejects_garbage(self):
        self.assertEqual(R.parse_fchart("not xml"), [])
        self.assertEqual(R.parse_fchart('<a><item data="2026|1|2" /></a>'), [])

    def test_fetch_records_errors_instead_of_raising(self):
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("", 500))[1], "HTTP 500")
        def boom(*a, **k):
            raise ConnectionError("down")
        self.assertIn("ConnectionError", R.fetch("x", 5, boom)[1])
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("<a/>"))[1], "빈 응답")


class Store(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)
        self.after_close = datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)

    def test_first_seen_value_is_kept_and_changes_are_logged(self):
        bars = R.parse_fchart(XML)
        a, r, _ = R.store({"005930": bars}, "t1", self.d, "2026-10-02", self.after_close)
        self.assertEqual((a, r), (2, 0))
        changed = [bars[0], ("2026-10-02", 105, 120, 100, 59, 2000)]          # 종가가 절반 — 액면분할 같은 조정
        a, r, _ = R.store({"005930": changed}, "t2", self.d, "2026-10-02", self.after_close)
        self.assertEqual((a, r), (0, 1))
        kept = [x for x in rows(os.path.join(self.d, "2026-10.csv.gz")) if x["date"] == "2026-10-02"][0]
        self.assertEqual((kept["close"], kept["fetchedAt"]), ("118", "t1"), "처음 받은 값을 덮어쓰지 않는다")
        rev = rows(os.path.join(self.d, "revisions.csv.gz"))
        self.assertEqual([(x["field"], x["old"], x["new"]) for x in rev], [("close", "118", "59")])

    def test_todays_bar_is_not_stored_before_the_close_is_settled(self):
        intraday = datetime.datetime(2026, 10, 2, 14, 0, tzinfo=KST)
        a, _, skipped = R.store({"005930": R.parse_fchart(XML)}, "t", self.d, "2026-10-02", intraday)
        self.assertEqual((a, skipped), (1, 1))

    def test_identical_refetch_writes_nothing_new(self):
        R.store({"005930": R.parse_fchart(XML)}, "t1", self.d, "2026-10-02", self.after_close)
        self.assertEqual(R.store({"005930": R.parse_fchart(XML)}, "t2", self.d, "2026-10-02", self.after_close)[:2], (0, 0))
        self.assertFalse(os.path.exists(os.path.join(self.d, "revisions.csv.gz")))


class UniverseAndRun(unittest.TestCase):
    def test_past_leaders_stay_in_the_universe_after_the_badge_is_gone(self):
        days = R.snapshot_days()
        today = set(R.universe(days[-1], lookback=1))
        wide = set(R.universe(days[-1]))
        self.assertTrue(today <= wide)
        import hyeoks_theme_abc as T
        from hyeoks_closing_bet import read_snapshot
        _, rws, _ = read_snapshot(os.path.join(R.SNAP_DIR, f"{days[-5]}_1505.csv.gz"))
        old_leaders = {x["code"] for x in T.build_B_current(T.build_A(rws)[0])[0]}
        self.assertTrue(old_leaders <= wide, "5관측일 전 대장은 배지가 없어도 계속 받는다")

    def test_empty_store_triggers_a_one_time_backfill(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        calls = []
        def get(url, timeout):
            calls.append(url)
            return Resp(XML)
        now = datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)
        out = R.run_bars("2026-10-02", get, bars_dir=d, now=now, sleep=lambda s: None)
        self.assertEqual(out["mode"], "백필")
        self.assertIn(f"count={R.BACKFILL_N}", calls[0])
        self.assertEqual(out["requested"], len(R.backfill_universe()) + len(R.INDEXES))
        out2 = R.run_bars("2026-10-02", get, bars_dir=d, now=now, sleep=lambda s: None)
        self.assertEqual(out2["mode"], "일일")
        self.assertIn(f"count={R.DAILY_N}", calls[-1])


class Report(unittest.TestCase):
    def test_report_has_tag_counts_and_no_return_numbers(self):
        struct = {"day": "2026-10-02", "prev": "2026-10-01", "leaders": 45, "prev_leaders": 44,
                  "kept_leader": 7, "swapped": 30, "themes_kept": 35}
        locked = {"observed": 3, "valid": 2, "bad": {}}
        bars = {"mode": "일일", "requested": 400, "ok": 398, "failed": 2, "fail_sample": {}, "added": 398,
                "revised": 0, "unsettled_skipped": 0}
        txt = R.report_text("2026-10-02", bars, struct, locked, bars_dir=tempfile.gettempdir())
        self.assertTrue(txt.startswith("[연구]"))
        self.assertIn("유효 비교일 2/60 (수익률 비공개)", txt)
        self.assertNotIn("%", txt, "보고에 수익률·비율 숫자를 넣지 않는다")

    def test_send_needs_a_token_and_never_leaks_it(self):
        self.assertEqual(R.send("x", None, ""), (False, "TELEGRAM_BOT_TOKEN 없음 — 보내지 않았다"))
        def boom(*a, **k):
            raise RuntimeError("https://api.telegram.org/botSECRET/sendMessage")
        ok, err = R.send("x", boom, "SECRET")
        self.assertFalse(ok)
        self.assertNotIn("SECRET", err)

    def test_send_goes_to_the_fixed_channel(self):
        seen = {}
        def post(url, data, timeout):
            seen.update(data)
            return Resp("", 200)
        with mock.patch.dict(os.environ, {"TELEGRAM_CHAT_ID_OVERRIDE": ""}):
            self.assertEqual(R.send("[연구] x", post, "T"), (True, ""))
        import telegram_target
        self.assertEqual(seen["chat_id"], telegram_target.CHAT_ID)

    def test_late_cron_after_midnight_targets_the_previous_day(self):
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 3, 1, 0, tzinfo=KST)), "2026-10-02")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)), "2026-10-02")

    def test_non_trading_days_are_skipped(self):
        self.assertFalse(R.trading_day("2026-10-05")[0])    # 개천절 대체공휴일
        self.assertTrue(R.trading_day("2026-10-06")[0])
        ok, why = R.trading_day("2027-01-04")
        self.assertFalse(ok)
        self.assertIn("범위 밖", why)


if __name__ == "__main__":
    unittest.main()
