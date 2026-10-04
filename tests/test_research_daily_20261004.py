"""연구 일일 수집·보고 — 처음 값 보존, 미확정 봉 제외, 배지 사라진 대장 계속 추적, 보고에 수익률 없음.

2026-10-05: Codex 교차 검토(docs/collaboration/2026-10-05_Codex_테마대장경로_인계검토_회신.md)의
R1~R6 합성 재현 9건을 **안전 동작이 통과하는 회귀 시험**으로 옮겼다(`CodexReviewR1toR6`).
모두 네트워크 없이 임시 폴더와 합성 값으로 돈다.
"""
import contextlib
import csv
import datetime
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import hyeoks_research_daily as R
from hyeoks_closing_bet import KST

XML = ('<?xml version="1.0" encoding="EUC-KR" ?><protocol><chartdata symbol="{sym}">'
       '<item data="20261001|100|110|90|105|1000" /><item data="20261002|105|120|100|118|2000" />'
       '</chartdata></protocol>')
AFTER = datetime.datetime(2026, 10, 6, 19, 40, tzinfo=KST)


def xml(sym="005930"):
    return XML.format(sym=sym)


def bar(day="2026-10-06", volume=123456789):
    return (day, 100.0, 110.0, 90.0, 105.0, volume)


class Resp:
    def __init__(self, text="", code=200):
        self.text, self.status_code = text, code


def rows(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class TmpDir(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)


class ParseAndFetch(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(R.parse_fchart(xml()), [("2026-10-01", 100, 110, 90, 105, 1000), ("2026-10-02", 105, 120, 100, 118, 2000)])

    def test_parse_rejects_garbage(self):
        self.assertEqual(R.parse_fchart("not xml"), [])
        self.assertEqual(R.parse_fchart('<a><item data="2026|1|2" /></a>'), [])

    def test_parse_counts_reject_reasons_and_zero_volume(self):
        text = ('<a><chartdata symbol="X">'
                '<item data="20261001|0|0|0|0|0" />'            # 0 가격
                '<item data="20261002|100|90|95|98|10" />'      # 고가 < 시가
                '<item data="20261005|100|100|100|100|0" />'    # 무거래 봉 — 보존
                '</chartdata></a>')
        bars, info = R.parse_fchart_detail(text)
        self.assertEqual([b[0] for b in bars], ["2026-10-05"])
        self.assertEqual(info["rejects"], {"0가격": 1, "가격관계": 1})
        self.assertEqual(info["zero_volume"], 1)
        self.assertEqual(info["symbol"], "X")

    def test_fetch_records_errors_instead_of_raising(self):
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("", 500))[1], "HTTP 500")
        def boom(*a, **k):
            raise ConnectionError("down")
        self.assertIn("ConnectionError", R.fetch("x", 5, boom)[1])
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("<a/>"))[1], "빈 응답")

    def test_fetch_rejects_a_reply_for_another_symbol(self):
        bars, err, _ = R.fetch("000660", 5, lambda *a, **k: Resp(xml("005930")))
        self.assertEqual(bars, [])
        self.assertIn("종목 불일치", err)

    def test_numbers_round_trip_exactly(self):
        self.assertEqual(R.num(123456789.0), "123456789")
        self.assertEqual(float(R.num(0.1 + 0.2)), 0.1 + 0.2)


class Store(TmpDir):
    def setUp(self):
        super().setUp()
        self.after_close = datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)

    def test_first_seen_value_is_kept_and_changes_are_logged(self):
        bars = R.parse_fchart(xml())
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
        a, _, skipped = R.store({"005930": R.parse_fchart(xml())}, "t", self.d, "2026-10-02", intraday)
        self.assertEqual((a, skipped), (1, 1))

    def test_identical_refetch_writes_nothing_new(self):
        R.store({"005930": R.parse_fchart(xml())}, "t1", self.d, "2026-10-02", self.after_close)
        self.assertEqual(R.store({"005930": R.parse_fchart(xml())}, "t2", self.d, "2026-10-02", self.after_close)[:2], (0, 0))
        self.assertFalse(os.path.exists(os.path.join(self.d, "revisions.csv.gz")))

    def test_per_code_receive_time_is_stored(self):
        R.store({"005930": [bar()]}, {"005930": "2026-10-06T19:41:07+09:00"}, self.d, "2026-10-06", AFTER)
        self.assertEqual(rows(os.path.join(self.d, "2026-10.csv.gz"))[0]["fetchedAt"], "2026-10-06T19:41:07+09:00")


class UniverseAndRun(TmpDir):
    @staticmethod
    def get_for(calls):
        def get(url, timeout):
            calls.append(url)
            sym = url.split("symbol=")[1].split("&")[0]
            return Resp(xml(sym))
        return get

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

    def test_first_run_backfills_every_code_then_daily_requests_are_small(self):
        calls = []
        now = datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)
        out = R.run_bars("2026-10-02", self.get_for(calls), bars_dir=self.d, now=now, sleep=lambda s: None)
        self.assertTrue(all(f"count={R.BACKFILL_N}" in u for u in calls))
        planned = set(R.backfill_universe(upto="2026-10-02")) | set(R.universe("2026-10-02"))
        self.assertEqual(out["plannedCodes"], len(planned) + len(R.INDEXES))
        self.assertEqual(out["covered"], out["plannedCodes"])
        self.assertEqual(out["status"], "OK")
        calls.clear()
        out2 = R.run_bars("2026-10-02", self.get_for(calls), bars_dir=self.d, now=now, sleep=lambda s: None)
        self.assertTrue(all(f"count={R.DAILY_N}" in u for u in calls))
        self.assertLessEqual(out2["plannedCodes"], out["plannedCodes"])

    def test_gap_since_last_bar_sets_the_request_size(self):
        self.assertEqual(R.request_size(None, "2026-10-06"), R.BACKFILL_N)
        self.assertEqual(R.request_size({"backfilled": True, "last": "2026-10-02"}, "2026-10-06"), 9)
        self.assertEqual(R.request_size({"backfilled": True, "last": "2025-01-02"}, "2026-10-06"), R.BACKFILL_N)

    def test_time_budget_stops_and_leaves_the_rest_pending(self):
        with mock.patch.object(R, "plan_codes", return_value=["005930", "000660"]), mock.patch.object(R, "INDEXES", ()):
            out = R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None, budget_s=-1)
        self.assertEqual((out["requested"], out["notAttempted"], out["status"]), (0, 2, "FAILED"))
        self.assertEqual(R.pending_codes(R.load_coverage(self.d)), ["000660", "005930"])


class CodexReviewR1toR6(TmpDir):
    """Codex 가 발견 당시 코드에서 실패로 재현한 9건 — 이제 안전 동작으로 통과해야 한다."""

    def test_R1_partial_backfill_retries_missing_history(self):
        calls, fail_first = [], {"000660"}
        def fetch(code, n, get):
            calls.append((code, n))
            if code in fail_first:
                fail_first.remove(code)
                return [], "synthetic timeout"
            return [bar()], ""
        with mock.patch.object(R, "backfill_universe", return_value=["005930", "000660"]), \
                mock.patch.object(R, "universe", return_value=["005930", "000660"]), \
                mock.patch.object(R, "INDEXES", ()), mock.patch.object(R, "fetch", side_effect=fetch):
            R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
            calls.clear()
            R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
        self.assertIn(("000660", 300), calls, "실패한 종목은 최근 5봉이 아니라 초기 기간을 다시 받는다")
        self.assertIn(("005930", R.DAILY_N), calls)

    def test_R1_pending_code_is_retried_even_after_leaving_the_universe(self):
        def fetch(code, n, get):
            return ([], "down", {}) if code == "000660" else ([bar()], "", {})
        uni = [["005930", "000660"], ["005930"]]
        with mock.patch.object(R, "backfill_universe", return_value=[]), \
                mock.patch.object(R, "universe", side_effect=lambda *a, **k: uni.pop(0)), \
                mock.patch.object(R, "INDEXES", ()), mock.patch.object(R, "fetch", side_effect=fetch):
            R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
            out = R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
        self.assertEqual(out["plannedCodes"], 2)

    def test_R2_midnight_run_keeps_finished_previous_session(self):
        now = datetime.datetime(2026, 10, 7, 1, 0, tzinfo=KST)
        self.assertEqual(R.target_day(now), "2026-10-06")
        added, _, skipped = R.store({"005930": [bar()]}, now.isoformat(), self.d, R.target_day(now), now)
        self.assertEqual((added, skipped), (1, 0))

    def test_R2_late_schedule_after_8am_still_targets_the_settled_session(self):
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 7, 9, 30, tzinfo=KST)), "2026-10-06")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 6, 15, 59, tzinfo=KST)), "2026-10-02")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 6, 16, 0, tzinfo=KST)), "2026-10-06")

    def test_R2_historical_replay_does_not_store_later_or_intraday_bars(self):
        now = datetime.datetime(2026, 10, 6, 14, 0, tzinfo=KST)
        R.store({"005930": [bar("2026-10-02"), bar("2026-10-06")]}, now.isoformat(), self.d, "2026-10-02", now)
        self.assertEqual(R.stored_dates(self.d), ["2026-10-02"])

    def test_R3_report_reads_this_runs_collector_state_only(self):
        st = {"runId": "77", "targetDay": "2026-10-06", "status": "DEGRADED", "plannedCodes": 2, "requested": 2,
              "responded": 1, "covered": 1, "missingTarget": 0, "failed": 1, "notAttempted": 0, "rejected": {},
              "added": 5, "revised": 0, "unsettledSkipped": 0, "startedAt": "s", "finishedAt": "f"}
        with mock.patch.object(R, "BARS_DIR", self.d):
            R.save_run(st, self.d)
            out = io.StringIO()
            patches = (mock.patch.object(R, "trading_day", return_value=(True, "")),
                       mock.patch.object(R, "structure_today", return_value=None),
                       mock.patch.object(R, "locked_status", return_value=None))
            with patches[0], patches[1], patches[2], contextlib.redirect_stdout(out):
                R.main(["--report", "--date", "2026-10-06"], env={"GITHUB_RUN_ID": "77", "RESEARCH_ARCHIVE": "원격 반영 abc1234"})
            self.assertIn("요청 2", out.getvalue())
            self.assertIn("DEGRADED", out.getvalue())
            self.assertIn("원격 반영 abc1234", out.getvalue())
            out = io.StringIO()
            with patches[0], patches[1], patches[2], contextlib.redirect_stdout(out):
                R.main(["--report", "--date", "2026-10-06"], env={"GITHUB_RUN_ID": "78"})
            self.assertIn("이번 실행의 수집 기록 없음", out.getvalue(), "다른 실행의 상태를 오늘 성공으로 쓰지 않는다")
        self.assertEqual(len(open(os.path.join(self.d, "runs.csv"), encoding="utf-8").read().splitlines()), 2)

    def test_R3_send_failure_is_a_nonzero_exit(self):
        with mock.patch.object(R, "trading_day", return_value=(True, "")), \
                mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), \
                mock.patch.object(R, "stored_dates", return_value=[]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(R.main(["--report", "--send", "--date", "2026-10-06"], env={}), 1)

    def test_R4_successful_prefix_survives_collection_interrupt(self):
        calls = []
        def fetch(code, n, get):
            calls.append(code)
            if len(calls) == 2:
                raise KeyboardInterrupt("synthetic runner interruption")
            return [bar()], ""
        with mock.patch.object(R, "backfill_universe", return_value=["005930", "000660"]), \
                mock.patch.object(R, "universe", return_value=[]), \
                mock.patch.object(R, "INDEXES", ()), mock.patch.object(R, "fetch", side_effect=fetch):
            with self.assertRaises(KeyboardInterrupt):
                R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
        self.assertTrue(R.stored_dates(self.d), "중단 전에 받은 것은 저장돼 있어야 한다")
        cov = R.load_coverage(self.d)["codes"]
        self.assertTrue(cov[calls[0]]["backfilled"])
        self.assertFalse(cov[calls[1]]["backfilled"], "중단된 종목은 다음 실행의 미처리로 남는다")

    def test_R5_volume_round_trip_is_exact(self):
        R.store({"005930": [bar()]}, AFTER.isoformat(), self.d, "2026-10-06", AFTER)
        stored = rows(os.path.join(self.d, "2026-10.csv.gz"))[0]
        self.assertEqual(stored["volume"], "123456789")

    def test_R5_parser_rejects_invalid_date_and_nonfinite_ohlcv(self):
        self.assertEqual(R.parse_fchart('<root><item data="20261340|nan|inf|-1|105|100" /></root>'), [])
        self.assertEqual(R.parse_fchart('<root><item data="20261006|nan|110|90|105|100" /></root>'), [])

    def test_R5_stale_reply_is_not_counted_as_target_coverage(self):
        with mock.patch.object(R, "backfill_universe", return_value=["005930"]), \
                mock.patch.object(R, "universe", return_value=[]), \
                mock.patch.object(R, "INDEXES", ()), \
                mock.patch.object(R, "fetch", return_value=([bar("2026-10-01")], "")):
            out = R.run_bars("2026-10-06", None, self.d, now=AFTER, sleep=lambda _: None)
        self.assertEqual((out["responded"], out["covered"], out["missingTarget"]), (1, 0, 1))
        self.assertNotEqual(out["status"], "OK")

    def test_R6_locked_status_respects_registered_cap(self):
        fake = {"date": "2027-12-30", "hold": {"H": 1}, "returns_hold": {"H": {"평균": 0.0}, "B100": {"평균": 0.0}}}
        with mock.patch.object(R.T, "collect", return_value=([fake], [])):
            got = R.locked_status()
        self.assertEqual(got["valid"], 0, "120거래일 상한(또는 달력 끝) 뒤의 날은 세지 않는다")


class Report(unittest.TestCase):
    BARS = {"status": "OK", "plannedCodes": 400, "requested": 400, "responded": 398, "covered": 397,
            "missingTarget": 1, "failed": 2, "notAttempted": 0, "rejected": {}, "zeroVolume": 0, "added": 398,
            "revised": 0, "unsettledSkipped": 0, "backfilledCodes": 400, "pendingAfter": 3,
            "startedAt": "2026-10-02T19:41:00+09:00", "finishedAt": "2026-10-02T19:48:00+09:00"}

    def test_report_has_tag_counts_and_no_return_numbers(self):
        struct = {"day": "2026-10-02", "prev": "2026-10-01", "leaders": 45, "prev_leaders": 44,
                  "kept_leader": 7, "swapped": 30, "themes_kept": 35}
        locked = {"observed": 3, "valid": 2, "bad": {}, "cap": None, "capWhy": "x"}
        txt = R.report_text("2026-10-02", self.BARS, struct, locked, bars_dir=tempfile.gettempdir(), archive="원격 반영 abc")
        self.assertTrue(txt.startswith("[연구]"))
        self.assertIn("유효 비교일 2/60", txt)
        self.assertIn("수익률 비공개", txt)
        self.assertIn("원격 보관: 원격 반영 abc", txt)
        self.assertNotIn("%", txt, "보고에 수익률·비율 숫자를 넣지 않는다")

    def test_report_text_does_not_change_when_returns_change(self):
        """방화벽 — 확증 구간의 합성 수익률을 바꿔도 보고 본문이 같아야 한다(Codex 권고: '%' 부재보다 강한 검사)."""
        def days_with(ret):
            return [{"date": "2026-10-06", "hold": {"H": 2},
                     "returns_hold": {"H": {"평균": ret}, "B100": {"평균": -ret}}}]
        texts = []
        for ret in (0.031, -0.047):
            with mock.patch.object(R.T, "collect", return_value=(days_with(ret), [])):
                texts.append(R.report_text("2026-10-06", self.BARS, None, R.locked_status(),
                                           bars_dir=tempfile.gettempdir()))
        self.assertEqual(texts[0], texts[1])

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

    def test_scheduled_run_on_a_weekday_holiday_does_not_re_report(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = R.main(["--report"], now=datetime.datetime(2026, 10, 5, 19, 40, tzinfo=KST), env={})
        self.assertEqual(code, 0)
        self.assertIn("예정 거래일이 아니다", out.getvalue())

    def test_status_rules(self):
        base = dict(self.BARS, failed=0, notAttempted=0, covered=400)
        self.assertEqual(R.status_of(base), "OK")
        self.assertEqual(R.status_of(dict(base, failed=1)), "DEGRADED")
        self.assertEqual(R.status_of(dict(base, covered=300)), "DEGRADED")
        self.assertEqual(R.status_of(dict(base, responded=0)), "FAILED")


if __name__ == "__main__":
    unittest.main()
