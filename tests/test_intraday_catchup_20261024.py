# 분봉 프로파일 빠진 날 메우기 시험 — 예약이 자정을 넘기거나 하루가 통째로 빠져도 다음 실행이 최근 5거래일을 채우는지 확인한다
"""2026-10-24 — 10/12 분봉은 예약이 9시간 늦은 10/13 01:14 에 돌아 '오늘(10/13)' 만 보다가 끝났고, 기한이 지나 사라졌다."""
import datetime
import os
import sys
import tempfile
import unittest
from unittest import mock

_OUT = sys.stdout
import hyeoks_intraday_collector as C  # noqa: E402 — import 때 sys.stdout 을 같은 버퍼 위 새 래퍼로 바꾼다
if sys.stdout is not _OUT:
    # 두 래퍼가 한 버퍼를 나눠 쓰면, 버려진 쪽이 정리되며 버퍼를 닫아 뒤 시험의 print 가 전부 깨진다(전체 시험 90건 오류로 확인).
    sys.stdout.detach()
    sys.stdout = _OUT

KST = datetime.timezone(datetime.timedelta(hours=9))
DAYS = ["2026-10-02", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-12", "2026-10-13"]


def at(day, hm):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


class CatchupDays(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def touch(self, *days):
        for d in days:
            open(os.path.join(self.dir, f"{d}.csv.gz"), "wb").close()

    def test_after_midnight_run_takes_the_finished_day(self):
        self.touch("2026-10-02", "2026-10-06", "2026-10-07", "2026-10-08")
        self.assertEqual(C.catchup_days(DAYS[:-1], at("2026-10-13", "01:14"), self.dir), ["2026-10-12"])

    def test_today_only_after_close_and_older_gaps_filled(self):
        self.touch("2026-10-07", "2026-10-12")
        self.assertEqual(C.catchup_days(DAYS, at("2026-10-13", "15:00"), self.dir), ["2026-10-06", "2026-10-08"])
        self.assertEqual(C.catchup_days(DAYS, at("2026-10-13", "15:40"), self.dir),
                         ["2026-10-06", "2026-10-08", "2026-10-13"])

    def test_window_is_five_trading_days(self):
        self.assertNotIn("2026-10-02", C.catchup_days(DAYS, at("2026-10-13", "16:00"), self.dir))
        self.assertEqual(len(C.catchup_days(DAYS, at("2026-10-13", "16:00"), self.dir)), C.CATCHUP_DAYS)

    def test_calendar_failure_falls_back_to_today(self):
        self.assertEqual(C.catchup_days([], at("2026-10-13", "16:00"), self.dir), ["2026-10-13"])


class MainLoop(unittest.TestCase):
    def run_main(self, now, argv=(), existing=()):
        d = tempfile.mkdtemp()
        for day in existing:
            open(os.path.join(d, f"{day}.csv.gz"), "wb").close()

        class FakeDT(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return now

        calls = []
        with mock.patch.object(C, "OUT_DIR", d), mock.patch.object(C, "trading_days", return_value=DAYS[:-1]), \
             mock.patch.object(C, "collect_day", side_effect=lambda day, *rest: calls.append(day) or 0), \
             mock.patch.object(C.datetime, "datetime", FakeDT), mock.patch.object(sys, "argv", ["x", *argv]), \
             mock.patch("builtins.print"):
            rc = C.main()
        return rc, calls

    def test_1012_case_is_collected_by_the_late_run(self):
        rc, calls = self.run_main(at("2026-10-13", "01:14"), existing=("2026-10-02", "2026-10-06", "2026-10-07", "2026-10-08"))
        self.assertEqual((rc, calls), (0, ["2026-10-12"]))

    def test_nothing_missing_does_nothing(self):
        rc, calls = self.run_main(at("2026-10-13", "01:14"), existing=DAYS[:-1])
        self.assertEqual((rc, calls), (0, []))

    def test_explicit_date_is_kept(self):
        _, calls = self.run_main(at("2026-10-13", "16:00"), ["--date", "2026-10-07"], existing=DAYS)
        self.assertEqual(calls, ["2026-10-07"])


class CollectDayGuards(unittest.TestCase):
    def test_today_before_close_is_not_saved(self):
        with mock.patch.object(C, "OUT_DIR", tempfile.mkdtemp()), mock.patch.object(C, "pick_targets") as pick, \
             mock.patch("builtins.print"):
            self.assertEqual(C.collect_day("2026-10-13", 300, False, at("2026-10-13", "14:00"), DAYS), 0)
            pick.assert_not_called()

    def test_day_outside_calendar_is_refused(self):
        with mock.patch.object(C, "pick_targets") as pick, mock.patch("builtins.print"):
            self.assertEqual(C.collect_day("2026-09-01", 300, False, at("2026-10-13", "16:00"), DAYS), 0)
            pick.assert_not_called()


if __name__ == "__main__":
    unittest.main()
