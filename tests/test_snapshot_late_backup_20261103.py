# 자정을 넘겨 늦게 도착한 스냅샷 백업 실행이 개장 확인 전에 시간 창에서 조용히 끝나는지 확인하는 시험
"""2026-11-03 — 11/02 15:07 백업 cron 이 00:38 에 도착해 개장 확인('지수 봉 없음')에서 실패했다.

시간 창 검사를 개장 확인보다 먼저 한다. 창 밖이면 개장 확인을 부르지 않고 0 으로 끝난다.
창 안에서는 예전처럼 개장 확인을 거치고, 확인 실패는 여전히 오류다(휴장으로 넘기지 않는다).
"""
import datetime
import unittest
from unittest import mock

import hyeoks_market_snapshot as M

KST = M.KST


def run(at, slot, trading):
    class FakeDT(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return at

    with mock.patch.object(M.datetime, "datetime", FakeDT), \
         mock.patch.object(M, "is_trading_day", side_effect=trading) as td, \
         mock.patch("sys.argv", ["x", "--slot", slot]), \
         mock.patch("builtins.print"):
        return M.main(), td


def fail(_day):
    raise RuntimeError("개장 확인 실패를 정상 휴장으로 처리하지 않습니다")


class LateBackupTests(unittest.TestCase):
    def test_after_midnight_backup_ends_quietly_without_open_check(self):
        rc, td = run(datetime.datetime(2026, 11, 3, 0, 38, 56, tzinfo=KST), "1505", fail)
        self.assertEqual(rc, 0)
        td.assert_not_called()

    def test_evening_backup_ends_quietly(self):
        rc, td = run(datetime.datetime(2026, 11, 2, 21, 19, tzinfo=KST), "1300", fail)
        self.assertEqual(rc, 0)
        td.assert_not_called()

    def test_inside_window_still_checks_and_still_fails_loudly(self):
        with self.assertRaises(RuntimeError):
            run(datetime.datetime(2026, 11, 3, 15, 2, tzinfo=KST), "1505", fail)

    def test_inside_window_holiday_ends_quietly(self):
        rc, td = run(datetime.datetime(2026, 11, 3, 15, 2, tzinfo=KST), "1505", lambda d: False)
        self.assertEqual(rc, 0)
        td.assert_called_once_with("2026-11-03")


if __name__ == "__main__":
    unittest.main()
