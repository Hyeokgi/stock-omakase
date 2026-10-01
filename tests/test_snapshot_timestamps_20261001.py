"""2026-10-01 — 스냅샷 메타에 가격 요청·수신·기록 시각을 남긴다(코덱스 교차점검).

`capturedAt` 은 **실행 시작** 시각이다(15:05 슬롯 실측 ~15:02:48). 전 종목 가격은 그 뒤
한 번의 요청으로 온다. 요청 직전·수신 직후·파일 기록 시각을 따로 남기고,
`capturedAt` 의 뜻과 기존 키는 그대로 둔다(시간창 판정·결측 감시·계좌 어댑터가 읽는다).

네트워크 없이 실제 `main()` 을 돌린다. 시세 응답·테마·업종·지수는 가짜로 끼운다.
"""
import datetime
import gzip
import os
import tempfile
import unittest
from unittest import mock

import hyeoks_market_snapshot as M
from hyeoks_closing_bet import read_snapshot

KST = M.KST


class _Resp:
    status_code = 200

    def __init__(self, rows):
        self._rows = rows

    def json(self):
        return self._rows


def _rows(n):
    return [{"itemcode": f"{i:06d}", "itemname": f"종목{i}", "tradeAmount": str(10_000 + i),
             "nowPrice": "1000", "openPrice": "990", "prevChangeRate": "1.0"}
            for i in range(n)]


class SnapshotTimestampTests(unittest.TestCase):
    def run_main(self, clock):
        """clock: datetime.now 가 차례로 돌려줄 시각들. 요청 사이 시간 흐름을 흉내 낸다."""
        tmp = tempfile.mkdtemp()
        ticks = iter(clock)

        class FakeDT(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return next(ticks)

        rows = _rows(M.MIN_ROWS)
        with mock.patch.object(M, "OUT_DIR", tmp), \
             mock.patch.object(M.SESSION, "get", return_value=_Resp(rows)), \
             mock.patch.object(M, "fetch_theme_data", return_value=([], {})), \
             mock.patch.object(M, "fetch_sector_data", return_value=([], {})), \
             mock.patch.object(M, "index_snapshot",
                               return_value={"KOSPI": ("1", "0"), "KOSDAQ": ("1", "0")}), \
             mock.patch.object(M, "write_readme", return_value=None), \
             mock.patch.object(M.datetime, "datetime", FakeDT), \
             mock.patch("sys.argv", ["x", "--slot", "1505", "--force"]), \
             mock.patch("builtins.print"):
            self.assertEqual(M.main(), 0)
        path = next(os.path.join(tmp, f) for f in os.listdir(tmp) if f.endswith("_1505.csv.gz"))
        with gzip.open(path, "rt", encoding="utf-8") as fp:
            first = fp.readline().strip().split(",")
        return path, first

    def setUp(self):
        base = datetime.datetime(2026, 10, 2, 15, 2, 48, tzinfo=KST)
        # now() 호출 순서: 시작 → 요청 직전 → 수신 직후 → 기록(메타 행)
        self.clock = [base, base + datetime.timedelta(seconds=1),
                      base + datetime.timedelta(seconds=9),
                      base + datetime.timedelta(seconds=40)]
        self.path, self.first = self.run_main(self.clock)
        self.meta = dict(kv.split("=", 1) for kv in self.first[1:] if "=" in kv)

    def test_captured_at_keeps_meaning_of_run_start(self):
        self.assertEqual(self.meta["capturedAt"], self.clock[0].isoformat())

    def test_price_request_and_receipt_are_recorded(self):
        self.assertEqual(self.meta["priceRequestedAt"], self.clock[1].isoformat())
        self.assertEqual(self.meta["priceReceivedAt"], self.clock[2].isoformat())

    def test_written_at_is_after_receipt(self):
        self.assertEqual(self.meta["writtenAt"], self.clock[3].isoformat())
        received = datetime.datetime.fromisoformat(self.meta["priceReceivedAt"])
        self.assertLessEqual(received, datetime.datetime.fromisoformat(self.meta["writtenAt"]))

    def test_existing_keys_keep_their_positions(self):
        """새 키는 **뒤에** 붙는다 — 기존 키의 순서를 바꾸지 않는다."""
        keys = [kv.split("=", 1)[0] for kv in self.first[1:]]
        self.assertEqual(keys[:6], ["capturedAt", "slot", "total", "kept", "KOSPI", "KOSDAQ"])
        self.assertEqual(keys[6:], ["priceRequestedAt", "priceReceivedAt", "writtenAt"])

    def test_existing_reader_still_parses_rows(self):
        meta, rows, dup = read_snapshot(self.path)
        self.assertEqual(len(rows), M.MIN_ROWS)
        self.assertEqual(dup, 0)
        self.assertIn("priceReceivedAt", meta)


if __name__ == "__main__":
    unittest.main()
