# 업종 구성 PARTIAL(허브 #21) 시험 — 가격 연결 부족은 그 업종 집계만 비우고 종목의 업종 연결은 지우지 않는지 확인한다
"""2026-10-24 — '기타' 업종(1,555종목 중 스캔 연결 2)이 매일 PARTIAL 이라 10/21~10/23 모든 종목의 upjongNos 가 지워졌다.
품질(membershipStatus)은 구성 목록을 끝까지 받았는지만 본다(naver_membership.membership_status 의 정의)."""
import csv
import gzip
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import hyeoks_market_snapshot as M
import naver_membership as N

MKT = {"000010": {"tradeAmount": "5000000000", "prevChangeRate": "2.0", "marketSum": "1"},
       "000020": {"tradeAmount": "3000000000", "prevChangeRate": "-1.0", "marketSum": "1"}}


def fetch(sectors, members, kind="upjong"):
    session = Mock()
    session.get.return_value.json.return_value = sectors
    with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {kind: ("업종", 1)}), \
         patch.object(M, "fetch_members", side_effect=members), patch.object(M.time, "sleep"):
        return M.fetch_sector_data(kind, MKT)


class SectorStatus(unittest.TestCase):
    def test_unpriced_members_blank_only_that_sector(self):
        rows, mapping = fetch(
            [{"no": "1", "name": "반도체", "totalCnt": 2}, {"no": "2", "name": "기타", "totalCnt": 3}],
            [[{"itemcode": "000010"}, {"itemcode": "000020"}],
             [{"itemcode": "000010"}, {"itemcode": "999990"}, {"itemcode": "999980"}]])
        by = {r["name"]: r for r in rows}
        self.assertEqual([by["반도체"]["membershipStatus"], by["기타"]["membershipStatus"]], ["OK", "OK"])
        self.assertEqual(N.membership_status(rows), "OK")
        self.assertEqual(by["반도체"]["sumTradeAmount"], "8000000000")
        self.assertEqual((by["기타"]["sumTradeAmount"], by["기타"]["matchedCount"], by["기타"]["fetchedCount"]), ("", 1, 3))
        self.assertEqual(sorted(n for _, n in mapping["000010"]), ["기타", "반도체"], "종목의 업종 연결은 남는다")

    def test_truncated_or_failed_list_still_wipes_links(self):
        rows, mapping = fetch([{"no": "1", "name": "반도체", "totalCnt": 3}], [[{"itemcode": "000010"}]])
        self.assertEqual((rows[0]["membershipStatus"], mapping), ("PARTIAL", {}))
        rows, mapping = fetch([{"no": "1", "name": "반도체", "totalCnt": 2}], [N.MembershipError("upjong HTTP 400")])
        self.assertEqual((rows[0]["membershipStatus"], mapping), ("FAILED", {}))


def write_gz(path, meta, header, rows):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["#meta"] + meta)
        w.writerow(header)
        for r in rows:
            w.writerow([r.get(k, "") for k in header])


class SummaryTable(unittest.TestCase):
    def readme(self, sectors):
        d = tempfile.mkdtemp()
        stock = {k: "0" for k in M.FIELDS}
        stock.update(itemcode="000010", itemname="시험", tradeStopYn="N", marketAlertType="")
        write_gz(os.path.join(d, "2026-10-23_1505.csv.gz"), ["capturedAt=2026-10-23T15:02:00+09:00", "total=1", "kept=1"],
                 M.FIELDS, [stock])
        base = {k: "" for k in M.SECTOR_FIELDS}
        rows = [dict(base, **r) for r in sectors]
        write_gz(os.path.join(d, "2026-10-23_1505_upjong.csv.gz"), ["membershipStatus=OK"], M.SECTOR_FIELDS, rows)
        with patch.object(M, "OUT_DIR", d):
            M.write_readme()
        return open(os.path.join(d, "README.md"), encoding="utf-8").read()

    def test_held_sector_is_left_out_and_named(self):
        text = self.readme([
            {"name": "반도체", "membershipStatus": "OK", "sumTradeAmount": "8000000000", "fetchedCount": "2", "matchedCount": "2"},
            {"name": "기타", "membershipStatus": "OK", "sumTradeAmount": "", "fetchedCount": "1555", "matchedCount": "2"}])
        self.assertIn("| 1 | 반도체", text)
        self.assertNotIn("| 2 | 기타", text)
        self.assertIn("집계 보류 1개: 기타", text)

    def test_incomplete_list_still_hides_the_table(self):
        text = self.readme([{"name": "반도체", "membershipStatus": "PARTIAL", "sumTradeAmount": "", "fetchedCount": "1"}])
        self.assertIn("구성 목록 불완전", text)
        self.assertNotIn("| 1 | 반도체", text)


class MainEndToEnd(unittest.TestCase):
    """Codex 검토(2026-10-24) — 저장 파일의 업종 상태 · 연결이 감시까지 통과하는지, 콘솔이 빈 집계를 1위로 찍지 않는지."""

    def run_main(self, members):
        import datetime
        import io
        import itertools
        from contextlib import redirect_stdout
        import hyeoks_snapshot_watch as W
        tmp = tempfile.mkdtemp()
        base = datetime.datetime(2026, 10, 26, 15, 2, 48, tzinfo=M.KST)
        ticks = itertools.chain([base], itertools.repeat(base + datetime.timedelta(seconds=30)))

        class FakeDT(datetime.datetime):
            @classmethod
            def now(cls, tz=None):
                return next(ticks)

        rows = [{"itemcode": f"{i:06d}", "itemname": f"종목{i}", "tradeAmount": str(10_000 + i),
                 "nowPrice": "1000", "openPrice": "990", "prevChangeRate": "1.0"} for i in range(M.MIN_ROWS)]
        lists = {"upjong": [{"no": "1", "name": "반도체", "totalCnt": 2}, {"no": "2", "name": "기타", "totalCnt": 2}],
                 "group": [{"no": "7", "name": "그룹가", "totalCnt": 1}, {"no": "8", "name": "그룹나", "totalCnt": 1}]}
        real = M.fetch_sector_data

        def sector(kind, mkt):
            with patch.object(M.SESSION, "get", return_value=Mock(json=Mock(return_value=lists[kind]))):
                return real(kind, mkt)

        out = io.StringIO()
        with patch.object(M, "OUT_DIR", tmp),              patch.object(M.SESSION, "get", return_value=Mock(status_code=200, json=Mock(return_value=rows))),              patch.object(M, "fetch_theme_data", return_value=([], {})),              patch.object(M, "fetch_sector_data", side_effect=sector),              patch.object(M, "fetch_members", side_effect=lambda kind, no, session: [{"itemcode": c} for c in members[no]]),              patch.dict(M.SECTORS, {"upjong": ("업종", 1), "group": ("그룹", 1)}),              patch.object(M.time, "sleep"),              patch.object(M, "index_snapshot", return_value={"KOSPI": ("1", "0"), "KOSDAQ": ("1", "0")}),              patch.object(M, "write_readme", return_value=None),              patch.object(M.datetime, "datetime", FakeDT),              patch("sys.argv", ["x", "--slot", "1505", "--force"]), redirect_stdout(out):
            self.assertEqual(M.main(), 0)
        path = os.path.join(tmp, "2026-10-26_1505.csv.gz")
        meta = W.read_meta(path)
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            fh.readline()
            links = {r["itemcode"]: r["upjongNames"] for r in csv.DictReader(fh)}
        return meta, W.membership_issues(meta), links, out.getvalue()

    def test_saved_file_keeps_links_and_passes_watch(self):
        meta, issues, links, log = self.run_main({"1": ["001499", "001498"], "2": ["001497", "999990"],
                                                  "7": ["001496"], "8": ["999980"]})
        self.assertEqual((meta["upjongMembershipStatus"], meta["groupMembershipStatus"]), ("OK", "OK"))
        self.assertEqual([i for i in issues if not i.startswith("theme")], [], "테마는 이 시험에서 비워 둔다")
        self.assertEqual((links["001499"], links["001497"]), ("반도체", "기타"))
        self.assertIn("업종 거래대금 1위: 반도체", log)
        self.assertIn("그룹 거래대금 1위: 그룹가", log)

    def test_all_sectors_unpriced_never_prints_a_zero_leader(self):
        meta, _, links, log = self.run_main({"1": ["999991", "999992"], "2": ["999993", "999994"],
                                             "7": ["999995"], "8": ["999996"]})
        self.assertEqual(meta["upjongMembershipStatus"], "OK")
        self.assertNotIn("업종 거래대금 1위", log)
        self.assertNotIn("그룹 거래대금 1위", log)
        self.assertIn("업종 모든 분류가 가격 연결 부족", log)


if __name__ == "__main__":
    unittest.main()
