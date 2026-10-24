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


if __name__ == "__main__":
    unittest.main()
