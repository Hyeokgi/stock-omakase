# 업종·그룹사 목록을 rankings v2 로 옮긴 변경(2026-11-08) 시험 — 주소 · 커서 · 구성 수 하한 · raw 열
"""2026-11-06 구 목록 `market/{upjong|group}/list` 가 404 로 사라져 업종 · 그룹 구성이 통째로 빠졌다.
목록은 v2(`industries` · `groups`), 구성종목은 구 stocklist(naver_membership) 를 그대로 쓴다.
hyeoks_naver_healthcheck 는 가져올 때 sys.stdout 을 바꿔 다른 시험의 출력을 닫으므로 여기서 부르지 않는다."""
import unittest
from unittest.mock import Mock, patch

import hyeoks_market_snapshot as M

MKT = {"000010": {"tradeAmount": "5", "prevChangeRate": "1.0"},
       "000020": {"tradeAmount": "3", "prevChangeRate": "-1.0"}}


def item(code, name, rise=0, fall=0, same=0, rate="1.5"):
    return {"code": code, "name": name, "changeRate": rate, "risingCount": str(rise),
            "fallingCount": str(fall), "unchangedCount": str(same), "totalTradingValue": "999"}


class SnapshotList(unittest.TestCase):
    def run_fetch(self, kind, pages, members):
        session = Mock()
        session.get.return_value.json.side_effect = pages
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {kind: ("분류", 2)}), \
             patch.object(M, "fetch_members", side_effect=members), patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_sector_data(kind, MKT)
        return rows, mapping, [c.args[0] for c in session.get.call_args_list]

    def test_follows_v2_cursor_and_dedups(self):
        pages = [{"items": [item("1", "가", 1, 1)], "hasNext": True, "cursor": "C1"},
                 {"items": [item("1", "가", 1, 1), item("2", "나", 1)], "hasNext": False, "cursor": None}]
        rows, mapping, urls = self.run_fetch(
            "upjong", pages, [[{"itemcode": "000010"}, {"itemcode": "000020"}], [{"itemcode": "000010"}]])
        self.assertIn("/rankings/v2/domestic/industries?", urls[0])
        self.assertTrue(urls[1].endswith("&cursor=C1"))
        self.assertEqual([r["no"] for r in rows], ["1", "2"], "커서가 겹친 분류는 한 번만")
        self.assertEqual(sorted(n for _, n in mapping["000010"]), ["가", "나"])

    def test_group_uses_groups_route(self):
        _, _, urls = self.run_fetch("group", [{"items": [item("7", "가", 1), item("8", "나", 1)],
                                               "hasNext": False}],
                                    [[{"itemcode": "000010"}], [{"itemcode": "000020"}]])
        self.assertIn("/rankings/v2/domestic/groups?", urls[0])

    def test_too_few_items_keeps_snapshot_without_sector(self):
        rows, mapping, _ = self.run_fetch("upjong", [{"items": [item("1", "가", 1)], "hasNext": False}], [])
        self.assertEqual((rows, mapping), ([], {}))

    def test_priced_count_is_a_lower_bound(self):
        # 시세 있는 구성 2 보다 적게 받으면 잘린 것, 같거나 많으면(거래정지 종목 포함) 완전하다.
        rows, _, _ = self.run_fetch(
            "upjong", [{"items": [item("1", "가", 1, 1), item("2", "나", 1)], "hasNext": False}],
            [[{"itemcode": "000010"}], [{"itemcode": "000010"}, {"itemcode": "000020"}]])
        by = {r["no"]: r for r in rows}
        self.assertEqual((by["1"]["membershipStatus"], by["1"]["truncated"]), ("PARTIAL", "Y"))
        self.assertEqual((by["2"]["membershipStatus"], by["2"]["truncated"]), ("OK", ""))

    def test_raw_columns_keep_only_same_meaning_fields(self):
        rows, _, _ = self.run_fetch(
            "upjong", [{"items": [item("1", "가", 1, 1, rate="2.5"), item("2", "나", 1)], "hasNext": False}],
            [[{"itemcode": "000010"}, {"itemcode": "000020"}], [{"itemcode": "000010"}]])
        r = rows[0]
        self.assertEqual((r["rawChangeRate"], r["rawRiseCnt"], r["rawFallCnt"], r["rawSteadyCnt"]),
                         ("2.5", "1", "1", "0"))
        self.assertNotIn("rawTotalAccAmount", r, "v2 거래대금은 단위가 달라 구 열에 섞지 않는다")
        self.assertNotIn("rawTotalCnt", r)


if __name__ == "__main__":
    unittest.main()
