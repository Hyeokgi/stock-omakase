# 네이버 구성종목 개편과 실패·정상 빈 목록 구분을 검증한다.
import unittest
import tempfile
import gzip
from pathlib import Path
import ast
import datetime
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import naver_membership as N
import hyeoks_market_snapshot as M


def item(code="005930", **kw):
    return dict(itemCode=code, itemName="시험", marketType="KOSPI",
                krx={"changeRate": "2.5", "tradingValue": "5000000000"},
                nxt={"changeRate": "99", "tradingValue": "1"}, **kw)


def response(items, more=False, cursor=None, status=200):
    return Mock(status_code=status, json=Mock(return_value={
        "items": items, "hasNext": more, "cursor": cursor}))


class MembershipTests(unittest.TestCase):
    def test_cursor_all_markets_and_krx_units(self):
        session = Mock()
        session.get.side_effect = [response([item()], True, "next+page"),
                                   response([item("0015N0")])]
        rows = N.fetch_members("theme", "174", session)
        self.assertEqual([r["itemcode"] for r in rows], ["005930", "0015N0"])
        self.assertEqual(rows[0]["prevChangeRate"], "2.5")
        self.assertEqual(rows[0]["tradeAmount"], "5000000000")
        qs = [parse_qs(urlparse(c.args[0]).query) for c in session.get.call_args_list]
        self.assertNotIn("marketType", qs[0])
        self.assertEqual(qs[0]["size"], ["100"])
        self.assertEqual(qs[1]["cursor"], ["next+page"])

    def test_http_error_never_empty_success(self):
        session = Mock()
        session.get.return_value = response([], status=400)
        with self.assertRaisesRegex(N.MembershipError, "theme.*HTTP 400"):
            N.fetch_members("theme", "174", session)
        self.assertEqual(session.get.call_count, 1)

    def test_partial_page_failure_does_not_return_partial_rows(self):
        session = Mock()
        session.get.side_effect = [response([item()], True, "n"), response([], status=503)]
        with self.assertRaises(N.MembershipError):
            N.fetch_members("group", "1", session)

    def test_empty_is_allowed_only_when_pagination_finished(self):
        session = Mock()
        session.get.return_value = response([])
        self.assertEqual(N.fetch_members("theme", "1", session), [])
        session.get.return_value = response([], True, "n")
        with self.assertRaises(N.MembershipError):
            N.fetch_members("theme", "1", session)

    def test_bad_schema_code_and_cursor_are_rejected(self):
        for data in [[], {"items": [], "hasNext": "false"},
                     {"items": [item("한글가나다라")], "hasNext": False},
                     {"items": [item()], "hasNext": True, "cursor": None}]:
            with self.subTest(data=data):
                session = Mock()
                session.get.return_value = Mock(status_code=200, json=Mock(return_value=data))
                with self.assertRaises(N.MembershipError):
                    N.fetch_members("theme", "1", session)

    def test_duplicate_and_page_budget_rejected(self):
        session = Mock()
        session.get.return_value = response([item()], True, "same")
        with self.assertRaises(N.MembershipError):
            N.fetch_members("theme", "1", session)
        session.get.return_value = response([item()], True, "n")
        with self.assertRaises(N.MembershipError):
            N.fetch_members("theme", "1", session, max_pages=1)

    def test_missing_krx_does_not_substitute_nxt(self):
        x = item(); x["krx"] = None
        session = Mock(); session.get.return_value = response([x])
        self.assertIsNone(N.fetch_members("theme", "1", session)[0]["tradeAmount"])

    def test_scanner_uses_krx_and_preserves_million_won_units(self):
        rows = [{"itemcode": "0015N0", "itemname": "시험", "prevChangeRate": "4.2", "tradeAmount": "5123456789"}]
        self.assertEqual(N.scanner_rows(rows), [{"code": "'0015N0", "name": "시험", "rate": 4.2, "value": 5123}])
        for value in (None, "NaN", "inf", "-1"):
            rows[0]["tradeAmount"] = value
            with self.assertRaises(N.MembershipError):
                N.scanner_rows(rows)


def scanner_functions(*names):
    import feature_telemetry
    tree = ast.parse(Path("omakase.py").read_text(encoding="utf-8"))
    funcs = [x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name in names]
    # HTML 부재→JSON 경로를 고정한다. 다른 시험의 bs4 대역에 의존하지 않는다.
    soup = Mock(title=None)
    soup.find.return_value = None
    env = dict(datetime=datetime, KST=datetime.timezone(datetime.timedelta(hours=9)),
               GLOBAL_SESSION=Mock(), BeautifulSoup=Mock(return_value=soup), pd=Mock(),
               THEME_SOURCE_STATE={}, THEME_BLACKLIST=[], TARGET_PERCENT=3,
               # 2026-10-24 — 표시 전용 '테마_현황' 탭 정보(theme_status.py). 선정은 읽지 않는다.
               THEME_STATUS={"dominated": [], "note": "", "input_written": ""},
               fetch_theme_list_json=Mock(return_value=[{"no": "1", "name": "시험"}]),
               fetch_theme_stocks_json=Mock(), MembershipError=N.MembershipError,
               TELEMETRY=feature_telemetry.Telemetry(), feature_telemetry=feature_telemetry,
               fetch_members=N.fetch_members, scanner_rows=N.scanner_rows)
    env["GLOBAL_SESSION"].get.return_value = Mock(content=b"<html></html>", text="", status_code=200)
    env["pd"].DataFrame.return_value = Mock(empty=True)
    exec(compile(ast.Module(body=funcs, type_ignores=[]), "omakase.py", "exec"), env)
    return env


class ScannerSourceTests(unittest.TestCase):
    def test_real_scanner_adapter_uses_shared_reader(self):
        env = scanner_functions("fetch_theme_stocks_json")
        env["GLOBAL_SESSION"].get.return_value = response([item("0015N0")])
        self.assertEqual(env["fetch_theme_stocks_json"]("1")[0]["value"], 5000)

    def test_fetch_failure_and_no_candidates_are_distinct(self):
        for fails in (True, False):
            with self.subTest(fails=fails):
                env = scanner_functions("get_real_money_themes")
                if fails:
                    env["fetch_theme_stocks_json"].side_effect = N.MembershipError("theme HTTP 400")
                else:
                    env["fetch_theme_stocks_json"].return_value = [{"rate": 1, "value": 5000}]
                env["get_real_money_themes"]()
                self.assertEqual(env["THEME_SOURCE_STATE"]["status"], "FAILED" if fails else "OK")
                self.assertEqual(env["TELEMETRY"].total(), 1 if fails else 0)
                # 표시 탭이 '구성 조회 불완전' 과 '조건 충족 없음' 을 가려 보여 준다(2026-10-24)
                self.assertIn("구성 조회 불완전" if fails else "등락률 3%", env["THEME_STATUS"]["note"])

    def test_failed_source_preserves_sheet_values_and_adds_warning_note(self):
        env = scanner_functions("update_google_sheet")
        env["THEME_SOURCE_STATE"]["status"] = "FAILED"
        doc = Mock(); empty = Mock(empty=True)
        env["update_google_sheet"](doc, empty, empty, empty, empty, False)
        sheet = doc.worksheet.return_value
        sheet.update.assert_not_called()
        sheet.batch_clear.assert_not_called()
        self.assertIn("최신 테마 확인 불가", sheet.update_note.call_args.args[1])
        sheet.update_note.side_effect = RuntimeError("offline")
        env["update_google_sheet"](doc, empty, empty, empty, empty, False)

    def test_shared_adapter_is_fingerprinted(self):
        import stability_gate
        self.assertIn("naver_membership.py", stability_gate.FINGERPRINT_FILES)


class SnapshotFailureTests(unittest.TestCase):
    def test_summary_does_not_turn_unknown_aggregate_into_zero(self):
        tree = ast.parse(Path("hyeoks_market_snapshot.py").read_text(encoding="utf-8"))
        func = next(x for x in ast.walk(tree) if isinstance(x, ast.FunctionDef) and x.name == "_agg_table")
        env = {"_f": M._f}
        exec(compile(ast.Module(body=[func], type_ignores=[]), "summary", "exec"), env)
        text = "\n".join(env["_agg_table"](
            [{"membershipStatus": "FAILED", "sumTradeAmount": ""}], "업종", "", 10, "sumTradeAmount"))
        self.assertIn("보류", text)
        self.assertNotIn("0억", text)

    def test_sector_failure_is_not_numeric_zero(self):
        session = Mock()
        session.get.return_value.json.return_value = [{"no": "1", "name": "시험", "totalCnt": 2}]
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", side_effect=N.MembershipError("group HTTP 400")), \
             patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_sector_data("group", {})
        self.assertEqual(mapping, {})
        self.assertEqual(rows[0]["membershipStatus"], "FAILED")
        self.assertEqual(rows[0]["sumTradeAmount"], "")
        self.assertEqual(rows[0]["risingCount"], "")

    def test_partial_theme_mapping_is_not_exposed_as_complete(self):
        session = Mock()
        session.get.return_value.json.return_value = {
            "items": [{"code": "1"}, {"code": "2"}], "hasNext": False}
        with patch.object(M, "SESSION", session), \
             patch.object(M, "fetch_members", side_effect=[
                 [{"itemcode": "005930"}], N.MembershipError("theme HTTP 400")]), \
             patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_theme_data()
        self.assertEqual(mapping, {})
        self.assertEqual(M.membership_status(rows), "PARTIAL")

    def test_duplicate_code_is_retried_once_and_recovers(self):
        # 2026-10-27 — 등락률 정렬 커서의 페이지 사이 순위 변동(추정)으로 생긴 중복은 한 번 다시 받으면 풀린다.
        session = Mock()
        session.get.return_value.json.return_value = [{"no": "1", "name": "시험", "totalCnt": 1}]
        mkt = {"005930": {"tradeAmount": "5", "prevChangeRate": "1"}}
        fm = Mock(side_effect=[N.MembershipError("upjong constituents invalid/duplicate code"),
                               [{"itemcode": "005930"}]])
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", fm), patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_sector_data("group", mkt)
        self.assertEqual(fm.call_count, 2)
        self.assertEqual(rows[0]["membershipStatus"], "OK")
        self.assertIn("005930", mapping)

    def test_retry_is_only_once_and_only_for_cursor_races(self):
        session = Mock()
        session.get.return_value.json.return_value = [{"no": "1", "name": "시험", "totalCnt": 1}]
        dup = N.MembershipError("upjong constituents invalid/duplicate code")
        fm = Mock(side_effect=[dup, dup])
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", fm), patch.object(M.time, "sleep"):
            rows, _ = M.fetch_sector_data("group", {})
        self.assertEqual(fm.call_count, 2, "두 번째도 실패하면 더 부르지 않는다")
        self.assertEqual(rows[0]["membershipStatus"], "FAILED")
        fm = Mock(side_effect=N.MembershipError("group HTTP 400"))
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", fm), patch.object(M.time, "sleep"):
            M.fetch_sector_data("group", {})
        self.assertEqual(fm.call_count, 1, "HTTP 오류는 재시도하지 않는다")

    def test_sector_complete_zero_is_a_real_zero(self):
        session = Mock()
        session.get.return_value.json.return_value = [{"no": "1", "name": "시험", "totalCnt": 1}]
        mkt = {"005930": {"tradeAmount": "0", "prevChangeRate": "0"}}
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", return_value=[{"itemcode": "005930"}]), \
             patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_sector_data("group", mkt)
        self.assertEqual(rows[0]["membershipStatus"], "OK")
        self.assertEqual(rows[0]["sumTradeAmount"], "0")
        self.assertIn("005930", mapping)

    def test_sector_missing_price_does_not_create_partial_zero(self):
        session = Mock()
        session.get.return_value.json.return_value = [{"no": "1", "name": "시험", "totalCnt": 1}]
        with patch.object(M, "SESSION", session), patch.dict(M.SECTORS, {"group": ("그룹", 1)}), \
             patch.object(M, "fetch_members", return_value=[{"itemcode": "005930"}]), \
             patch.object(M.time, "sleep"):
            rows, mapping = M.fetch_sector_data("group", {})
        # 2026-10-24 정정(허브 #21) — 구성 목록은 완전하므로 품질은 OK, 가격이 안 이어진 집계만 비운다.
        #   예전에는 PARTIAL + 연결 삭제였고, '기타' 업종 한 줄 때문에 모든 종목의 업종 연결이 지워졌다.
        self.assertEqual(rows[0]["membershipStatus"], "OK")
        self.assertEqual(rows[0]["sumTradeAmount"], "")
        self.assertEqual(rows[0]["matchedCount"], 0)
        self.assertIn("005930", mapping)


class ConsumerQualityTests(unittest.TestCase):
    def test_old_zero_and_explicit_partial_fail_quality(self):
        rows = {"005930": {"themeNos": "1"}}
        self.assertTrue(N.theme_membership_ok({}, rows))
        self.assertFalse(N.theme_membership_ok({"themeMembershipStatus": "PARTIAL"}, rows))
        self.assertFalse(N.theme_membership_ok({}, {"005930": {"themeNos": ""}}))

    def test_watch_catches_existing_file_with_no_mapping(self):
        import hyeoks_snapshot_watch as W
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "sample.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8") as f:
                f.write('#meta,themeMembershipStatus=FAILED\nitemcode,themeNos\n005930,\n')
            meta = W.read_meta(path)
        self.assertEqual(meta["_rows"], 1)
        self.assertEqual(W.membership_issues(meta), ["theme 구성 FAILED"])

    def test_hold_quality_counts_bad_membership_as_missing_entry(self):
        import hyeoks_theme_abc as A
        quality, hold, reasons = A.quality_check([{"date": "2026-10-19", "skip": "테마구성불완전"}])
        self.assertTrue(hold)
        self.assertEqual(quality["일별"]["진입결측일"], ["2026-10-19"])

    def test_firstday_does_not_turn_unknown_leaders_into_controls(self):
        import hyeoks_leader_firstday as F
        candidate = {"code": "005930", "rate": 2, "amt": 10}
        days = [{"date": "2026-10-19", "A": [candidate], "B": set(),
                 "membership_ok": False, "breadth": .5},
                {"date": "2026-10-20", "A": [candidate], "B": {"005930"}, "breadth": .5}]
        self.assertEqual(F.registry(days), [])
        self.assertEqual(len(days), 2)  # 가격 시계는 그대로 둔다.


if __name__ == "__main__":
    unittest.main()
