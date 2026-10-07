"""OpenDART 공시 수집 dart-disclosures-v1 — 처음 본 시각·15:05 범위·멈춤·공개 범위."""
import datetime
import os
import tempfile
import unittest
from unittest import mock

import hyeoks_dart_disclosures as D

KST = D.KST


def item(no, code="005930", name="회사", rpt="주요사항보고서"):
    return {"rcept_no": no, "corp_code": "00126380", "stock_code": code, "corp_name": name, "corp_cls": "Y",
            "report_nm": rpt, "rcept_dt": no[:8], "rm": "", "flr_nm": name}


class Resp:
    def __init__(self, js, code=200):
        self.status_code, self._js = code, js

    def json(self):
        return self._js


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


def at(hm, day="2026-10-08"):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


class Dart(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def feed(self, by_poll):
        """조회 차례별 Y 목록. K 는 빈 목록(013)."""
        calls = {"n": 0, "params": []}

        def get(url, params, timeout):
            calls["params"].append(dict(params))
            if params["corp_cls"] == "K":
                return Resp({"status": "013"})
            k = calls["n"]
            calls["n"] += 1
            items = by_poll[min(k, len(by_poll) - 1)]
            return Resp({"status": "000", "total_page": 1, "list": items})
        return get, calls

    def test_first_seen_and_prev_poll_and_1505_rule(self):
        get, calls = self.feed([[item("20261008000001")],
                                [item("20261008000002"), item("20261008000001")],
                                [item("20261008000003"), item("20261008000002"), item("20261008000001")]])
        c = Clock(at("14:50"))
        rows, st = D.run("2026-10-08", "2026-10-07", get, "k", at("15:12"), 600, c, c.sleep, run_id="9")
        got = {r["rcept_no"]: (r["first_seen_at"][11:16], r["prev_poll_at"][11:16]) for r in rows}
        self.assertEqual(got, {"20261008000001": ("14:50", ""), "20261008000002": ("15:00", "14:50"),
                               "20261008000003": ("15:10", "15:00")})
        self.assertEqual(st["seenBy1505"], 2, "15:05 이후 처음 본 것은 15:05 판단에 쓰지 않는다")
        self.assertEqual((st["coverage1505"], st["lastPollBefore1505"][11:16]), ("완전", "15:00"))
        self.assertEqual(calls["params"][0]["bgn_de"], "20261007", "첫 조회는 전 거래일부터")
        self.assertEqual(calls["params"][2]["bgn_de"], "20261008")

    def test_stop_status_halts_without_retry(self):
        def get(url, params, timeout):
            return Resp({"status": "020", "message": "limit"})
        c = Clock(at("09:10"))
        rows, st = D.run("2026-10-08", "2026-10-07", get, "k", at("15:08"), 600, c, c.sleep)
        self.assertEqual((st["status"], st["polls"]), ("중단", 0))
        self.assertIn("한도초과", st["stopReason"])

    def test_failed_poll_does_not_mark_items_seen(self):
        state = {"n": 0}

        def get(url, params, timeout):
            state["n"] += 1
            if state["n"] == 2:                       # 첫 조회의 K 페이지에서 실패
                raise ConnectionError("x")
            if params["corp_cls"] == "K":
                return Resp({"status": "013"})
            return Resp({"status": "000", "total_page": 1, "list": [item("20261008000001")]})
        c = Clock(at("14:55"))
        rows, st = D.run("2026-10-08", "2026-10-07", get, "k", at("15:06"), 600, c, c.sleep)
        self.assertEqual(st["pollFailures"], 1)
        self.assertEqual([r["first_seen_at"][11:16] for r in rows], ["15:05"], "실패한 조회의 부분 결과로 '처음 봄' 을 만들지 않는다")

    def test_coverage_gap(self):
        self.assertEqual(D.coverage([at("14:30")], "2026-10-08")[1], "불완전")
        self.assertEqual(D.coverage([at("15:06")], "2026-10-08")[1], "조회없음")

    def test_public_log_has_counts_only_and_private_file_receipt(self):
        get, _ = self.feed([[item("20261008000001", name="비밀회사", rpt="비밀공시")]])
        c = Clock(at("15:00"))
        rows, st = D.run("2026-10-08", "2026-10-07", get, "k", at("15:05"), 600, c, c.sleep, run_id="7")
        with mock.patch.object(D.R, "PRIVATE_DIR", os.path.join(self.d, "p")):
            rc = D.store(rows, st, lambda n, b: "drive123", runs_dir=self.d, private_dir=os.path.join(self.d, "p"))
        D.log_run(st, self.d)
        self.assertEqual(rc["status"], "접수")
        public = ""
        for f in ("dart_runs.csv", "private_receipts.csv"):
            with open(os.path.join(self.d, f), encoding="utf-8") as fh:
                public += fh.read()
        self.assertNotIn("비밀", public)
        self.assertIn("research_dart_2026-10-08_7.json.gz", public)

    def test_holiday_and_late_start_do_nothing(self):
        out = D.main([], env={"DART_API_KEY": "k"}, get=lambda *a, **k: self.fail("호출하면 안 된다"),
                     clock=lambda: at("10:00", "2026-10-09"))
        self.assertEqual(out, 0)
        out = D.main([], env={"DART_API_KEY": "k"}, get=lambda *a, **k: self.fail("호출하면 안 된다"),
                     clock=lambda: at("15:30"))
        self.assertEqual(out, 0)


if __name__ == "__main__":
    unittest.main()


class KisProbe(unittest.TestCase):
    def test_shape_drops_titles_and_names(self):
        import hyeoks_kis_news_probe as K
        js = {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "정상", "output": [
            {"data_dt": "20261008", "data_tm": "091500", "hts_pbnt_titl_cntt": "비밀제목", "kor_isnm1": "비밀회사",
             "iscd1": "005930", "news_ofer_entp_code": "2"}]}
        out = K.shape(js)
        self.assertEqual((out["rows"], out["rows_with_stock_code"], out["first"]), (1, 1, "20261008 091500"))
        self.assertTrue(out["has_title_field"] and out["has_name_fields"])
        self.assertNotIn("비밀", str(out))
