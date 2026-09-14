import datetime as dt
import unittest
from naver_health_policy import REQUIRED, RETIRED, evaluate, price_fresh, flow_fresh, KST
from notify_naver_health import notification


def all_ok():
    return {k: {"ok": True, "severity": "중요", "desc": k, "detail": "", "error": ""} for k in REQUIRED}


def session(day):
    d = dt.date.fromisoformat(day)
    return d.weekday() < 5 and day not in {"2026-09-24", "2026-09-25"}


class PolicyTests(unittest.TestCase):
    def test_retired_html_does_not_fail_working_functions(self):
        r = all_ok()
        for key in RETIRED: r[key]["ok"] = False
        self.assertEqual(evaluate(r)["alerts"], [])
        self.assertTrue(evaluate(r)["transitions"])

    def test_supply_all_fail_is_fatal(self):
        r = all_ok()
        for key in ("frgn_html", "frgn_detail", "frgn_json"): r[key]["ok"] = False
        self.assertEqual(evaluate(r)["alerts"][0]["severity"], "치명적")

    def test_one_supply_fallback_is_enough(self):
        r = all_ok()
        r["frgn_html"]["ok"] = r["frgn_detail"]["ok"] = False
        self.assertEqual(evaluate(r)["alerts"], [])

    def test_risk_both_fail(self):
        r = all_ok()
        r["risk_pages"]["ok"] = r["risk_json"]["ok"] = False
        self.assertEqual(evaluate(r)["alerts"][0]["name"], "위험종목")

    def test_independent_morning_failure_not_hidden_by_news_fallback(self):
        r = all_ok(); r["morning_news_v2"]["ok"] = False
        self.assertTrue(evaluate(r)["alerts"])

    def test_missing_probe_is_incomplete(self):
        r = all_ok(); del r["stock_basic"]
        self.assertFalse(evaluate(r)["complete"])
        self.assertTrue(evaluate(r)["alerts"])

    def test_optional_failure_is_advisory(self):
        r = all_ok(); r["sise_bulk"]["ok"] = False
        self.assertFalse(evaluate(r)["alerts"])
        self.assertTrue(evaluate(r)["advisory"])

    def test_old_regression_no_notification(self):
        r = all_ok()
        for k in RETIRED: r[k]["ok"] = False
        self.assertIsNone(notification({"schema":"naver-health-v2","run_id":"1","results":r},"0","1"))

    def test_missing_or_crashed_result_not_success(self):
        for p, code in [({}, "0"), ({"schema":"naver-health-v2","run_id":"1","results":all_ok()}, "2")]:
            with self.assertRaises(ValueError): notification(p, code, "1")

    def test_previous_run_rejected(self):
        with self.assertRaises(ValueError):
            notification({"schema":"naver-health-v2","run_id":"old","results":all_ok()},"0","new")

    def test_failure_notification(self):
        r=all_ok(); r["price_fresh"]["ok"]=False; r["price_fresh"]["severity"]="치명적"
        self.assertIn("실제 원천 장애",notification({"schema":"naver-health-v2","run_id":"1","results":r},"1","1"))

    def test_live_stale_price(self):
        now=dt.datetime(2026,9,14,10,tzinfo=KST)
        self.assertFalse(price_fresh("2026-09-14T09:30:00+09:00",now,session)[0])
        self.assertTrue(price_fresh("2026-09-14T09:59:00+09:00",now,session)[0])

    def test_premarket_friday_valid(self):
        now=dt.datetime(2026,9,14,8,tzinfo=KST)
        self.assertTrue(price_fresh("2026-09-11T15:30:00+09:00",now,session)[0])

    def test_holiday_price(self):
        now=dt.datetime(2026,9,27,10,tzinfo=KST)
        self.assertTrue(price_fresh("2026-09-23T15:30:00+09:00",now,session)[0])

    def test_future_and_timezone_missing(self):
        now=dt.datetime(2026,9,14,10,tzinfo=KST)
        self.assertFalse(price_fresh("2026-09-14T11:00:00+09:00",now,session)[0])
        self.assertFalse(price_fresh("2026-09-14T10:00:00",now,session)[0])

    def test_daily_flow_allows_previous_session(self):
        now=dt.datetime(2026,9,14,10,tzinfo=KST)
        self.assertTrue(flow_fresh(["20260911"],now,session)[0])
        self.assertFalse(flow_fresh(["20260910"],now,session)[0])
        self.assertFalse(flow_fresh([],now,session)[0])


if __name__ == "__main__":
    unittest.main()
