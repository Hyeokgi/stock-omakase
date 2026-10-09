"""한투 시황·공시 제목 수집 kis-news-v1 — 넘침 의심·중복·15:05·공개 범위·보고 한 줄."""
import datetime
import os
import tempfile
import unittest

import hyeoks_kis_news as N
import hyeoks_research_daily as R


def news(i, hhmmss, code=""):
    return {"cntt_usiq_srno": str(i), "data_dt": "20261008", "data_tm": hhmmss, "hts_pbnt_titl_cntt": f"비밀제목{i}",
            "iscd1": code, "kor_isnm1": "비밀회사" if code else "", "news_ofer_entp_code": "2"}


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


def at(hm):
    return datetime.datetime.fromisoformat(f"2026-10-08T{hm}:00+09:00")


class KisNews(unittest.TestCase):
    def test_overflow_detected_only_when_full_page_is_all_new(self):
        p1 = [news(i, f"0900{i % 60:02d}") for i in range(100, 140)]
        p2 = [news(i, f"0905{i % 60:02d}") for i in range(140, 160)] + p1[:20]        # 겹침 → 넘침 아님
        p3 = [news(i, f"0915{i % 60:02d}") for i in range(200, 240)]                   # 40건 모두 새 것 → 넘침 의심
        feed = iter([p1, p2, p3])
        c = Clock(at("09:10"))
        rows, st, over = N.run("2026-10-08", lambda: next(feed), at("09:31"), 600, c, c.sleep, run_id="1")
        self.assertEqual(st["polls"], 3)
        self.assertEqual(st["overflowPolls"], 1)
        self.assertEqual(over[0]["poll"], 3)
        self.assertEqual(len(rows), 40 + 20 + 40, "중복은 한 번만, 넘침이면 받은 최신 40건만")

    def test_1505_rule_needs_both_publish_and_receive_before_cut(self):
        feed = iter([[news(1, "150400"), news(2, "150300")], [news(3, "150600"), news(1, "150400")]])
        c = Clock(at("15:00"))
        rows, st, _ = N.run("2026-10-08", lambda: next(feed), at("15:12"), 600, c, c.sleep)
        self.assertEqual(st["publishedBy1505"], 2)

    def test_repeated_permission_errors_stop(self):
        def bad():
            raise PermissionError("rt_cd 1 EGW00123")
        c = Clock(at("09:10"))
        _, st, _ = N.run("2026-10-08", bad, at("15:08"), 600, c, c.sleep)
        self.assertEqual((st["status"], st["pollFailures"]), ("중단", N.STOP_AFTER))

    def test_public_files_have_no_titles_and_report_line_warns(self):
        d = tempfile.mkdtemp()
        feed = iter([[news(i, "090000", "005930") for i in range(40)], [news(i, "091000") for i in range(40, 80)]])
        c = Clock(at("09:10"))
        rows, st, over = N.run("2026-10-08", lambda: next(feed), at("09:21"), 600, c, c.sleep, run_id="5")
        N.store(rows, over, st, lambda n, b: "drv", runs_dir=d, private_dir=os.path.join(d, "p"))
        N.log_run(st, d)
        public = ""
        for f in ("kis_news_runs.csv", "private_receipts.csv"):
            with open(os.path.join(d, f), encoding="utf-8") as fh:
                public += fh.read()
        self.assertNotIn("비밀", public)
        line = R.materials_summary("2026-10-08", d)
        self.assertIn("넘침 의심 1회", line)
        self.assertIn("종목코드 40", line)
        self.assertNotIn("%", line)

    def test_holiday_does_nothing(self):
        out = N.main([], env={"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s"}, get=lambda *a, **k: self.fail(),
                     post=lambda *a, **k: self.fail(), clock=lambda: datetime.datetime.fromisoformat("2026-10-09T10:00:00+09:00"))
        self.assertEqual(out, 0)



class IntervalV2Test(unittest.TestCase):
    def test_default_interval_is_five_minutes(self):
        """2026-10-09 사용자 지시 — 10분 → 5분(kis-news-v2)."""
        import inspect
        self.assertEqual(N.VERSION, "kis-news-v2")
        self.assertIn('add_argument("--every", type=int, default=300)', inspect.getsource(N.main))

if __name__ == "__main__":
    unittest.main()
