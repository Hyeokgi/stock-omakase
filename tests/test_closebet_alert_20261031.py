"""15:05 종베 후보 알림 closebet-alert-v1 — 기준 · 금요일 1~2위 · 시각 가드 · 거래량 배수 · 고지와 웹앱 링크."""
import datetime
import unittest

import hyeoks_closebet_alert as C
import telegram_target as T
from test_friday_dip_alert_20261023 import ENV, Clock, FakeKis, Post, stock, world


def at(day, hm):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


def hot_world():
    s = world()                                                      # 금요일 1~2위 = 111110(+10%) · 222220(+13%)
    s["666660"] = stock("0001", 1000, 1180, 1180, 80_000_000, None, None)   # +18% · 944억 → 15% 후보
    s["777770"] = stock("1001", 1000, 1160, 1160, 10_000_000, None, None)   # +16% 지만 116억 → 제외
    return s


def cand(code, ret, amount, mult=None):
    return {"code": code, "name": f"이름{code}", "ret": ret, "amount": amount, "mult": mult, "vol": 1, "price": 1}


class Choose(unittest.TestCase):
    def test_thresholds_and_order(self):
        cs = [cand("a", 0.15, 5e10), cand("b", 0.149, 9e10), cand("c", 0.295, 9e10), cand("d", 0.2, 4.9e10),
              cand("e", 0.25, 8e10), cand("f", 0.11, 7e10)]
        self.assertEqual([c["code"] for c in C.choose(cs, C.HOT_LO, 5)], ["e", "a"])
        self.assertEqual([c["code"] for c in C.choose(cs, C.FRI_LO, 2)], ["b", "e"])

    def test_preferred_shares_are_dropped(self):
        rows = {"111115": {"prdy_ctrt": "20.0", "stck_prpr": "1000", "acml_vol": "9"},
                "111110": {"prdy_ctrt": "20.0", "stck_prpr": "1000", "acml_vol": "9"}}
        self.assertEqual([c["code"] for c in C.rows_to_cands(rows)], ["111110"])


class VolAvg(unittest.TestCase):
    def kis(self, n):
        rows = [{"stck_bsop_date": f"202610{d:02d}", "acml_vol": "100"} for d in range(1, n + 1)]
        rows.append({"stck_bsop_date": "20261030", "acml_vol": "999999"})            # 오늘은 빼고 센다
        return lambda path, tr, p: {"rt_cd": "0", "output2": rows}

    def test_twenty_days_needed(self):
        self.assertEqual(C.vol_avg20(self.kis(25), "a", "20261030"), 100)
        self.assertIsNone(C.vol_avg20(self.kis(19), "a", "20261030"))


class Message(unittest.TestCase):
    def test_mark_footer_and_no_results(self):
        text = C.message("2026-10-30", "15:05", [cand("a", 0.2, 6e10, 3.35), cand("b", 0.18, 5e10, 3.4)],
                         [cand("a", 0.2, 6e10)])
        # 2026-10-31 — 15:05 누적 배수는 관측 1(하루 전체 기준)과 달라 ✓ 를 붙이지 않는다
        self.assertIn("1. 이름a(a) +20.0% · 600억 · 거래량 3.4배\n", text)
        self.assertIn("2. 이름b(b) +18.0% · 500억 · 거래량 3.4배\n", text)
        self.assertNotIn("✓", text)
        self.assertIn("조건 표시는 하지 않습니다", text)
        self.assertIn("금요일 1·2위", text)
        self.assertIn("30억 조건은 확인 안 함", text)
        self.assertNotIn("빠진 종목", text)
        self.assertTrue(text.endswith(T.footer("home")))
        self.assertNotIn("수익률", text)

    def test_weekday_has_no_friday_section_and_empty_is_said(self):
        text = C.message("2026-10-29", "15:05", [], None)
        self.assertIn("(목)", text)
        self.assertIn("해당 종목 없음", text)
        self.assertNotIn("금요일 1·2위", text)


class Main(unittest.TestCase):
    def run_main(self, start, argv=(), stocks=None):
        k, post, c = FakeKis(stocks or hot_world()), Post(), Clock(start)
        rc = C.main(list(argv), env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep)
        return rc, k, post, c

    def test_friday_sends_hot_and_top_two_after_waiting(self):
        rc, _, post, c = self.run_main(at("2026-10-23", "14:55"))
        self.assertEqual((rc, len(post.sent)), (0, 1))
        self.assertGreaterEqual(c.t, at("2026-10-23", "15:05"))
        text = post.sent[0]["text"]
        self.assertIn("1. 비밀종목666660(666660) +18.0%", text)
        self.assertNotIn("777770", text)
        self.assertIn("1위 비밀종목666660", text)                # +18% 도 금요일 10% 기준에 든다
        self.assertIn("2위 비밀종목111110", text)
        self.assertNotIn("비밀종목222220", text)

    def test_late_start_sends_nothing(self):
        rc, k, post, _ = self.run_main(at("2026-10-23", "15:18"))
        self.assertEqual((rc, k.calls, post.sent), (0, [], []))

    def test_holiday_skips(self):
        rc, k, _, _ = self.run_main(at("2026-10-24", "15:00"))
        self.assertEqual((rc, k.calls), (0, []))

    def test_dry_run_does_not_send(self):
        rc, _, post, _ = self.run_main(at("2026-10-23", "15:05"), ["--day", "2026-10-22", "--dry-run"])
        self.assertEqual((rc, post.sent), (0, []))


if __name__ == "__main__":
    unittest.main()
