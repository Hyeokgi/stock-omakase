"""금요일 알림 E단계 — 대상 없어도 한 줄 · 20:02 '20:00 확정' 추가 메시지 · 고지와 웹앱 금요일 탭 링크."""
import tempfile
import unittest

import hyeoks_friday_dip_alert as A
import telegram_target as T
from test_friday_dip_alert_20261023 import ENV, Clock, FakeKis, Post, fri, world


def p(code, nx, kr, nx20, kr20, close=1000):
    return {"code": code, "close": close, "ret": 0.15, "amount": 6e10, "nx": nx, "kr": kr, "nx20": nx20, "kr20": kr20}


class CloseMessage(unittest.TestCase):
    def test_nothing_at_1940_or_2000_means_no_extra(self):
        self.assertEqual(A.close_message("2026-10-23", [p("a", 990, None, 985, None)], {}), (None, 0))

    def test_hit_only_at_2000_is_announced(self):
        text, n = A.close_message("2026-10-23", [p("a", 990, None, 965, None), p("b", 1000, 1000, 1000, None)], {"a": "가"})
        self.assertEqual(n, 1)
        self.assertIn("가(a) — 20:00 NXT 965원 (-3.5%)", text)
        self.assertIn("← 조건 해당", text)
        self.assertIn("해당 1종목", text)

    def test_hit_at_1940_but_not_2000_says_so(self):
        text, n = A.close_message("2026-10-23", [p("a", 960, None, 990, None)], {})
        self.assertEqual(n, 0)
        self.assertIn("조건 밖에서 끝났습니다", text)

    def test_exact_minus_three_counts_and_missing_trade_is_shown(self):
        text, n = A.close_message("2026-10-23", [p("a", 970, None, 970, None), p("b", 990, None, None, None)], {})
        self.assertEqual(n, 1)
        self.assertIn("거래 없음", text)


class Footer(unittest.TestCase):
    def test_messages_end_with_notice_and_friday_tab(self):
        tail = T.footer("friday")
        self.assertTrue(tail.endswith(T.WEBAPP_URL + "#friday"))
        self.assertIn(T.NOTICE, tail)
        for text in (A.empty_message("2026-10-23"), A.message("2026-10-23", "19:40", [p("a", 990, None, 0, 0)], {})[0],
                     A.close_message("2026-10-23", [p("a", 960, None, 960, None)], {})[0]):
            self.assertTrue(text.endswith(tail))
            self.assertNotIn("매수 추천", text.replace("매매 권유 아님", ""))


class EmptyFriday(unittest.TestCase):
    def test_no_candidates_still_sends_one_line_and_is_final(self):
        s = world()
        for v in s.values():
            v.update(vol=1)
        k, post, c, d = FakeKis(s), Post(), Clock(fri("19:41")), tempfile.mkdtemp()
        rc = A.main([], env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep, runs_dir=d, sheet=lambda day, rows: None)
        self.assertEqual((rc, len(post.sent)), (0, 1))
        self.assertIn("종목이 없습니다", post.sent[0]["text"])
        self.assertTrue(A.already_done("2026-10-23", d))


if __name__ == "__main__":
    unittest.main()
