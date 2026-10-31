"""알림 정직하게(2026-10-31 전략 평가 후속) — ETF · ETN 은 빼고 다음 종목으로, 관리 · 경고 · 스팩은 표시, 순위 잘림 경고, 관측 ⑥ 범위 문구."""
import datetime
import unittest

import hyeoks_closebet_alert as C
import hyeoks_friday_dip_alert as A
from test_friday_dip_alert_20261023 import ENV, Clock, FakeKis, Post, stock, world


def at(day, hm):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


def etf_world():
    s = world()                                                      # 1~2위 = 111110(690억) · 222220(560억)
    s["888880"] = stock("0001", 1000, 1110, 1110, 900_000_000, 1110, 1110)   # +11% · 9,990억 → 거래대금 1위지만 ETF
    s["888880"]["status"] = {"rprs_mrkt_kor_name": "ETF", "bstp_kor_isnm": "ETF(실물복제/수익증권)"}
    s["222220"]["status"] = {"mang_issu_cls_code": "Y", "mrkt_warn_cls_code": "02"}
    return s


class TopStocks(unittest.TestCase):
    def kis(self, status):
        def call(path, tr, p):
            if p["FID_INPUT_ISCD"] == "boom":
                raise RuntimeError("x")
            return {"output": status.get(p["FID_INPUT_ISCD"], {})}
        return call

    def test_funds_are_skipped_and_the_next_one_moves_up(self):
        ranked = [{"code": "etf"}, {"code": "etn"}, {"code": "a"}, {"code": "b"}, {"code": "c"}]
        status = {"etf": {"rprs_mrkt_kor_name": "ETF"}, "etn": {"bstp_kor_isnm": "ETN"}, "b": {"mrkt_warn_cls_code": "03"}}
        got = A.top_stocks(self.kis(status), ranked, 2)
        self.assertEqual([(r["code"], r["tags"]) for r in got], [("a", []), ("b", ["투자위험"])])

    def test_status_failure_keeps_the_stock_with_a_note(self):
        got = A.top_stocks(self.kis({}), [{"code": "boom"}], 2)
        self.assertEqual(got, [{"code": "boom", "tags": ["상태 확인 못 함"]}])

    def test_spac_is_marked_by_name(self):
        self.assertEqual(A.tag_text({"tags": ["관리종목"]}, "에이스스팩1호"), " [관리종목 · 스팩]")
        self.assertEqual(A.tag_text({}, "보통회사"), "")


class FridayMessage(unittest.TestCase):
    def test_scope_note_tags_and_cut_warning(self):
        picks = [{"code": "a", "close": 1000, "ret": 0.2, "amount": 6e10, "nx": 990, "kr": None, "tags": ["투자경고"]}]
        text, _ = A.message("2026-11-06", "19:40", picks, {"a": "이름a"}, cut=True)
        self.assertIn("1위 이름a(a) [투자경고] — KRX 종가", text)
        self.assertIn("그중 거래대금 1·2위만 봅니다", text)
        self.assertIn("빠진 종목이 있을 수 있습니다", text)
        text, _ = A.message("2026-11-06", "19:40", picks, {"a": "이름a"})
        self.assertNotIn("빠진 종목", text)


class Mains(unittest.TestCase):
    def test_friday_alert_skips_etf_and_marks_warning(self):
        k, post, c = FakeKis(etf_world()), Post(), Clock(at("2026-10-23", "19:40"))
        sheet = lambda day, rows: None
        A.main([], env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep, runs_dir=self.tmp(), sheet=sheet)
        text = post.sent[0]["text"]
        self.assertNotIn("888880", text)
        self.assertIn("1위 비밀종목111110(111110)", text)
        self.assertIn("2위 비밀종목222220(222220) [관리종목 · 투자경고]", text)

    def test_closebet_skips_etf_on_friday(self):
        k, post, c = FakeKis(etf_world()), Post(), Clock(at("2026-10-23", "15:05"))
        C.main([], env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep)
        text = post.sent[0]["text"]
        self.assertNotIn("888880", text)
        self.assertIn("1위 비밀종목111110(111110)", text)
        self.assertIn("2위 비밀종목222220(222220) [관리종목 · 투자경고]", text)

    def tmp(self):
        import tempfile
        d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, d, True)
        return d


if __name__ == "__main__":
    unittest.main()
