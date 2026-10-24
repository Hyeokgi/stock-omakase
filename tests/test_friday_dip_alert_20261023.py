"""금요일 밤 하락 알림 friday-dip-v1 — 순위 구간 나누기 · 15:30 종가 · 1~2위 · 중복 실행 · 공개 기록 범위."""
import datetime
import os
import tempfile
import unittest
from unittest import mock

import hyeoks_friday_dip_alert as A
import hyeoks_research_daily as R

DAY8 = "20261023"


def rank_row(code, rate, price=10000, vol=10_000_000, name=None):
    return {"stck_shrn_iscd": code, "prdy_ctrt": f"{rate:.2f}", "stck_prpr": str(price), "acml_vol": str(vol),
            "hts_kor_isnm": name or f"비밀종목{code}"}


class Resp:
    def __init__(self, js, status=200):
        self.js, self.status_code = js, status

    def json(self):
        return self.js


class FakeKis:
    """tr_id 별로 가짜 응답. stocks — {코드: dict(market, rate, prev, close1530, evening, vol, nx, after)}."""

    def __init__(self, stocks):
        self.stocks, self.calls = stocks, []

    def get(self, url, headers=None, params=None, timeout=None):
        tr, p = headers["tr_id"], params
        self.calls.append(tr)
        if tr == "FHPST01700000":
            lo, hi = float(p["fid_rsfl_rate1"]), float(p["fid_rsfl_rate2"])
            assert p["fid_input_price_1"] == "" and p["fid_input_price_2"] == "", "이 가짜 세계는 가격 나누기가 필요 없다"
            rows = [rank_row(c, s["rate"], s["evening"], s["vol"]) for c, s in sorted(self.stocks.items())
                    if s["market"] == p["fid_input_iscd"] and lo <= s["rate"] <= hi]
            return Resp({"rt_cd": "0", "output": rows[:A.PAGE]})
        code = p["FID_INPUT_ISCD"]
        s = self.stocks[code]
        if tr == "FHKST03010100" and p["FID_COND_MRKT_DIV_CODE"] == "J":
            out = [{"stck_bsop_date": "20261022", "stck_clpr": str(s["prev"]), "acml_vol": "1"},
                   {"stck_bsop_date": DAY8, "stck_clpr": str(s["evening"]), "acml_vol": str(s["vol"])}]
            return Resp({"rt_cd": "0", "output2": out})
        if tr == "FHKST03010100":            # NX
            out = [] if s["nx"] is None else [{"stck_bsop_date": DAY8, "stck_clpr": str(s["nx"]), "acml_vol": "5"}]
            return Resp({"rt_cd": "0", "output2": out})
        if tr == "FHKST03010230":
            hh = p["FID_INPUT_HOUR_1"]
            bars = [{"stck_bsop_date": DAY8, "stck_cntg_hour": "153000", "stck_prpr": str(s["close1530"]), "cntg_vol": "9"}]
            if hh >= "160000" and s["after"] is not None:
                bars.insert(0, {"stck_bsop_date": DAY8, "stck_cntg_hour": "193900", "stck_prpr": str(s["after"]),
                                "cntg_vol": "3"})
            return Resp({"rt_cd": "0", "output2": bars})
        raise AssertionError(tr)


class Post:
    def __init__(self):
        self.sent = []

    def __call__(self, url, json=None, data=None, headers=None, timeout=None):
        if url.endswith("/oauth2/tokenP"):
            return Resp({"access_token": "t"})
        self.sent.append(data)
        return Resp({"ok": True})


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


def fri(hm):
    return datetime.datetime.fromisoformat(f"2026-10-23T{hm}:00+09:00")


def stock(market, prev, close, evening, vol, nx, after):
    return {"market": market, "prev": prev, "close1530": close, "evening": evening, "vol": vol, "nx": nx, "after": after,
            "rate": round((evening / prev - 1) * 100, 2)}


def world():
    return {
        "111110": stock("1001", 1000, 1150, 1100, 60_000_000, 1110, 1100),   # +15% · 690억 · 저녁 −4.3% → 해당
        "222220": stock("0001", 10000, 11200, 11300, 5_000_000, 11300, None),  # +12% · 560억 · 저녁 +0.9%
        "333330": stock("1001", 1000, 1120, 1120, 30_000_000, None, None),   # +12% 지만 336억 → 제외
        "444445": stock("0001", 1000, 1200, 1200, 90_000_000, 1200, 1200),   # 우선주 → 제외
        "555550": stock("1001", 1000, 1296, 1296, 90_000_000, 1296, 1296),   # +29.6% → 제외
    }


ENV = {"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s", "TELEGRAM_BOT_TOKEN": "b", "GITHUB_RUN_ID": "7"}


class Scan(unittest.TestCase):
    @staticmethod
    def band_of(rows):
        """rows — {코드: (등락률, 가격)}. 실제 API 처럼 등락률 · 가격 양끝 포함, 최대 30개."""
        def band(iscd, lo, hi, plo=None, phi=None):
            if iscd != "0001":
                return []
            hit = [rank_row(c, r, p) for c, (r, p) in sorted(rows.items())
                   if lo <= r <= hi and (plo is None or plo <= p <= phi)]
            return hit[:A.PAGE]
        return band

    def test_full_band_is_split_until_complete(self):
        rows = {f"{i:05d}0": (-5 + i * 0.05, 1000 + i) for i in range(500)}
        got, calls, cut = A.scan(self.band_of(rows))
        self.assertEqual(set(got), set(rows))
        self.assertEqual(cut, [])
        self.assertGreater(calls, 500 // A.PAGE)

    def test_flat_bucket_is_split_by_price(self):
        """2026-10-25 Codex — 보합(0.00%)이 30개를 넘어도 가격으로 나눠 숨은 종목(종가 +10% 뒤 저녁에 빠진 종목)을 찾는다."""
        rows = {f"{i:05d}0": (0.0, 500 + i * 37) for i in range(75)}
        rows.update({f"{i:05d}0": (3.0 + i / 100, 2000 + i) for i in range(100, 120)})
        got, _, cut = A.scan(self.band_of(rows))
        self.assertEqual((set(got), cut), (set(rows), []))

    def test_same_rate_and_same_price_overflow_is_recorded(self):
        rows = {f"{i:05d}0": (0.0, 1000) for i in range(A.PAGE + 1)}
        got, calls, cut = A.scan(self.band_of(rows))
        self.assertTrue(cut)
        self.assertTrue(all(lo <= 0 <= hi and pa <= 1000 <= pb and pb - pa < 1 for _, lo, hi, pa, pb in cut))
        self.assertLess(calls, 200, "가격 나누기가 한 갈래로만 내려가야 한다")

    def test_prelist_keeps_common_shares_with_big_proxy_amount(self):
        rows = {"111110": rank_row("111110", 12, 10000, 4_000_000), "111115": rank_row("111115", 12, 10000, 9_000_000),
                "222220": rank_row("222220", 12, 10000, 2_000_000)}
        self.assertEqual(A.prelist(rows), ["111110"])


class Pick(unittest.TestCase):
    def test_close_comes_from_1530_bar_not_evening_daily(self):
        k = FakeKis({"111110": stock("1001", 1000, 1100, 1060, 60_000_000, None, None)})
        bar = A.day_bar(lambda path, tr, p: k.get("", {"tr_id": tr}, p).json(), "111110", DAY8)
        self.assertEqual(bar["close"], 1100)
        self.assertAlmostEqual(bar["ret"], 0.10)

    def test_thresholds_and_top_two_by_amount(self):
        bars = [{"code": "a", "ret": 0.10, "amount": 5e10}, {"code": "b", "ret": 0.295, "amount": 9e11},
                {"code": "c", "ret": 0.2, "amount": 4.9e10}, {"code": "d", "ret": 0.15, "amount": 7e10},
                {"code": "e", "ret": 0.12, "amount": 6e10}, None]
        self.assertEqual([b["code"] for b in A.pick(bars)], ["d", "e"])

    def test_message_flags_exact_minus_three(self):
        p = [{"code": "x", "close": 1000, "ret": 0.2, "amount": 6e10, "nx": 970, "kr": None},
             {"code": "y", "close": 1000, "ret": 0.2, "amount": 6e10, "nx": 980, "kr": 990}]
        text, hits = A.message("2026-10-23", "19:40", p, {"x": "가"})
        self.assertEqual(hits, 1)
        self.assertIn("거래 없음", text)
        self.assertIn("← 3% 이상 하락", text)


class Main(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def run_main(self, start, argv=(), stocks=None):
        k, post, c = FakeKis(stocks or world()), Post(), Clock(start)
        self.clock_now = c
        rc = A.main(list(argv), env=ENV, get=k.get, post=post, clock=c, sleep=c.sleep, runs_dir=self.dir)
        return rc, k, post, c

    def log(self):
        path = os.path.join(self.dir, "friday_dip_runs.csv")
        return open(path, encoding="utf-8").read() if os.path.exists(path) else ""

    def test_hit_waits_until_1940_sends_and_logs_counts_only(self):
        rc, k, post, c = self.run_main(fri("19:05"))
        self.assertEqual(rc, 0)
        self.assertGreaterEqual(c.t, fri("19:40"))
        self.assertEqual(len(post.sent), 1)
        text = post.sent[0]["text"]
        self.assertIn("비밀종목111110", text)
        self.assertIn("비밀종목222220", text)
        self.assertNotIn("333330", text)
        self.assertNotIn("444445", text)
        self.assertNotIn("555550", text)
        log = self.log()
        self.assertIn("발송성공", log)
        for secret in ("111110", "222220", "비밀종목"):
            self.assertNotIn(secret, log, "공개 기록에 종목 정보가 들어가면 안 된다")

    def test_second_run_same_day_does_nothing(self):
        self.run_main(fri("19:05"))
        rc, k, post, _ = self.run_main(fri("19:45"))
        self.assertEqual((rc, k.calls, post.sent), (0, [], []))

    def test_no_drop_logs_without_sending(self):
        s = world()
        s["111110"].update(nx=1150, after=1149)
        rc, _, post, _ = self.run_main(fri("19:41"), stocks=s)
        self.assertEqual((rc, post.sent), (0, []))
        self.assertIn("해당없음", self.log())

    def test_late_start_and_non_friday_skip(self):
        rc, k, post, _ = self.run_main(fri("19:59"))
        self.assertEqual((rc, k.calls, post.sent), (0, [], []))
        self.assertIn("늦은시작", self.log())
        rc, k, _, _ = self.run_main(fri("19:05") - datetime.timedelta(days=1))
        self.assertEqual((rc, k.calls), (0, []))

    def test_dry_run_neither_sends_nor_logs(self):
        rc, _, post, _ = self.run_main(fri("19:05"), ["--day", "2026-10-23", "--dry-run"])
        self.assertEqual((rc, post.sent, self.log()), (0, [], ""))

    def test_failed_send_allows_retry(self):
        def bad_post(url, json=None, data=None, headers=None, timeout=None):
            return Resp({"access_token": "t"}) if url.endswith("tokenP") else Resp({}, 500)
        c = Clock(fri("19:40"))
        A.main([], env=ENV, get=FakeKis(world()).get, post=bad_post, clock=c, sleep=c.sleep, runs_dir=self.dir)
        self.assertIn("발송실패", self.log())
        self.assertFalse(A.already_done("2026-10-23", self.dir))
        self.assertEqual(R.send_status(False, "HTTP 500"), "발송실패")
        # Codex 검토 — 재실행이 실제로 다시 보낸다
        rc, _, post, _ = self.run_main(fri("19:45"))
        self.assertEqual((rc, len(post.sent)), (0, 1))

    def test_incomplete_scan_is_not_final(self):
        """2026-10-25 Codex — 가격으로도 못 나눈 구간이 남으면 '대상없음 · 해당없음' 으로 확정하지 않는다."""
        real = A.scan
        s = world()
        for v in s.values():
            v.update(vol=1)                                   # 거래대금 미달 → 대상 없음
        with mock.patch.object(A, "scan", lambda band: real(band)[:2] + ([("1001", 0.0, 0.01, 1000, 1000)],)):
            rc, _, post, _ = self.run_main(fri("19:41"), stocks=s)
        self.assertEqual((rc, post.sent), (0, []))
        self.assertIn("조회불완전", self.log())
        self.assertFalse(A.already_done("2026-10-23", self.dir))

    def test_no_send_after_deadline_even_if_started_in_time(self):
        with mock.patch.object(A, "SEND_BY", "19:41"):
            rc, _, post, _ = self.run_main(fri("19:41"))
        self.assertEqual((rc, post.sent), (0, []))
        self.assertIn("늦은발송생략", self.log())

    def test_picks_are_made_at_check_time_not_at_start(self):
        """2026-10-25 Codex — 18:00 백업이 먼저 고르면 거래량 기준 시각이 달라진다. 고르기 전에 19:40 까지 기다린다."""
        seen = []
        real = A.scan

        def spy(band):
            seen.append(self.clock_now())
            return real(band)
        with mock.patch.object(A, "scan", spy):
            self.run_main(fri("18:00"))
        self.assertGreaterEqual(seen[0], fri("19:40"))


if __name__ == "__main__":
    unittest.main()
