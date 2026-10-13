"""15:05 잠정 수급 기록 flow-estimate-v1 — 후보·호출·멈춤·공개 범위·시험 호출·대조 지표."""
import contextlib
import csv
import datetime
import gzip
import io
import json
import os
import tempfile
import unittest

import hyeoks_flow_estimate as F


def at(hm, day="2026-10-14"):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


class Resp:
    def __init__(self, js, status=200, text=""):
        self.js, self.status_code, self.text = js, status, text

    def json(self):
        if isinstance(self.js, Exception):
            raise self.js
        return self.js


def quote(code, chg, amt, cap=2000e8, vol=1_000_000, stop="N"):
    return {"itemcode": code, "itemname": "비밀종목", "sosok": "1", "nowPrice": "10000", "prevChangeRate": str(chg),
            "tradeVolume": str(vol), "tradeAmount": str(int(amt)), "marketSum": str(int(cap)), "tradeStopYn": stop}


def market(*extra):
    filler = [quote(f"1{i:05d}", 0.5, 1e8) for i in range(1600)]
    return filler + list(extra)


def kis(org, frg, late="5"):
    """입력구분 1(이른 것)과 late(늦은 것) 두 행. 늦은 행의 값이 15:05 에 보이는 값이다."""
    return {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "정상처리", "output1": {"stck_prpr": "10000"},
            "output2": [{"bsop_hour_gb": "1", "frgn_fake_ntby_qty": "-1", "orgn_fake_ntby_qty": "-1",
                         "sum_fake_ntby_qty": "-2"},
                        {"bsop_hour_gb": late, "frgn_fake_ntby_qty": str(frg), "orgn_fake_ntby_qty": str(org),
                         "sum_fake_ntby_qty": str(org + frg)}]}


def fake_get(kis_by_code, mkt=None):
    calls = []

    def get(url, headers=None, params=None, timeout=None):
        calls.append(url)
        if url == F.MARKET_URL:
            return Resp(mkt if mkt is not None else [])
        if url == F.BASE + F.PATH:
            v = kis_by_code[params["MKSC_SHRN_ISCD"]]
            return v.pop(0) if isinstance(v, list) else v
        raise AssertionError(url)
    get.calls = calls
    return get


def post_ok(*a, **k):
    return Resp({"access_token": "tok"})


class Candidates(unittest.TestCase):
    def test_k12_universe_sorted_and_capped(self):
        rows = [quote("900100", 12.0, 80e8), quote("900200", 9.99, 900e8), quote("900300", 30.0, 600e8),
                quote("900400", 15.0, 49e8), quote("900500", 20.0, 300e8, stop="Y"), quote("ABCDE", 20, 900e8),
                quote("900600", 10.0, 50e8)]
        got, capped = F.candidates(rows)
        self.assertEqual([x["itemcode"] for x in got], ["900300", "900100", "900600"],
                         "10%↑·50억↑, 거래대금 큰 순. 상한가 근접도 부른다(일치율 표본), 정지·코드 형식 아님은 뺀다")
        self.assertEqual(capped, 0)
        got, capped = F.candidates(rows, cap=2)
        self.assertEqual((len(got), capped), (2, 1))

    def test_latest_input_skips_blank_cells(self):
        rows = [{"bsop_hour_gb": "3", "orgn_fake_ntby_qty": "+1,200"}, {"bsop_hour_gb": "5", "orgn_fake_ntby_qty": ""},
                {"bsop_hour_gb": "4", "orgn_fake_ntby_qty": "-300"}]
        self.assertEqual(F.latest(rows, F.FIELDS["org"]), (-300.0, 4))
        self.assertEqual(F.latest([], F.FIELDS["org"]), (None, None))
        self.assertIsNone(F.num(""))
        self.assertEqual(F.num("0"), 0.0)


class Record(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.runs, self.priv = os.path.join(self.d, "runs"), os.path.join(self.d, "priv")

    def run_main(self, get, clock, uploader=lambda n, b: "drv"):
        with contextlib.redirect_stdout(io.StringIO()):
            return F.main([], env={"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s", "GITHUB_RUN_ID": "77"}, get=get,
                          post=post_ok, clock=clock, sleep=clock.sleep, uploader=uploader, runs_dir=self.runs,
                          private_dir=self.priv)

    def run_row(self):
        with open(os.path.join(self.runs, F.RUNS_FILE), encoding="utf-8") as fh:
            return list(csv.DictReader(fh))[0]

    def public_text(self):
        out = ""
        for f in os.listdir(self.runs):
            with open(os.path.join(self.runs, f), encoding="utf-8") as fh:
                out += fh.read()
        return out

    def test_records_privately_and_public_has_counts_only(self):
        mkt = market(quote("900100", 12.0, 80e8), quote("900200", 25.0, 700e8))
        rl = {"rt_cd": "1", "msg_cd": F.RATE_LIMIT, "msg1": "초당 거래건수를 초과"}
        get = fake_get({"900100": [Resp(rl, 500), Resp(kis(987651, 987652))], "900200": Resp(kis(-987653, 987654))},
                       mkt)
        c = Clock(at("15:03"))
        self.assertEqual(self.run_main(get, c), 0)
        pub = self.public_text()
        for secret in ("900100", "900200", "98765", "비밀종목"):
            self.assertNotIn(secret, pub, "공개 장부에 종목코드·종목명·수급 값이 없어야 한다")
        st = self.run_row()
        self.assertEqual((st["candidates"], st["called"], st["ok"], st["rateLimitRetries"], st["latestFilled"]),
                         ("2", "2", "2", "1", "2"))
        self.assertEqual((st["status"], st["privateStatus"], st["inputCodes"]), ("완료", "접수", "1:2|5:2"))
        with gzip.open(os.path.join(self.priv, st["privateFile"]), "rt", encoding="utf-8") as fh:
            body = json.load(fh)
        self.assertEqual([r["code"] for r in body["records"]], ["900200", "900100"])
        self.assertEqual(body["records"][1]["output2"][1]["orgn_fake_ntby_qty"], "987651")
        self.assertEqual(body["records"][0]["quote"]["prevChangeRate"], "25.0")

    def test_skips_outside_window_holiday_and_duplicate(self):
        boom = fake_get({})
        for clk in (Clock(at("13:02")), Clock(at("15:26")), Clock(at("15:03", "2026-10-09"))):
            self.assertEqual(self.run_main(boom, clk), 0)
        self.assertEqual(boom.calls, [], "창 밖·휴장일엔 아무것도 부르지 않는다")
        get = fake_get({"900100": Resp(kis(1, 1))}, market(quote("900100", 12.0, 80e8)))
        self.assertEqual(self.run_main(get, Clock(at("15:03"))), 0)
        again = fake_get({})
        self.assertEqual(self.run_main(again, Clock(at("15:10"))), 0)
        self.assertEqual(again.calls, [], "이미 기록한 날은 중복 실행으로 보고 끝낸다")

    def test_failed_upload_is_retried_next_run(self):
        get = fake_get({"900100": Resp(kis(1, 1))}, market(quote("900100", 12.0, 80e8)))

        def bad(n, b):
            raise RuntimeError("drive down")
        self.assertEqual(self.run_main(get, Clock(at("15:03")), uploader=bad), 1)
        self.assertFalse(F.already_done("2026-10-14", self.runs), "보관 실패한 날은 다음 실행이 다시 기록한다")

    def test_repeated_permission_errors_stop(self):
        err = {"rt_cd": "1", "msg_cd": "EGW00123", "msg1": "권한 없음"}
        codes = [f"9001{i:02d}" for i in range(6)]
        get = fake_get({c: Resp(err) for c in codes}, market(*[quote(c, 12.0, 80e8 + i) for i, c in enumerate(codes)]))
        self.assertEqual(self.run_main(get, Clock(at("15:03"))), 1)
        st = self.run_row()
        self.assertEqual((st["status"], st["called"]), ("중단", str(F.STOP_AFTER)))

    def test_stops_calling_before_market_close(self):
        codes = [f"9002{i:02d}" for i in range(5)]
        get = fake_get({c: Resp(kis(1, 1)) for c in codes})
        c = Clock(at("15:25"))
        with contextlib.redirect_stdout(io.StringIO()):
            recs, st = F.record("2026-10-14", [quote(x, 12.0, 80e8) for x in codes], get, {}, c, c.sleep, pause=90)
        self.assertEqual(st["status"], "중단")
        self.assertLess(len(recs), 5)
        self.assertTrue(all(r["calledAt"] < "2026-10-14T15:29" for r in recs))

    def test_market_list_too_short_is_logged_without_kis_calls(self):
        get = fake_get({}, [quote("900100", 12.0, 80e8)])
        self.assertEqual(self.run_main(get, Clock(at("15:03"))), 1)
        self.assertEqual(get.calls, [F.MARKET_URL])
        self.assertIn("중단", self.public_text())


class Probe(unittest.TestCase):
    def test_prints_structure_without_values(self):
        get = fake_get({"005930": Resp(kis(987651, 0)), "086520": Resp({"rt_cd": "1", "msg_cd": "EGW00123",
                                                                       "msg1": "권한 없음"})})
        c, buf = Clock(at("14:45")), io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = F.main(["--probe"], env={"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s"}, get=get, post=post_ok,
                        clock=c, sleep=c.sleep)
        out = buf.getvalue()
        self.assertEqual(rc, 1, "한 종목이라도 오류면 1")
        self.assertNotIn("98765", out)
        self.assertIn("입력구분 5 · 외국인 0 · 기관 값 · 합 값", out)
        self.assertIn("frgn_fake_ntby_qty", out)


def pair(day, code, po, pf, fo, ff, vol=100_000, chg=12.0, vm=None, u2=False):
    return {"day": day, "code": code, "p_org": po, "p_frg": pf, "f_org": fo, "f_frg": ff, "volume": vol,
            "change": chg, "k12": chg < F.LIMIT_CHANGE, "u2": u2, "vm": vm}


class Reconcile(unittest.TestCase):
    def test_metrics_and_trivial_baseline(self):
        ps = [pair("2026-10-15", "900100", 5, 5, 5, 5),        # 동반 통과 — 일치
              pair("2026-10-15", "900200", 5, 5, -5, 5),       # 잠정 통과 → 확정 기관 음수
              pair("2026-10-16", "900300", -5, 5, -5, 5),      # 둘 다 불통과 — 일치
              pair("2026-10-16", "900400", -5, -5, 5, 5),      # 확정만 통과
              pair("2026-10-16", "900500", None, 5, 5, 5)]     # 잠정 없음 → 분모 밖
        s = F.summarize(ps)
        self.assertEqual((s["pairs"], s["complete"], s["noProvisional"], s["days"]), (5, 4, 1, 2))
        m = s["groups"]["전체 호출 종목"]["metrics"]
        self.assertAlmostEqual(m["기관 부호 일치"]["value"], 2 / 4)
        self.assertAlmostEqual(m["외국인 부호 일치"]["value"], 3 / 4)
        self.assertAlmostEqual(m["동반 조건 일치"]["value"], 2 / 4)
        self.assertEqual((m["정밀도(잠정 통과 중 확정도 통과)"]["value"], m["정밀도(잠정 통과 중 확정도 통과)"]["n"]), (0.5, 2))
        self.assertEqual((m["재현율(확정 통과 중 잠정도 통과)"]["value"], m["재현율(확정 통과 중 잠정도 통과)"]["n"]), (0.5, 2))
        self.assertAlmostEqual(m["확정 통과 비율(기준선)"]["value"], 2 / 4)
        lo, hi = m["외국인 부호 일치"]["ci95"]
        self.assertTrue(0 <= lo <= 0.75 <= hi <= 1)

    def test_size_strata_use_1505_volume(self):
        ps = [pair("2026-10-15", "900100", 5, 5, 2_000, 5), pair("2026-10-15", "900200", -5, 5, 500, 5)]
        m = F.summarize(ps)["groups"]["전체 호출 종목"]["metrics"]
        self.assertEqual(m["기관 부호 일치 — 확정 크기 1%↑"]["n"], 1)
        self.assertEqual(m["기관 부호 일치 — 확정 크기 1% 미만"]["value"], 0.0)

    def test_one_pick_per_day_compares_provisional_and_final(self):
        ps = [pair("2026-10-15", "900100", 5, 5, -5, 5, vm=9.0),       # 잠정 1등, 확정 불통과
              pair("2026-10-15", "900200", 5, 5, 5, 5, vm=4.0),        # 확정으로는 이 종목
              pair("2026-10-15", "900300", 5, 5, 5, 5, vm=20.0, chg=29.6),   # 상한가 근접 — 고르기에서 뺀다
              pair("2026-10-16", "900500", 5, 5, 5, 5, vm=3.0),
              pair("2026-10-16", "900400", 5, 5, 5, 5, vm=3.0)]        # 동률 → 종목코드 순
        p = F.picks(ps)
        self.assertEqual((p["days"], p["provisionalPickDays"], p["finalPickDays"], p["sameCodeDays"],
                          p["provisionalPickFinalPass"]), (2, 2, 2, 1, 1))

    def test_finals_paging_and_volume_multiple_excludes_same_day(self):
        pages = {"20261017": [{"bizdate": "20261016", "foreignerPureBuyQuant": "+1,200", "organPureBuyQuant": "-30"},
                              {"bizdate": "20261015", "foreignerPureBuyQuant": "5", "organPureBuyQuant": "6"}],
                 "20261015": [{"bizdate": "20261014", "foreignerPureBuyQuant": "-7", "organPureBuyQuant": "8"}]}
        asked = []

        def get(url, headers=None, timeout=None):
            biz = url.rsplit("=", 1)[1]
            asked.append(biz)
            return Resp(pages.get(biz, []))
        got, fails = F.fetch_finals({"900100": {"2026-10-14", "2026-10-16"}}, get, sleep=lambda s: None)
        self.assertEqual(fails, 0)
        self.assertEqual(got[("2026-10-16", "900100")], (1200.0, -30.0))
        self.assertEqual(got[("2026-10-14", "900100")], (-7.0, 8.0))
        self.assertNotIn(("2026-10-15", "900100"), got, "필요한 날만 남긴다")
        self.assertEqual(asked, ["20261017", "20261015"])

        days = [(datetime.date(2026, 9, 1) + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(25)]
        xml = "<chartdata symbol='900100'>" + "".join(
            f"<item data='{d}|100|110|90|100|{9_999_999 if d == '20260925' else 1000}'/>" for d in days) + "</chartdata>"
        vm = F.volume_multiples([("2026-09-25", "900100", 3000)], lambda url, timeout=None: Resp(None, text=xml),
                                sleep=lambda s: None)
        self.assertAlmostEqual(vm[("2026-09-25", "900100")], 3.0, msg="그날 봉(거래량 9,999,999)은 평균에 넣지 않는다")

    def test_reconcile_skips_today_and_writes_private_summary(self):
        d = tempfile.mkdtemp()
        for day, run, req in (("2026-10-15", "1", "15:03"), ("2026-10-15", "2", "15:20"), ("2026-10-16", "3", "15:03")):
            body = {"version": F.VERSION, "day": day, "runId": run, "marketRequestedAt": f"{day}T{req}:00+09:00",
                    "records": [{"code": "900100", "rt_cd": "0", "quote": {"prevChangeRate": "12", "tradeVolume": "1000"},
                                 "output2": kis(5 if run == "1" else -5, 5)["output2"]}]}
            with gzip.open(os.path.join(d, f"research_flowest_{day}_{run}.json.gz"), "wt", encoding="utf-8") as fh:
                json.dump(body, fh)

        def get(url, headers=None, timeout=None):
            if "trend" in url:
                return Resp([{"bizdate": "20261015", "foreignerPureBuyQuant": "3", "organPureBuyQuant": "4"}])
            return Resp(None, text="<chartdata symbol='900100'></chartdata>")
        out = os.path.join(d, "out")
        c = Clock(at("09:00", "2026-10-16"))
        with contextlib.redirect_stdout(io.StringIO()):
            rc = F.main(["--reconcile", d, "--out", out], get=get, post=post_ok, clock=c, sleep=c.sleep)
        self.assertEqual(rc, 0)
        with open(os.path.join(out, "summary.json"), encoding="utf-8") as fh:
            s = json.load(fh)
        self.assertEqual(s["recordDays"], ["2026-10-15"], "오늘(10/16) 기록은 확정 전이라 뺀다")
        self.assertEqual(s["complete"], 1)
        m = s["groups"]["전체 호출 종목"]["metrics"]["기관 부호 일치"]
        self.assertEqual(m["value"], 1.0, "같은 날 실행이 둘이면 시세를 먼저 받은(15:03) 실행을 쓴다")


if __name__ == "__main__":
    unittest.main()
