"""급등 사건 과거 1분봉 수집 minute-backfill-v1 — 목록 묶기·하루 받기·달 수집·이어 받기·공개 범위·합치기."""
import base64
import contextlib
import csv
import datetime
import gzip
import hashlib
import io
import os
import tempfile
import unittest

import hyeoks_minute_backfill as M


def at(hm, day="2026-10-15"):
    return datetime.datetime.fromisoformat(f"{day}T{hm}:00+09:00")


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


class Resp:
    def __init__(self, js):
        self.js, self.status_code = js, 200

    def json(self):
        return self.js


def session_times():
    """09:00~15:19 매분 + 15:30 — 정규장 381봉(15:20~15:29 는 종가 동시호가라 봉이 없다)."""
    out, t = [], datetime.datetime(2000, 1, 1, 9, 0)
    while t.strftime("%H%M") <= "1519":
        out.append(t.strftime("%H%M%S"))
        t += datetime.timedelta(minutes=1)
    return out + ["153000"]


def bar(day, hms, price=987650):
    return {"stck_bsop_date": day, "stck_cntg_hour": hms, "stck_oprc": str(price), "stck_hgpr": str(price + 10),
            "stck_lwpr": str(price - 10), "stck_prpr": str(price), "cntg_vol": "123", "acml_tr_pbmn": "1"}


class FakeKis:
    """주식일별분봉조회 흉내 — 입력 시각 이하(포함) 봉을 늦은 것부터 120개. 그날이 모자라면 전날 봉을 이어 붙인다."""

    def __init__(self, days, inclusive=True, ignore_hour=False, errors=None):
        self.days, self.inclusive, self.ignore_hour = days, inclusive, ignore_hour
        self.errors, self.calls = dict(errors or {}), []

    def __call__(self, url, headers=None, params=None, timeout=None):
        if url == M.BASE + M.PROGRAM_PATH:
            return Resp({"rt_cd": "0", "msg_cd": "MCA00000", "output": [{"bsop_hour": "153000", "stck_prpr": "987650"}]})
        code, day, hour = params["FID_INPUT_ISCD"], params["FID_INPUT_DATE_1"], params["FID_INPUT_HOUR_1"]
        self.calls.append((code, day, hour))
        err = self.errors.get((code, day))
        if err:
            return Resp({"rt_cd": "1", "msg_cd": err, "msg1": "오류"})
        if self.ignore_hour:
            hour = M.START_HOUR
        rows = [bar(day, t) for t in reversed(self.days.get((code, day), []))
                if (t <= hour if self.inclusive else t < hour)]
        if len(rows) < 120:
            prev = f"{int(day) - 1}"
            rows += [bar(prev, t) for t in reversed(session_times())][:120 - len(rows)]
        return Resp({"rt_cd": "0", "msg_cd": "MCA00000", "output1": {"hts_kor_isnm": "비밀종목"}, "output2": rows[:120]})


def post_ok(*a, **k):
    return Resp({"access_token": "tok"})


def write_csv(path, header, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


EVENTS = [("900100", "20251015", "20251016"), ("900200", "20251015", "20251016"),
          ("900100", "20251016", "20251017"), ("900300", "20251103", "20251104")]


def secret_of(events):
    return base64.b64encode(gzip.compress(M.canonical(events).encode(), mtime=0)).decode()


class EventList(unittest.TestCase):
    def test_pack_unpack_roundtrip_with_calendar_t1(self):
        d = tempfile.mkdtemp()
        write_csv(os.path.join(d, "ev.csv"), ["code", "date", "ret"], [["900100", "2025-10-15", "16"], ["1234", "20251031", "20"]])
        write_csv(os.path.join(d, "cal.csv"), ["date"], [["20251015"], ["20251016"], ["20251031"], ["20251103"]])
        out = os.path.join(d, "secret.txt")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(M.pack(os.path.join(d, "ev.csv"), os.path.join(d, "cal.csv"), out), 0)
        with open(out, encoding="ascii") as fh:
            ev = M.unpack(fh.read())
        self.assertEqual(ev, [("900100", "20251015", "20251016"), ("001234", "20251031", "20251103")],
                         "t1 = 달력의 다음 날(월말 → 다음 달), 앞자리 0 복원")
        self.assertNotIn("900100", buf.getvalue(), "묶을 때도 종목코드는 찍지 않는다")

    def test_bad_lines_are_refused_and_real_size_fits_secret(self):
        bad = base64.b64encode(gzip.compress(b"900100,20251015,20251016\n9001,2025,x\n")).decode()
        with self.assertRaises(ValueError):
            M.unpack(bad)
        with self.assertRaises(ValueError):
            M.unpack(secret_of([("900100", "20251016", "20251015")]))     # t1 이 t 보다 앞
        big = [(f"{i:06d}", f"2026{1 + i % 8:02d}{1 + i % 27:02d}", f"2026{1 + i % 8:02d}{2 + i % 27:02d}")
               for i in range(1785)]
        self.assertLess(len(secret_of(big)), M.SECRET_LIMIT, "1,785건(코드가 모두 다른 최악)도 비밀값 한도 안")

    def test_code_days_are_t_and_t1_deduped_oldest_first(self):
        self.assertEqual(M.code_days(EVENTS, "2025-10"),
                         [("20251015", "900100"), ("20251015", "900200"), ("20251016", "900100"),
                          ("20251016", "900200"), ("20251017", "900100")])
        self.assertEqual(M.months_of(EVENTS), ["2025-10", "2025-11"])


class FetchDay(unittest.TestCase):
    def test_full_day_by_paging_inclusive_and_exclusive(self):
        for inclusive in (True, False):
            kis = FakeKis({("900100", "20251015"): session_times()}, inclusive=inclusive)
            rows, st = M.fetch_day(kis, {}, "900100", "20251015", lambda s: None, 0)
            self.assertEqual((st["status"], st["rows"], st["first"], st["last"]), ("완전", 381, "0900", "1530"))
            self.assertEqual(len({r[2] for r in rows}), 381, "겹친 봉은 한 번만")
            self.assertLessEqual(st["calls"], 5)
            self.assertEqual(rows[0][:3], ["900100", "20251015", "0900"])
            self.assertTrue(all(r[1] == "20251015" for r in rows), "전날 봉은 담지 않는다")

    def test_expired_day_is_empty_and_hour_ignoring_api_stops(self):
        rows, st = M.fetch_day(FakeKis({}), {}, "900100", "20251015", lambda s: None, 0)
        self.assertEqual((st["status"], rows, st["calls"]), ("빈 날", [], 1))
        kis = FakeKis({("900100", "20251015"): session_times()}, ignore_hour=True)
        rows, st = M.fetch_day(kis, {}, "900100", "20251015", lambda s: None, 0)
        self.assertEqual((st["status"], st["calls"]), ("일부", 2), "같은 페이지만 오면 두 번째에서 멈춘다(무한 반복 없음)")

    def test_bad_price_rows_are_rejected(self):
        self.assertEqual(M.parse_rows([dict(bar("20251015", "100000"), stck_hgpr="1")], "20251015")[3], 1)

    def test_rate_limit_retries_then_error_marks_day_failed(self):
        seq = iter([{"rt_cd": "1", "msg_cd": M.RATE_LIMIT}, {"rt_cd": "1", "msg_cd": "EGW00123"}])
        rows, st = M.fetch_day(lambda *a, **k: Resp(next(seq)), {}, "900100", "20251015", lambda s: None, 0)
        self.assertEqual((st["status"], st["rateLimitRetries"], st["error"]), ("실패", 1, "EGW00123"))


class Collect(unittest.TestCase):
    def setUp(self):
        d = tempfile.mkdtemp()
        self.runs, self.priv = os.path.join(d, "runs"), os.path.join(d, "priv")
        days = {(c, day): session_times() for c, t, t1 in EVENTS for day in (t, t1)}
        days[("900200", "20251016")] = session_times()[200:]         # 오전이 빈 날 → 일부
        self.kis = FakeKis(days)
        self.env = {"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s", "GITHUB_RUN_ID": "77", M.EVENTS_ENV: secret_of(EVENTS)}

    def run_main(self, months="all", kis=None, uploader=lambda n, b: "drv", budget=M.TIME_BUDGET_S, clock=None):
        clock = clock or Clock(at("21:00"))
        with contextlib.redirect_stdout(io.StringIO()):
            return M.main_collect(months, self.env, kis or self.kis, post_ok, clock, clock.sleep, uploader,
                                  self.runs, self.priv, budget)

    def runs_rows(self):
        with open(os.path.join(self.runs, M.RUNS_FILE), encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def public_text(self):
        out = ""
        for f in os.listdir(self.runs):
            with open(os.path.join(self.runs, f), encoding="utf-8") as fh:
                out += fh.read()
        return out

    def test_collects_months_privately_and_public_has_counts_only(self):
        self.assertEqual(self.run_main(), 0)
        oct_, nov = self.runs_rows()
        self.assertEqual((oct_["month"], oct_["codeDays"], oct_["full"], oct_["partial"], oct_["status"],
                          oct_["privateStatus"]), ("2025-10", "5", "4", "1", "완료", "접수"))
        self.assertEqual((nov["month"], nov["full"], nov["eventsSha"]), ("2025-11", "2", M.events_sha(EVENTS)))
        for secret in ("900100", "900200", "900300", "98765", "비밀종목"):
            self.assertNotIn(secret, self.public_text(), "공개 장부에 종목코드·가격·종목명이 없어야 한다")
        priv = [f for f in os.listdir(self.priv) if f.endswith(".csv.gz")]
        self.assertEqual(sorted(priv), ["research_minute_2025-10_77.csv.gz", "research_minute_2025-11_77.csv.gz"])
        with gzip.open(os.path.join(self.priv, "research_minute_2025-10_77.csv.gz"), "rt", encoding="utf-8") as fh:
            body = list(csv.reader(fh))
        self.assertEqual(body[0], M.FIELDS)
        self.assertEqual(len(body) - 1, 4 * 381 + 181)

    def test_all_skips_finished_months_but_named_month_reruns(self):
        self.run_main()
        again = FakeKis({})
        self.assertEqual(self.run_main(kis=again), 0)
        self.assertEqual(again.calls, [], "같은 목록으로 끝난 달은 건너뛴다(이어 받기)")
        self.run_main(months="2025-11", kis=again)
        self.assertTrue(again.calls, "달을 적으면 다시 받는다")
        self.assertEqual(self.run_main(months="2024-01"), 1, "목록에 없는 달은 거부")

    def test_failed_days_leave_month_open_for_all(self):
        kis = FakeKis(self.kis.days, errors={("900300", "20251103"): "EGW00500"})
        self.assertEqual(self.run_main(kis=kis), 1)
        self.assertEqual(self.runs_rows()[1]["status"], "일부 실패")
        fresh = FakeKis(self.kis.days)
        self.run_main(kis=fresh)
        self.assertTrue(all(c[1] in ("20251103", "20251104") for c in fresh.calls), "실패가 남은 달만 다시 받는다")

    def test_repeated_errors_stop_the_run(self):
        errs = {(c, d): "EGW00123" for c, t, t1 in EVENTS for d in (t, t1)}
        self.assertEqual(self.run_main(kis=FakeKis(self.kis.days, errors=errs)), 1)
        rows = self.runs_rows()
        self.assertEqual((len(rows), rows[0]["status"], rows[0]["attempted"]), (1, "중단", str(M.STOP_AFTER)))

    def test_time_budget_stops_and_upload_failure_is_reported(self):
        self.assertEqual(self.run_main(budget=0), 1)
        self.assertEqual((self.runs_rows()[0]["status"], self.runs_rows()[0]["attempted"]), ("중단", "0"))

        def bad(n, b):
            raise RuntimeError("drive down")
        os.remove(os.path.join(self.runs, M.RUNS_FILE))
        self.assertEqual(self.run_main(uploader=bad), 1)
        self.assertEqual(self.runs_rows()[0]["privateStatus"], "실패")

    def test_missing_or_broken_secret_makes_no_calls(self):
        kis = FakeKis({})
        for value in ("", "bm90IGd6aXA="):
            self.env[M.EVENTS_ENV] = value
            self.assertEqual(self.run_main(kis=kis), 1)
        self.assertEqual(kis.calls, [])

    def test_big_month_is_split_under_upload_limit(self):
        rows = [[f"9{i:05d}", "20251015", f"{h:04d}", 1, 1, 1, 1, i] for i in range(40) for h in range(900, 1300)]
        files = M.private_files("2025-10", "77", rows, [], {}, limit=20_000)
        data = [b for n, b in files if n.endswith(".csv.gz")]
        self.assertGreater(len(data), 1)
        self.assertTrue(all(len(b) <= 20_000 for b in data))
        self.assertEqual(sum(len(gzip.decompress(b).decode().splitlines()) - 1 for b in data), len(rows))


class Merge(unittest.TestCase):
    def test_keeps_most_complete_version_and_checks_receipts(self):
        d = tempfile.mkdtemp()
        src, out = os.path.join(d, "src"), os.path.join(d, "out")
        os.makedirs(src)
        full = [["900100", "20251015", t[:4], 10, 11, 9, 10, 5] for t in session_times()]
        files = {"research_minute_2025-10_1.csv.gz": M._csv_gz(full[:100]),
                 "research_minute_2025-10_2.csv.gz": M._csv_gz(full),
                 "research_minute_2025-10_3.csv.gz": M._csv_gz(full[:10])}
        for n, b in files.items():
            with open(os.path.join(src, n), "wb") as fh:
                fh.write(b)
        rec = os.path.join(d, "receipts.csv")
        write_csv(rec, ["kind", "name", "sha256"],
                  [["minute", n, hashlib.sha256(b).hexdigest()] for n, b in files.items() if not n.endswith("3.csv.gz")]
                  + [["minute", "research_minute_2025-10_3.csv.gz", "0" * 64]])
        ev = os.path.join(d, "ev.csv")
        write_csv(ev, ["code", "date", "t1"], [["900100", "20251015", "20251016"]])
        with contextlib.redirect_stdout(io.StringIO()):
            rc = M.merge(src, out, rec, ev)
        self.assertEqual(rc, 1, "sha 가 안 맞는 파일이 있으면 알린다")
        with open(os.path.join(out, "minute_status.csv"), encoding="utf-8") as fh:
            st = list(csv.DictReader(fh))
        self.assertEqual((len(st), st[0]["rows"], st[0]["status"], st[0]["source"]),
                         (1, "381", "완전", "research_minute_2025-10_2.csv.gz"))


class Probe(unittest.TestCase):
    def test_prints_structure_without_prices(self):
        kis = FakeKis({("005930", "20251015"): session_times()})
        c, buf = Clock(at("21:00")), io.StringIO()
        with contextlib.redirect_stdout(buf):
            M.probe({"KIS_APP_KEY": "k", "KIS_APP_SECRET": "s"}, kis, post_ok, c, c.sleep)
        out = buf.getvalue()
        self.assertNotIn("98765", out)
        self.assertNotIn("비밀종목", out)
        self.assertIn("20251015: 완전 · 381봉", out)
        self.assertIn("두 응답이 같다", out)


if __name__ == "__main__":
    unittest.main()
