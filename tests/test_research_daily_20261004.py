"""연구 일일 수집·보고 — 상태 없는 300봉 창 수집, 원자료 비공개 보관, 미확정 봉 제외, 보고에 수익률 없음.

2026-10-05 ①: Codex 교차 검토(docs/collaboration/2026-10-05_Codex_테마대장경로_인계검토_회신.md) R1~R6 합성 재현을
안전 동작이 통과하는 회귀 시험으로 옮겼다.
2026-10-05 ②: Codex 후속 권고(docs/collaboration/2026-10-05_Codex_연구수집보완후_진행권고.md)의 추가 재현 두 건
(A 시간 제한 반복 시 뒤 종목 굶음, B 일부 봉 탈락을 완료로 인정)과 **가격 원자료 비공개 보관**을 시험한다.
모두 네트워크 없이 임시 폴더와 합성 값으로 돈다. 드라이브 업로드는 가짜 함수로 바꿔 끼운다.
"""
import contextlib
import csv
import datetime
import gzip
import io
import json
import os
import pathlib
import shutil
import tempfile
import unittest
from unittest import mock

import hyeoks_research_daily as R
from hyeoks_closing_bet import KST

XML = ('<?xml version="1.0" encoding="EUC-KR" ?><protocol><chartdata symbol="{sym}">'
       '<item data="20261001|100|110|90|105|1000" /><item data="20261002|105|120|100|118|2000" />'
       '</chartdata></protocol>')
AFTER = datetime.datetime(2026, 10, 6, 19, 40, tzinfo=KST)


def xml(sym="005930"):
    return XML.format(sym=sym)


def bar(day="2026-10-06", volume=123456789):
    return (day, 100.0, 110.0, 90.0, 105.0, volume)


class Resp:
    def __init__(self, text="", code=200):
        self.text, self.status_code = text, code


class Up:
    """가짜 드라이브 창구 — 받은 파일을 보관한다."""
    def __init__(self, fail=False):
        self.files, self.fail = {}, fail

    def __call__(self, name, data):
        if self.fail:
            raise RuntimeError("synthetic upload failure")
        self.files[name] = data
        self.raw = getattr(self, "raw", {})
        self.raw[name] = data
        return f"id-{len(self.files)}"

    def bars(self):
        out = []
        for n, d in sorted(self.files.items()):
            if n.endswith(".csv.gz"):
                out += list(csv.DictReader(io.StringIO(gzip.decompress(d).decode("utf-8"))))
        return out

    def status(self):
        n = next(n for n in self.files if n.endswith("_status.json.gz"))
        return json.loads(gzip.decompress(self.files[n]))


class TmpDir(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.p = os.path.join(self.d, "private")
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)

    def run_bars(self, day="2026-10-06", fetch_fn=None, codes=("005930", "000660"), up=None, **kw):
        up = up or Up()
        patches = [mock.patch.object(R, "universe", return_value=list(codes)),
                   mock.patch.object(R, "backfill_universe", return_value=list(kw.pop("rest", []))),
                   mock.patch.object(R, "INDEXES", ())]
        if fetch_fn:
            patches.append(mock.patch.object(R, "fetch", side_effect=fetch_fn))
        with contextlib.ExitStack() as stack:
            for p in patches:
                stack.enter_context(p)
            out = R.run_bars(day, kw.pop("get", None), runs_dir=self.d, now=kw.pop("now", AFTER),
                             sleep=lambda _: None, uploader=up, private_dir=self.p, **kw)
        return out, up


class ParseAndFetch(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(R.parse_fchart(xml()), [("2026-10-01", 100, 110, 90, 105, 1000), ("2026-10-02", 105, 120, 100, 118, 2000)])

    def test_parse_rejects_garbage(self):
        self.assertEqual(R.parse_fchart("not xml"), [])
        self.assertEqual(R.parse_fchart('<a><item data="2026|1|2" /></a>'), [])

    def test_parse_counts_reject_reasons_and_zero_volume(self):
        text = ('<a><chartdata symbol="X">'
                '<item data="20261001|0|0|0|0|0" />'            # 0 가격
                '<item data="20261002|100|90|95|98|10" />'      # 고가 < 시가
                '<item data="20261005|100|100|100|100|0" />'    # 무거래 봉 — 보존
                '</chartdata></a>')
        bars, info = R.parse_fchart_detail(text)
        self.assertEqual([b[0] for b in bars], ["2026-10-05"])
        self.assertEqual(info["rejects"], {"0가격": 1, "가격관계": 1})
        self.assertEqual(info["zero_volume"], 1)

    def test_fetch_records_errors_instead_of_raising(self):
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("", 500))[1], "HTTP 500")
        def boom(*a, **k):
            raise ConnectionError("down")
        self.assertIn("ConnectionError", R.fetch("x", 5, boom)[1])
        self.assertEqual(R.fetch("x", 5, lambda *a, **k: Resp("<a/>"))[1], "빈 응답")

    def test_fetch_rejects_a_reply_for_another_symbol(self):
        bars, err, _ = R.fetch("000660", 5, lambda *a, **k: Resp(xml("005930")))
        self.assertEqual(bars, [])
        self.assertIn("종목 불일치", err)

    def test_numbers_round_trip_exactly(self):
        self.assertEqual(R.num(123456789.0), "123456789")
        self.assertEqual(float(R.num(0.1 + 0.2)), 0.1 + 0.2)


class PrivateStorage(TmpDir):
    def test_prices_go_to_the_private_uploader_not_the_public_summary(self):
        out, up = self.run_bars(fetch_fn=lambda c, n, g: ([bar()], ""))
        self.assertEqual(out["privateStatus"], "보관 완료")
        self.assertEqual(len(up.bars()), 2)
        self.assertEqual(up.bars()[0]["volume"], "123456789", "원천 숫자 그대로")
        R.save_run(out, self.d)
        public = "".join(pathlib.Path(self.d, f).read_text(encoding="utf-8") for f in ("last_run.json", "runs.csv"))
        self.assertNotIn("123456789", public, "공개 요약에 가격·거래량을 넣지 않는다")
        self.assertNotIn("005930", public, "공개 요약에 종목별 상태를 넣지 않는다")
        self.assertIn("research_bars_2026-10-06_", public, "비공개 파일 이름은 남긴다")

    def test_upload_failure_is_FAILED_and_the_runner_copy_stays(self):
        out, _ = self.run_bars(fetch_fn=lambda c, n, g: ([bar()], ""), up=Up(fail=True))
        self.assertEqual((out["privateStatus"], out["status"]), ("보관 실패", "FAILED"))
        self.assertTrue(os.listdir(self.p))

    def test_large_bar_files_are_split_by_code(self):
        rows = [["2026-10-06", f"{i:06d}", "1", "1", "1", "1", str(i * 7919), "t"] for i in range(2000)]
        with mock.patch.object(R, "PART_LIMIT_BYTES", 4000):
            files = R.private_files("2026-10-06", "9", rows, {})
        parts = [n for n, _ in files if n.endswith(".csv.gz")]
        self.assertGreater(len(parts), 1)
        self.assertTrue(all("part" in n for n in parts))
        back = sum(len(gzip.decompress(b).decode().splitlines()) - 1 for n, b in files if n.endswith(".csv.gz"))
        self.assertEqual(back, 2000)

    def test_every_run_requests_the_full_window(self):
        seen = []
        self.run_bars(fetch_fn=lambda c, n, g: (seen.append(n), ([bar()], ""))[1])
        self.assertEqual(set(seen), {R.FETCH_N})


class CodexFollowUpAandB(TmpDir):
    def test_A_repeated_time_limits_still_reach_later_codes(self):
        """Codex 후속 A — 첫 종목까지만 처리할 예산을 두 번 줘도 두 번째 실행은 남은 종목을 실제로 받는다."""
        calls = []
        t = iter(range(0, 10_000, 10))
        def fetch(code, n, get):
            calls.append(code)
            return [bar()], ""
        with mock.patch.object(R.time, "monotonic", side_effect=lambda: next(t)):
            # 우선 구간 없음, 나머지 2종목. 예산 15초 = 첫 종목만.
            self.run_bars(fetch_fn=fetch, codes=(), rest=["000001", "000002"], budget_s=15)
            self.run_bars(fetch_fn=fetch, codes=(), rest=["000001", "000002"], budget_s=15)
        self.assertEqual(calls, ["000001", "000002"])

    def test_A_priority_codes_are_never_starved(self):
        calls = []
        t = iter(range(0, 10_000, 10))
        with mock.patch.object(R.time, "monotonic", side_effect=lambda: next(t)):
            out, _ = self.run_bars(fetch_fn=lambda c, n, g: (calls.append(c), ([bar()], ""))[1],
                                   codes=("P1",), rest=["000001", "000002"], budget_s=15)
        self.assertEqual(calls[0], "P1")
        self.assertEqual(out["notAttempted"], 2)

    def test_B_a_rejected_old_bar_is_not_counted_as_complete(self):
        """Codex 후속 B — 대상일 봉은 있지만 다른 봉 하나가 검사에서 탈락하면 '완전' 이 아니다."""
        def fetch(code, n, get):
            return [bar("2026-10-05"), bar()], "", {"rejects": {"가격관계": 1}, "zero_volume": 0}
        out, up = self.run_bars(fetch_fn=fetch, codes=("005930",))
        self.assertEqual(out["completeness"], {"검사탈락": 1})
        self.assertEqual(up.status()["codes"]["005930"]["class"], "검사탈락")

    def test_B_short_reply_is_recorded_not_certified(self):
        out, up = self.run_bars(fetch_fn=lambda c, n, g: ([bar()], ""), codes=("005930",))
        self.assertEqual(out["completeness"], {"짧은응답": 1})
        self.assertEqual(up.status()["codes"]["005930"]["bars"], 1)

    def test_B_full_window_is_complete(self):
        full = [(f"2025-{(i // 28) % 12 + 1:02d}-{i % 28 + 1:02d}", 1.0, 1.0, 1.0, 1.0, 1.0) for i in range(R.FETCH_N - 1)]
        out, _ = self.run_bars(fetch_fn=lambda c, n, g: (full + [bar()], ""), codes=("005930",))
        self.assertEqual(out["completeness"], {"완전": 1})


class CodexReviewR1toR6(TmpDir):
    """Codex 1차 검토 재현 — 상태 없는 수집에서도 안전 동작이 유지되는지."""

    def test_R1_failed_code_is_requested_again_next_run(self):
        calls, fail_first = [], {"000660"}
        def fetch(code, n, get):
            calls.append((code, n))
            if code in fail_first:
                fail_first.remove(code)
                return [], "synthetic timeout"
            return [bar()], ""
        out1, _ = self.run_bars(fetch_fn=fetch)
        calls.clear()
        out2, _ = self.run_bars(fetch_fn=fetch)
        self.assertEqual(out1["failed"], 1)
        self.assertIn(("000660", R.FETCH_N), calls)
        self.assertEqual(out2["failed"], 0)

    def test_R1_past_candidates_keep_being_tracked(self):
        days = R.snapshot_days()
        first, rest = R.plan_order(days[-1])
        everything = set(first) | set(rest)
        self.assertTrue(set(R.backfill_universe(upto=days[-1])) <= everything)
        self.assertEqual(len(first) + len(rest), len(everything), "중복 없이")

    def test_R2_midnight_run_keeps_finished_previous_session(self):
        now = datetime.datetime(2026, 10, 7, 1, 0, tzinfo=KST)
        self.assertEqual(R.target_day(now), "2026-10-06")
        out, up = self.run_bars(day=R.target_day(now), now=now, fetch_fn=lambda c, n, g: ([bar()], ""), codes=("005930",))
        self.assertEqual((out["covered"], out["unsettledSkipped"]), (1, 0))

    def test_R2_late_schedule_after_8am_still_targets_the_settled_session(self):
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 7, 9, 30, tzinfo=KST)), "2026-10-06")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 6, 15, 59, tzinfo=KST)), "2026-10-02")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 6, 16, 0, tzinfo=KST)), "2026-10-06")

    def test_R2_historical_replay_does_not_store_later_or_intraday_bars(self):
        now = datetime.datetime(2026, 10, 6, 14, 0, tzinfo=KST)
        out, up = self.run_bars(day="2026-10-02", now=now, codes=("005930",),
                                fetch_fn=lambda c, n, g: ([bar("2026-10-02"), bar("2026-10-06")], ""))
        self.assertEqual({r["date"] for r in up.bars()}, {"2026-10-02"})
        self.assertEqual(out["covered"], 1)

    def test_R2_intraday_today_is_unsettled_and_not_covered(self):
        now = datetime.datetime(2026, 10, 6, 14, 0, tzinfo=KST)
        out, up = self.run_bars(day="2026-10-06", now=now, codes=("005930",),
                                fetch_fn=lambda c, n, g: ([bar("2026-10-02"), bar("2026-10-06")], ""))
        self.assertEqual((out["unsettledSkipped"], out["covered"]), (1, 0))

    def test_R3_report_reads_this_runs_collector_state_only(self):
        st = {"runId": "77", "targetDay": "2026-10-06", "status": "DEGRADED", "plannedCodes": 2, "requested": 2,
              "responded": 1, "covered": 1, "missingTarget": 0, "failed": 1, "notAttempted": 0, "rejected": {},
              "rowsStored": 5, "unsettledSkipped": 0, "startedAt": "s", "finishedAt": "f", "completeness": {"완전": 1},
              "privateStatus": "보관 완료", "privateFiles": [{"name": "research_bars_x.csv.gz", "bytes": 1, "id": "i"}]}
        R.save_run(st, self.d)
        patches = (mock.patch.object(R, "RUNS_DIR", self.d),
                   mock.patch.object(R, "trading_day", return_value=(True, "")),
                   mock.patch.object(R, "structure_today", return_value=None),
                   mock.patch.object(R, "locked_status", return_value=None))
        out = io.StringIO()
        with patches[0], patches[1], patches[2], patches[3], contextlib.redirect_stdout(out):
            R.main(["--report", "--date", "2026-10-06"], env={"GITHUB_RUN_ID": "77", "RESEARCH_ARCHIVE": "원격 반영 abc1234"})
        self.assertIn("요청 2", out.getvalue())
        self.assertIn("DEGRADED", out.getvalue())
        self.assertIn("원격 반영 abc1234", out.getvalue())
        self.assertIn("비공개 보관(드라이브): 보관 완료", out.getvalue())
        out = io.StringIO()
        with patches[0], patches[1], patches[2], patches[3], contextlib.redirect_stdout(out):
            R.main(["--report", "--date", "2026-10-06"], env={"GITHUB_RUN_ID": "78"})
        self.assertIn("이번 실행의 수집 기록 없음", out.getvalue(), "다른 실행의 상태를 오늘 성공으로 쓰지 않는다")

    def test_R3_send_failure_is_a_nonzero_exit(self):
        with mock.patch.object(R, "trading_day", return_value=(True, "")), \
                mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), \
                mock.patch.object(R, "RUNS_DIR", self.d), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(R.main(["--report", "--send", "--date", "2026-10-06"], env={}), 1)

    def test_R4_successful_prefix_survives_collection_interrupt(self):
        calls = []
        def fetch(code, n, get):
            calls.append(code)
            if len(calls) == 2:
                raise KeyboardInterrupt("synthetic runner interruption")
            return [bar()], ""
        up = Up()
        with self.assertRaises(KeyboardInterrupt):
            self.run_bars(fetch_fn=fetch, up=up)
        self.assertEqual({r["code"] for r in up.bars()}, {calls[0]}, "중단 전에 받은 것은 비공개로 올라가 있어야 한다")

    def test_R5_parser_rejects_invalid_date_and_nonfinite_ohlcv(self):
        self.assertEqual(R.parse_fchart('<root><item data="20261340|nan|inf|-1|105|100" /></root>'), [])
        self.assertEqual(R.parse_fchart('<root><item data="20261006|nan|110|90|105|100" /></root>'), [])

    def test_R5_stale_reply_is_not_counted_as_target_coverage(self):
        out, _ = self.run_bars(fetch_fn=lambda c, n, g: ([bar("2026-10-01")], ""), codes=("005930",))
        self.assertEqual((out["responded"], out["covered"], out["missingTarget"]), (1, 0, 1))
        self.assertEqual(out["completeness"], {"대상일없음": 1})
        self.assertNotEqual(out["status"], "OK")

    def test_R6_locked_status_respects_registered_cap(self):
        fake = {"date": "2027-12-30", "hold": {"H": 1}, "returns_hold": {"H": {"평균": 0.0}, "B100": {"평균": 0.0}}}
        with mock.patch.object(R.T, "collect", return_value=([fake], [])):
            got = R.locked_status()
        self.assertEqual(got["valid"], 0, "120거래일 상한(또는 달력 끝) 뒤의 날은 세지 않는다")


class CodexR7toR11(TmpDir):
    """Codex 2026-10-05 진행 계획 §3 재현 다섯 건 — 기대 동작으로 통과해야 한다."""

    def test_R7_priority_segment_rotates_under_repeated_budget_stops(self):
        calls = []
        def fetch(code, n, get):
            calls.append(code)
            return [bar()], ""
        for i in range(2):
            t = iter([0, 0, 2, 2, 2])
            with mock.patch.object(R.time, "monotonic", side_effect=lambda: next(t)):
                out, _ = self.run_bars(fetch_fn=fetch, codes=("000001", "000002"), rest=["000003"], budget_s=1,
                                       run_id=str(i))
        self.assertEqual(calls, ["000001", "000002"], "우선 구간 뒤쪽도 다음 실행에 받는다")
        self.assertEqual(R._read_json(os.path.join(self.d, "resume.json"), {})["priorityOffset"], 0)

    def test_R7_fetch_timing_is_recorded(self):
        out, _ = self.run_bars(fetch_fn=lambda c, n, g: ([bar()], ""))
        self.assertTrue({"avgFetchMs", "p95FetchMs", "maxFetchMs"} <= set(out))

    def test_R8_other_date_screen_is_not_todays_1500(self):
        path = os.path.join(self.d, "stockinfo7_obs.csv")
        import hyeoks_theme_source_collect as C
        C.append_obs([{"day": "2026-10-06", "runId": "r", "requestedAt": "2026-10-06T15:02:00+09:00",
                       "receivedAt": "2026-10-06T15:02:01+09:00", "parsedAt": "2026-10-06T15:02:01+09:00",
                       "http": 200, "class": "날짜불일치", "header": "2026-10-02 15시", "screenDay": "2026-10-02",
                       "screenHour": 15, "cards": 1, "rows": 1, "digest": "a", "privateFile": ""}], self.d)
        s = R.theme_obs_summary("2026-10-06", self.d)
        self.assertIn("결측", s)
        self.assertIn("날짜불일치 1", s)
        self.assertTrue(os.path.exists(path))

    def test_R8_14h_screen_is_not_substituted_and_late_15h_is_not_backdated(self):
        import hyeoks_theme_source_collect as C
        def row(t, hour, cls):
            return {"day": "2026-10-06", "runId": "r", "requestedAt": t, "receivedAt": t, "parsedAt": t, "http": 200,
                    "class": cls, "header": f"2026-10-06 {hour}시", "screenDay": "2026-10-06", "screenHour": hour,
                    "cards": 1, "rows": 1, "digest": str(hour), "privateFile": "f"}
        C.append_obs([row("2026-10-06T14:55:00+09:00", 14, "첫관측"), row("2026-10-06T15:06:00+09:00", 15, "갱신")], self.d)
        s = R.theme_obs_summary("2026-10-06", self.d)
        self.assertIn("15:05 판단 화면 결측 — 그 시각 최신은 14시본(대체하지 않음)", s)
        self.assertIn("15시본 첫 확보 15:06:00", s)

    def test_R9_bar_receipts_have_sha256_and_drive_id(self):
        out, up = self.run_bars(fetch_fn=lambda c, n, g: ([bar()], ""))
        rows = list(csv.DictReader(open(os.path.join(self.d, "private_receipts.csv"), encoding="utf-8")))
        self.assertEqual({r["kind"] for r in rows}, {"bars"})
        import hashlib
        for r in rows:
            self.assertEqual(r["sha256"], hashlib.sha256(up.raw[r["name"]]).hexdigest())
            self.assertTrue(r["driveId"] and r["status"] == "접수" and r["verified"] == "")

    def test_R11_report_only_retry_after_collection_succeeded(self):
        R.save_run(dict(Report.BARS, runId="primary", targetDay="2026-10-06"), self.d)
        R.log_report("2026-10-06", "primary", "primary", "발송실패", "HTTP 502", self.d)
        calls = []
        with mock.patch.object(R, "RUNS_DIR", self.d), mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), \
                mock.patch.object(R, "run_bars", side_effect=AssertionError("수집을 반복하면 안 된다")), \
                mock.patch.object(R, "send", side_effect=lambda *a: (calls.append(1), (True, ""))[1]), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(R.main(["--bars"], now=AFTER, env={"GITHUB_RUN_ID": "backup"}), 0)
            self.assertEqual(R.main(["--report", "--send"], now=AFTER, env={"GITHUB_RUN_ID": "backup"}), 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(R.report_state("2026-10-06", self.d), "발송성공")

    def test_R11_ambiguous_send_is_not_auto_resent(self):
        R.save_run(dict(Report.BARS, runId="primary", targetDay="2026-10-06"), self.d)
        R.log_report("2026-10-06", "primary", "primary", "응답불명", "ReadTimeout", self.d)
        with mock.patch.object(R, "RUNS_DIR", self.d), \
                mock.patch.object(R, "send", side_effect=AssertionError("응답불명은 자동 재발송 안 함")), \
                contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(R.main(["--report", "--send"], now=AFTER, env={"GITHUB_RUN_ID": "backup"}), 0)
        self.assertIn("응답불명", out.getvalue())

    def test_R11_send_outcomes_are_classified(self):
        with mock.patch.object(R, "RUNS_DIR", self.d), mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), contextlib.redirect_stdout(io.StringIO()):
            R.save_run(dict(Report.BARS, runId="a", targetDay="2026-10-06"), self.d)
            with mock.patch.object(R, "send", return_value=(False, "ReadTimeout")):
                R.main(["--report", "--send", "--date", "2026-10-06"], env={"GITHUB_RUN_ID": "a"})
        self.assertEqual(R.report_state("2026-10-06", self.d), "응답불명")

    def test_R11_connection_failure_is_retryable_but_timeout_is_ambiguous(self):
        self.assertEqual(R.send_status(False, "HTTP 502"), "발송실패")
        self.assertEqual(R.send_status(False, "ConnectionError"), "발송실패")
        self.assertEqual(R.send_status(False, "TELEGRAM_BOT_TOKEN 없음 — 보내지 않았다"), "발송실패")
        self.assertEqual(R.send_status(False, "ReadTimeout"), "응답불명")
        self.assertEqual(R.send_status(True, ""), "발송성공")

    def test_R10_workflows_read_latest_state_before_running(self):
        root = pathlib.Path(__file__).resolve().parent.parent
        for wf in ("research_daily.yml", "theme_source_collect.yml"):
            txt = (root / ".github/workflows" / wf).read_text(encoding="utf-8")
            with self.subTest(wf):
                self.assertIn("git pull --ff-only origin main", txt)
                self.assertIn("RESEARCH_STATE_COMMIT", txt)
        txt = (root / ".github/workflows/research_daily.yml").read_text(encoding="utf-8")
        self.assertIn("보고 상태 커밋", txt)


class Report(TmpDir):
    BARS = {"status": "OK", "plannedCodes": 400, "requested": 400, "responded": 398, "covered": 397,
            "missingTarget": 1, "failed": 2, "notAttempted": 0, "rejected": {}, "zeroVolume": 0, "rowsStored": 119400,
            "unsettledSkipped": 0, "completeness": {"완전": 390, "짧은응답": 7}, "privateStatus": "보관 완료",
            "privateFiles": [{"name": "a"}], "startedAt": "2026-10-02T19:41:00+09:00",
            "finishedAt": "2026-10-02T19:48:00+09:00"}

    def test_report_has_tag_counts_and_no_return_numbers(self):
        struct = {"day": "2026-10-02", "prev": "2026-10-01", "leaders": 45, "prev_leaders": 44,
                  "kept_leader": 7, "swapped": 30, "themes_kept": 35}
        locked = {"observed": 3, "valid": 2, "bad": {}, "cap": None, "capWhy": "x"}
        txt = R.report_text("2026-10-02", self.BARS, struct, locked, archive="원격 반영 abc",
                            theme_obs="관측 24회 · 갱신 4 · 오류 0 · 15시본 15:03:10 (15:05 전 수신)")
        self.assertTrue(txt.startswith("[연구]"))
        self.assertIn("유효 비교일 2/60", txt)
        self.assertIn("수익률 비공개", txt)
        self.assertIn("공개 요약 커밋: 원격 반영 abc", txt)
        self.assertIn("stockinfo7: 관측 24회", txt)
        self.assertNotIn("%", txt, "보고에 수익률·비율 숫자를 넣지 않는다")

    def test_report_text_does_not_change_when_returns_change(self):
        """방화벽 — 확증 구간의 합성 수익률을 바꿔도 보고 본문이 같아야 한다."""
        def days_with(ret):
            return [{"date": "2026-10-06", "hold": {"H": 2},
                     "returns_hold": {"H": {"평균": ret}, "B100": {"평균": -ret}}}]
        texts = []
        for ret in (0.031, -0.047):
            with mock.patch.object(R.T, "collect", return_value=(days_with(ret), [])):
                texts.append(R.report_text("2026-10-06", self.BARS, None, R.locked_status()))
        self.assertEqual(texts[0], texts[1])

    def test_theme_obs_summary_from_public_meta(self):
        path = os.path.join(self.d, "stockinfo7_obs.csv")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("day,runId,requestedAt,receivedAt,http,class,header,cards,rows,digest,privateFile\n"
                     "2026-10-06,1,2026-10-06T14:55:00+09:00,2026-10-06T14:55:01+09:00,200,첫관측,2026-10-06 14시,20,100,a,f\n"
                     "2026-10-06,1,2026-10-06T15:03:00+09:00,2026-10-06T15:03:02+09:00,200,갱신,2026-10-06 15시,20,100,b,f\n"
                     "2026-10-06,1,2026-10-06T15:13:00+09:00,,,접속실패,Timeout,,,,\n")
        s = R.theme_obs_summary("2026-10-06", self.d)
        self.assertIn("관측 3회", s)
        self.assertIn("갱신 1", s)
        self.assertIn("오류 1", s)
        self.assertIn("15:05 판단 화면 15시본 (15:03:02 확보)", s)
        self.assertEqual(R.theme_obs_summary("2026-10-07", self.d), "")
        self.assertIsNone(R.theme_obs_summary("2026-10-06", os.path.join(self.d, "none")))

    def test_send_needs_a_token_and_never_leaks_it(self):
        self.assertEqual(R.send("x", None, ""), (False, "TELEGRAM_BOT_TOKEN 없음 — 보내지 않았다"))
        def boom(*a, **k):
            raise RuntimeError("https://api.telegram.org/botSECRET/sendMessage")
        ok, err = R.send("x", boom, "SECRET")
        self.assertFalse(ok)
        self.assertNotIn("SECRET", err)

    def test_send_goes_to_the_fixed_channel(self):
        seen = {}
        def post(url, data, timeout):
            seen.update(data)
            return Resp("", 200)
        with mock.patch.dict(os.environ, {"TELEGRAM_CHAT_ID_OVERRIDE": ""}):
            self.assertEqual(R.send("[연구] x", post, "T"), (True, ""))
        import telegram_target
        self.assertEqual(seen["chat_id"], telegram_target.CHAT_ID)

    def test_late_cron_after_midnight_targets_the_previous_day(self):
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 3, 1, 0, tzinfo=KST)), "2026-10-02")
        self.assertEqual(R.target_day(datetime.datetime(2026, 10, 2, 19, 40, tzinfo=KST)), "2026-10-02")

    def test_non_trading_days_are_skipped(self):
        self.assertFalse(R.trading_day("2026-10-05")[0])    # 개천절 대체공휴일
        self.assertTrue(R.trading_day("2026-10-06")[0])
        ok, why = R.trading_day("2027-01-04")
        self.assertFalse(ok)
        self.assertIn("범위 밖", why)

    def test_scheduled_run_on_a_weekday_holiday_does_not_re_report(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = R.main(["--report"], now=datetime.datetime(2026, 10, 5, 19, 40, tzinfo=KST), env={})
        self.assertEqual(code, 0)
        self.assertIn("예정 거래일이 아니다", out.getvalue())

    def test_second_scheduled_run_for_the_same_day_is_skipped(self):
        """GAS(주) + 깃허브 예약(백업)이 둘 다 와도 같은 대상일을 두 번 수집·보고하지 않는다."""
        R.save_run({"runId": "1", "targetDay": "2026-10-06", "status": "OK", "plannedCodes": 1}, self.d)
        R.log_report("2026-10-06", "1", "1", "발송성공", "", self.d)
        out = io.StringIO()
        with mock.patch.object(R, "RUNS_DIR", self.d), contextlib.redirect_stdout(out):
            code = R.main(["--bars", "--report", "--send"], now=AFTER, env={"GITHUB_RUN_ID": "2"})
        self.assertEqual(code, 0)
        self.assertIn("중복 실행 생략", out.getvalue())

    def test_failed_first_run_is_retried_by_the_backup(self):
        R.save_run({"runId": "1", "targetDay": "2026-10-06", "status": "FAILED", "plannedCodes": 1}, self.d)
        with mock.patch.object(R, "RUNS_DIR", self.d), mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), contextlib.redirect_stdout(io.StringIO()) as out:
            R.main(["--report"], now=AFTER, env={"GITHUB_RUN_ID": "2"})
        self.assertNotIn("중복 실행 생략", out.getvalue())

    def test_same_run_report_step_is_not_skipped(self):
        R.save_run(dict(self.BARS, runId="7", targetDay="2026-10-06"), self.d)
        with mock.patch.object(R, "RUNS_DIR", self.d), mock.patch.object(R, "structure_today", return_value=None), \
                mock.patch.object(R, "locked_status", return_value=None), contextlib.redirect_stdout(io.StringIO()) as out:
            R.main(["--report"], now=AFTER, env={"GITHUB_RUN_ID": "7"})
        self.assertNotIn("중복 실행 생략", out.getvalue())

    def test_status_rules(self):
        base = dict(self.BARS, failed=0, notAttempted=0, covered=400)
        self.assertEqual(R.status_of(base), "OK")
        self.assertEqual(R.status_of(dict(base, failed=1)), "DEGRADED")
        self.assertEqual(R.status_of(dict(base, covered=300)), "DEGRADED")
        self.assertEqual(R.status_of(dict(base, responded=0)), "FAILED")
        self.assertEqual(R.status_of(dict(base, privateStatus="보관 실패")), "FAILED")

    def test_raw_price_paths_are_not_committed(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        wf = pathlib.Path(root, ".github/workflows/research_daily.yml").read_text(encoding="utf-8")
        self.assertNotIn("git add data/daily_bars", wf)
        self.assertIn("git add data/research_runs", wf)
        gi = pathlib.Path(root, ".gitignore").read_text(encoding="utf-8")
        self.assertIn("data/daily_bars/", gi)
        self.assertIn("data/research_private/", gi)


if __name__ == "__main__":
    unittest.main()
