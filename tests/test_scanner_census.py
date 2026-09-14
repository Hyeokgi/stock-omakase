# -*- coding: utf-8 -*-
"""스캐너 타점 인구조사 보존 — 켜기 전 기준선이 정직하게 남는가.

이 검사가 지키려는 것은 숫자 하나가 아니라 **전/후 비교의 성립 조건**이다.
밴드 설정이 행에 안 남으면 나중에 "이 줄은 켜기 전인가 후인가"를 기억에
의존해 판단하게 된다. 그건 비교가 아니다.
"""
import csv
import datetime
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import scanner_census as sc                                   # noqa: E402
import hyeoks_closing_bet as cb                               # noqa: E402

KST = sc.KST


def at(h, m, day="2026-09-14"):
    d = datetime.date.fromisoformat(day)
    return datetime.datetime(d.year, d.month, d.day, h, m, tzinfo=KST)


class BandSettings(unittest.TestCase):
    def test_off_records_current_fixed_threshold(self):
        self.assertEqual(sc.band_settings(False, {}), ("off", 20.0))
        self.assertEqual(sc.band_settings(True, {}), ("off", 20.0))

    def test_on_uses_regime_dependent_values(self):
        env = {"ENVELOPE_BAND": "on"}
        self.assertEqual(sc.band_settings(False, env), ("on", 12.0))
        self.assertEqual(sc.band_settings(True, env), ("on", 20.0))

    def test_on_respects_explicit_overrides(self):
        env = {"ENVELOPE_BAND": "1", "ENVELOPE_PCT_NORMAL": "15",
               "ENVELOPE_PCT_WARNING": "25"}
        self.assertEqual(sc.band_settings(False, env), ("on", 15.0))
        self.assertEqual(sc.band_settings(True, env), ("on", 25.0))

    def test_garbage_override_falls_back_not_crashes(self):
        env = {"ENVELOPE_BAND": "on", "ENVELOPE_PCT_NORMAL": "twelve"}
        self.assertEqual(sc.band_settings(False, env), ("on", 12.0))


class Window(unittest.TestCase):
    def test_window_edges(self):
        self.assertFalse(sc.in_window(at(14, 39)))
        self.assertTrue(sc.in_window(at(14, 40)))      # 열림 포함
        self.assertTrue(sc.in_window(at(15, 9)))
        self.assertFalse(sc.in_window(at(15, 10)))     # 닫힘 제외

    def test_trading_day_uses_the_shared_calendar_file(self):
        nt = {"2026-09-25"}
        self.assertTrue(sc.is_trading_day("2026-09-14", nt))   # 월
        self.assertFalse(sc.is_trading_day("2026-09-19", nt))  # 토
        self.assertFalse(sc.is_trading_day("2026-09-20", nt))  # 일
        self.assertFalse(sc.is_trading_day("2026-09-25", nt))  # 등록 휴장일

    def test_same_calendar_file_parses_identically_in_both_readers(self):
        """스캐너와 종베 분석기가 같은 파일을 같은 규칙으로 읽어야 한다.

        파서를 복제했으므로(의존 방향 때문에) 어긋남을 여기서 잡는다.
        """
        self.assertEqual(sc.load_nontrading(), cb.load_nontrading())

    def test_missing_calendar_file_still_filters_weekends(self):
        self.assertEqual(sc.load_nontrading("nope/none.txt"), set())
        self.assertFalse(sc.is_trading_day("2026-09-19", set()))


class Recording(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="census_")
        self.path = os.path.join(self.dir, "sub", "census.csv")
        self.nt = set()

    def rec(self, now, **kw):
        env = kw.pop("env", {})
        args = dict(scanned=710, price_pass=0, price_tv_pass=0, envelope_pass=0,
                    oversold=0, knife_wait=0,
                    warning_market=False, kospi_rate=-3.37)
        args.update(kw)
        return sc.record(now=now, path=self.path, nontrading=self.nt,
                         env=env, **args)

    def rows(self):
        with open(self.path, encoding="utf-8", newline="") as fp:
            return list(csv.DictReader(fp))

    def test_records_once_and_only_once_per_day(self):
        ok, _ = self.rec(at(14, 51))
        self.assertTrue(ok)
        ok2, why = self.rec(at(15, 1))
        self.assertFalse(ok2)
        self.assertIn("이미 기록", why)
        self.assertEqual(len(self.rows()), 1)

    def test_outside_window_is_skipped_with_a_reason(self):
        ok, why = self.rec(at(11, 0))
        self.assertFalse(ok)
        self.assertIn("기록 창", why)
        self.assertFalse(os.path.exists(self.path))

    def test_non_trading_day_is_skipped(self):
        self.nt = {"2026-09-14"}
        ok, why = self.rec(at(14, 51))
        self.assertFalse(ok)
        self.assertEqual(why, "휴장일")

    def test_row_carries_todays_measured_numbers(self):
        self.rec(at(14, 51), scanned=710, envelope_pass=0, oversold=0, knife_wait=0)
        r = self.rows()[0]
        self.assertEqual(r["date"], "2026-09-14")
        self.assertEqual((r["scanned"], r["envelope_pass"]), ("710", "0"))
        self.assertEqual(r["kospi_rate"], "-3.37")
        self.assertTrue(r["captured_at"].startswith("2026-09-14T14:51"))

    def test_band_state_is_stored_in_the_row_not_remembered(self):
        """켜기 전/후 경계가 데이터 안에 있어야 한다. 이게 이 파일의 존재 이유다."""
        self.rec(at(14, 51))
        self.nt = set()
        self.rec(at(14, 51, "2026-09-15"), env={"ENVELOPE_BAND": "on"})
        before, after = self.rows()
        self.assertEqual((before["band_mode"], before["band_pct"]), ("off", "20.0"))
        self.assertEqual((after["band_mode"], after["band_pct"]), ("on", "12.0"))

    def test_append_only_never_rewrites_earlier_rows(self):
        self.rec(at(14, 51))
        first = open(self.path, encoding="utf-8").read()
        self.rec(at(14, 45, "2026-09-15"))
        self.assertTrue(open(self.path, encoding="utf-8").read().startswith(first))
        self.assertEqual(len(self.rows()), 2)

    def test_header_written_once(self):
        self.rec(at(14, 51))
        self.rec(at(14, 45, "2026-09-15"))
        body = open(self.path, encoding="utf-8").read()
        self.assertEqual(body.count("envelope_pass"), 1)

    def test_warning_market_changes_the_recorded_turnover_floor(self):
        self.rec(at(14, 51), warning_market=True)
        r = self.rows()[0]
        self.assertEqual(r["min_turnover"], "10000000000")
        self.assertEqual(r["warning_market"], "Y")

    def test_failure_never_escapes_to_the_scanner(self):
        """관측을 지키려다 수집을 잃지 않는다. 대신 접두사로 눈에 띄게 만든다."""
        ok, why = sc.record(scanned=1, price_pass=0, price_tv_pass=0, envelope_pass=0,
                            oversold=0, knife_wait=0,
                            warning_market=False, now=at(14, 51),
                            path=os.devnull + "/x/y.csv", nontrading=set())
        self.assertFalse(ok)
        self.assertIn("기록 실패", why)


class Provenance(unittest.TestCase):
    """옮겨 적은 줄과 직접 관측한 줄을 섞지 않는다."""

    def test_live_rows_are_always_marked_scanner(self):
        row = sc.build_row(at(14, 51), 710, 0, 0, 0, 0, 0, False, None, {})
        self.assertEqual(row["source"], sc.SOURCE_LIVE)

    def test_seeded_row_leaves_unsplittable_stages_blank(self):
        """옛 로그는 복합 조건 통과 수 하나뿐이다. 쪼갤 수 없으면 비워 둔다."""
        path = Path(__file__).resolve().parents[1] / sc.CENSUS_PATH
        r = list(csv.DictReader(path.open(encoding="utf-8")))[0]
        self.assertEqual(r["envelope_pass"], "0")
        self.assertEqual((r["price_pass"], r["price_tv_pass"]), ("", ""))

    def test_seeded_row_declares_the_log_it_came_from(self):
        path = Path(__file__).resolve().parents[1] / sc.CENSUS_PATH
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
        self.assertTrue(rows, "기준선 파일이 비어 있다")
        seeded = [r for r in rows if r["source"].startswith("log:")]
        self.assertEqual(len(seeded), 1)
        r = seeded[0]
        self.assertEqual((r["date"], r["scanned"], r["envelope_pass"]),
                         ("2026-09-14", "710", "0"))
        self.assertEqual(r["band_mode"], "off")
        # 로그에 없던 값을 지어내지 않았는지
        self.assertEqual((r["warning_market"], r["kospi_rate"]), ("", ""))
        self.assertEqual(r["run_id"], "34811150549")

    def test_baseline_file_has_no_row_written_while_the_band_was_on(self):
        """기준선 구간에 켜진 줄이 섞이면 '켜기 전'이 아니게 된다."""
        path = Path(__file__).resolve().parents[1] / sc.CENSUS_PATH
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
        self.assertTrue(all(r["band_mode"] in ("off", "on") for r in rows))


class CompoundGate(unittest.TestCase):
    """관문①은 곱이다. 통과 0을 '이격 이탈 0' 으로 읽지 않는다."""

    def test_stages_are_recorded_separately(self):
        row = sc.build_row(at(14, 51), scanned=710, price_pass=9,
                           price_tv_pass=2, envelope_pass=0, oversold=0,
                           knife_wait=0, warning_market=False, kospi_rate=None, env={})
        self.assertEqual((row["price_pass"], row["price_tv_pass"],
                          row["envelope_pass"]), ("9", "2", "0"))

    def test_scanner_no_longer_claims_price_stage_from_the_compound_count(self):
        src = (Path(__file__).resolve().parents[1] / "omakase.py").read_text(encoding="utf-8")
        self.assertNotIn("-20% 이탈 종목 자체가 0 → ENVELOPE_BAND", src)
        self.assertIn("이격 조건 자체를 통과한 종목이 0", src)
        self.assertIn("거래대금 문턱에서 전멸", src)

    def test_scanner_carries_sub_conditions_into_the_census(self):
        src = (Path(__file__).resolve().parents[1] / "omakase.py").read_text(encoding="utf-8")
        for flag in ("_env_price_ok", "_env_tv_ok", "_env_notup_ok", "_env_rate_ok"):
            self.assertIn(flag, src)


class ExecutionBinding(unittest.TestCase):
    """날짜 상수 하나로는 '이 표본이 어느 정책에서 나왔나' 를 인증하지 못한다."""

    def test_row_binds_policy_code_and_run(self):
        row = sc.build_row(at(14, 51), 710, 0, 0, 0, 0, 0, False, None,
                           {"GITHUB_RUN_ID": "12345"})
        self.assertEqual(row["policy_id"], "oversold-veto-v2")
        self.assertEqual(row["policy_since"], "2026-09-07")
        self.assertEqual(row["run_id"], "12345")
        self.assertRegex(row["code_sha"], r"^[0-9a-f]{8}$")

    def test_code_sha_tracks_file_content_not_the_commit(self):
        import tempfile
        a = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8")
        a.write("x = 1\n"); a.close()
        first = sc.code_sha(a.name)
        with open(a.name, "a", encoding="utf-8") as fp:
            fp.write("y = 2\n")
        self.assertNotEqual(first, sc.code_sha(a.name))

    def test_missing_file_gives_empty_sha_not_a_crash(self):
        self.assertEqual(sc.code_sha("nope/none.py"), "")


class CalendarFailure(unittest.TestCase):
    """달력을 못 읽은 관측을 정상 관측처럼 두지 않는다."""

    def test_checked_loader_reports_failure(self):
        self.assertEqual(sc.load_nontrading_checked("nope/none.txt"), (set(), False))
        got, ok = sc.load_nontrading_checked()
        self.assertTrue(ok)
        self.assertIn("2026-09-25", got)

    def test_empty_calendar_counts_as_failure(self):
        import tempfile
        f = tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8")
        f.write("# 주석만 있다\n"); f.close()
        self.assertEqual(sc.load_nontrading_checked(f.name), (set(), False))

    def test_row_marks_calendar_state(self):
        bad = sc.build_row(at(14, 51), 710, 0, 0, 0, 0, 0, False, None, {},
                           calendar_ok=False)
        self.assertEqual(bad["calendar_ok"], "N")


class Gaps(unittest.TestCase):
    """보존이 조용히 실패하면 파일은 그냥 짧아질 뿐이다. 그래서 센다."""

    def setUp(self):
        import tempfile
        self.path = os.path.join(tempfile.mkdtemp(prefix="gaps_"), "c.csv")

    def write(self, dates):
        with open(self.path, "w", encoding="utf-8", newline="") as fp:
            w = csv.DictWriter(fp, fieldnames=sc.HEADER)
            w.writeheader()
            for d in dates:
                row = {k: "" for k in sc.HEADER}
                row["date"] = d
                w.writerow(row)

    def test_finds_missing_trading_days(self):
        self.write(["2026-09-14", "2026-09-16"])          # 9/15(화)가 빈다
        self.assertEqual(sc.gaps(self.path, set(), datetime.date(2026, 9, 16)),
                         ["2026-09-15"])

    def test_weekends_and_holidays_are_not_gaps(self):
        self.write(["2026-09-17", "2026-09-21"])          # 18(금)은 빠졌고 19·20은 주말
        got = sc.gaps(self.path, {"2026-09-18"}, datetime.date(2026, 9, 21))
        self.assertEqual(got, [])

    def test_empty_file_has_no_gaps(self):
        self.write([])
        self.assertEqual(sc.gaps(self.path, set(), datetime.date(2026, 9, 21)), [])


class ScannerWiring(unittest.TestCase):
    def test_scanner_calls_the_recorder_next_to_its_census_print(self):
        src = (Path(__file__).resolve().parents[1] / "omakase.py").read_text(encoding="utf-8")
        self.assertIn("scanner_census", src)
        self.assertIn("scanner_census.record(", src)
        self.assertIn("타점 인구조사", src)

    def test_workflow_commits_the_census(self):
        wf = (Path(__file__).resolve().parents[1]
              / ".github/workflows/main.yml").read_text(encoding="utf-8")
        self.assertIn("data/scanner_census", wf)
        self.assertIn("contents: write", wf)

    def test_scanner_surfaces_a_recording_failure_loudly(self):
        src = (Path(__file__).resolve().parents[1] / "omakase.py").read_text(encoding="utf-8")
        self.assertIn("인구조사 보존 실패", src)


if __name__ == "__main__":
    unittest.main()
