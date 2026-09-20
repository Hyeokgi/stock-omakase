# -*- coding: utf-8 -*-
"""2026-09-20 — 코덱스 방어 로직의 결함 3건을 고정한다.

`d01d911` 이 넣은 방어(연속 거래일 강제·중복/역순 거부)는 방향이 옳았지만
두 가지가 빠져 있었다. 둘 다 재현으로 확인한 뒤 고쳤다.

  ① `record()` 가 순서를 막지 않아 **역순 행이 쓰일 수 있었다.**
     그 방아쇠는 `docs/안정화_종료선_2026-09-18.md` 가 복구 절차로 적어 둔
     "finalizer workflow_dispatch 로 빠진 과거 거래일 수동 판정" 이었다.
  ② `streak()` 가 이력 전체를 검사해 이상 행이 하나라도 있으면 `return 0` 이었다.
     `runs.csv` 는 append-only 이고 과거 행을 고치지 않으므로 **영구 0** 이었다.
     3/3 을 달성한 뒤에도 복구 경로가 없었다.
"""
import datetime
import os
import tempfile
import unittest

import stability_gate as G

FP = "testfp000000"
EV = {k: True for k in G.KEYS}

# 실제 KRX 거래일만 쓴다. 9/24·25 는 추석 휴장이라 PASS 를 적어도 근거가 아니다.
D = ["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23",
     "2026-09-28", "2026-09-29", "2026-09-30"]


def row(day, verdict=G.PASS, fp=FP):
    return {"cycle_date": day, "verdict": verdict, "fingerprint": fp}


class TradingDayFixtureTests(unittest.TestCase):
    """시험 데이터 자체가 거래일인지 먼저 확인한다(내가 한 번 틀렸다)."""

    def test_fixture_days_are_real_trading_days(self):
        for d in D:
            self.assertTrue(G.is_trading_day(d), d)

    def test_chuseok_days_are_not_trading_days(self):
        for d in ("2026-09-24", "2026-09-25"):
            self.assertFalse(G.is_trading_day(d), d)


class StreakPropertiesTests(unittest.TestCase):
    """`d01d911` 이 지키려던 성질 — 회복을 넣어도 유지되어야 한다."""

    def s(self, rows):
        return G.streak(rows=rows, fp=FP)

    def test_three_consecutive_trading_days_count(self):
        self.assertEqual(self.s([row("2026-09-21"), row("2026-09-22"), row("2026-09-23")]), 3)

    def test_weekend_is_skipped_not_counted_as_a_gap(self):
        self.assertEqual(self.s([row("2026-09-18"), row("2026-09-21"), row("2026-09-22")]), 3)

    def test_holiday_run_is_skipped(self):
        """9/23 → 9/28. 추석·주말이 사이에 있어도 연속이다."""
        self.assertEqual(self.s([row("2026-09-22"), row("2026-09-23"), row("2026-09-28")]), 3)

    def test_a_missing_weekday_breaks_the_streak(self):
        self.assertEqual(self.s([row("2026-09-21"), row("2026-09-23")]), 1)

    def test_skip_on_a_trading_day_is_not_evidence(self):
        self.assertEqual(
            self.s([row("2026-09-21"), row("2026-09-22", G.SKIP), row("2026-09-23")]), 1)

    def test_pass_on_a_non_trading_day_is_not_evidence(self):
        self.assertEqual(
            self.s([row("2026-09-28"), row("2026-09-29"), row("2026-09-24")]), 0)

    def test_a_different_fingerprint_breaks_the_streak(self):
        self.assertEqual(
            self.s([row("2026-09-21", fp="other"), row("2026-09-22"), row("2026-09-23")]), 2)

    def test_a_fail_breaks_the_streak(self):
        self.assertEqual(
            self.s([row("2026-09-21"), row("2026-09-22", G.FAIL), row("2026-09-23")]), 1)


class AnomalyRecoveryTests(unittest.TestCase):
    """② 이상 이력은 **그 지점에서 끊고**, 이후 정상 거래일부터 다시 센다.

    무시하지도 않고(연속으로 잇지 않는다), 영구 차단하지도 않는다.
    """

    def s(self, rows):
        return G.streak(rows=rows, fp=FP)

    TAIL = [row("2026-09-28"), row("2026-09-29"), row("2026-09-30")]

    def test_out_of_order_history_does_not_permanently_zero_the_streak(self):
        rows = [row("2026-09-22"), row("2026-09-21")] + self.TAIL
        self.assertEqual(self.s(rows), 3, "역순 1회가 이후 연속을 영구 차단하면 안 된다")

    def test_duplicate_history_does_not_permanently_zero_the_streak(self):
        rows = [row("2026-09-21"), row("2026-09-21")] + self.TAIL
        self.assertEqual(self.s(rows), 3)

    def test_unparsable_date_does_not_permanently_zero_the_streak(self):
        rows = [row("엉뚱한날짜")] + self.TAIL
        self.assertEqual(self.s(rows), 3)

    def test_the_anomaly_still_breaks_continuity(self):
        """회복은 '이상 행을 무시한다' 가 아니다. 그 앞은 세지 않는다."""
        rows = [row("2026-09-22"), row("2026-09-21"), row("2026-09-23")]
        self.assertEqual(self.s(rows), 1, "9/23 하나만 세야 한다")

    def test_an_anomaly_at_the_newest_end_leaves_no_countable_tail(self):
        """회복은 '이상해도 센다' 가 아니다. 최신 행이 이상하면 셀 꼬리가 없다.

        `tests/test_audit_20260920.py` 가 고정한 성질이다 — 그대로 지킨다.
        """
        rows = [row("2026-09-21"), row("2026-09-22"), row("2026-09-23")]
        self.assertEqual(self.s(rows + [rows[-1]]), 0, "최신 행이 중복이면 0")
        self.assertEqual(self.s(list(reversed(rows))), 0, "이력 전체가 역순이면 0")

    def test_rows_before_the_anomaly_are_never_joined_across_it(self):
        rows = [row("2026-09-18"), row("2026-09-21"),
                row("2026-09-18"),                      # 중복·역순
                row("2026-09-22"), row("2026-09-23")]
        self.assertEqual(self.s(rows), 2, "9/22·23 만. 이상 행을 건너뛰어 잇지 않는다")


class BackfillRefusalTests(unittest.TestCase):
    """① 역순 append 는 거부하고, 판정은 **감사 기록**으로 보존한다."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.runs = os.path.join(self.tmp.name, "runs.csv")
        self.audit = os.path.join(self.tmp.name, "backfill_audit.csv")
        self._orig = G.AUDIT_LOG
        G.AUDIT_LOG = self.audit
        for d in ("2026-09-21", "2026-09-22", "2026-09-23"):
            G.record(d, "r", "cycle", EV, path=self.runs, fp=FP)

    def tearDown(self):
        G.AUDIT_LOG = self._orig
        self.tmp.cleanup()

    def test_the_operational_log_reaches_three(self):
        self.assertEqual(G.streak(path=self.runs, fp=FP), 3)

    def test_an_earlier_date_is_refused(self):
        ok, why = G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertFalse(ok)
        self.assertIn("역순", why)
        self.assertIn("2026-09-18", why)
        self.assertIn("2026-09-23", why, "무엇보다 이른지 말해야 한다")

    def test_the_refusal_names_the_audit_file(self):
        _, why = G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertIn("backfill_audit", why, "어디에 남겼는지 말하지 않으면 조용한 폐기다")

    def test_the_refused_judgement_is_not_lost(self):
        G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertTrue(os.path.exists(self.audit))
        rows = G.load(self.audit)
        self.assertEqual([r["cycle_date"] for r in rows], ["2026-09-18"])
        self.assertEqual(rows[0]["verdict"], G.PASS)

    def test_the_operational_log_is_untouched(self):
        G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertEqual([r["cycle_date"] for r in G.load(self.runs)],
                         ["2026-09-21", "2026-09-22", "2026-09-23"])

    def test_the_streak_survives_the_refused_backfill(self):
        G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertEqual(G.streak(path=self.runs, fp=FP), 3,
                         "복구 절차를 쓴 대가로 3/3 을 잃으면 안 된다")

    def test_a_later_date_is_still_accepted(self):
        ok, _ = G.record("2026-09-28", "r", "cycle", EV, path=self.runs, fp=FP)
        self.assertTrue(ok)
        self.assertEqual(G.streak(path=self.runs, fp=FP), 4)

    def test_an_unwritable_audit_path_does_not_turn_refusal_into_a_crash(self):
        """거부가 본질이다. 감사 쓰기 실패가 finalizer 를 죽이면 안 된다.

        첫 구현이 실제로 그랬다 — `_audit()` 의 예외가 그대로 올라갔다.
        """
        G.AUDIT_LOG = "/proc/없는경로/audit.csv"
        ok, why = G.record("2026-09-18", "manual", "dispatch", EV, path=self.runs, fp=FP)
        self.assertFalse(ok)
        self.assertIn("역순", why)
        self.assertIn("감사 기록 실패", why, "조용히 넘기지도 않는다")
        self.assertEqual(G.streak(path=self.runs, fp=FP), 3)

    def test_a_duplicate_is_still_refused_without_an_audit_row(self):
        ok, why = G.record("2026-09-23", "r", "cycle", EV, path=self.runs, fp=FP)
        self.assertFalse(ok)
        self.assertIn("이미 기록된", why)
        self.assertFalse(os.path.exists(self.audit), "중복은 감사 기록 대상이 아니다")


class FingerprintCoverageTests(unittest.TestCase):
    def test_the_shared_code_rule_is_in_the_fingerprint(self):
        """규칙을 위임했으면 위임받은 모듈도 지문에 있어야 한다."""
        self.assertIn("krx_code.py", G.FINGERPRINT_FILES)


if __name__ == "__main__":
    unittest.main()
