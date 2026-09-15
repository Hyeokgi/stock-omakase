# -*- coding: utf-8 -*-
"""§3-5-7 (2026-09-15 사전등록) 을 코드로 고정한다.

게이트는 아직 만들지 않았다. 그러나 **규칙은 지금 동결할 수 있고, 동결해야 한다.**
날짜를 나중에 사람이 고르면 그것이 결과를 보고 고르는 것이 되기 때문이다.

여기서 거는 것은 셋이다.
  1. 트랙 E 평가일 산출 규칙 — 블록 종료 후 10번째 검증 거래일
  2. 2027 달력 미검증 구간은 **평일 근사로 진행하지 않는다** (fail-closed)
  3. 트랙 집합의 정의 — 특히 `EXPLORATORY` 상수를 트랙 E 로 착각하지 않는다
"""
import unittest
import hyeoks_trading_calendar as cal
import hyeoks_verdict as v

# §3-5-6 이 고정한 달력 블록. 이 표는 여기서 바꾸지 않는다.
BLOCKS = {"A": ("2026-10-06", "2026-11-06"),
          "B": ("2026-11-09", "2026-12-09"),
          "C": ("2026-12-10", "2027-01-12")}
MATURITY_SESSIONS = 10        # 트랙 E 최대 호라이즌 T+10 에서 나온 값. 여유일 없음.


def evaluation_date(block_end, nontrading, sessions=MATURITY_SESSIONS):
    """블록 종료일 이후 `sessions` 번째 **검증된** 거래일. 달력 밖이면 예외가 난다."""
    d = block_end
    for _ in range(sessions):
        d = cal.next_trading_day(d, nontrading)
    return d


class TrackEEvaluationDate(unittest.TestCase):
    def setUp(self):
        self.nt = cal.load_nontrading()

    def test_block_a_evaluates_2026_11_20(self):
        self.assertEqual(evaluation_date(BLOCKS["A"][1], self.nt), "2026-11-20")

    def test_block_b_evaluates_2026_12_23(self):
        """12/25 휴장은 10번째 거래일 뒤에 온다 — 결과에 영향이 없다는 것까지 건다."""
        self.assertEqual(evaluation_date(BLOCKS["B"][1], self.nt), "2026-12-23")

    def test_block_c_is_fail_closed_until_2027_calendar_verified(self):
        """2027 달력이 없으면 **평일로 추정하지 않는다.** 조용히 답을 내면 안 된다."""
        with self.assertRaises(ValueError):
            evaluation_date(BLOCKS["C"][1], self.nt)

    def test_maturity_is_exactly_ten_not_a_knob(self):
        """T+10 이 성숙 기간을 이미 정의한다. 임의의 여유일을 더하면 손잡이가 하나 는다."""
        self.assertEqual(MATURITY_SESSIONS, max(v.HORIZON["리포트TOP2_단기"],
                                                v.HORIZON["리포트TOP2_중기"]))

    def test_rule_is_sessions_not_calendar_days(self):
        """A 의 +10 거래일은 달력일로는 14일이다. 달력일 근사와 갈린다."""
        import datetime
        end = datetime.date.fromisoformat(BLOCKS["A"][1])
        got = datetime.date.fromisoformat(evaluation_date(BLOCKS["A"][1], self.nt))
        self.assertEqual((got - end).days, 14)


class TrackMembership(unittest.TestCase):
    """§3-5-7 ⚠️ — `EXPLORATORY` 상수는 트랙 E 가 아니다."""

    def test_exploratory_constant_includes_long_channel(self):
        """이 상수는 `collect(current_policy=True)` 의 정책일 필터용이고 장기를 포함한다."""
        self.assertIn(v.LONG_CHANNEL, v.EXPLORATORY)
        self.assertEqual(len(v.EXPLORATORY), 3)

    def test_track_e_must_exclude_long_channel(self):
        """트랙 E 로 쓰면 H=60 채널이 +10거래일에 평가된다 — 성숙도 1/6."""
        track_e = tuple(c for c in v.EXPLORATORY if c != v.LONG_CHANNEL)
        self.assertEqual(track_e, ("리포트TOP2_단기", "리포트TOP2_중기"))
        self.assertTrue(all(v.HORIZON[c] <= MATURITY_SESSIONS for c in track_e))
        self.assertGreater(v.HORIZON[v.LONG_CHANNEL], MATURITY_SESSIONS)

    def test_confirmatory_set_is_closed_and_disjoint_from_exploratory(self):
        self.assertEqual(v.CONFIRMATORY, ("차트TOP2", "수급TOP2"))
        self.assertEqual(v.HOLM_M, 2)
        self.assertFalse(set(v.CONFIRMATORY) & set(v.EXPLORATORY))

    def test_three_tracks_cover_every_judged_channel(self):
        """C ∪ E ∪ L 이 판정 대상 전체와 일치해야 한다. 빠진 채널은 경로가 사라진다."""
        judged = {c for c in v.HORIZON if c not in v.CONTROL_LIKE}
        track_e = {c for c in v.EXPLORATORY if c != v.LONG_CHANNEL}
        self.assertEqual(set(v.CONFIRMATORY) | track_e | {v.LONG_CHANNEL}, judged)


class TrackAlphas(unittest.TestCase):
    """§3-5-7 의 α 표. `holm` 에 α 를 넣을 수 있게 된 뒤라야 걸 수 있는 테스트다."""

    def test_track_c_first_threshold_is_0_00625(self):
        self.assertEqual(v.holm([("차트TOP2", 0.006), ("수급TOP2", 0.4)], 2, alpha=0.0125),
                         {"차트TOP2"})
        self.assertEqual(v.holm([("차트TOP2", 0.007), ("수급TOP2", 0.4)], 2, alpha=0.0125),
                         set())

    def test_track_e_uses_uncorrected_005(self):
        """탐색에 트랙 C 의 문턱을 쓰면 사전등록보다 8배 엄격해진다."""
        self.assertEqual(v.holm([("리포트TOP2_단기", 0.03)], 1, alpha=0.05),
                         {"리포트TOP2_단기"})
        self.assertEqual(v.holm([("리포트TOP2_단기", 0.03)], 1, alpha=0.00625), set())

    def test_passed_remains_closed(self):
        """U1~U5 가 정해져도 `passed` 는 마지막까지 fail-closed 다 (구현 순서 확정)."""
        import io as _io, re
        src = _io.open("hyeoks_verdict.py", encoding="utf-8").read()
        self.assertRegex(src, r"\n    passed = set\(\)")


if __name__ == '__main__':
    unittest.main()
