"""거래일·가격 독립 근거 대조 — 검사가 **실제로 발동하는가**.

일치하는 것만 보여주는 검증은 검증이 아니다. 어긋남을 만들어 걸리는지 본다.
"""
import unittest
import hyeoks_evidence as E
from hyeoks_trading_calendar import ClosureCalendar

SCOPE = {"version": "krx-scheduled-2026h2-v1", "checked_at": "2026-09-13",
         "start": "2026-08-01", "end": "2026-12-31",
         "sources": ["https://kasi.example", "https://krx.example"]}


def cal(days=(), start="2026-08-01", end="2026-12-31"):
    c = ClosureCalendar(days)
    c.start, c.end = start, end
    return c


class ScheduledSessionTests(unittest.TestCase):
    def test_weekend_excluded(self):
        # 2026-09-05 토, 09-06 일
        s = E.scheduled_sessions("2026-09-04", "2026-09-07", cal())
        self.assertEqual(s, ["2026-09-04", "2026-09-07"])

    def test_closure_excluded(self):
        s = E.scheduled_sessions("2026-09-23", "2026-09-28",
                                 cal(("2026-09-24", "2026-09-25")))
        self.assertEqual(s, ["2026-09-23", "2026-09-28"])

    def test_outside_scope_blocked_not_approximated(self):
        """범위 밖은 **막는다** — 주말 규칙으로 추정하지 않는다."""
        with self.assertRaises(ValueError):
            E.scheduled_sessions("2027-01-04", "2027-01-05", cal())

    def test_reversed_range_rejected(self):
        with self.assertRaises(ValueError):
            E.scheduled_sessions("2026-09-10", "2026-09-01", cal())


class AgreementTests(unittest.TestCase):
    W = ("2026-09-07", "2026-09-11")          # 월~금, 휴장 없음
    ALL = ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11"]

    def test_full_agreement(self):
        a = E.calendar_agreement(self.ALL, *self.W, cal())
        self.assertTrue(a["agree"])
        self.assertEqual(a["expected_only"], [])
        self.assertEqual(a["observed_only"], [])

    def test_missing_observation_detected(self):
        """예정 거래일인데 관측이 없다 — 임시휴장이거나 수집 실패다."""
        a = E.calendar_agreement([d for d in self.ALL if d != "2026-09-09"],
                                 *self.W, cal())
        self.assertFalse(a["agree"])
        self.assertEqual(a["expected_only"], ["2026-09-09"])
        self.assertEqual(a["observed_only"], [])

    def test_observation_on_registered_holiday_detected(self):
        """관측은 있는데 예정 휴장일 — **달력이 틀렸다.** 더 심각한 쪽이다."""
        a = E.calendar_agreement(self.ALL, *self.W, cal(("2026-09-09",)))
        self.assertFalse(a["agree"])
        self.assertEqual(a["observed_only"], ["2026-09-09"])
        self.assertEqual(a["expected_only"], [])

    def test_observations_outside_range_ignored(self):
        a = E.calendar_agreement(self.ALL + ["2026-08-03"], *self.W, cal())
        self.assertTrue(a["agree"])


class EvidenceTests(unittest.TestCase):
    W = ("2026-09-07", "2026-09-11")
    ALL = ["2026-09-07", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11"]

    def test_verified_only_on_agreement(self):
        ev = E.calendar_evidence(E.calendar_agreement(self.ALL, *self.W, cal()),
                                 SCOPE, "2026-09-11")
        self.assertEqual(ev["status"], "verified")
        self.assertEqual(ev["sessions"], self.ALL)
        self.assertIn("krx-scheduled-2026h2-v1", ev["evidence"])
        self.assertIn("kasi.example", ev["evidence"])

    def test_mismatch_never_becomes_verified(self):
        a = E.calendar_agreement(self.ALL[:-1], *self.W, cal())
        ev = E.calendar_evidence(a, SCOPE, "2026-09-11")
        self.assertEqual(ev["status"], "unverified")
        self.assertEqual(ev["evidence"], "")
        self.assertEqual(ev["sessions"], [])

    def test_sessions_capped_at_as_of(self):
        ev = E.calendar_evidence(E.calendar_agreement(self.ALL, *self.W, cal()),
                                 SCOPE, "2026-09-09")
        self.assertEqual(ev["sessions"], self.ALL[:3])

    def test_states_what_it_does_not_establish(self):
        """과장 방지 — 실제 장 운영 인증이 아니라고 블록 안에 박혀 있어야 한다."""
        ev = E.calendar_evidence(E.calendar_agreement(self.ALL, *self.W, cal()),
                                 SCOPE, "2026-09-11")
        self.assertIn("실제 장 운영 인증이 아니다", ev["does_not_establish"])


class PriceTests(unittest.TestCase):
    def bars(self, **kw):
        return {"000001": {"2026-09-07": dict(open=1000, high=1100, low=900, close=1050),
                           "2026-09-08": dict(open=kw.get("o2", 1050), high=1100,
                                              low=1000, close=1080)}}

    def snaps(self, open_price=1050, prev_close=1050):
        return {"2026-09-07": {"000001": {"open_price": 1000,
                                          "prev_close_derived": 990}},
                "2026-09-08": {"000001": {"open_price": open_price,
                                          "prev_close_derived": prev_close}}}

    def test_agreement_counts(self):
        r = E.price_agreement(self.bars(), self.snaps())
        self.assertEqual(r["mismatch_open"], 0)
        self.assertEqual(r["mismatch_prev_close"], 0)
        self.assertEqual(r["checked_open"], 2)
        self.assertEqual(r["checked_prev_close"], 1)   # 첫날은 전일이 없다

    def test_corporate_action_shows_up_in_prev_close(self):
        """일봉은 소급 조정되고 스냅샷은 그날 값이다 — 여기서 어긋난다."""
        r = E.price_agreement(self.bars(), self.snaps(prev_close=210))
        self.assertEqual(r["mismatch_prev_close"], 1)
        self.assertEqual(r["worst_prev_close"][0][0], "000001")

    def test_open_mismatch_detected(self):
        r = E.price_agreement(self.bars(), self.snaps(open_price=1300))
        self.assertEqual(r["mismatch_open"], 1)

    def test_tolerance_absorbs_rounding(self):
        r = E.price_agreement(self.bars(), self.snaps(open_price=1052))  # +0.19%
        self.assertEqual(r["mismatch_open"], 0)

    def test_zero_is_not_a_price(self):
        r = E.price_agreement(self.bars(), self.snaps(open_price=0))
        self.assertEqual(r["checked_open"], 1)         # 0 은 아예 안 센다

    def test_price_evidence_is_always_unverified(self):
        """같은 벤더의 다른 엔드포인트다 — 일치해도 verified 가 되지 않는다."""
        ev = E.price_evidence(E.price_agreement(self.bars(), self.snaps()))
        self.assertEqual(ev["status"], "unverified")
        self.assertEqual(ev["evidence"], "")
        self.assertIn("같은 벤더", ev["reason"])
        self.assertIn("KRX", ev["needs"])


class GateIntegrationTests(unittest.TestCase):
    """도구가 낸 블록을 **진짜 게이트**가 어떻게 받는가.

    이게 이 작업의 결론이다 — 달력은 열리고 가격은 닫힌 채로 남아야 한다.
    """

    def bundle(self):
        import sys
        sys.path.insert(0, 'tests')
        from test_account_nav import fixture
        b = fixture()
        b['schema'] = 'account-input-v3'
        b['theme_policy'] = 'none'
        return b

    def cal_ev(self, b, drop_last=False):
        obs = b['sessions'][:-1] if drop_last else b['sessions']
        a = E.calendar_agreement(obs, b['sessions'][0], b['sessions'][-1], cal())
        return E.calendar_evidence(a, SCOPE, b['as_of'])

    def blocked_on(self, b):
        import hyeoks_account as A
        try:
            A.validate_bundle(b)
            return None
        except A.InputError as e:
            return str(e)

    def test_no_evidence_blocks_on_calendar(self):
        self.assertIn('calendar_verification', self.blocked_on(self.bundle()))

    def test_tool_calendar_evidence_opens_calendar_gate(self):
        b = self.bundle()
        b['calendar_verification'] = self.cal_ev(b)
        msg = self.blocked_on(b)
        self.assertIsNotNone(msg)
        self.assertIn('price_verification', msg)          # 달력은 지나갔다
        self.assertNotIn('calendar_verification', msg)

    def test_mismatched_calendar_never_opens_the_gate(self):
        b = self.bundle()
        b['calendar_verification'] = self.cal_ev(b, drop_last=True)
        b['price_verification'] = {'status': 'verified', 'evidence': 'x'}
        self.assertIn('calendar_verification', self.blocked_on(b))

    def test_tool_price_evidence_does_not_open_the_gate(self):
        """도구는 가격을 인증하지 않는다 — 스스로 통과시키면 게이트가 무의미해진다."""
        b = self.bundle()
        b['calendar_verification'] = self.cal_ev(b)
        b['price_verification'] = E.price_evidence(E.price_agreement({}, {}))
        self.assertIn('price_verification', self.blocked_on(b))

    def test_human_supplied_price_evidence_completes_the_gate(self):
        b = self.bundle()
        b['calendar_verification'] = self.cal_ev(b)
        b['price_verification'] = {'status': 'verified',
                                   'evidence': '사람이 넣은 독립 자료 참조'}
        self.assertIsNone(self.blocked_on(b))


if __name__ == "__main__":
    unittest.main()
