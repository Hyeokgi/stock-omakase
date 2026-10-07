"""에피소드 등록부 v1 — 분모 보존·대조군 고정·상태 규칙·앞 날짜 불변·가격 결과 없음."""
import unittest
from unittest import mock

import hyeoks_episode_registry as R


def it(code, theme, rate, amt):
    return {"code": code, "theme": theme, "rate": rate, "amt": amt, "alert": ""}


def day(date, items, leaders, codes=None):
    A = {x["code"]: x for x in items}
    return {"date": date, "codes": set(codes if codes is not None else A), "A": A,
            "B": {c: A[c]["theme"] for c in leaders}}


BASE = [it("L", "t1", 9, 500), it("P1", "t1", 2, 480), it("P2", "t1", 1, 100), it("P3", "t1", 3, 10),
        it("U", "t1", 29.9, 490), it("X", "t2", 5, 300), it("Y", "t2", 1, 50), it("Z", "t3", 0, 70)]


class Registry(unittest.TestCase):
    def test_primary_controls_same_theme_nearest_liquidity_without_limit_up(self):
        eps, *_ = R.build([day("2026-10-06", BASE, ["L", "X"])])
        L = [e for e in eps if e["code"] == "L"][0]
        self.assertEqual(L["primary"], ["P1", "P2"], "같은 테마·거래대금 가까운 순, 상한가 근처 U 제외")
        self.assertEqual(L["theme"], "t1")
        self.assertNotIn("X", L["secondary"], "보조 대조군도 그날 대장은 뺀다")

    def test_control_that_becomes_leader_is_kept_and_flagged_once(self):
        d1 = day("2026-10-06", BASE, ["L"])
        d2 = day("2026-10-07", BASE, ["P1"])
        d3 = day("2026-10-08", BASE, ["P1"])
        eps, st, ev, _ = R.build([d1, d2, d3])
        L = [e for e in eps if e["code"] == "L"][0]
        self.assertEqual(L["primary"], ["P1", "P2"], "교체·삭제하지 않는다")
        self.assertEqual([(e["code"], e["date"]) for e in ev], [("P1", "2026-10-07")])
        self.assertTrue(any(e["code"] == "P1" for e in eps), "대조군이었던 종목도 자기 에피소드를 연다")

    def test_one_open_episode_per_code(self):
        eps, *_ = R.build([day("2026-10-06", BASE, ["L"]), day("2026-10-07", BASE, ["L"])])
        self.assertEqual(sum(1 for e in eps if e["code"] == "L"), 1)

    def test_badge_loss_is_not_an_end_missing_is_not_an_end(self):
        days = [day("2026-10-06", BASE, ["L"]), day("2026-10-07", BASE, [], codes=[c for c in "P1 P2".split()]),
                day("2026-10-08", BASE, [])]
        _, st, _, _ = R.build(days)
        s = [r["status"] for r in st if r["episode"] == "2026-10-06_L"]
        self.assertEqual(s, ["관측 결측", "진행 중"])

    def test_absent_run_ends_and_cap_censors(self):
        with mock.patch.object(R, "ABSENT_END", 2), mock.patch.object(R, "ADMIN_CAP_OBS", 3):
            gone = [day("2026-10-06", BASE, ["L"])] + [day(f"2026-10-0{7 + j}", BASE, [], codes=[]) for j in range(2)]
            eps, st, _, _ = R.build(gone)
            self.assertEqual([e["end_status"] for e in eps if e["code"] == "L"], ["종료(소멸 추정)"])
            stay = [day(f"2026-10-0{6 + j}", BASE, ["L"] if j == 0 else []) for j in range(4)]
            eps, st, _, _ = R.build(stay)
            L = [e for e in eps if e["code"] == "L"][0]
            self.assertEqual((L["end_status"], L["end"]), ("상한 도달", "2026-10-09"))

    def test_adding_later_days_never_changes_earlier_registration(self):
        d = [day("2026-10-06", BASE, ["L"]), day("2026-10-07", BASE, ["X"]), day("2026-10-08", BASE, ["P1"])]
        a, *_ = R.build(d[:2])
        b, *_ = R.build(d)
        keep = lambda eps: [{k: e[k] for k in ("id", "primary", "secondary", "secondary_seed", "theme")} for e in eps]
        self.assertEqual(keep(b)[:len(a)], keep(a))
        self.assertEqual(R.digest(b[:len(a)]), R.digest(a))

    def test_secondary_controls_are_reproducible(self):
        d = day("2026-10-06", BASE, ["L"])
        self.assertEqual(R.secondary_controls(d), R.secondary_controls(d))

    def test_summary_has_no_codes_or_prices(self):
        days = [day("2026-10-06", BASE, ["L", "X"])]
        out = R.summary(days, *R.build(days))
        text = str(out)
        for code in ("'L'", "'P1'", "'X'"):
            self.assertNotIn(code, text)
        self.assertNotIn("nowPrice", text)

    def test_real_snapshots_build(self):
        days = R.load_obs()
        eps, st, ev, daily = R.build(days)
        self.assertEqual(len(daily), len(days))
        self.assertTrue(all(e["t0"] <= days[-1]["date"] for e in eps))


if __name__ == "__main__":
    unittest.main()
