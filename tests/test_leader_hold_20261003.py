"""2026-10-03 — 대장 유지형 종베 `leader-hold-v1` 사전등록의 코드 약속.

지킬 것
  · 공통 위험 기준은 비교군(B100c)과 H 에 **똑같이** 적용된다 — H 에만 붙으면 효과를 가를 수 없다.
  · 고가유지율 98% 경계, 상한가 29.5% 경계, 경보 미확인은 정상으로 가정하지 않는다.
  · 확증 구간(진입일 ≥ 2026-10-06) 수익률은 유효 비교일 60일 전에는 **어떤 출력에도** 나오지 않는다.
  · 주 평가는 첫 60 유효 비교일로 한 번, 고정 표본이다(61번째 날이 값을 바꾸지 못한다).
  · 부트스트랩은 같은 입력에 같은 구간을 준다. 120거래일 상한은 달력 범위를 넘으면 추정하지 않는다.
"""
import gzip
import json
import os
import shutil
import tempfile
import unittest

import hyeoks_theme_abc as T
from hyeoks_closing_bet import COST, scan_dates
from hyeoks_trading_calendar import load_nontrading, next_trading_day

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def item(code, amt=T.MIN_BREAKOUT_TV * 2, P=1000, high=1000, rate=5.0, alert="00"):
    return {"code": code, "name": code, "P": P, "amt": amt, "rate": rate, "high": high,
            "theme": code, "has_theme": True, "alert": alert}


class CommonRiskFilter(unittest.TestCase):
    def test_reasons_and_boundaries(self):
        kept, ex = T.risk_filter([
            item("ok"), item("lim", rate=29.5), item("near", rate=29.4),
            item("w1", alert="01"), item("w2", alert="02"), item("w3", alert="03"),
            item("unk", alert=""), item("none", alert=None)])
        self.assertEqual(sorted(x["code"] for x in kept), ["near", "ok"])
        self.assertEqual(ex, {"경보미확인": 2, "시장경보": 3, "상한가근접": 1})

    def test_an_item_is_counted_once_by_its_first_reason(self):
        kept, ex = T.risk_filter([item("both", rate=29.9, alert="03"), item("blank", rate=29.9, alert="")])
        self.assertEqual((kept, sum(ex.values())), ([], 2))
        self.assertEqual((ex["시장경보"], ex["경보미확인"], ex["상한가근접"]), (1, 1, 0))

    def test_same_filter_on_comparison_group_and_H(self):
        """H 에만 위험 제외가 붙지 않는다 — 위험종목은 B100c 에도 H 에도 없다."""
        leaders = [item("clean"), item("limit", rate=29.8), item("warn", alert="02"),
                   item("small", amt=T.MIN_BREAKOUT_TV - 1)]
        b100, excl = T.build_B100(leaders)
        h, _ = T.build_H(b100)
        self.assertEqual([x["code"] for x in b100], ["clean"])
        self.assertEqual([x["code"] for x in h], ["clean"])
        self.assertEqual(excl["상한가근접"] + excl["시장경보"], 2)
        self.assertTrue({x["code"] for x in h} <= {x["code"] for x in b100})

    def test_sensitivity_variant_keeps_risky_items(self):
        raw, excl = T.build_B100([item("clean"), item("limit", rate=29.8)], apply_risk=False)
        self.assertEqual(sorted(x["code"] for x in raw), ["clean", "limit"])
        self.assertEqual(sum(excl.values()), 0)


class HoldRatio(unittest.TestCase):
    def test_98_percent_boundary_is_inclusive_and_exact(self):
        pool = [item("at", P=980, high=1000), item("below", P=979, high=1000),
                item("above", P=1000, high=1000), item("big", P=4900, high=5000)]
        h, _ = T.build_H(pool)
        self.assertEqual(sorted(x["code"] for x in h), ["above", "at", "big"])

    def test_missing_high_is_dropped_and_counted(self):
        h, no_high = T.build_H([item("a", high=0), item("b", high=-1), item("c")])
        self.assertEqual(([x["code"] for x in h], no_high), (["c"], 2))

    def test_top_k_orders_by_turnover_then_code(self):
        pool = [item("b", amt=5), item("a", amt=5), item("c", amt=9), item("d", amt=1)]
        self.assertEqual([x["code"] for x in T.top_k(pool, 3)], ["c", "a", "b"])
        self.assertEqual(len(T.top_k(pool)), T.TOPK)


class Statistics(unittest.TestCase):
    def test_bootstrap_is_deterministic_and_seeded(self):
        xs = [0.01, -0.02, 0.03, 0.0, 0.015, -0.01, 0.02, 0.005, -0.004, 0.012]
        a, b = T.block_bootstrap_ci(xs), T.block_bootstrap_ci(xs)
        self.assertEqual(a, b)
        self.assertNotEqual(a, T.block_bootstrap_ci(xs, seed=1))
        self.assertLess(a[0], a[1])

    def test_constant_series_has_a_point_interval(self):
        lo, hi = T.block_bootstrap_ci([0.04] * 60)
        self.assertAlmostEqual(lo, 0.04, places=12)
        self.assertAlmostEqual(hi, 0.04, places=12)

    def test_short_or_empty_series_do_not_crash(self):
        self.assertEqual(T.block_bootstrap_ci([]), (None, None))
        self.assertEqual(len(T.block_bootstrap_ci([0.01, 0.02])), 2)

    def test_max_drawdown(self):
        self.assertAlmostEqual(T.max_drawdown([0.1, -0.1]), 0.1)
        self.assertEqual(T.max_drawdown([0.01, 0.02]), 0.0)
        self.assertAlmostEqual(T.max_drawdown([0.5, -0.5, 0.5, -0.5]), 0.625)

    def test_classification_table(self):
        c = T.classify_hold
        self.assertEqual(c(0.003, 0.0005, 0.002, 0.001), "후보 격상 검토 가능")
        self.assertEqual(c(0.003, 0.0005, 0.002, -0.0001), "불확정", "최고일을 빼면 음수")
        self.assertEqual(c(0.003, 0.0005, -0.001, 0.001), "불확정", "비용 후 평균이 음수")
        self.assertEqual(c(0.003, -0.0005, 0.002, 0.001), "불확정", "구간 하한이 0 이하")
        self.assertEqual(c(0.0, 0.0, 0.0, 0.0), "폐기", "점추정 0 이하는 폐기")
        self.assertEqual(c(-0.001, -0.002, 0.01, 0.01), "폐기")
        self.assertEqual(c(None, None, 0, 0), "불확정")

    def test_120_trading_day_cap_never_guesses_beyond_the_calendar(self):
        nt = load_nontrading()
        self.assertEqual(T.trading_day_n("2026-10-06", 1, nt), ("2026-10-06", ""))
        # 10/9 한글날 휴장 — 10/6, 7, 8 다음은 10/12
        self.assertEqual([T.trading_day_n("2026-10-06", n, nt)[0] for n in (2, 3, 4)],
                         ["2026-10-07", "2026-10-08", "2026-10-12"])
        d, why = T.trading_day_n("2026-10-06", T.CAP_TRADING_DAYS, nt)
        self.assertIsNone(d)
        self.assertIn("범위 밖", why)


def fake_day(date, h=10, b=12, h_ret=0.12, b_ret=0.08, drop_h=None, drop_b=None, n_h=None):
    """품질 점검 시험용 합성 하루. `n_h` 는 수익률을 붙인 H 종목 수(기본: 후보 − 제외)."""
    drop_h, drop_b = dict(drop_h or {}), dict(drop_b or {})
    zero = {"익일시가없음": 0, "익일소멸": 0, "익일제한폭이탈": 0, "기업행사조정의심": 0}
    dh, db = {**zero, **drop_h}, {**zero, **drop_b}
    n_h = h - sum(dh.values()) if n_h is None else n_h
    return {"date": date, "hold": {"H": h, "B100": b, "H2": min(h, 2), "B100raw": b, "Hraw": h,
                                   "risk_excl": {}, "H_고가결측": 0},
            "returns_hold": {"H": {"n": n_h, "평균": h_ret if n_h else None},
                             "B100": {"n": b - sum(db.values()), "평균": b_ret},
                             "H2": {"n": 2, "평균": h_ret}, "B100raw": {"n": b, "평균": b_ret},
                             "Hraw": {"n": h, "평균": h_ret}},
            "ret_drop_hold": {"H": dh, "B100": db}}


def sixty(**kw_first):
    days = []
    for i in range(T.EVAL_VALID_DAYS):
        # 날짜 문자열만 필요하다 — CONFIRM_FROM 이상이고 단조 증가하면 된다
        days.append(fake_day(f"2027-01-{i + 1:02d}" if i < 28 else f"2027-02-{i - 27:02d}"))
    return days


class QualityHold(unittest.TestCase):
    """출구 가격 결측이 많으면 격상도 폐기도 말하지 않는다."""

    def test_clean_sample_is_classified_normally(self):
        days = sixty()
        ev = T.evaluate_hold(days, days)
        self.assertFalse(ev["품질보류"])
        self.assertEqual(ev["분류"], "후보 격상 검토 가능")

    def test_exactly_five_percent_missing_exits_is_not_a_hold(self):
        days = sixty()
        for k in range(6):                      # 6일 × 5종목 = 30 / 후보 600 = 5.0%
            days[k] = fake_day(days[k]["date"], drop_h={"익일시가없음": 5})
        ev = T.evaluate_hold(days, days)
        self.assertAlmostEqual(ev["품질"]["H"]["시가결측률"], 0.05)
        self.assertFalse(ev["품질보류"], "상한과 같으면 보류하지 않는다(초과일 때만)")

    def test_above_five_percent_missing_exits_holds_the_verdict(self):
        days = sixty()
        for k in range(7):                      # 35 / 600 = 5.83%
            days[k] = fake_day(days[k]["date"], drop_h={"익일시가없음": 5})
        ev = T.evaluate_hold(days, days)
        self.assertTrue(ev["품질보류"])
        self.assertEqual(ev["분류"], "품질 보류")
        self.assertTrue(any("H 익일 시가 결측" in r for r in ev["품질사유"]))

    def test_delisted_exits_count_as_missing_too(self):
        days = sixty()
        for k in range(7):
            days[k] = fake_day(days[k]["date"], drop_h={"익일소멸": 5})
        self.assertTrue(T.evaluate_hold(days, days)["품질보류"])

    def test_comparison_group_gaps_hold_the_verdict_as_well(self):
        days = sixty()
        for k in range(8):                      # B100c 720 중 40 = 5.56%
            days[k] = fake_day(days[k]["date"], drop_b={"익일시가없음": 5})
        ev = T.evaluate_hold(days, days)
        self.assertTrue(ev["품질보류"])
        self.assertTrue(any(r.startswith("B100 ") for r in ev["품질사유"]))

    def test_guard_drops_alone_can_hold_the_verdict(self):
        days = sixty()
        for k in range(13):                     # 제한폭 이탈 65 / 600 = 10.8% (시가 결측은 0)
            days[k] = fake_day(days[k]["date"], drop_h={"익일제한폭이탈": 5})
        ev = T.evaluate_hold(days, days)
        self.assertEqual(ev["품질"]["H"]["시가결측"], 0)
        self.assertTrue(ev["품질보류"])
        self.assertTrue(any("가드 제외" in r for r in ev["품질사유"]))

    def test_a_day_with_candidates_but_no_returns_is_counted_not_dropped(self):
        """무효일(후보는 있었으나 수익률이 전부 없음)을 조용히 빼면 불리한 사례가 사라진다."""
        days = sixty()
        bad = fake_day("2026-12-31", h=10, drop_h={"익일시가없음": 10})     # 전부 결측 → 무효일
        window = sorted(days + [bad], key=lambda d: d["date"])
        valid, why = T.valid_hold_days(window)
        self.assertEqual(why.get("가드로전부제외"), 1)
        self.assertEqual(len(valid), 60)
        ev = T.evaluate_hold(valid, window)
        self.assertEqual(ev["품질"]["H"]["시가결측"], 10, "무효일의 결측이 분자에 들어가야 한다")
        self.assertEqual(ev["품질"]["H"]["후보"], 610)

    def test_quality_hold_overrides_every_other_class(self):
        for args in ((0.003, 0.0005, 0.002, 0.001), (-0.001, -0.002, 0.01, 0.01), (None, None, 0, 0)):
            self.assertEqual(T.classify_hold(*args, quality_hold=True), "품질 보류")


def missing_next_day(date, h=10, b=12, note="익일달력결측"):
    """진입일 후보는 확인되지만 익일 자료가 없어 수익률 계산이 안 된 하루 (build_day 가 만드는 모양)."""
    return {"date": date, "hold": {"H": h, "B100": b, "H2": min(h, 2), "B100raw": b, "Hraw": h,
                                   "risk_excl": {}, "H_고가결측": 0},
            "returns": None, "returns_hold": None, "returns_note": note, "exit_date": "2027-01-04"}


class MaturedMissingQuality(unittest.TestCase):
    """코덱스 회귀: 정상일 후보 10 + 익일달력결측일 후보 10 → 이전 구현은 후보 10 · 결측 0 · 보류 False 였다."""

    def test_codex_reproduction_case(self):
        days = [fake_day("2027-01-04", h=10, b=12), missing_next_day("2027-01-05", h=10, b=12)]
        q, hold, reasons = T.quality_check(days)
        self.assertEqual((q["H"]["후보"], q["H"]["시가결측"]), (20, 10), "결측일 후보가 분모·분자에 들어가야 한다")
        self.assertEqual((q["B100"]["후보"], q["B100"]["시가결측"]), (24, 12))
        self.assertTrue(hold, "50% 결측은 보류여야 한다")
        self.assertEqual(q["일별"]["성숙후결측일"], ["2027-01-05"])

    def test_every_matured_missing_note_is_counted(self):
        for note in T.MATURED_MISSING_NOTES:
            with self.subTest(note=note):
                q, hold, _ = T.quality_check([missing_next_day("2027-01-05", note=note)])
                self.assertEqual((q["H"]["후보"], q["H"]["시가결측"], hold), (10, 10, True))

    def test_immature_day_is_not_a_missing_day(self):
        days = [fake_day("2027-01-04"), missing_next_day("2027-01-05", note="익일미성숙")]
        q, hold, _ = T.quality_check(days)
        self.assertEqual((q["H"]["후보"], q["H"]["시가결측"], hold), (10, 0, False))
        self.assertEqual(q["일별"]["미성숙일"], ["2027-01-05"])
        self.assertEqual(q["일별"]["성숙후결측일"], [])

    def test_small_matured_gap_inside_a_big_clean_sample_does_not_hold(self):
        days = [fake_day(f"2027-01-{i + 1:02d}") for i in range(30)] + [missing_next_day("2027-02-01", h=2, b=3)]
        q, hold, _ = T.quality_check(days)
        self.assertEqual((q["H"]["후보"], q["H"]["시가결측"]), (302, 2))
        self.assertFalse(hold)

    def test_unclassifiable_day_holds_instead_of_passing_as_normal(self):
        for note in ("달력범위밖", "수익률미계산"):
            with self.subTest(note=note):
                q, hold, reasons = T.quality_check([fake_day("2027-01-04"), missing_next_day("2027-01-05", note=note)])
                self.assertTrue(hold)
                self.assertTrue(any("판정 불가" in r for r in reasons))
                self.assertEqual(q["H"]["후보"], 10, "분류할 수 없는 날은 분모에 넣지도 않는다")

    # ── 진입 자료 자체가 없는 날 ───────────────────────────────────────
    def test_entry_data_missing_days_are_recorded_separately(self):
        exp = [f"2027-01-{i + 4:02d}" for i in range(20)]
        days = [fake_day(d) for d in exp[:-1]]                       # 마지막 하루는 관측 자체가 없다
        q, hold, _ = T.quality_check(days, exp)
        self.assertEqual(q["일별"]["진입결측일"], [exp[-1]])
        self.assertEqual(q["일별"]["예정거래일"], 20)
        self.assertFalse(hold, "1/20 = 5.0% 는 임계와 같으므로 보류하지 않는다")

    def test_entry_data_missing_above_threshold_holds(self):
        exp = [f"2027-01-{i + 4:02d}" for i in range(20)]
        days = [fake_day(d) for d in exp[:-2]]
        q, hold, reasons = T.quality_check(days, exp)
        self.assertTrue(hold)
        self.assertTrue(any("진입 자료 결측 거래일 2/20" in r for r in reasons), reasons)
        self.assertEqual((q["H"]["후보"], q["H"]["시가결측"]), (180, 0),
                         "후보 분모를 모르므로 가격 결측률의 분모·분자에 섞지 않는다")

    def test_snapshot_missing_skip_row_counts_as_entry_missing(self):
        exp = ["2027-01-04", "2027-01-05"]
        days = [fake_day("2027-01-04"), {"date": "2027-01-05", "skip": "스냅샷없음"}]
        q, _, _ = T.quality_check(days, exp)
        self.assertEqual(q["일별"]["진입결측일"], ["2027-01-05"])

    def test_closed_day_file_is_not_an_expected_trading_day(self):
        days = [fake_day("2027-01-04"), {"date": "2027-01-09", "skip": "휴장일파일"}]
        q, hold, _ = T.quality_check(days)
        self.assertEqual((q["일별"]["예정거래일"], q["일별"]["진입결측일"], hold), (1, [], False))

    def test_expected_trading_days_follow_the_calendar_not_the_files(self):
        nt = load_nontrading()
        self.assertEqual(T.expected_trading_days("2026-10-06", "2026-10-13", nt),
                         ["2026-10-06", "2026-10-07", "2026-10-08", "2026-10-12", "2026-10-13"])
        self.assertEqual(T.expected_trading_days("2026-12-29", "2027-01-05", nt), ["2026-12-29", "2026-12-30"],
                         "달력 범위를 넘으면 평일로 추정하지 않고 멈춘다")


# ── 합성 스냅샷 ───────────────────────────────────────────────────────
HDR = ("itemcode,itemname,tradeAmount,nowPrice,openPrice,highPrice,"
       "prevChangeRate,topThemeNo,themeNos,tradeStopYn,manageStatusGb,marketAlertType")
BIG = T.MIN_BREAKOUT_TV * 2


def day_rows(open_px, amt_scale=1):
    """테마 5개. 각 테마는 대장(L)+추종(F). 대장 시가가 `open_px` 로 기록된다(= 전 거래일의 청산가).

    L1·L2 : 고가=현재가(고가유지 100%) → H.      L3 : 고가가 10% 위(90.9%) → B100c 지만 H 아님.
    L4    : 등락률 29.6% → 상한가 근접.          L5 : 시장경보 02.  둘 다 공통 기준에서 빠진다.
    """
    rows = []
    spec = [("L1", 1000, 1000, 5.0, "00"), ("L2", 1000, 1000, 5.0, "00"), ("L3", 1000, 1100, 5.0, "00"),
            ("L4", 1000, 1000, 29.6, "00"), ("L5", 1000, 1000, 5.0, "02")]
    for i, (code, price, high, rate, alert) in enumerate(spec, 1):
        op = open_px if code in ("L1", "L2") else (1000 if code == "L3" else 1000)
        rows.append([code, code, BIG * amt_scale, price, op, high, rate, f"T{i}", f"T{i}", "N", "0", alert])
        rows.append([f"F{i}", f"F{i}", BIG // 2 * amt_scale, 500, 500, 500, 1.0, f"T{i}", f"T{i}", "N", "0", "00"])
    return rows


def write_day(tmp, day, rows):
    for slot in ("1505", "1300"):
        with gzip.open(os.path.join(tmp, f"{day}_{slot}.csv.gz"), "wt", encoding="utf-8") as f:
            f.write(f"#meta,slot={slot}\n{HDR}\n")
            for r in rows:
                f.write(",".join(str(x) for x in r) + "\n")


class SyntheticStudy(unittest.TestCase):
    EXPLORE_OPEN, CONFIRM_OPEN = 1100, 1123          # 대장 수익률 +10.000% / +12.300%

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp()
        snap = os.path.join(ROOT, "data", "market_snapshot")
        shutil.copy(os.path.join(snap, "nontrading.txt"), cls.tmp)
        with open(os.path.join(snap, "calendar_scope.json"), encoding="utf-8") as f:
            meta = json.load(f)
        meta["end"] = "2027-12-31"                    # 합성 시험용 — 2027 휴장은 넣지 않는다
        with open(os.path.join(cls.tmp, "calendar_scope.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)
        nt = load_nontrading(cls.tmp)
        days = ["2026-09-28"]
        while len(days) < 5 + 62:                     # 탐색 5일(9/28~10/2) + 확증 62일
            days.append(next_trading_day(days[-1], nt))
        cls.days = days
        cls.confirm_idx = days.index(T.CONFIRM_FROM)
        for i, d in enumerate(days):
            # d 의 시가 = 전 거래일 진입분의 청산가. 확증 진입일(≥10/6)의 청산은 CONFIRM_OPEN.
            px = cls.EXPLORE_OPEN if i <= cls.confirm_idx else cls.CONFIRM_OPEN
            if i == len(days) - 1:                    # 마지막 날 시가는 61번째 유효일의 청산가 — 극단값(+20%)
                px = 1200
            write_day(cls.tmp, d, day_rows(px))
        cls.nt = nt

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def collect(self, upto=None, tmp=None):
        days, _ = T.collect(tmp or self.tmp, with_returns=True)
        return days if upto is None else [d for d in days if d["date"] <= upto]

    def test_confirmation_starts_on_the_first_trading_day_after_the_holiday(self):
        self.assertEqual(self.days[self.confirm_idx], "2026-10-06")
        self.assertNotIn("2026-10-05", self.days)

    def test_layers_use_the_same_risk_rule_and_h_is_a_subset(self):
        d = self.collect("2026-09-28")[0]
        self.assertEqual((d["hold"]["B100"], d["hold"]["H"], d["hold"]["H2"]), (3, 2, 2))
        self.assertEqual(d["hold"]["risk_excl"]["상한가근접"], 1)
        self.assertEqual(d["hold"]["risk_excl"]["시장경보"], 1)
        self.assertEqual((d["hold"]["B100raw"], d["hold"]["Hraw"]), (5, 4))

    def test_returns_are_computed_for_both_layers(self):
        d = self.collect("2026-09-29")[0]
        self.assertAlmostEqual(d["returns_hold"]["H"]["평균"], 0.10, places=9)
        self.assertAlmostEqual(d["returns_hold"]["B100"]["평균"], 0.20 / 3, places=9)

    def test_valid_days_exclude_the_unmatured_last_day(self):
        days = self.collect()
        valid, bad = T.valid_hold_days(days)
        self.assertEqual(len(valid), 61)
        self.assertEqual(bad.get("익일미성숙"), 1)
        self.assertTrue(all(v["date"] >= T.CONFIRM_FROM for v in valid))

    def test_no_signal_day_is_not_a_valid_comparison_day(self):
        days = [dict(d) for d in self.collect()]
        k = next(i for i, d in enumerate(days) if d["date"] == "2026-10-07")
        days[k] = dict(days[k], hold=dict(days[k]["hold"], H=0))
        valid, bad = T.valid_hold_days(days)
        self.assertEqual(len(valid), 60)
        self.assertEqual(bad.get("H무신호"), 1)

    # ── 결과 접근 통제 ───────────────────────────────────────────────
    def test_report_before_sixty_valid_days_prints_no_confirmation_numbers(self):
        # 탐색 5일 + 확증 21일(마지막은 미성숙) 만 남긴 사본 — 성숙 25일이라 Stage 1
        tmp = tempfile.mkdtemp()
        try:
            for fn in ("nontrading.txt", "calendar_scope.json"):
                shutil.copy(os.path.join(self.tmp, fn), tmp)
            for d in self.days[: 5 + 21]:
                for slot in ("1505", "1300"):
                    shutil.copy(os.path.join(self.tmp, f"{d}_{slot}.csv.gz"), tmp)
            out = T.report(tmp, today="2026-10-20")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        self.assertIn("유효 비교일 20 / 60", out)
        self.assertIn("확증 구간 수익률은 유효 비교일이 60일이 되기 전에는 열람하지 않는다", out)
        for leaked in ("12.300", "8.200", "+4.100", "+4.1", "주 평가", "분류:"):
            self.assertNotIn(leaked, out, f"확증 구간 숫자가 새어 나왔다: {leaked}")
        self.assertIn("+10.000%", out, "탐색 구간 숫자는 보인다")
        self.assertIn("P1(유효 20일) 도달", out)
        self.assertIn("확증 구간 21일은 아래 평균에 **넣지 않았다**", out)

    def test_old_abc_table_excludes_confirmation_days(self):
        tmp = tempfile.mkdtemp()
        try:
            for fn in ("nontrading.txt", "calendar_scope.json"):
                shutil.copy(os.path.join(self.tmp, fn), tmp)
            for d in self.days[: 5 + 21]:
                for slot in ("1505", "1300"):
                    shutil.copy(os.path.join(self.tmp, f"{d}_{slot}.csv.gz"), tmp)
            out = T.report(tmp, today="2026-10-20")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
        table = out.split("## 📈 Stage 1")[1].split("## 🧭")[0]
        self.assertNotIn("12.300", table)
        self.assertIn("| A | 5 |", table, "탐색 5일만 집계")

    def test_evaluation_uses_first_sixty_valid_days_only(self):
        out = T.report(self.tmp, today="2026-12-31")
        self.assertIn("유효 비교일 61 / 60", out)
        self.assertIn("주 평가", out)
        self.assertIn("첫 60일", out)
        # D = 12.3% − (12.3+12.3+0)/3 = +4.100% ; 61번째 날(+20%)이 섞이면 값이 달라진다
        self.assertIn("**+4.100%**", out)
        self.assertIn("후보 격상 검토 가능", out)
        self.assertIn("자동 승인이 아니다", out)

    def test_evaluation_object_is_a_fixed_sample(self):
        valid, _ = T.valid_hold_days(self.collect())
        ev = T.evaluate_hold(valid)
        self.assertEqual(ev["n"], T.EVAL_VALID_DAYS)
        self.assertEqual(ev["끝날"], valid[T.EVAL_VALID_DAYS - 1]["date"])
        self.assertAlmostEqual(ev["D"], 0.041, places=9)
        self.assertAlmostEqual(ev["H_비용후"], 0.123 - COST, places=9)
        self.assertIsNone(T.evaluate_hold(valid[: T.EVAL_VALID_DAYS - 1]))

    # ── 120 거래일 상한 ──────────────────────────────────────────────
    def test_cap_excludes_entries_after_the_cap_date(self):
        days = self.collect()
        cap, _ = T.trading_day_n(T.CONFIRM_FROM, 40, self.nt)
        valid, bad = T.valid_hold_days(days, T.CONFIRM_FROM, cap)
        self.assertEqual(len(valid), 40)
        self.assertTrue(all(v["date"] <= cap for v in valid))
        self.assertGreaterEqual(bad.get("상한초과", 0), 20)
        self.assertIsNone(T.evaluate_hold(valid))

    def test_after_the_cap_the_study_ends_even_if_sixty_valid_days_pile_up_later(self):
        out = T.report(self.tmp, today="2026-12-31", cap_days=40)
        self.assertIn("자료 부족 종료", out)
        self.assertIn("평가하지 않는다", out)
        for leaked in ("주 평가", "분류:", "+4.100", "12.300"):
            self.assertNotIn(leaked, out)
        self.assertIn("유효 비교일 40 / 60", out)

    def test_cap_that_still_contains_sixty_valid_days_evaluates(self):
        out = T.report(self.tmp, today="2026-12-31", cap_days=60)
        self.assertIn("주 평가", out)
        self.assertNotIn("자료 부족 종료", out)

    def test_cap_boundary_day_itself_is_included(self):
        days = self.collect()
        cap, _ = T.trading_day_n(T.CONFIRM_FROM, 10, self.nt)
        valid, _ = T.valid_hold_days(days, T.CONFIRM_FROM, cap)
        self.assertEqual(valid[-1]["date"], cap)
        self.assertEqual(len(valid), 10)

    def test_report_shows_quality_rows_on_evaluation(self):
        out = T.report(self.tmp, today="2026-12-31")
        self.assertIn("품질: H 후보", out)
        self.assertIn("품질: B100c 후보", out)
        self.assertNotIn("품질 보류", out)

    def test_default_cli_has_no_override_flag(self):
        with open(os.path.join(ROOT, "hyeoks_theme_abc.py"), encoding="utf-8") as f:
            src = f.read()
        main = src.split("def main():")[1]
        self.assertNotIn("confirm", main.lower())

    def test_calendar_scope_of_the_synthetic_dir_is_not_production(self):
        self.assertEqual(scan_dates(self.tmp)[0], "2026-09-28")


class SyntheticGaps(unittest.TestCase):
    """실제 스냅샷 파일을 지웠을 때 — build_day → valid_hold_days → evaluate_hold 전체 경로."""
    N_CONFIRM = 80

    @classmethod
    def setUpClass(cls):
        cls.base = tempfile.mkdtemp()
        snap = os.path.join(ROOT, "data", "market_snapshot")
        shutil.copy(os.path.join(snap, "nontrading.txt"), cls.base)
        with open(os.path.join(snap, "calendar_scope.json"), encoding="utf-8") as f:
            meta = json.load(f)
        meta["end"] = "2027-12-31"
        with open(os.path.join(cls.base, "calendar_scope.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f)
        nt = load_nontrading(cls.base)
        days = ["2026-09-28"]
        while len(days) < 5 + cls.N_CONFIRM:
            days.append(next_trading_day(days[-1], nt))
        cls.days = days
        cls.ci = days.index(T.CONFIRM_FROM)
        for i, d in enumerate(days):
            write_day(cls.base, d, day_rows(1100 if i <= cls.ci else 1123))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.base, ignore_errors=True)

    def variant(self, drop_idx):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        for fn in os.listdir(self.base):
            shutil.copy(os.path.join(self.base, fn), tmp)
        for i in drop_idx:
            for slot in ("1505", "1300"):
                os.remove(os.path.join(tmp, f"{self.days[self.ci + i]}_{slot}.csv.gz"))
        return tmp

    def evaluate(self, tmp):
        days, _ = T.collect(tmp, with_returns=True)
        valid, _ = T.valid_hold_days(days)
        ev = T.evaluate_hold(valid, [d for d in days if d["date"] >= T.CONFIRM_FROM], load_nontrading(tmp))
        return ev, valid

    def test_complete_data_has_no_quality_findings(self):
        ev, valid = self.evaluate(self.variant([]))
        self.assertEqual(len(valid), self.N_CONFIRM - 1)
        self.assertFalse(ev["품질보류"])
        dq = ev["품질"]["일별"]
        self.assertEqual((dq["진입결측일"], dq["성숙후결측일"], dq["판정불가"]), ([], [], []))

    def test_one_missing_day_is_counted_on_both_sides_without_holding(self):
        tmp = self.variant([10])
        ev, valid = self.evaluate(tmp)
        dq = ev["품질"]["일별"]
        self.assertEqual(dq["진입결측일"], [self.days[self.ci + 10]])
        self.assertEqual(dq["성숙후결측일"], [self.days[self.ci + 9]], "지운 날의 앞날은 익일 파일이 없다")
        self.assertEqual(ev["품질"]["H"]["시가결측"], 2, "앞날 후보 2종목이 결측으로 센다")
        self.assertFalse(ev["품질보류"])
        self.assertEqual(len(valid), self.N_CONFIRM - 1 - 2)

    def test_many_missing_days_hold_the_verdict_and_say_why(self):
        tmp = self.variant([10, 20, 30, 40, 50])
        ev, _ = self.evaluate(tmp)
        self.assertEqual(len(ev["품질"]["일별"]["진입결측일"]), 5)
        self.assertEqual(len(ev["품질"]["일별"]["성숙후결측일"]), 5)
        self.assertEqual(ev["품질"]["H"]["시가결측"], 10)
        self.assertTrue(ev["품질보류"])
        self.assertEqual(ev["분류"], "품질 보류")
        self.assertTrue(any("진입 자료 결측 거래일 5/" in r for r in ev["품질사유"]), ev["품질사유"])

    def test_report_prints_the_dates_and_the_hold(self):
        out = T.report(self.variant([10, 20, 30, 40, 50]), today="2027-03-01")
        self.assertIn("분류: 품질 보류", out)
        self.assertIn(self.days[self.ci + 10], out)
        self.assertIn("진입 자료 결측 5일", out)
        self.assertIn("성숙 후 익일 파일 결측 5일", out)


if __name__ == "__main__":
    unittest.main()
