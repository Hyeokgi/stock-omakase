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

    def test_default_cli_has_no_override_flag(self):
        with open(os.path.join(ROOT, "hyeoks_theme_abc.py"), encoding="utf-8") as f:
            src = f.read()
        main = src.split("def main():")[1]
        self.assertNotIn("confirm", main.lower())

    def test_calendar_scope_of_the_synthetic_dir_is_not_production(self):
        self.assertEqual(scan_dates(self.tmp)[0], "2026-09-28")


if __name__ == "__main__":
    unittest.main()
