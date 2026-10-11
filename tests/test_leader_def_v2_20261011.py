# 대장 정의 v2 후보 구조 기록기 — 정의 고정·미래 파일 무관·수익률 경로 없음·공개 출력에 종목 없음을 확인하는 시험
"""docs/사전등록_2026-10-11_대장정의_v2_후보.md §2·§3 의 구현 시험."""
import csv
import datetime
import gzip
import io
import os
import pathlib
import shutil
import tempfile
import unittest

import hyeoks_leader_def_v2 as V
import stability_gate as G

ROOT = pathlib.Path(__file__).resolve().parent.parent
STOCK_HDR = ["itemcode", "itemname", "nowPrice", "highPrice", "prevChangeRate", "tradeVolume", "tradeAmount",
             "manageStatusGb", "tradeStopYn", "marketAlertType", "themeNos", "topThemeNo"]
BIG = 10_000_000_000   # 100억 — A 의 50억을 넘긴다


def stock(code, rate, amt, nos, top, vol=1000, alert="00"):
    return {"itemcode": code, "itemname": f"n{code}", "nowPrice": "1000", "highPrice": "1000",
            "prevChangeRate": str(rate), "tradeVolume": str(vol), "tradeAmount": str(amt),
            "manageStatusGb": "0", "tradeStopYn": "N", "marketAlertType": alert,
            "themeNos": "|".join(nos), "topThemeNo": top}


def write_gz(path, meta, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(meta)
    w.writerow(header)
    for r in rows:
        w.writerow([r.get(h, "") for h in header])
    with gzip.open(path, "wt", encoding="utf-8") as fp:
        fp.write(buf.getvalue())


def it(code, rate, amt, alert="00"):
    return {"code": code, "rate": rate, "amt": amt, "alert": alert, "P": 1000.0, "high": 1000.0}


class Locked(unittest.TestCase):
    def test_registered_constants(self):
        self.assertEqual((V.STUDY_ID, V.START, V.N_T, V.N_T_H, V.H_MIN_RATE, V.H_DAYS_TARGET, V.CAP,
                          V.SURGE_WINDOW, V.SURGE_MULT, V.CUTOFF, V.SLOT_CLOSE),
                         ("leader-def-v2", "2026-10-12", 20, 3, 15.0, 60, "2027-06-30", 10, 2.0, "15:05:00", (15, 25)))
        self.assertEqual(V.OUT.replace("\\", "/"), "data/research_runs/leader_def_v2_structure.csv")

    def test_slot_close_matches_the_collector_window(self):
        import hyeoks_market_snapshot as S
        self.assertEqual(S.SLOTS["1505"][1], V.SLOT_CLOSE)

    def test_not_a_fingerprint_target(self):
        for name in ("hyeoks_leader_def_v2.py", ".github/workflows/research_daily.yml"):
            self.assertNotIn(name, G.FINGERPRINT_FILES)

    def test_no_price_outcome_path(self):
        src = (ROOT / "hyeoks_leader_def_v2.py").read_text(encoding="utf-8")
        code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
        for word in ("openPrice", "exit_open", "day_returns", "layer_returns", "with_returns", "next_trading_snapshots"):
            self.assertNotIn(word, code, f"구조 기록기에 가격 결과 경로가 있으면 안 된다: {word}")


class Definitions(unittest.TestCase):
    def test_top_themes_by_change_rate_ties_by_code(self):
        th = [{"code": "9", "rate": 1.0}, {"code": "2", "rate": 5.0}, {"code": "1", "rate": 5.0}, {"code": "3", "rate": 3.0}]
        self.assertEqual(V.top_themes(th, 3), ["1", "2", "3"])

    def test_theme_nos_parsing(self):
        self.assertEqual(V.theme_nos({"themeNos": "5|7||8|"}), {"5", "7", "8"})
        self.assertEqual(V.theme_nos({"themeNos": ""}), set())

    def test_multi_membership_finds_leader_the_representative_theme_misses(self):
        rows = {"a": {"themeNos": "T|BIG"}, "b": {"themeNos": "T|BIG"}, "c": {"themeNos": "BIG"}}
        items = [it("a", 12.0, 3e10), it("b", 15.0, 2e10), it("c", 1.0, 9e10)]
        leaders, slots = V.build_v2m(items, rows, ["T"])
        self.assertEqual([x["code"] for x in leaders], ["b"])
        self.assertEqual((leaders[0]["theme"], leaders[0]["theme_rank"]), ("T", 1))
        self.assertEqual(slots, {"small": 0, "dominance": 0, "dup": 0})

    def test_small_dominance_and_duplicate_slots(self):
        rows = {"a": {"themeNos": "X|Y"}, "b": {"themeNos": "X|Y"}, "c": {"themeNos": "Z"},
                "d": {"themeNos": "D"}, "e": {"themeNos": "D"}}
        items = [it("a", 10.0, 2e10), it("b", 5.0, 1.5e10), it("c", 9.0, 2e10),
                 it("d", 3.0, 5e10), it("e", 8.0, 1e10)]
        leaders, slots = V.build_v2m(items, rows, ["X", "Y", "Z", "D"])
        self.assertEqual([x["code"] for x in leaders], ["a"], "Y 의 대장도 a — 하루 한 번만")
        self.assertEqual(slots, {"small": 1, "dominance": 1, "dup": 1})

    def test_surge_uses_ten_prior_days_and_counts_short_baseline(self):
        leaders = [it("s", 5, 1), it("n", 5, 1), it("m", 5, 1)]
        rows = {"s": {"tradeVolume": "200"}, "n": {"tradeVolume": "199"}, "m": {"tradeVolume": "999"}}
        prior = [{"s": 100.0, "n": 100.0, "m": 100.0} for _ in range(9)] + [{"s": 100.0, "n": 100.0}]
        passed, short = V.surge_split(leaders, rows, prior)
        self.assertEqual(([x["code"] for x in passed], short), (["s"], 1), "정확히 2배는 통과, 하루 값이 없으면 기준 부족")
        _, short9 = V.surge_split(leaders, rows, prior[1:])
        self.assertEqual(short9, 3, "직전 관측일이 10개 미만이면 모두 기준 부족")

    def test_user_set_threshold_is_inclusive(self):
        rows = {c: {"themeNos": "T"} for c in "ab"}
        passed, leaders, below = V.build_v2h([it("a", 15.0, 3e10), it("b", 1.0, 2e10)], rows, ["T"])
        self.assertEqual(([x["code"] for x in passed], below), (["a"], 0), "정확히 15% 는 통과")
        passed, leaders, below = V.build_v2h([it("a", 14.99, 3e10), it("b", 1.0, 2e10)], rows, ["T"])
        self.assertEqual((passed, [x["code"] for x in leaders], below), ([], ["a"], 1))

    def test_user_set_does_not_refill_a_theme_whose_leader_is_below(self):
        """대장(거래대금 상위 5 중 등락률 1위)이 15% 미만이면, 상위 5 밖의 25% 종목으로 채우지 않는다."""
        rates = [10.0, 9.0, 8.0, 7.0, 14.99, 25.0]
        items = [it(f"c{i}", r, (6 - i) * 1e10) for i, r in enumerate(rates)]
        rows = {x["code"]: {"themeNos": "T"} for x in items}
        passed, leaders, below = V.build_v2h(items, rows, ["T"])
        self.assertEqual((passed, [x["code"] for x in leaders], below), ([], ["c4"], 1))

    def test_unreadable_theme_rate_is_none_and_left_out_of_ranking(self):
        th = [{"code": "1", "rate": None}, {"code": "2", "rate": 1.0}]
        self.assertEqual(V.top_themes(th, 3), ["2"])
        self.assertIsNone(V._num(""))
        self.assertIsNone(V._num("abc"))
        self.assertEqual(V._num("1,234.5"), 1234.5)

    def test_default_as_of_waits_for_the_slot_window_to_close(self):
        at = lambda hm: datetime.datetime.fromisoformat(f"2026-10-12T{hm}:00+09:00")
        self.assertEqual(V.default_as_of(at("15:24")), "2026-10-11")
        self.assertEqual(V.default_as_of(at("15:25")), "2026-10-12")
        self.assertEqual(V.default_as_of(at("19:40")), "2026-10-12")


class Recording(unittest.TestCase):
    """합성 스냅샷 폴더 — 9/28~10/9 의 평일 (등록 휴장 없음, 달력 범위 파일 없음)."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.days = ["2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01", "2026-10-02",
                     "2026-10-05", "2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09",
                     "2026-10-12", "2026-10-13", "2026-10-14"]
        for d in self.days[:11]:
            self.put(d)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def put(self, d, theme=True, vol_hot=1000, stock_file=True, price_at="15:02:53", theme_at="15:01:17",
            t1_rate="9.0", extra_theme=None):
        rows = [stock("000001", 12.0, 3 * BIG, ["T1", "T9"], "T9"),
                stock("000002", 15.0, 2 * BIG, ["T1", "T9"], "T9", vol=vol_hot),
                stock("000003", 20.0, 9 * BIG, ["T9"], "T9"),
                stock("000004", 30.0, 2 * BIG, ["T2"], "T2"),
                stock("000005", 4.0, 1 * BIG, ["T2"], "T2")]
        meta = ["#meta", f"capturedAt={d}T15:02:50+09:00", "slot=1505"]
        if price_at:
            meta.append(f"priceReceivedAt={d}T{price_at}+09:00")
        meta.append(f"writtenAt={d}T15:04:40+09:00")
        if stock_file:
            write_gz(os.path.join(self.dir, f"{d}_1505.csv.gz"), meta, STOCK_HDR, rows)
        if theme:
            th = [{"code": "T1", "changeRate": t1_rate, "n": 2},
                  {"code": "T2", "changeRate": "8.0", "n": 2},
                  {"code": "T9", "changeRate": "0.5", "n": 3}] + ([extra_theme] if extra_theme else [])
            write_gz(os.path.join(self.dir, f"{d}_1505_theme.csv.gz"), ["#meta", f"capturedAt={d}T15:02:50+09:00"],
                     ["ranking", "code", "name", "changeRate", "risingCount", "fallingCount", "unchangedCount", "updatedAt"],
                     [{"ranking": str(i), "code": t["code"], "name": t["code"].lower(), "changeRate": t["changeRate"],
                       "risingCount": str(t["n"]), "fallingCount": "0", "unchangedCount": "0",
                       "updatedAt": f"{d}T{theme_at}+09:00"} for i, t in enumerate(th, 1)])

    def test_rows_start_at_S_and_count_structure(self):
        rows = V.collect(self.dir, as_of="2026-10-12")
        self.assertEqual([r["date"] for r in rows], ["2026-10-12"])
        r = rows[0]
        self.assertEqual(r["status"], "기록")
        self.assertEqual((r["price_at"], r["theme_at"]), ("2026-10-12T15:02:53+09:00", "2026-10-12T15:01:17+09:00"))
        self.assertEqual((r["theme_rate_fail"], r["member_unknown"]), (0, 0))
        self.assertEqual((r["cur_pre"], r["cur"]), (2, 1), "현행: T9 대장 000003(20%) · T2 대장 000004 는 상한가 근접 제외")
        self.assertEqual((r["v2h_pre"], r["v2h"], r["v2h_below"], r["v2h_overlap"]), (3, 2, 0, 1),
                         "사용자 기준: 상위 3 테마 대장 000002(15%)·000004(30%, 제외)·000003(20%) — 현행과 겹침 000003")
        self.assertEqual((r["v2m_pre"], r["v2m"]), (3, 2), "v2M: T1→000002 · T2→000004(제외) · T9→000003")
        self.assertEqual((r["overlap"], r["v2m_only"], r["cur_only"]), (1, 1, 0))
        self.assertEqual((r["top3_cur_slots"], r["topN_cur_slots"]), (2, 2), "T1 자리에는 현행 대장이 없다")
        self.assertEqual(r["v2s_pre"], 0, "평소 거래량 그대로 — 급증 아님")
        self.assertEqual(r["v2s_short"], 0)

    def test_surge_passes_when_volume_doubles(self):
        self.put("2026-10-12", vol_hot=2000)
        r = V.collect(self.dir, as_of="2026-10-12")[0]
        self.assertEqual((r["v2s_pre"], r["v2s"]), (1, 1))

    def test_earlier_rows_do_not_depend_on_later_files(self):
        before = V.collect(self.dir, as_of="2026-10-14")
        self.put("2026-10-13", vol_hot=99999)
        self.put("2026-10-14", theme=False)
        after = V.collect(self.dir, as_of="2026-10-14")
        self.assertEqual(after[0], before[0], "t 의 구조는 t 이후 파일과 무관해야 한다")
        self.assertEqual([r["status"] for r in after], ["기록", "기록", "결측(테마)"])

    def test_missing_scheduled_day_is_kept_as_a_row(self):
        os.remove(os.path.join(self.dir, "2026-10-12_1505.csv.gz"))
        os.remove(os.path.join(self.dir, "2026-10-12_1505_theme.csv.gz"))
        self.put("2026-10-13")
        rows = V.collect(self.dir, as_of="2026-10-13")
        self.assertEqual([(r["date"], r["status"]) for r in rows], [("2026-10-12", "결측(둘다)"), ("2026-10-13", "기록")])

    def test_days_after_the_last_file_are_rows_up_to_as_of(self):
        """마지막 파일 뒤로 계속 빠진 날, 테마 파일만 있는 날도 행이 된다 (Codex 의견 ②)."""
        self.put("2026-10-13", stock_file=False)
        rows = V.collect(self.dir, as_of="2026-10-15")
        self.assertEqual([(r["date"], r["status"]) for r in rows],
                         [("2026-10-12", "기록"), ("2026-10-13", "결측(종목)"),
                          ("2026-10-14", "결측(둘다)"), ("2026-10-15", "결측(둘다)")])
        self.assertEqual(V.collect(self.dir, as_of="2026-10-11"), [], "기준일이 S 전이면 행이 없다")

    def test_late_or_unknown_times_are_not_observation_days(self):
        cases = [(dict(price_at="15:05:00"), "기록"),
                 (dict(price_at="15:05:01"), "시각초과"),
                 (dict(theme_at="15:05:01"), "시각초과"),
                 (dict(price_at=""), "시각불명"),
                 (dict(theme_at="bad"), "시각불명")]
        for kw, want in cases:
            self.put("2026-10-12", **kw)
            self.assertEqual(V.collect(self.dir, as_of="2026-10-12")[0]["status"], want, kw)

    def test_unreadable_theme_rate_marks_the_day_incomplete(self):
        self.put("2026-10-12", t1_rate="")
        r = V.collect(self.dir, as_of="2026-10-12")[0]
        self.assertEqual((r["status"], r["theme_rate_fail"]), ("불완전(테마등락률)", 1))

    def test_top_theme_with_no_member_found_is_a_lookup_failure_not_small(self):
        self.put("2026-10-12", extra_theme={"code": "T5", "changeRate": "12.0", "n": 7})
        r = V.collect(self.dir, as_of="2026-10-12")[0]
        self.assertEqual((r["status"], r["member_unknown"]), ("불완전(소속)", 1))

    def test_summary_counts_user_set_days_toward_the_target(self):
        self.put("2026-10-13")
        self.put("2026-10-14", price_at="15:06:00")
        txt = V.summary(V.collect(self.dir, as_of="2026-10-14"))
        self.assertIn("관측일 2일", txt)
        self.assertIn("시각초과 1일", txt)
        self.assertIn(f"사용자 기준 대장이 있는 관측일 2/{V.H_DAYS_TARGET}", txt)

    def test_written_csv_has_counts_only(self):
        out = os.path.join(self.dir, "out", "s.csv")
        rows = V.collect(self.dir, as_of="2026-10-12")
        V.write(rows, out)
        txt = pathlib.Path(out).read_text(encoding="utf-8")
        self.assertEqual(txt.splitlines()[0].split(","), V.FIELDS)
        for code in ("000001", "000002", "000003", "000004", "000005"):
            self.assertNotIn(code, txt, "공개 출력에 종목코드가 있으면 안 된다")
        V.write(V.collect(self.dir, as_of="2026-10-12"), out)
        self.assertEqual(pathlib.Path(out).read_text(encoding="utf-8"), txt, "같은 입력이면 같은 파일")


class Workflow(unittest.TestCase):
    def test_research_daily_runs_recorder_before_public_commit(self):
        wf = (ROOT / ".github/workflows/research_daily.yml").read_text(encoding="utf-8")
        i = wf.index("python hyeoks_leader_def_v2.py --record")
        self.assertLess(i, wf.index("💾 공개 요약 커밋"), "기록 파일이 공개 요약 커밋에 함께 들어가야 한다")
        self.assertIn("git add data/research_runs", wf)


if __name__ == "__main__":
    unittest.main()
