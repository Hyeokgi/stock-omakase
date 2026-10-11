# 대장 정의 v2 후보 구조 기록기 — 정의 고정·미래 파일 무관·수익률 경로 없음·공개 출력에 종목 없음을 확인하는 시험
"""docs/사전등록_2026-10-11_대장정의_v2_후보.md §2·§3 의 구현 시험."""
import csv
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
        self.assertEqual((V.STUDY_ID, V.START, V.N_T, V.N_T_SUB, V.SURGE_WINDOW, V.SURGE_MULT),
                         ("leader-def-v2", "2026-10-12", 20, 3, 10, 2.0))
        self.assertEqual(V.OUT.replace("\\", "/"), "data/research_runs/leader_def_v2_structure.csv")

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

    def put(self, d, theme=True, vol_hot=1000):
        rows = [stock("000001", 12.0, 3 * BIG, ["T1", "T9"], "T9"),
                stock("000002", 15.0, 2 * BIG, ["T1", "T9"], "T9", vol=vol_hot),
                stock("000003", 20.0, 9 * BIG, ["T9"], "T9"),
                stock("000004", 30.0, 2 * BIG, ["T2"], "T2"),
                stock("000005", 4.0, 1 * BIG, ["T2"], "T2")]
        write_gz(os.path.join(self.dir, f"{d}_1505.csv.gz"), ["#meta", f"capturedAt={d}T15:02:50+09:00"], STOCK_HDR, rows)
        if theme:
            write_gz(os.path.join(self.dir, f"{d}_1505_theme.csv.gz"), ["#meta", "themes=3"],
                     ["ranking", "code", "name", "changeRate"],
                     [{"ranking": "1", "code": "T1", "name": "t1", "changeRate": "9.0"},
                      {"ranking": "2", "code": "T2", "name": "t2", "changeRate": "8.0"},
                      {"ranking": "3", "code": "T9", "name": "t9", "changeRate": "0.5"}])

    def test_rows_start_at_S_and_count_structure(self):
        rows = V.collect(self.dir)
        self.assertEqual([r["date"] for r in rows], ["2026-10-12"])
        r = rows[0]
        self.assertEqual(r["status"], "기록")
        self.assertEqual((r["cur_pre"], r["cur"]), (2, 1), "현행: T9 대장 000003(20%) · T2 대장 000004 는 상한가 근접 제외")
        self.assertEqual((r["v2m_pre"], r["v2m"]), (3, 2), "v2M: T1→000002 · T2→000004(제외) · T9→000003")
        self.assertEqual((r["overlap"], r["v2m_only"], r["cur_only"]), (1, 1, 0))
        self.assertEqual((r["top3_cur_slots"], r["topN_cur_slots"]), (2, 2), "T1 자리에는 현행 대장이 없다")
        self.assertEqual(r["v2s_pre"], 0, "평소 거래량 그대로 — 급증 아님")
        self.assertEqual(r["v2s_short"], 0)

    def test_surge_passes_when_volume_doubles(self):
        self.put("2026-10-12", vol_hot=2000)
        r = V.collect(self.dir)[0]
        self.assertEqual((r["v2s_pre"], r["v2s"]), (1, 1))

    def test_earlier_rows_do_not_depend_on_later_files(self):
        before = V.collect(self.dir)
        self.put("2026-10-13", vol_hot=99999)
        self.put("2026-10-14", theme=False)
        after = V.collect(self.dir)
        self.assertEqual(after[0], before[0], "t 의 구조는 t 이후 파일과 무관해야 한다")
        self.assertEqual([r["status"] for r in after], ["기록", "기록", "결측(테마)"])

    def test_missing_scheduled_day_is_kept_as_a_row(self):
        os.remove(os.path.join(self.dir, "2026-10-12_1505.csv.gz"))
        os.remove(os.path.join(self.dir, "2026-10-12_1505_theme.csv.gz"))
        self.put("2026-10-13")
        rows = V.collect(self.dir)
        self.assertEqual([(r["date"], r["status"]) for r in rows], [("2026-10-12", "결측(둘다)"), ("2026-10-13", "기록")])

    def test_written_csv_has_counts_only(self):
        out = os.path.join(self.dir, "out", "s.csv")
        rows = V.collect(self.dir)
        V.write(rows, out)
        txt = pathlib.Path(out).read_text(encoding="utf-8")
        self.assertEqual(txt.splitlines()[0].split(","), V.FIELDS)
        for code in ("000001", "000002", "000003", "000004", "000005"):
            self.assertNotIn(code, txt, "공개 출력에 종목코드가 있으면 안 된다")
        V.write(V.collect(self.dir), out)
        self.assertEqual(pathlib.Path(out).read_text(encoding="utf-8"), txt, "같은 입력이면 같은 파일")


class Workflow(unittest.TestCase):
    def test_research_daily_runs_recorder_before_public_commit(self):
        wf = (ROOT / ".github/workflows/research_daily.yml").read_text(encoding="utf-8")
        i = wf.index("python hyeoks_leader_def_v2.py --record")
        self.assertLess(i, wf.index("💾 공개 요약 커밋"), "기록 파일이 공개 요약 커밋에 함께 들어가야 한다")
        self.assertIn("git add data/research_runs", wf)


if __name__ == "__main__":
    unittest.main()
