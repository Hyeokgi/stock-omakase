"""Feature Store — 미래를 담지 않는가, 조인 불가 값을 박는가, 스캐너를 죽이지 않는가."""
import csv
import gzip
import os
import tempfile
import unittest
import feature_store as F


def row(name, code, v1=None, v2=None, theme="", n=35):
    r = [""] * n
    r[0], r[1], r[19] = name, code, theme
    if v1 is not None:
        r[29] = str(v1)
    if v2 is not None:
        r[31] = str(v2)
    r[33] = "77"                      # rs
    return r


RESULTS = [row("가", "'000001", v1=95, theme="반도체"),
           row("나", "000002", v1=70, v2=61, theme="2차전지"),
           row("다", "000003", v1=40, theme="")]


class ShapeTests(unittest.TestCase):
    def test_header_is_context_then_result(self):
        self.assertEqual(F.HEADER[:len(F.CONTEXT_FIELDS)], F.CONTEXT_FIELDS)
        self.assertEqual(len(F.HEADER), len(F.CONTEXT_FIELDS) + len(F.RESULT_FIELDS))

    def test_no_future_columns(self):
        """🔴 선정 시점에 존재할 수 없는 값은 한 칸도 없어야 한다."""
        banned = ("ret", "alpha", "수익", "t+", "T+", "pnl", "future", "outcome")
        for h in F.HEADER:
            for b in banned:
                self.assertNotIn(b.lower(), h.lower(), f"미래 열 의심: {h}")

    def test_code_normalized(self):
        rows = F.build_rows("2026-09-18", RESULTS)
        idx = F.HEADER.index("code")
        self.assertEqual([r[idx] for r in rows], ["000001", "000002", "000003"])

    def test_broken_row_skipped_others_kept(self):
        rows = F.build_rows("2026-09-18", [RESULTS[0], ["짧음"], RESULTS[1]])
        self.assertEqual(len(rows), 2)


class PointInTimeTests(unittest.TestCase):
    """나중에 조인할 수 없는 넷을 지금 박는가."""

    def rows(self):
        return F.build_rows(
            "2026-09-18", RESULTS,
            static_db={"000002": True},
            theme_rank={"반도체": 1, "2차전지": 5},
            theme_hist_max={"반도체": 12.3},
            picked={"000001": "차트TOP2"},
            candidate_codes=["000001", "000002"],
            gate_codes=["000002"],
            kospi_rate=-0.85, warning_market=True, index_above_ma5=False)

    def col(self, rows, name):
        return [r[F.HEADER.index(name)] for r in rows]

    def test_is_junk_embedded(self):
        """DB_정적데이터는 매일 덮어쓰인다 — 나중에 조인하면 오늘 명단이 과거를 물들인다."""
        self.assertEqual(self.col(self.rows(), "is_junk"), ["UNKNOWN", "Y", "UNKNOWN"])

    def test_theme_rank_and_hist_max_embedded(self):
        r = self.rows()
        self.assertEqual(self.col(r, "theme_rank"), [1, 5, ""])
        self.assertEqual(self.col(r, "theme_hist_max"), [12.3, "", ""])

    def test_rs_percentile_flag(self):
        """r[33]은 스캔 후 백분위로 덮어써진다 — 어느 쪽인지 남겨야 한다."""
        self.assertEqual(self.col(self.rows(), "rs_is_percentile"), ["Y", "Y", "Y"])
        rows = F.build_rows("2026-09-18", RESULTS, rs_is_percentile=False)
        self.assertEqual(self.col(rows, "rs_is_percentile"), ["N", "N", "N"])

    def test_pool_membership_recorded(self):
        r = self.rows()
        self.assertEqual(self.col(r, "in_candidate_pool"), ["Y", "Y", "N"])
        self.assertEqual(self.col(r, "gate_passed"), ["N", "Y", "N"])
        self.assertEqual(self.col(r, "picked_by"), ["차트TOP2", "", ""])

    def test_market_context_on_every_row(self):
        r = self.rows()
        self.assertTrue(all(x == -0.85 for x in self.col(r, "kospi_rate")))
        self.assertTrue(all(x is True for x in self.col(r, "warning_market")))

    def test_picked_matches_even_with_apostrophe(self):
        rows = F.build_rows("2026-09-18", RESULTS, picked={"1": "차트TOP2"})
        self.assertEqual(rows[0][F.HEADER.index("picked_by")], "차트TOP2")


class WriteTests(unittest.TestCase):
    def tmp(self):
        return tempfile.mkdtemp()

    def test_writes_gzip_with_header(self):
        d = self.tmp()
        ok, _ = F.record("2026-09-18", RESULTS, root=d)
        self.assertTrue(ok)
        with gzip.open(F.path_for("2026-09-18", d), "rt", encoding="utf-8") as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[0], F.HEADER)
        self.assertEqual(len(rows), 1 + len(RESULTS))

    def test_does_not_overwrite_existing_day(self):
        d = self.tmp()
        F.record("2026-09-18", RESULTS, root=d)
        ok, msg = F.record("2026-09-18", RESULTS, root=d)
        self.assertFalse(ok)
        self.assertIn("이미", msg)

    def test_no_partial_file_left_on_success(self):
        d = self.tmp()
        F.record("2026-09-18", RESULTS, root=d)
        self.assertEqual([f for f in os.listdir(d) if f.endswith(".tmp")], [])

    # 🔴 가장 중요 — 관측을 지키려다 수집을 잃으면 안 된다
    def test_never_raises_on_unwritable_path(self):
        ok, msg = F.record("2026-09-18", RESULTS, root="/proc/nope/cannot")
        self.assertFalse(ok)
        self.assertTrue(msg.startswith("기록 실패"), msg)

    def test_never_raises_on_garbage(self):
        for bad in (None, 123, [None], [[]], "문자열"):
            try:
                ok, _ = F.record("2026-09-18", bad, root=self.tmp())
                self.assertFalse(ok)
            except Exception as e:                      # noqa: BLE001
                self.fail(f"예외가 밖으로 나왔다: {e!r}")

    def test_does_not_mutate_results(self):
        """관측만 추가한다 — 입력을 건드리지 않는다."""
        import copy
        before = copy.deepcopy(RESULTS)
        F.record("2026-09-18", RESULTS, root=self.tmp())
        self.assertEqual(RESULTS, before)


class CallSiteTests(unittest.TestCase):
    """omakase 호출부와 **같은 모양**으로 부른다.

    오늘까지 같은 부류의 배선 사고를 네 번 겪었다 — 순수 함수는 통과하는데
    생산 경로에서 죽거나(미import·미정의 변수) 조용히 빈칸이 되는 경우.
    """

    def test_call_site_shape(self):
        d = tempfile.mkdtemp()
        results = [row(f"종목{i}", f"{i:06d}", v1=100 - i, v2=50 + i,
                       theme="반도체" if i % 2 else "")
                   for i in range(1, 8)]
        candidate_pool = results[:5]
        gate_passed = results[:3]
        # omakase 의 new_rows: [tid, date, channel, name, "'code", ...]
        new_rows = [["t1", "2026-09-18", "차트TOP2", "종목1", "'000001"],
                    ["t2", "2026-09-18", "수급TOP2", "종목2", "'000002"],
                    ["t3", "2026-09-18", "랜덤2", "종목7", "'000007"]]
        picked = {}
        for nr in new_rows:
            picked.setdefault(str(nr[4]), nr[2])

        ok, msg = F.record(
            "2026-09-18", results, picked=picked, run_id="999", root=d,
            kospi_rate=0.84, warning_market=True, index_above_ma5=True,
            static_db={"000004": {'is_junk': True}}, theme_rank={"반도체": 2},
            theme_hist_max={"반도체": 9.9},
            candidate_codes=[r[1] for r in candidate_pool],
            gate_codes=[r[1] for r in gate_passed])
        self.assertTrue(ok, msg)

        with gzip.open(F.path_for("2026-09-18", d), "rt", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 7)
        # 🔴 picked_by 가 실제로 채워지는가 (아포스트로피 정규화)
        by = {r["code"]: r["picked_by"] for r in rows}
        self.assertEqual(by["000001"], "차트TOP2")
        self.assertEqual(by["000002"], "수급TOP2")
        self.assertEqual(by["000007"], "랜덤2")
        self.assertEqual(by["000003"], "")
        # 풀 소속
        self.assertEqual([r["in_candidate_pool"] for r in rows],
                         ["Y"] * 5 + ["N"] * 2)
        self.assertEqual([r["gate_passed"] for r in rows], ["Y"] * 3 + ["N"] * 4)
        # 조인 불가 값
        self.assertEqual(by and rows[3]["is_junk"], "Y")      # 000004
        self.assertEqual(rows[0]["theme_rank"], "2")
        # 피처가 실제로 실렸는가
        self.assertEqual(rows[0]["v1_score"], "99")
        self.assertEqual(rows[0]["name"], "종목1")

    def test_missing_context_still_records(self):
        """맥락 인자가 없어도 죽지 않고 남긴다 — 관측이 최우선이다."""
        d = tempfile.mkdtemp()
        ok, msg = F.record("2026-09-19", RESULTS, root=d)
        self.assertTrue(ok, msg)


if __name__ == "__main__":
    unittest.main()
