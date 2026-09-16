"""순위 풀 적재 — 관측만 추가하고, 무슨 일이 있어도 스캐너를 죽이지 않는다."""
import csv
import os
import tempfile
import unittest
import rank_pool as R


def row(name, code, v1=None, v2=None, n=36):
    r = [""] * n
    r[R.IDX_NAME], r[R.IDX_CODE] = name, code
    if v1 is not None:
        r[R.IDX_V1] = str(v1)
    if v2 is not None:
        r[R.IDX_V2] = str(v2)
    return r


RANKED = [row("가", "000001", v1=95), row("나", "000002", v1=88),
          row("다", "000003", v1=70), row("라", "000004", v1=52)]


class BuildTests(unittest.TestCase):
    def test_keeps_caller_order_does_not_resort(self):
        """호출부가 쓴 순서를 그대로 남긴다 — '그때 무엇을 봤는가' 가 보존돼야 한다."""
        shuffled = [RANKED[2], RANKED[0], RANKED[1]]
        rows = R.build_rows("2026-09-16", "차트TOP2", shuffled,
                            ["000003"], R.IDX_V1)
        self.assertEqual([r[3] for r in rows], ["000003", "000001", "000002"])
        self.assertEqual([r[2] for r in rows], [1, 2, 3])

    def test_marks_picked(self):
        rows = R.build_rows("2026-09-16", "차트TOP2", RANKED,
                            ["000001", "000002"], R.IDX_V1)
        self.assertEqual([r[6] for r in rows], ["Y", "Y", "N", "N"])

    def test_picked_code_normalized(self):
        """시트는 아포스트로피를 붙이고 앞 0 을 지운다 — 그래도 맞춰야 한다."""
        rows = R.build_rows("2026-09-16", "차트TOP2", RANKED, ["'1"], R.IDX_V1)
        self.assertEqual(rows[0][6], "Y")

    def test_band_marks_ineligible_without_dropping_them(self):
        """🔴 밴드가 켜지면 실제 픽이 원점수 1위가 아니다.

        원점수 순위는 그대로 남기되 적격 여부와 사유를 같이 남겨야
        '현행 정책에서의 3위'와 '필터 전 원점수 3위'를 갈라 볼 수 있다.
        """
        rows = R.build_rows("2026-09-16", "수급TOP2", RANKED, ["000002"],
                            R.IDX_V1, eligible_codes=["000002", "000003"],
                            exclusion="V2_BAND")
        self.assertEqual([r[3] for r in rows], ["000001", "000002", "000003", "000004"])
        self.assertEqual([r[7] for r in rows], ["N", "Y", "Y", "N"])   # eligible
        self.assertEqual(rows[0][8], "V2_BAND")                        # exclusion
        self.assertEqual(rows[1][8], "")
        self.assertTrue(all(r[10] == 2 for r in rows))                 # eligible_size
        self.assertEqual(rows[1][6], "Y")                              # 2위가 실제 픽

    def test_no_filter_means_everything_eligible(self):
        rows = R.build_rows("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1)
        self.assertTrue(all(r[7] == "Y" and r[8] == "" for r in rows))
        self.assertTrue(all(r[10] == 4 for r in rows))

    def test_records_pool_size_not_just_top_n(self):
        """3~5위 질문에 답하려면 **전체 풀 크기**를 알아야 한다."""
        rows = R.build_rows("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1, top_n=2)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(r[9] == 4 for r in rows))

    def test_broken_row_skipped_others_kept(self):
        rows = R.build_rows("2026-09-16", "차트TOP2",
                            [RANKED[0], ["짧은행"], RANKED[1]], [], R.IDX_V1)
        self.assertEqual([r[3] for r in rows], ["000001", "000002"])

    def test_score_column_differs_by_channel(self):
        supply = [row("가", "000001", v2=61), row("나", "000002", v2=44)]
        rows = R.build_rows("2026-09-16", "수급TOP2", supply, [], R.IDX_V2)
        self.assertEqual([r[5] for r in rows], [61.0, 44.0])

    def test_code_sha_recorded(self):
        rows = R.build_rows("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1,
                            sha="abc12345")
        self.assertTrue(all(r[12] == "abc12345" for r in rows))


class RecordTests(unittest.TestCase):
    def tmp(self):
        return os.path.join(tempfile.mkdtemp(), "pool.csv")

    def test_writes_header_once(self):
        p = self.tmp()
        R.record("2026-09-16", "차트TOP2", RANKED, ["000001"], R.IDX_V1, path=p)
        R.record("2026-09-17", "차트TOP2", RANKED, ["000001"], R.IDX_V1, path=p)
        with open(p, encoding="utf-8") as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[0], R.HEADER)
        self.assertEqual(sum(1 for r in rows if r == R.HEADER), 1)

    def test_rerun_same_day_does_not_duplicate(self):
        p = self.tmp()
        ok1, _ = R.record("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1, path=p)
        ok2, msg = R.record("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1, path=p)
        self.assertTrue(ok1)
        self.assertFalse(ok2)
        self.assertIn("이미 기록됨", msg)

    def test_same_day_other_channel_still_recorded(self):
        p = self.tmp()
        R.record("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1, path=p)
        ok, _ = R.record("2026-09-16", "수급TOP2", RANKED, [], R.IDX_V2, path=p)
        self.assertTrue(ok)

    def test_empty_pool_is_not_an_error(self):
        ok, msg = R.record("2026-09-16", "차트TOP2", [], [], R.IDX_V1, path=self.tmp())
        self.assertFalse(ok)
        self.assertNotIn("기록 실패", msg)

    # 🔴 이 검사가 가장 중요하다 — 관측을 지키려다 수집을 잃으면 안 된다
    def test_never_raises_on_unwritable_path(self):
        ok, msg = R.record("2026-09-16", "차트TOP2", RANKED, [], R.IDX_V1,
                           path="/proc/nope/cannot/write.csv")
        self.assertFalse(ok)
        self.assertTrue(msg.startswith("기록 실패"), msg)

    def test_never_raises_on_garbage_input(self):
        for bad in (None, 123, [None], [[]]):
            try:
                ok, msg = R.record("2026-09-16", "차트TOP2", bad, [], R.IDX_V1,
                                   path=self.tmp())
                self.assertFalse(ok)
            except Exception as e:                       # noqa: BLE001
                self.fail(f"예외가 밖으로 나왔다: {e!r}")


class CallSiteTests(unittest.TestCase):
    """omakase 호출부와 **같은 모양**으로 부른다.

    오늘 세 번 같은 부류의 사고가 났다 — 순수 함수는 통과하는데 생산 경로에서
    죽는 경우(degraded_msg NameError, as_of 타입, hyeoks_tajeom 미import).
    그래서 호출부를 그대로 흉내 낸다.
    """

    def test_policy_id_is_per_channel_not_global(self):
        """🔴 2026-09-16 회귀 — 처음엔 모든 채널에 `oversold-veto-v2` 를 박았다.

        그건 과매도 태그를 다루는 **리포트 중기 채널의 모수**이고 차트·수급 선정과
        아무 상관이 없다. U3("정책 동일성은 채널별")을 스스로 어긴 것이었다.
        """
        chart = R.policy_id("차트TOP2")
        supply = R.policy_id("수급TOP2")
        self.assertNotIn("oversold", chart)
        self.assertNotIn("oversold", supply)
        self.assertNotEqual(chart, supply)
        self.assertIn("v1", chart)
        self.assertIn("v2", supply)

    def test_policy_id_changes_when_band_switch_flips(self):
        """스위치를 켜고 정책 ID 가 안 바뀌면 그게 조용한 정책 변경이다."""
        self.assertNotEqual(R.policy_id("수급TOP2"),
                            R.policy_id("수급TOP2", "45-79"))

    def test_call_site_shape(self):
        import os as _os, tempfile
        path = _os.path.join(tempfile.mkdtemp(), "pool.csv")
        # omakase.py:3129 candidate_pool 과 같은 모양 (35칸 이상)
        pool = [row(f"종목{i}", f"{i:06d}", v1=100 - i * 3) for i in range(1, 8)]
        gate = pool[:4]
        chart_top2 = sorted(pool, key=lambda x: x[29], reverse=True)[:2]
        supply_top2 = sorted(gate, key=lambda x: x[31], reverse=True)[:2]
        for ch, ranked, picked, sidx in (
                ("차트TOP2", sorted(pool, key=lambda x: x[29], reverse=True),
                 [r[1] for r in chart_top2], 29),
                ("수급TOP2", sorted(gate, key=lambda x: x[31], reverse=True),
                 [r[1] for r in supply_top2], 31)):
            ok, msg = R.record("2026-09-16", ch, ranked, picked, sidx,
                               run_id="123", sha="deadbeef", path=path)
            self.assertTrue(ok, msg)
        with open(path, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        chart = [r for r in rows if r["channel"] == "차트TOP2"]
        self.assertEqual(len(chart), 7)
        self.assertEqual([r["picked"] for r in chart][:2], ["Y", "Y"])
        self.assertTrue(all(r["picked"] == "N" for r in chart[2:]))
        self.assertTrue(all(r["policy_id"].startswith("chart-top2")
                            for r in chart))

    def test_selection_is_not_touched(self):
        """관측만 추가한다 — record 가 입력 목록을 바꾸지 않는다."""
        import copy, os as _os, tempfile
        pool = [row(f"종목{i}", f"{i:06d}", v1=100 - i) for i in range(1, 6)]
        before = copy.deepcopy(pool)
        R.record("2026-09-16", "차트TOP2", pool, ["000001"], 29,
                 path=_os.path.join(tempfile.mkdtemp(), "p.csv"))
        self.assertEqual(pool, before)


if __name__ == "__main__":
    unittest.main()
