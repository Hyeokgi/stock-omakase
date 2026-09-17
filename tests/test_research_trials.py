"""트랙 R 시도 기록 — 상한이 **말이 아니라 코드로** 막히는가."""
import os
import tempfile
import unittest
import research_trials as T


class NormTests(unittest.TestCase):
    def test_order_does_not_create_new_combination(self):
        """같은 조합을 순서만 바꿔 두 번 세면 상한이 무의미해진다."""
        self.assertEqual(T.norm_features(["v2_score", "v1_score"]),
                         T.norm_features(["v1_score", "v2_score"]))

    def test_single_feature_accepts_string(self):
        self.assertEqual(T.norm_features("v1_score"), "v1_score")

    def test_blank_rejected(self):
        self.assertEqual(T.norm_features(["", "  "]), "")


class CapTests(unittest.TestCase):
    def tmp(self):
        return os.path.join(tempfile.mkdtemp(), "trials.csv")

    def test_stage_cap_blocks(self):
        p = self.tmp()
        for i in range(T.STAGE_CAP[1]):
            T.record(1, f"feat{i}", path=p, verdict="탈락")
        with self.assertRaises(T.CapExceeded):
            T.record(1, "one_more", path=p)

    def test_other_stage_still_open_after_one_fills(self):
        p = self.tmp()
        for i in range(T.STAGE_CAP[1]):
            T.record(1, f"feat{i}", path=p)
        n = T.record(2, ["feat0", "feat1"], path=p)
        self.assertEqual(n, T.STAGE_CAP[1] + 1)

    def test_total_cap_blocks_even_if_stage_has_room(self):
        p = self.tmp()
        k = 0
        for stage in (1, 2, 3):
            for i in range(T.STAGE_CAP[stage]):
                T.record(stage, f"s{stage}f{i}", path=p)
                k += 1
        self.assertEqual(k, T.TOTAL_CAP)
        with self.assertRaises(T.CapExceeded):
            T.record(3, "extra", path=p, allow_duplicate=True)

    def test_duplicate_combination_rejected(self):
        p = self.tmp()
        T.record(1, "v1_score", path=p)
        with self.assertRaises(ValueError):
            T.record(1, "v1_score", path=p)
        with self.assertRaises(ValueError):
            T.record(2, ["v1_score"], path=p)      # 순서·단계 달라도 같은 조합

    def test_bad_stage_rejected(self):
        with self.assertRaises(ValueError):
            T.record(4, "x", path=self.tmp())

    def test_remaining_counts_down(self):
        p = self.tmp()
        self.assertEqual(T.remaining(p)["total"], T.TOTAL_CAP)
        T.record(1, "v1_score", path=p)
        self.assertEqual(T.remaining(p)["total"], T.TOTAL_CAP - 1)
        self.assertEqual(T.remaining(p)[1], T.STAGE_CAP[1] - 1)


class RecordTests(unittest.TestCase):
    def tmp(self):
        return os.path.join(tempfile.mkdtemp(), "trials.csv")

    def test_failed_trials_are_kept(self):
        """🔴 실패도 남긴다 — 이게 '몇 개 중 골랐나' 의 분모다."""
        p = self.tmp()
        T.record(1, "a", path=p, verdict="탈락")
        T.record(1, "b", path=p, verdict="탈락")
        T.record(1, "c", path=p, verdict="생존")
        self.assertEqual(T.counts(p)["total"], 3)
        self.assertIn("탈락", T.summary(p))

    def test_header_written_once(self):
        import csv as _csv
        p = self.tmp()
        T.record(1, "a", path=p)
        T.record(1, "b", path=p)
        with open(p, encoding="utf-8") as f:
            rows = list(_csv.reader(f))
        self.assertEqual(rows[0], T.HEADER)
        self.assertEqual(sum(1 for r in rows if r == T.HEADER), 1)

    def test_statistics_stored(self):
        import csv as _csv
        p = self.tmp()
        T.record(1, "v1_score", path=p, n_days=24, ic_mean=0.031,
                 ic_p=0.04, ic_boot_p=0.09, lift={2: 1.8, 3: 1.2, 5: 0.6},
                 verdict="보류", sha="abc12345")
        r = list(_csv.DictReader(open(p, encoding="utf-8")))[0]
        self.assertEqual(r["ic_mean"], "0.031")
        self.assertEqual(r["ic_boot_p"], "0.09")
        self.assertEqual(r["lift_k5"], "0.6")
        self.assertEqual(r["prereg"], T.PREREG)
        self.assertEqual(r["code_sha"], "abc12345")

    def test_trial_numbers_increase(self):
        p = self.tmp()
        self.assertEqual([T.record(1, x, path=p) for x in "abc"], [1, 2, 3])

    def test_summary_shows_caps(self):
        s = T.summary(self.tmp())
        self.assertIn(str(T.TOTAL_CAP), s)
        self.assertIn("기록되지 않은 시도", s)


class PreregConsistencyTests(unittest.TestCase):
    """코드 상한이 사전등록 문서와 어긋나면 둘 중 하나가 거짓말이 된다."""

    def test_caps_match_document(self):
        doc = open(f"docs/{T.PREREG}.md", encoding="utf-8").read()
        self.assertIn("**15**", doc)
        self.assertIn("**45**", doc)
        self.assertIn("**20**", doc)
        self.assertIn("**80**", doc)
        self.assertEqual(T.STAGE_CAP, {1: 15, 2: 45, 3: 20})
        self.assertEqual(T.TOTAL_CAP, sum(T.STAGE_CAP.values()))


if __name__ == "__main__":
    unittest.main()
