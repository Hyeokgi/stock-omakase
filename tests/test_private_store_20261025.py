# 후보 자료 비공개 보관 시험 — 드라이브 업로드 · 공개 색인에 종목 정보 없음 · 접수만 '이미 기록됨' 으로 치는지 확인한다
"""2026-10-25 사용자 결정(허브 §8, Codex 검토 P1) — Feature Store · 순위 풀을 공개 저장소에서 드라이브로."""
import csv
import gzip
import os
import tempfile
import unittest

import feature_store as F
import private_store as P
import rank_pool as RP

NAME, CODE = "비밀종목", "123450"


def fs_file(d, day, n=2):
    os.makedirs(d, exist_ok=True)
    with gzip.open(os.path.join(d, f"{day}.csv.gz"), "wt", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["name", "code", "price"])
        for i in range(n):
            w.writerow([NAME, CODE, 1000 + i])


def pool_file(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(RP.HEADER)
        for day, ch in rows:
            w.writerow([day, ch, 1, CODE, NAME] + [""] * (len(RP.HEADER) - 5))


class Up:
    def __init__(self, fail=False):
        self.fail, self.sent = fail, []

    def __call__(self, name, data):
        if self.fail:
            raise ConnectionError("down")
        self.sent.append((name, data))
        return f"id-{len(self.sent)}"


class Upload(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.fs, self.pool = os.path.join(self.d, "fs"), os.path.join(self.d, "census", "rank_pool.csv")
        self.index = os.path.join(self.d, "census", "private_index.csv")

    def up(self, uploader, run="R1"):
        return P.upload_pending(uploader, run, fs_dir=self.fs, pool_path=self.pool, index=self.index)

    def test_new_files_go_to_drive_and_index_has_no_stock_info(self):
        fs_file(self.fs, "2026-10-26", 3)
        pool_file(self.pool, [("2026-10-26", "차트TOP2"), ("2026-10-26", "차트TOP2"), ("2026-10-26", "수급TOP2")])
        u = Up()
        self.assertEqual(self.up(u), (3, 0))
        self.assertEqual(sorted(n for n, _ in u.sent), ["feature_store_2026-10-26_R1.csv.gz", "rank_pool_2026-10-26_R1.csv.gz"])
        rows = P.load(self.index)
        self.assertEqual({(r["kind"], r["channel"], r["rows"]) for r in rows},
                         {("feature_store", "", "3"), ("rank_pool", "차트TOP2", "2"), ("rank_pool", "수급TOP2", "1")})
        text = open(self.index, encoding="utf-8").read()
        self.assertNotIn(NAME, text)
        self.assertNotIn(CODE, text)
        with gzip.open(__import__("io").BytesIO(dict(u.sent)["rank_pool_2026-10-26_R1.csv.gz"]), "rt", encoding="utf-8") as fh:
            self.assertIn(NAME, fh.read(), "드라이브 사본에는 원자료가 그대로 있다")

    def test_second_run_uploads_nothing(self):
        fs_file(self.fs, "2026-10-26")
        self.up(Up())
        u = Up()
        self.assertEqual(self.up(u, "R2"), (0, 0))
        self.assertEqual(u.sent, [])

    def test_failed_upload_is_logged_but_not_counted_as_recorded(self):
        fs_file(self.fs, "2026-10-26")
        pool_file(self.pool, [("2026-10-26", "차트TOP2")])
        self.assertEqual(self.up(Up(fail=True)), (0, 2))
        self.assertFalse(P.recorded("feature_store", "2026-10-26", path=self.index))
        self.assertFalse(P.recorded("rank_pool", "2026-10-26", "차트TOP2", path=self.index))
        self.assertIn("실패", open(self.index, encoding="utf-8").read())
        self.assertEqual(self.up(Up(), "R2"), (2, 0), "뒤 회차가 다시 올린다")

    def test_only_unrecorded_channels_of_a_day_are_sent(self):
        pool_file(self.pool, [("2026-10-26", "차트TOP2")])
        self.up(Up())
        pool_file(self.pool, [("2026-10-26", "차트TOP2"), ("2026-10-26", "수급TOP2")])
        u = Up()
        self.assertEqual(self.up(u, "R2"), (1, 0))
        self.assertEqual(P.load(self.index)[-1]["channel"], "수급TOP2")


class DedupeFromIndex(unittest.TestCase):
    """새 러너에는 앞 회차 파일이 없다 — 기본 경로에서는 색인의 접수 행이 '이미 기록됨' 이다."""

    def setUp(self):
        self.old = os.getcwd()
        self.d = tempfile.mkdtemp()
        os.chdir(self.d)
        os.makedirs("data/scanner_census")
        with open(P.INDEX, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=P.COLS)
            w.writeheader()
            w.writerow({"kind": "feature_store", "date": "2026-10-26", "status": P.DONE})
            w.writerow({"kind": "rank_pool", "date": "2026-10-26", "channel": "차트TOP2", "status": P.DONE})
            w.writerow({"kind": "rank_pool", "date": "2026-10-26", "channel": "수급TOP2", "status": "실패"})

    def tearDown(self):
        os.chdir(self.old)

    def test_feature_store_default_path_uses_index(self):
        ok, msg = F.record("2026-10-26", [["x"] * 40])
        self.assertEqual((ok, msg), (False, "2026-10-26 이미 기록됨(드라이브 접수)"))
        self.assertFalse(os.path.exists(F.path_for("2026-10-26")))

    def test_rank_pool_default_path_uses_index_and_ignores_failures(self):
        self.assertTrue(RP.already_recorded("2026-10-26", "차트TOP2"))
        self.assertFalse(RP.already_recorded("2026-10-26", "수급TOP2"))
        self.assertFalse(RP.already_recorded("2026-10-27", "차트TOP2"))

    def test_explicit_paths_do_not_read_the_index(self):
        other = os.path.join(self.d, "elsewhere.csv")
        self.assertFalse(RP.already_recorded("2026-10-26", "차트TOP2", path=other))


if __name__ == "__main__":
    unittest.main()
