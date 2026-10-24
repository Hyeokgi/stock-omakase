# 스냅샷 결측 감시의 알려진 구멍 시험 — 원인을 확인해 적어 둔 구멍은 실패로 세지 않고, 목록에 없는 구멍만 실패로 보는지 확인한다
"""2026-10-24 사용자 결정(허브 #21) — '새 빈칸만 실패로 보게 바꿔줘'. 10/12 부터 매일 빨간불이었지만 새 구멍은 없었다."""
import gzip
import os
import sys
import tempfile
import unittest
from unittest import mock

import hyeoks_snapshot_watch as W

DAY = "2026-10-21"


def snapshot(d, day, slot, upjong="OK"):
    with gzip.open(os.path.join(d, f"{day}_{slot}.csv.gz"), "wt", encoding="utf-8") as fh:
        fh.write(f"#meta,capturedAt={day}T13:02:00+09:00,themeMembershipStatus=OK,"
                 f"upjongMembershipStatus={upjong},groupMembershipStatus=OK\n")
        fh.write("itemcode,themeNos,upjongNos,groupNos\n000010,1,2,3\n")


class Split(unittest.TestCase):
    def test_known_items_are_separated_and_grouped_by_reason(self):
        known = {(DAY, "1300 upjong 구성 PARTIAL"): "기타 규칙", (DAY, "1505 upjong 구성 PARTIAL"): "기타 규칙"}
        new, old = W.split_known([(DAY, ["1300 upjong 구성 PARTIAL", "1505 upjong 구성 PARTIAL", "분봉(기한만료)"])], known)
        self.assertEqual(new, [(DAY, ["분봉(기한만료)"])])
        self.assertEqual(old, [(DAY, ["1300 upjong 구성 PARTIAL, 1505 upjong 구성 PARTIAL — 기타 규칙"])])

    def test_missing_or_broken_list_means_everything_is_new(self):
        self.assertEqual(W.load_known(os.path.join(tempfile.mkdtemp(), "없음.csv")), {})
        bad = os.path.join(tempfile.mkdtemp(), "bad.csv")
        with open(bad, "w", encoding="utf-8") as fh:
            fh.write("날짜,무엇\n2026-10-21,x\n")
        with mock.patch("builtins.print"):
            self.assertEqual(W.load_known(bad), {})

    def test_repository_list_entries_match_the_watch_wording(self):
        known = W.load_known()
        self.assertIn(("2026-10-12", "분봉(기한만료)"), known)
        self.assertIn(("2026-10-23", "1505 upjong 구성 PARTIAL"), known)
        self.assertTrue(all(why for why in known.values()), "사유 없는 줄이 있다")


class MainExitCode(unittest.TestCase):
    def run_main(self, known_rows, upjong="PARTIAL"):
        snap, intra = tempfile.mkdtemp(), tempfile.mkdtemp()
        for slot in ("1300", "1505"):
            snapshot(snap, DAY, slot, upjong)
        for suffix in ("theme", "upjong", "group"):
            open(os.path.join(snap, f"{DAY}_1505_{suffix}.csv.gz"), "wb").close()
        open(os.path.join(intra, f"{DAY}.csv.gz"), "wb").close()
        known = os.path.join(tempfile.mkdtemp(), "known.csv")
        with open(known, "w", encoding="utf-8") as fh:
            fh.write("date,item,reason\n" + "".join(f"{d},{i},{r}\n" for d, i, r in known_rows))
        with mock.patch.dict(os.environ, {"WATCH_TEST_DAYS": DAY}), mock.patch.object(W, "SNAP_DIR", snap), \
             mock.patch.object(W, "INTRA_DIR", intra), mock.patch.object(W, "KNOWN_GAPS", known), \
             mock.patch.object(W, "backup_cron_report"), mock.patch.object(sys, "argv", ["x"]), \
             mock.patch("builtins.print") as p:
            rc = W.main()
        return rc, "\n".join(" ".join(str(a) for a in c.args) for c in p.call_args_list)

    def test_unknown_gap_fails(self):
        rc, log = self.run_main([])
        self.assertEqual(rc, 2)
        self.assertIn("❌ 복구 불가 구멍", log)

    def test_known_gap_passes_but_is_still_shown(self):
        rc, log = self.run_main([(DAY, f"{s} upjong 구성 PARTIAL", "기타 규칙") for s in ("1300", "1505")])
        self.assertEqual(rc, 0)
        self.assertIn("📌 알려진 구멍", log)
        self.assertIn("기타 규칙", log)
        self.assertIn("새 구멍 없음", log)

    def test_partly_known_still_fails_on_the_rest(self):
        rc, log = self.run_main([(DAY, "1300 upjong 구성 PARTIAL", "기타 규칙")])
        self.assertEqual(rc, 2)
        self.assertIn("1505 upjong 구성 PARTIAL", log.split("❌ 복구 불가 구멍")[1])

    def test_clean_day_is_zero(self):
        rc, log = self.run_main([], upjong="OK")
        self.assertEqual(rc, 0)
        self.assertIn("구멍 없음", log)


if __name__ == "__main__":
    unittest.main()
