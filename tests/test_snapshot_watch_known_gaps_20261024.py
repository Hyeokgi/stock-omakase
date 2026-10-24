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

    def test_rows_without_reason_or_reason_column_are_not_exempt(self):
        """2026-10-25 Codex — 사유 없는 줄 · reason 열 없는 목록은 면제하지 않는다."""
        d = tempfile.mkdtemp()
        no_col, empty = os.path.join(d, "a.csv"), os.path.join(d, "b.csv")
        with open(no_col, "w", encoding="utf-8") as fh:
            fh.writelines(["date,item\n", "2026-10-21,분봉(기한만료)\n"])
        with open(empty, "w", encoding="utf-8") as fh:
            fh.writelines(["date,item,reason\n", "2026-10-21,분봉(기한만료), \n", "2026-10-22,분봉(기한만료),확인함\n"])
        with mock.patch("builtins.print"):
            self.assertEqual(W.load_known(no_col), {})
            self.assertEqual(W.load_known(empty), {("2026-10-22", "분봉(기한만료)"): "확인함"})

    def test_repository_list_entries_match_the_watch_wording(self):
        known = W.load_known()
        self.assertIn(("2026-10-12", "분봉(기한만료)"), known)
        self.assertIn(("2026-10-23", "1505 upjong 구성 PARTIAL"), known)
        self.assertTrue(all(why for why in known.values()), "사유 없는 줄이 있다")


class MainExitCode(unittest.TestCase):
    def run_main(self, known_rows, upjong="PARTIAL", corrupt=None, extra_day=None):
        snap, intra = tempfile.mkdtemp(), tempfile.mkdtemp()
        for day in filter(None, (DAY, extra_day)):
            for slot in ("1300", "1505"):
                snapshot(snap, day, slot, upjong if day == DAY else "OK")
            for suffix in ("theme", "upjong", "group"):
                open(os.path.join(snap, f"{day}_1505_{suffix}.csv.gz"), "wb").close()
            open(os.path.join(intra, f"{day}.csv.gz"), "wb").close()
        if corrupt:
            with open(os.path.join(snap, f"{DAY}_{corrupt}.csv.gz"), "wb") as fh:
                fh.write(b"not gzip")
        known = os.path.join(tempfile.mkdtemp(), "known.csv")
        with open(known, "w", encoding="utf-8") as fh:
            fh.write("date,item,reason\n" + "".join(f"{d},{i},{r}\n" for d, i, r in known_rows))
        with mock.patch.dict(os.environ, {"WATCH_TEST_DAYS": ",".join(filter(None, (DAY, extra_day)))}), mock.patch.object(W, "SNAP_DIR", snap), \
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

    def test_corrupt_past_file_is_a_new_gap_even_if_the_day_is_known(self):
        """2026-10-25 Codex — 마지막 거래일이 아닌 날의 파일이 깨지면 예전에는 ✅ 로 보였다."""
        rc, log = self.run_main([(DAY, f"{s} upjong 구성 PARTIAL", "기타 규칙") for s in ("1300", "1505")],
                                corrupt="1300", extra_day="2026-10-22")
        self.assertEqual(rc, 2)
        self.assertIn(f"{DAY}: 1300 파일 손상", log)

    def test_clean_day_is_zero(self):
        rc, log = self.run_main([], upjong="OK")
        self.assertEqual(rc, 0)
        self.assertIn("구멍 없음", log)


if __name__ == "__main__":
    unittest.main()
