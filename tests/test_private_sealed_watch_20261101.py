# 봉인 사본 감시 시험 — 복원 안 된 봉인 사본을 찾고, artifact 유무 · 만료로 급함을 가르고, 복원 명령을 적는지 본다
"""2026-11-01 전략 평가 우선순위 3 — 봉인 사본은 14일 artifact 라 만료 전에 PC 에서 복원해야 한다."""
import csv
import datetime
import os
import tempfile
import unittest

import private_sealed_watch as W
import private_store as P

NOW = datetime.datetime(2026, 11, 2, 10, 0, tzinfo=datetime.timezone.utc)


def row(kind, day, status, run_id="111", channel="", at="2026-10-30T15:10:00+09:00"):
    return {"kind": kind, "date": day, "channel": channel, "status": status, "run_id": run_id, "at": at}


def art(run_id, expires="2026-11-13T06:10:00Z", expired=False):
    return {"name": f"private-fallback-{run_id}-1", "expires_at": expires, "expired": expired}


class Unresolved(unittest.TestCase):
    def test_sealed_without_later_accept(self):
        rows = [row("feature_store", "2026-10-30", P.SEALED),
                row("rank_pool", "2026-10-30", P.SEALED, channel="단기"),
                row("rank_pool", "2026-10-30", P.DONE, run_id="restore", channel="단기"),   # 복원됨
                row("feature_store", "2026-10-29", P.DONE)]
        got = W.unresolved(rows)
        self.assertEqual(list(got), ["111"])
        self.assertEqual([r["kind"] for r in got["111"]], ["feature_store"])

    def test_nothing_sealed(self):
        self.assertEqual(W.unresolved([row("feature_store", "2026-10-30", P.DONE)]), {})


class Check(unittest.TestCase):
    def test_live_artifact_needs_restore(self):
        got = W.check([row("feature_store", "2026-10-30", P.SEALED)], lambda r: [art(r), {"name": "evidence-x"}], NOW)
        run_id, days, state, exp, urgent, name = got[0]
        self.assertEqual((run_id, days, urgent, name), ("111", ["2026-10-30"], True, "private-fallback-111-1"))
        self.assertIn("만료까지 10.8일", state)
        self.assertTrue(exp.startswith("2026-11-13T06:10"))

    def test_missing_artifact_is_urgent_until_grace_ends(self):
        got = W.check([row("feature_store", "2026-10-30", P.SEALED)], lambda r: [], NOW)
        self.assertEqual(got[0][2:5], ("원격 사본 없음 — 원본 잃음", "", True))
        old = row("feature_store", "2026-10-01", P.SEALED, at="2026-10-01T15:10:00+09:00")
        got = W.check([old], lambda r: [art(r, expired=True)], NOW)
        self.assertEqual(got[0][2:5], ("만료됨 — 원본 잃음", "", False))

    def test_lookup_failure_is_urgent(self):
        def boom(r):
            raise OSError("x")
        got = W.check([row("feature_store", "2026-10-30", P.SEALED)], boom, NOW)
        self.assertEqual((got[0][2], got[0][4]), ("artifact 조회 실패 OSError", True))


class Main(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, self.d, True)
        self.index = os.path.join(self.d, "index.csv")
        self.summary = os.path.join(self.d, "summary.md")

    def write(self, rows):
        with open(self.index, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=P.COLS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    def test_clean_index_passes_quietly(self):
        self.write([row("feature_store", "2026-10-30", P.DONE)])
        rc = W.main(env={"GITHUB_STEP_SUMMARY": self.summary}, artifacts=lambda r: [], now=NOW, index=self.index)
        self.assertEqual(rc, 0)
        with open(self.summary, encoding="utf-8") as fh:
            self.assertIn("복원할 봉인 사본 없음", fh.read())

    def test_sealed_fails_with_restore_commands(self):
        self.write([row("feature_store", "2026-10-30", P.SEALED)])
        rc = W.main(env={"GITHUB_STEP_SUMMARY": self.summary}, artifacts=lambda r: [art(r)], now=NOW, index=self.index)
        self.assertEqual(rc, 1)
        with open(self.summary, encoding="utf-8") as fh:
            text = fh.read()
        self.assertIn("gh run download 111 -R Hyeokgi/stock-omakase -n private-fallback-111-1", text)
        self.assertIn("python private_store.py --restore $env:TEMP\\private-fallback-111-1", text)
        self.assertIn("git add data/scanner_census/private_index.csv", text)

    def test_real_index_reads(self):
        """저장소 색인을 그대로 읽을 수 있어야 한다(열 이름 · 상태 값이 private_store 와 같다)."""
        self.assertIsInstance(W.check(P.load(), lambda r: [art(r)], NOW), list)


if __name__ == "__main__":
    unittest.main()
