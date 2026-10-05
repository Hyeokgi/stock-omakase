"""stockinfo7 병행 수집 — 원자료 비공개 업로드, 공개 메타에 이름 없음, 15:10 체크포인트, 중단 시 보관.
사용자 지시 2026-10-05 ("stockinfo7는 그대로 수집해. 상용화 할 생각은 없어."). 네트워크 없이 합성 HTML 로만 돈다.
"""
import csv
import datetime
import gzip
import json
import os
import shutil
import tempfile
import unittest

import hyeoks_theme_source_collect as C

KST = datetime.timezone(datetime.timedelta(hours=9))


def page(stock, header="2026-10-06 15시 테마랭킹"):
    return "".join(f"<div>{x}</div>" for x in [
        header, "비밀테마", "10%", "&nbsp;",
        stock, "12%", "&nbsp;&nbsp;시총", "1,000", "억", "&nbsp;거래 100억",
    ])


class Resp:
    def __init__(self, text, code=200):
        self.text, self.status_code = text, code


class Up:
    def __init__(self, fail=False):
        self.files, self.fail = {}, fail

    def __call__(self, name, data):
        if self.fail:
            raise RuntimeError("synthetic")
        self.files[name] = json.loads(gzip.decompress(data))
        return "id"


class Clock:
    """호출마다 1분씩 가는 KST 시계. sleep 은 시계를 그만큼 민다."""
    def __init__(self, h, m):
        self.t = datetime.datetime(2026, 10, 6, h, m, tzinfo=KST)

    def now(self):
        self.t += datetime.timedelta(seconds=1)
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


class CollectTests(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, ignore_errors=True)

    def run_collect(self, pages, start=(14, 50), until=(15, 30), up=None):
        it = iter(pages)
        clock = Clock(*start)
        up = up or Up()
        out = C.collect("2026-10-06", lambda *a, **k: next(it), now=clock.now, sleep=clock.sleep, until=until,
                        every=600, run_id="9", uploader=up, runs_dir=self.d, private_dir=os.path.join(self.d, "p"))
        with open(os.path.join(self.d, "stockinfo7_obs.csv"), encoding="utf-8") as fh:
            meta = list(csv.DictReader(fh))
        return out, up, meta

    def test_changed_screens_go_private_and_meta_has_no_names(self):
        pages = [Resp(page("비밀A", "2026-10-06 14시 테마랭킹")), Resp(page("비밀A", "2026-10-06 14시 테마랭킹")),
                 Resp(page("비밀B")), Resp("", 503), Resp(page("비밀B"))]
        out, up, meta = self.run_collect(pages, until=(15, 35))
        self.assertEqual((out["observations"], out["valid"], out["updates"], out["errors"]), (5, 4, 1, 1))
        kinds = [m["class"] for m in meta]
        self.assertEqual(kinds, ["첫관측", "동일", "갱신", "HTTP오류", "동일"])
        saved = [o for f in up.files.values() for o in f["observations"]]
        self.assertEqual([o["meta"]["class"] for o in saved], ["첫관측", "갱신"], "바뀐 화면만 비공개로")
        self.assertIn("비밀B", saved[1]["html"])
        public = open(os.path.join(self.d, "stockinfo7_obs.csv"), encoding="utf-8").read()
        self.assertNotIn("비밀", public, "공개 메타에 테마·종목 이름이 없다")
        self.assertTrue(all(m["receivedAt"].startswith("2026-10-06T") for m in meta if m["class"] != "접속실패"))

    def test_checkpoint_after_1510_and_final_flush(self):
        pages = [Resp(page("A", "2026-10-06 14시 테마랭킹")), Resp(page("B")), Resp(page("C")), Resp(page("D"))]
        out, up, meta = self.run_collect(pages, start=(14, 55), until=(15, 30))
        self.assertEqual(len(up.files), 2, "15:10 이후 첫 관측에서 한 번, 끝에서 한 번")
        self.assertTrue(all(m["privateFile"] for m in meta if m["class"] in ("첫관측", "갱신")))

    def test_interrupt_still_flushes(self):
        def get(*a, **k):
            if get.n:
                raise KeyboardInterrupt
            get.n += 1
            return Resp(page("A"))
        get.n = 0
        clock = Clock(14, 0)
        up = Up()
        with self.assertRaises(KeyboardInterrupt):
            C.collect("2026-10-06", get, now=clock.now, sleep=clock.sleep, until=(16, 35), uploader=up,
                      runs_dir=self.d, private_dir=os.path.join(self.d, "p"))
        self.assertEqual(len(up.files), 1)

    def test_upload_failure_is_marked(self):
        out, up, meta = self.run_collect([Resp(page("A"))], start=(15, 20), until=(15, 25), up=Up(fail=True))
        self.assertEqual(out["uploadFailed"], 1)
        self.assertTrue(meta[0]["privateFile"].startswith("보관실패:"))

    def test_interval_floor(self):
        clock = Clock(15, 0)
        slept = []
        C.collect("2026-10-06", lambda *a, **k: Resp(page("A")), now=clock.now,
                  sleep=lambda s: (slept.append(s), clock.sleep(s)), until=(15, 20), every=60, uploader=Up(),
                  runs_dir=self.d, private_dir=os.path.join(self.d, "p"))
        self.assertTrue(slept and min(slept) >= C.MIN_EVERY_S)


if __name__ == "__main__":
    unittest.main()
