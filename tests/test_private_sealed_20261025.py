# 후보 자료 봉인 사본 시험(전체 검토 A) — 업로드 실패 때 같은 바이트를 암호화해 남기고, PC 복구로 드라이브에 옮기고, 판정이 보존을 확인하는지 본다
"""2026-10-25 — 스캐너는 첫 적재 회차에서만 후보 자료를 만든다. 그 회차 업로드가 실패하면 원본이 사라졌다."""
import csv
import gzip
import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import evidence_builder as E
import private_store as P
import rank_pool as RP

HAS_OPENSSL = shutil.which("openssl") is not None
KEY = "test-key-0123456789"
NAME = "비밀종목"


class Up:
    def __init__(self, fail=False):
        self.fail, self.sent = fail, {}

    def __call__(self, name, data):
        if self.fail:
            raise ConnectionError("down")
        self.sent[name] = data
        return f"id-{len(self.sent)}"


def make(d):
    fs = os.path.join(d, "fs")
    os.makedirs(fs)
    with gzip.open(os.path.join(fs, "2026-10-26.csv.gz"), "wt", encoding="utf-8", newline="") as fh:
        csv.writer(fh).writerows([["name", "code"], [NAME, "123450"]])
    pool = os.path.join(d, "rank_pool.csv")
    with open(pool, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(RP.HEADER)
        w.writerow(["2026-10-26", "차트TOP2", 1, "123450", NAME] + [""] * (len(RP.HEADER) - 5))
    return fs, pool


@unittest.skipUnless(HAS_OPENSSL, "openssl 없음")
class Sealed(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.fs, self.pool = make(self.d)
        self.index, self.fb = os.path.join(self.d, "index.csv"), os.path.join(self.d, "fb")

    def up(self, uploader, key):
        return P.upload_pending(uploader, "R1", fs_dir=self.fs, pool_path=self.pool, index=self.index,
                                key=key, fallback_dir=self.fb)

    def test_failed_upload_is_sealed_not_lost(self):
        self.assertEqual(self.up(Up(fail=True), KEY), (0, 2))
        st = {r["kind"]: r["status"] for r in P.load(self.index)}
        self.assertEqual(st, {"feature_store": P.SEALED, "rank_pool": P.SEALED})
        files = sorted(os.listdir(self.fb))
        self.assertEqual(files, ["feature_store_2026-10-26_R1.csv.gz.enc", "manifest.json", "rank_pool_2026-10-26_R1.csv.gz.enc"])
        for f in files:
            raw = open(os.path.join(self.fb, f), "rb").read()
            self.assertNotIn(NAME.encode("utf-8"), raw, f"{f} 에 평문이 남았다")
            self.assertNotIn(b"123450", raw)
        self.assertTrue(P.preserved("feature_store", "2026-10-26", path=self.index))
        self.assertFalse(P.failed("rank_pool", "2026-10-26", path=self.index))

    def test_restore_unseals_same_bytes_and_records_receipt(self):
        original = open(os.path.join(self.fs, "2026-10-26.csv.gz"), "rb").read()
        self.up(Up(fail=True), KEY)
        u = Up()
        with mock.patch("builtins.print"):
            self.assertEqual(P.restore(self.fb, u, key=KEY, index=self.index), (2, 0))
        self.assertEqual(u.sent["feature_store_2026-10-26_R1.csv.gz"], original, "같은 바이트여야 한다")
        self.assertTrue(P.recorded("feature_store", "2026-10-26", path=self.index))
        self.assertTrue(P.recorded("rank_pool", "2026-10-26", "차트TOP2", path=self.index))

    def test_wrong_key_cannot_restore(self):
        self.up(Up(fail=True), KEY)
        with mock.patch("builtins.print"):
            self.assertEqual(P.restore(self.fb, Up(), key="wrong", index=self.index), (0, 2))

    def test_without_key_nothing_plain_is_kept_and_gate_sees_failure(self):
        self.assertEqual(self.up(Up(fail=True), ""), (0, 2))
        self.assertFalse(os.path.exists(self.fb))
        self.assertFalse(P.preserved("feature_store", "2026-10-26", path=self.index))
        ok, why = E.private_preserved("2026-10-26", {"rows": 750}, {"rows": 2}, index=self.index)
        self.assertEqual((ok, why), (False, "비공개 보존 없음(feature_store · rank_pool)"))

    def test_gate_accepts_drive_or_sealed_and_skips_old_days(self):
        self.up(Up(fail=True), KEY)
        self.assertEqual(E.private_preserved("2026-10-26", {"rows": 750}, {"rows": 2}, index=self.index), (True, ""))
        self.assertEqual(E.private_preserved("2026-10-23", {"rows": 750}, {"rows": 2}, index=self.index), (True, ""))
        self.assertEqual(E.private_preserved("2026-10-26", {"rows": 750}, {"rows": 0}, index=os.path.join(self.d, "none.csv"))[0],
                         False)


class Manifest(unittest.TestCase):
    @unittest.skipUnless(HAS_OPENSSL, "openssl 없음")
    def test_manifest_has_no_stock_info(self):
        d = tempfile.mkdtemp()
        fs, pool = make(d)
        P.upload_pending(Up(fail=True), "R1", fs_dir=fs, pool_path=pool, index=os.path.join(d, "i.csv"),
                         key=KEY, fallback_dir=os.path.join(d, "fb"))
        text = open(os.path.join(d, "fb", "manifest.json"), encoding="utf-8").read()
        self.assertNotIn(NAME, text)
        self.assertNotIn("123450", text)
        self.assertEqual({x["kind"] for x in json.loads(text)}, {"feature_store", "rank_pool"})


if __name__ == "__main__":
    unittest.main()
