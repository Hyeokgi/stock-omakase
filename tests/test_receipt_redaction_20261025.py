# 공개 영수증 가리기 시험(전체 검토 B) — 거래 ID · 계측 샘플의 종목코드가 공개 영수증에 남지 않고, 판정 결속은 같은 키로 이어지는지 확인한다
"""2026-10-25 — 거래 ID 는 `날짜_채널_종목코드` 라 공개 영수증이 그날 선정 종목을 드러냈다."""
import json
import os
import tempfile
import unittest
from unittest import mock

import evidence_builder as E
import hyeoks_verdict as v
import production_receipt as R
from tests.test_r1_policy_binding import CHART_POLICY, D, NEW, contains, header, pool, row

CODE = "123450"
TID = f"{D}_차트TOP2_{CODE}"


def payload(exp, found):
    return {"today_str": D, "ledger": {"expected_trade_ids": exp, "found_trade_ids": found},
            "telemetry": {"by_feature": {"v1": {"OBSERVE": 1}}, "samples": {"v1": [CODE]}}}


class Redact(unittest.TestCase):
    def test_with_key_ids_are_hmac_and_sets_still_compare(self):
        out = R.redact(payload([TID, "x2"], [TID]), key="k")
        led = out["ledger"]
        self.assertEqual(led["id_hash"], R.ID_HASH)
        self.assertEqual(led["expected_trade_ids"][0], R.id_hash(TID, "k"))
        self.assertEqual(set(led["expected_trade_ids"]) - set(led["found_trade_ids"]), {R.id_hash("x2", "k")})
        self.assertNotIn(CODE, json.dumps(out, ensure_ascii=False))
        self.assertEqual(out["telemetry"]["samples"], {"v1": 1})

    def test_without_key_ids_become_tokens(self):
        with mock.patch("builtins.print"):
            out = R.redact(payload([TID, "x2"], [TID]), key="")
        self.assertEqual(out["ledger"]["id_hash"], R.ID_TOKEN)
        self.assertNotIn(CODE, json.dumps(out, ensure_ascii=False))
        self.assertEqual(len(set(out["ledger"]["expected_trade_ids"]) - set(out["ledger"]["found_trade_ids"])), 1)

    def test_emit_writes_only_the_redacted_copy(self):
        d = tempfile.mkdtemp()
        with mock.patch.dict(os.environ, {R.ID_KEY_ENV: "k"}):
            ok, _ = R.emit(D, "scanner", payload([TID], [TID]), root=d, run_id="r1")
        self.assertTrue(ok)
        text = "".join(open(os.path.join(dp, f), encoding="utf-8").read() for dp, _, fs in os.walk(d) for f in fs)
        self.assertNotIn(CODE, text)

    def test_already_redacted_is_not_hashed_twice(self):
        once = R.redact(payload([TID], [TID]), key="k")
        self.assertEqual(R.redact(once, key="k")["ledger"], once["ledger"])


class VerdictBinding(unittest.TestCase):
    def rows_pools(self):
        return [header(), row(TID, D, "차트TOP2", CODE)], [pool(D, "차트TOP2", CODE, "r1", NEW, CHART_POLICY)]

    def receipt(self, key):
        p = R.redact(payload([TID], [TID]), key=key)
        return {"kind": "scanner", "cycle_date": D, "run_id": "r1", "code_sha": NEW, "payload": p}

    def test_hashed_receipt_binds_with_the_same_key(self):
        rows, pools = self.rows_pools()
        with mock.patch.dict(os.environ, {R.ID_KEY_ENV: "k"}):
            b = v.bind_policy(rows, [self.receipt("k")], pools, contains)[1]
        self.assertTrue(b["ok"], b)

    def test_wrong_or_missing_key_does_not_bind(self):
        rows, pools = self.rows_pools()
        with mock.patch.dict(os.environ, {R.ID_KEY_ENV: "other"}):
            self.assertEqual(v.bind_policy(rows, [self.receipt("k")], pools, contains)[1]["reason"], "receipt_missing")
        env = {k: x for k, x in os.environ.items() if k != R.ID_KEY_ENV}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(v.bind_policy(rows, [self.receipt("k")], pools, contains)[1]["reason"], "receipt_missing")


class EvidenceLedger(unittest.TestCase):
    def test_missing_trade_is_still_detected_after_redaction(self):
        led = R.redact(payload([TID, "x2"], [TID]), key="k")["ledger"]
        missing = sorted(set(led["expected_trade_ids"]) - set(led["found_trade_ids"]))
        self.assertEqual(len(missing), 1)
        self.assertTrue(hasattr(E, "build"))


if __name__ == "__main__":
    unittest.main()
