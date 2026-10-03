"""2026-10-03 — 동결 업로드의 일시 오류 재시도.

10/2 15:02 동결 업로드가 비JSON 응답 **한 번**으로 실패했고, 그 하나로 워크플로가 빨개져
그날 Gate 가 FAIL 로 박제됐다. 같은 창구가 3분 뒤(15:05:33)에는 배지 보관을 4초 만에 받았다.

지킬 것
  · `upload()` 기본값은 **한 번만** 시도한다 — 배지 보관이 "애매한 업로드는 자동 재시도하지 않는다"
    는 정책으로 이 함수를 그대로 부른다. 동결 경로만 재시도한다.
  · 일시 오류(네트워크·시간 초과·JSON 아닌 응답·id 없는 응답)만 재시도하고 코드 결함은 즉시 올린다.
  · 호출 중에는 짧게(리포트를 늦추지 않는다), 리포트 발송 뒤에는 길게(러너의 사본을 다시 올린다).
  · 끝내 실패하면 조용히 넘기지 않는다 — 상태 파일은 ok=false 로 남고 확인 단계가 잡을 빨갛게 한다.
  · 엉뚱한 사본을 올리지 않는다.

네트워크 없이 `requests.post` 를 가짜로 끼우고 `sleep` 은 기록만 한다.
"""
import base64
import contextlib
import datetime
import gzip
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import requests

import hyeoks_run_freeze as F

ROOT = pathlib.Path(__file__).resolve().parents[1]
KST = F.KST


class Resp:
    def __init__(self, body=None, error=None):
        self.body, self.error = body, error

    def json(self):
        if self.error is not None:
            raise self.error
        return self.body


def bad_json():
    return Resp(error=json.JSONDecodeError("Expecting value", "", 0))     # 10/2 에 실제로 난 오류


def ok(fid="FILE1"):
    return Resp({"id": fid})


def make_bundle():
    pool = [{"code": "000660", "name": "SK하이닉스", "score": 88.5, "v1_score": 88,
             "v2_score": None, "v3_score": 40, "rs_grade": 92, "tajeom_raw": "대장",
             "type": "NORMAL", "theme_name": "반도체", "curr_p": 210000}]
    return F.build_bundle("analyst", pool=pool, prompt="P", response="R",
                          picks={"short_term_code": "000660"}, code_sha="abcd1234",
                          now=datetime.datetime(2026, 10, 6, 15, 0, 5, tzinfo=KST))


def quiet():
    return contextlib.redirect_stdout(io.StringIO())


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class UploadRetryTests(unittest.TestCase):
    def call(self, script, **kw):
        sleeps = []
        with mock.patch("requests.post", side_effect=script) as post, quiet():
            try:
                result = F.upload("http://x", "n.json.gz", b"DATA", sleep=sleeps.append, **kw)
                error = None
            except Exception as e:                                    # noqa: BLE001
                result, error = None, e
        return result, error, post, sleeps

    def test_default_is_a_single_attempt(self):
        """배지 보관 정책 — 애매한 업로드를 자동으로 되풀이하지 않는다."""
        result, error, post, sleeps = self.call([bad_json(), ok()])
        self.assertIsNone(result)
        self.assertIsInstance(error, ValueError)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(sleeps, [])

    def test_default_signature_keeps_that_policy(self):
        import inspect
        self.assertEqual(inspect.signature(F.upload).parameters["backoff"].default, ())

    def test_recovers_after_transient_failures(self):
        result, error, post, sleeps = self.call(
            [bad_json(), requests.exceptions.ReadTimeout("timed out"), ok("F9")],
            backoff=(5, 15))
        self.assertEqual(result, "F9")
        self.assertIsNone(error)
        self.assertEqual(post.call_count, 3)
        self.assertEqual(sleeps, [5, 15])

    def test_every_attempt_sends_the_same_bytes(self):
        """다시 올려도 이름·내용이 같다 — 중복이 생겨도 내용은 동일하다."""
        _, _, post, _ = self.call([bad_json(), ok()], backoff=(5,))
        payloads = [c.kwargs["json"] for c in post.call_args_list]
        self.assertEqual(payloads[0], payloads[1])
        self.assertEqual(base64.b64decode(payloads[0]["base64"]), b"DATA")
        self.assertEqual(payloads[0]["filename"], "n.json.gz")

    def test_gives_up_and_raises_the_last_error(self):
        result, error, post, sleeps = self.call(
            [bad_json(), requests.exceptions.ConnectionError("c"), bad_json()], backoff=(5, 15))
        self.assertIsNone(result)
        self.assertIsInstance(error, ValueError)                      # 마지막 오류가 그대로 올라간다
        self.assertEqual(post.call_count, 3)
        self.assertEqual(sleeps, [5, 15])                             # 마지막 실패 뒤에는 기다리지 않는다

    def test_response_without_file_id_is_retried_then_reported(self):
        result, error, post, _ = self.call([Resp({"error": "x"})] * 3, backoff=(5, 15))
        self.assertIsInstance(error, F.NoFileId)
        self.assertIsInstance(error, RuntimeError)                    # 옛 호출자가 잡던 형태 그대로
        self.assertIn("파일 id 가 없다", str(error))
        self.assertEqual(post.call_count, 3)

    def test_non_dict_json_is_not_an_attribute_error(self):
        _, error, _, _ = self.call([Resp(["not", "a", "dict"])])
        self.assertIsInstance(error, F.NoFileId)

    def test_code_defects_are_not_retried(self):
        result, error, post, sleeps = self.call([TypeError("코드 결함")], backoff=(5, 15))
        self.assertIsInstance(error, TypeError)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(sleeps, [])

    def test_deadline_stops_retrying(self):
        """후속 재시도는 전체 시간에 상한이 있다 — 잡의 timeout 안에서 끝낸다."""
        result, error, post, sleeps = self.call(
            [bad_json(), bad_json(), ok()], backoff=(100, 100), deadline=50, clock=lambda: 0)
        self.assertIsNone(result)
        self.assertEqual(post.call_count, 1)
        self.assertEqual(sleeps, [])

    def test_trace_records_each_attempt(self):
        trace = []
        self.call([bad_json(), ok()], backoff=(5,), trace=trace)
        self.assertEqual(len(trace), 2)
        self.assertIn("실패", trace[0])
        self.assertIn("성공", trace[1])


class FreezeInlineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.status = os.path.join(self.tmp.name, "st.json")
        self.local = os.path.join(self.tmp.name, "local")
        self.bundle = make_bundle()

    def run_freeze(self, script, **kw):
        sleeps = []
        with mock.patch("requests.post", side_effect=script) as post, quiet():
            out = F.freeze("http://x", self.bundle, local_dir=kw.pop("local_dir", self.local),
                           status_path=self.status, sleep=sleeps.append, **kw)
        return out, post, sleeps, read_json(self.status)

    def test_success_on_second_attempt_is_recorded_as_such(self):
        (okf, name, detail), post, sleeps, st = self.run_freeze([bad_json(), ok("F2")])
        self.assertTrue(okf)
        self.assertIn("2회째 시도에서 성공", detail)
        self.assertTrue(st["ok"])
        self.assertEqual(st["attempts"], 2)
        self.assertEqual(sleeps, [5])

    def test_clean_first_try_is_unchanged(self):
        (okf, _, detail), post, sleeps, st = self.run_freeze([ok()])
        self.assertTrue(okf)
        self.assertNotIn("회째", detail)
        self.assertEqual((post.call_count, sleeps), (1, []))

    def test_total_failure_stays_a_failure_and_is_marked_retryable(self):
        (okf, name, detail), post, sleeps, st = self.run_freeze([bad_json()] * 3)
        self.assertFalse(okf)
        self.assertTrue(detail.startswith("업로드 실패"))             # 옛 문구 접두 유지
        self.assertIn("3회 시도", detail)
        self.assertFalse(st["ok"])
        self.assertTrue(st["required"])
        self.assertTrue(st["retryable"])
        self.assertEqual(st["attempts"], 3)
        self.assertEqual(sleeps, [5, 15])
        self.assertTrue(os.path.exists(os.path.join(self.local, name)))   # 다시 올릴 사본

    def test_without_a_local_copy_it_is_not_retryable(self):
        (okf, _, _), _, _, st = self.run_freeze([bad_json()] * 3, local_dir=None)
        self.assertFalse(okf)
        self.assertFalse(st["retryable"])

    def test_empty_bundle_never_touches_the_network(self):
        self.bundle = F.build_bundle("analyst", pool=[], prompt="P", response="R",
                                     now=datetime.datetime(2026, 10, 6, 15, 0, 5, tzinfo=KST))
        (okf, _, detail), post, _, st = self.run_freeze([ok()])
        self.assertFalse(okf)
        self.assertEqual(post.call_count, 0)
        self.assertIn("비어", detail)
        self.assertNotIn("retryable", st)


class RetryPendingTests(unittest.TestCase):
    """리포트를 다 보낸 뒤, 러너에 남은 사본을 길게 다시 올린다."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.status = os.path.join(self.tmp.name, "st.json")
        self.local = os.path.join(self.tmp.name, "local")
        self.bundle = make_bundle()

    def fail_first(self, retryable_dir=True):
        with mock.patch("requests.post", side_effect=[bad_json()] * 3), quiet():
            _, self.name, _ = F.freeze("http://x", self.bundle,
                                       local_dir=self.local if retryable_dir else None,
                                       status_path=self.status, sleep=lambda s: None)

    def retry(self, script, **kw):
        sleeps = []
        with mock.patch("requests.post", side_effect=script) as post, quiet():
            out = F.retry_pending(status_path=self.status, local_dir=self.local,
                                  gas_url="http://x", sleep=sleeps.append, **kw)
        return out, post, sleeps, read_json(self.status)

    def test_recovers_and_flips_the_status_the_check_step_reads(self):
        """10/2 의 경우 — 3분 뒤 창구가 정상이면 첫 후속 시도에서 보존된다."""
        self.fail_first()
        (action, why), post, sleeps, st = self.retry([ok("LATE1")])
        self.assertEqual(action, "recovered")
        self.assertTrue(st["ok"])
        self.assertTrue(st["required"])
        self.assertIn("후속 재시도로 보존", st["detail"])
        self.assertIn("총 4회째", st["detail"])                       # 앞선 3회 + 후속 1회
        self.assertIn("업로드 실패", st["detail"])                     # 앞선 실패 사유가 남는다
        self.assertEqual(st["attempts"], 4)
        self.assertEqual(post.call_count, 1)

    def test_uploads_exactly_the_saved_copy(self):
        self.fail_first()
        _, post, _, _ = self.retry([ok()])
        sent = base64.b64decode(post.call_args.kwargs["json"]["base64"])
        self.assertEqual(sent, pathlib.Path(self.local, self.name).read_bytes())
        self.assertEqual(post.call_args.kwargs["json"]["filename"], self.name)

    def test_still_failing_stays_a_failure_with_full_accounting(self):
        self.fail_first()
        (action, why), post, sleeps, st = self.retry([bad_json()] * 4)
        self.assertEqual(action, "failed")
        self.assertFalse(st["ok"])
        self.assertTrue(st["retryable"])
        self.assertEqual(st["attempts"], 3 + 4)
        self.assertIn("총 7회 시도", st["detail"])
        self.assertEqual(sleeps, list(F.UPLOAD_BACKOFF_LATE))

    def test_budget_caps_the_total_wait(self):
        self.fail_first()
        # 시계가 0 → 예산 10초. 첫 대기(20초)가 예산을 넘으므로 더 시도하지 않는다.
        (action, _), post, sleeps, _ = self.retry([bad_json()] * 4, budget_seconds=10,
                                                  clock=lambda: 0)
        self.assertEqual(action, "failed")
        self.assertEqual((post.call_count, sleeps), (1, []))

    def test_does_nothing_without_a_status_file(self):
        action, why = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual(action, "none")

    def test_does_nothing_when_already_ok(self):
        F.write_status(True, "a", "ok", path=self.status)
        with mock.patch("requests.post") as post:
            action, _ = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))

    def test_does_nothing_when_not_a_freeze_round(self):
        F.write_status(True, None, "20시 브리핑", path=self.status, required=False)
        with mock.patch("requests.post") as post:
            action, _ = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))

    def test_does_not_retry_failures_that_are_not_uploads(self):
        """빈 묶음·코드 예외는 다시 올려도 나아지지 않는다 — 그대로 둔다."""
        F.write_status(False, None, "동결 코드 자체가 예외: boom", path=self.status)
        with mock.patch("requests.post") as post:
            action, why = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))
        self.assertFalse(read_json(self.status)["ok"])

    def test_missing_copy_is_left_failing(self):
        self.fail_first(retryable_dir=False)
        with mock.patch("requests.post") as post:
            action, _ = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))
        self.assertFalse(read_json(self.status)["ok"])

    def test_a_copy_that_does_not_match_its_name_is_not_uploaded(self):
        self.fail_first()
        pathlib.Path(self.local, self.name).write_bytes(F.bundle_bytes({"kind": "analyst", "x": 1}))
        with mock.patch("requests.post") as post:
            action, why = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))
        self.assertIn("이름과 맞지 않는다", why)

    def test_a_corrupt_copy_is_not_uploaded(self):
        self.fail_first()
        pathlib.Path(self.local, self.name).write_bytes(b"not gzip")
        with mock.patch("requests.post") as post:
            action, _ = F.retry_pending(status_path=self.status, local_dir=self.local, gas_url="x",
                                         sleep=lambda s: None)
        self.assertEqual((action, post.call_count), ("none", 0))

    def test_name_survives_the_round_trip_for_real_bundles(self):
        """사본 검증이 정상 사본을 거부하지 않는가 — 실수·None·한글이 섞인 묶음으로."""
        data = F.bundle_bytes(self.bundle)
        again = json.loads(gzip.decompress(data).decode("utf-8"))
        self.assertEqual(F.bundle_name(again), F.bundle_name(self.bundle))

    def test_cli_is_harmless_without_a_status_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = subprocess.run([sys.executable, str(ROOT / "hyeoks_run_freeze.py"), "--retry-pending"],
                               cwd=d, capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("후속 재시도 대상 아님", p.stdout)


class BadgeArchivePolicyUnchangedTests(unittest.TestCase):
    """배지 보관은 여전히 **번들당 한 번만** 시도한다."""

    def test_archive_default_uploader_does_not_retry(self):
        import badge_observations as B
        with tempfile.TemporaryDirectory() as d:
            folder = pathlib.Path(d) / "20261006T145200000000"
            folder.mkdir()
            (folder / "started.json.gz").write_bytes(gzip.compress(json.dumps({"x": 1}).encode()))
            with mock.patch("requests.post", side_effect=[bad_json(), ok(), ok()]) as post, quiet():
                failures = B.archive(d)
        self.assertEqual(failures, 1)
        self.assertEqual(post.call_count, 1, "배지 보관이 자동 재시도하면 정책이 깨진다")


class WorkflowWiringTests(unittest.TestCase):
    """후속 재시도 단계가 두 워크플로에서 확인 단계 **앞에** 있고 잡을 죽이지 않는가."""

    def steps(self, name):
        import yaml
        d = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8"))
        return list(d["jobs"].values())[0]["steps"]

    def test_retry_step_precedes_the_check_in_both_workflows(self):
        for wf in ("ai_report.yml", "phase2_exit.yml"):
            with self.subTest(wf):
                names = [s.get("name", "") for s in self.steps(wf)]
                i = next(k for k, n in enumerate(names) if n.startswith("🧊 실행 입력 동결 재시도"))
                j = next(k for k, n in enumerate(names) if n.startswith("🧊 실행 입력 동결 확인"))
                self.assertLess(i, j)
                step = self.steps(wf)[i]
                self.assertEqual(step["if"], "always()")
                self.assertIs(step["continue-on-error"], True)
                self.assertEqual(step["run"].strip(), "python hyeoks_run_freeze.py --retry-pending")
                self.assertLessEqual(step["timeout-minutes"], 10)

    def test_check_step_is_unchanged_so_failure_is_still_loud(self):
        for wf in ("ai_report.yml", "phase2_exit.yml"):
            with self.subTest(wf):
                step = next(s for s in self.steps(wf)
                            if s.get("name", "").startswith("🧊 실행 입력 동결 확인"))
                self.assertEqual(step["if"], "always()")
                self.assertNotIn("continue-on-error", step)
                self.assertIn("sys.exit(1)", step["run"])

    def test_the_candidate_pool_copy_never_reaches_the_public_repo(self):
        """사본은 후보 풀 = 시스템의 신호다. 공개 저장소에 커밋되면 되돌릴 수 없다."""
        ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("data/run_freeze/", ignored)
        self.assertIn("freeze_status.json", ignored)
        sh = (ROOT / ".github" / "receipt_commit.sh").read_text(encoding="utf-8")
        self.assertIn("git add -- data/receipts", sh)
        self.assertNotIn("run_freeze", sh)


if __name__ == "__main__":
    unittest.main()
