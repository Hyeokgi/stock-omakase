"""2026-10-03 — 판정기의 GitHub 실행 결론 조회: 일시 오류만 재시도하고, 끝내 확정 못 하면 불확정으로 표시한다.

이전 판은 `run_conclusion()` 을 **한 번** 불러 URLError·5xx·JSON 깨짐이면 그 워크플로의 결론을 비워 두었다.
Builder 는 그것을 "결론없음" 으로 읽어 `no_unexplained_failure` 를 거짓으로 만들었고, runs.csv 는 append-only 이며
같은 거래일을 두 번 기록하지 않으므로 **인프라 잡음 하나가 그날을 영구 FAIL 로 박제**할 수 있었다.

지킬 것
  · 일시 오류(연결·시간 초과·5xx·429·깨진 본문)만 재시도한다. 404·401 같은 확정 오류는 재시도하지 않는다.
  · 재시도 끝에도 일시 오류면 "불확정" — FAIL 도 PASS 도 아니다. WORKFLOW_PENDING 으로 내보낸다.
  · 이 스크립트의 stdout 은 $GITHUB_ENV 로 간다 — `NAME=값` 줄 외에는 한 줄도 찍지 않는다.
  · 전체 시간에 상한이 있다(finalizer 의 timeout-minutes 안에서 끝낸다).
  · `run_conclusion()` 자체는 그대로다(기존 시험이 그 본문을 본다).
"""
import contextlib
import http.client
import importlib.util
import io
import json
import os
import pathlib
import subprocess
import sys
import unittest
import urllib.error
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_module():
    spec = importlib.util.spec_from_file_location(
        "workflow_states_under_test", ROOT / ".github" / "workflow_states.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


WS = load_module()


def http_error(code):
    return urllib.error.HTTPError("https://api.github.com/x", code, "msg", {}, None)


def quiet_err():
    return contextlib.redirect_stderr(io.StringIO())


class TransientClassificationTests(unittest.TestCase):
    def test_table(self):
        cases = [
            (http_error(500), True), (http_error(502), True), (http_error(503), True),
            (http_error(504), True), (http_error(429), True), (http_error(408), True),
            (http_error(403), True),                                   # rate limit 도 403 으로 온다
            (http_error(404), False), (http_error(401), False), (http_error(422), False),
            (urllib.error.URLError("연결 실패"), True),
            (TimeoutError("timed out"), True),
            (ConnectionResetError("reset"), True),
            (OSError("network unreachable"), True),
            (http.client.IncompleteRead(b"par"), True),
            (json.JSONDecodeError("Expecting value", "", 0), True),    # 10/2 에 실제로 난 오류 모양
            (UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), True),
            (ValueError("unknown url type: 'x'"), False),              # 요청을 만들다 난 결함 — 재시도로 안 낫는다
        ]
        for exc, expect in cases:
            with self.subTest(exc=repr(exc)):
                self.assertIs(WS.is_transient(exc), expect)


class LookupRetryTests(unittest.TestCase):
    def lookup(self, script, **kw):
        fetch = mock.Mock(side_effect=script)
        sleeps = []
        with quiet_err():
            out = WS.lookup_with_retry("o/r", "123", "tok", ".github/workflows/main.yml",
                                       fetch=fetch, sleep=sleeps.append, **kw)
        return out, fetch, sleeps

    def test_recovers_from_transient_errors(self):
        out, fetch, sleeps = self.lookup(
            [urllib.error.URLError("x"), http_error(503), ("success", "")])
        self.assertEqual(out, ("success", "", False))
        self.assertEqual(fetch.call_count, 3)
        self.assertEqual(sleeps, [3, 10])

    def test_persistent_transient_error_is_inconclusive_not_a_failure(self):
        out, fetch, sleeps = self.lookup([urllib.error.URLError("x")] * 4)
        conclusion, why, inconclusive = out
        self.assertEqual(conclusion, "")
        self.assertTrue(inconclusive)
        self.assertIn("4회 시도", why)
        self.assertEqual(fetch.call_count, 4)
        self.assertEqual(sleeps, list(WS.RETRY_WAITS))

    def test_definitive_http_errors_are_not_retried(self):
        for code in (404, 401, 422):
            with self.subTest(code=code):
                out, fetch, sleeps = self.lookup([http_error(code)])
                self.assertEqual(out[0], "")
                self.assertFalse(out[2], "확정 오류를 불확정으로 보류하면 안 된다")
                self.assertEqual((fetch.call_count, sleeps), (1, []))

    def test_a_definitive_answer_is_returned_even_when_it_is_a_refusal(self):
        """다른 워크플로의 run 이라 인정하지 않는 것은 '답' 이다 — 재시도도 보류도 아니다."""
        out, fetch, sleeps = self.lookup([("", "워크플로 불일치 기대=a 실제=b")])
        self.assertEqual(out, ("", "워크플로 불일치 기대=a 실제=b", False))
        self.assertEqual(fetch.call_count, 1)

    def test_code_defects_still_crash_loudly(self):
        with self.assertRaises(AttributeError):
            self.lookup([AttributeError("'list' object has no attribute 'get'")])

    def test_deadline_bounds_the_total_wait(self):
        out, fetch, sleeps = self.lookup([urllib.error.URLError("x")] * 4,
                                         deadline=5, clock=lambda: 0)
        self.assertTrue(out[2])
        self.assertEqual(sleeps, [3])            # 3초는 예산 안, 다음 10초는 넘으므로 멈춘다
        self.assertEqual(fetch.call_count, 2)

    def test_real_run_conclusion_path_with_a_flaky_network(self):
        """가짜 fetch 가 아니라 실제 `run_conclusion` 으로 — urlopen 만 흔들어 본다."""
        import io as _io
        body = json.dumps({"path": ".github/workflows/main.yml", "status": "completed",
                           "conclusion": "success"}).encode()
        sleeps = []
        with mock.patch.object(WS.urllib.request, "urlopen",
                               side_effect=[urllib.error.URLError("boom"), http_error(502),
                                            _io.BytesIO(body)]), quiet_err():
            out = WS.lookup_with_retry("o/r", "123", "tok", ".github/workflows/main.yml",
                                       sleep=sleeps.append)
        self.assertEqual(out, ("success", "", False))
        self.assertEqual(sleeps, [3, 10])

    def test_real_run_conclusion_still_refuses_other_workflows(self):
        import io as _io
        body = json.dumps({"path": ".github/workflows/other.yml", "status": "completed",
                           "conclusion": "success"}).encode()
        with mock.patch.object(WS.urllib.request, "urlopen", return_value=_io.BytesIO(body)), \
                quiet_err():
            out = WS.lookup_with_retry("o/r", "123", "tok", ".github/workflows/main.yml",
                                       sleep=lambda s: None)
        self.assertEqual(out[0], "")
        self.assertIn("워크플로 불일치", out[1])
        self.assertFalse(out[2])


class MainOutputTests(unittest.TestCase):
    """stdout 은 $GITHUB_ENV 로 간다. 줄 하나가 어긋나면 다음 스텝이 통째로 죽는다."""

    def run_main(self, results, with_receipts=True):
        def fake_latest(day, kind, fingerprint=None):
            return {"run_id": "123"} if with_receipts else None

        def fake_lookup(repo, run_id, token, expect, **kw):
            return results[expect.rsplit("/", 1)[1]]

        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(WS.production_receipt, "latest", fake_latest), \
                mock.patch.object(WS, "lookup_with_retry", fake_lookup), \
                mock.patch("sys.argv", ["workflow_states.py", "o/r", "2026-10-02"]), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = WS.main()
        env = dict(line.split("=", 1) for line in out.getvalue().splitlines())
        return rc, env, out.getvalue(), err.getvalue()

    OK = ("success", "", False)

    def test_all_resolved(self):
        rc, env, _, _ = self.run_main({"main.yml": self.OK, "ai_report.yml": self.OK,
                                       "earnings_collector.yml": self.OK})
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(env["WORKFLOW_STATES"]),
                         {"main.yml": "success", "ai_report.yml": "success",
                          "earnings_collector.yml": "success"})
        self.assertEqual(json.loads(env["WORKFLOW_PENDING"]), [])

    def test_an_inconclusive_lookup_is_pending_and_absent_from_states(self):
        rc, env, _, err = self.run_main({"main.yml": self.OK,
                                         "ai_report.yml": ("", "일시 오류가 이어졌다", True),
                                         "earnings_collector.yml": self.OK})
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(env["WORKFLOW_PENDING"]), ["ai_report.yml"])
        self.assertNotIn("ai_report.yml", json.loads(env["WORKFLOW_STATES"]))
        self.assertIn("판정을 보류한다", err)
        self.assertNotIn("결론을 못 얻은 워크플로: ['ai_report.yml']", err,
                         "보류한 워크플로를 '못 얻었다' 로도 세면 같은 사실을 두 번 말한다")

    def test_a_decided_failure_is_reported_as_a_failure(self):
        _, env, _, _ = self.run_main({"main.yml": self.OK, "ai_report.yml": ("failure", "", False),
                                      "earnings_collector.yml": self.OK})
        self.assertEqual(json.loads(env["WORKFLOW_STATES"])["ai_report.yml"], "failure")
        self.assertEqual(json.loads(env["WORKFLOW_PENDING"]), [])

    def test_a_refusal_is_neither_a_conclusion_nor_pending(self):
        """다른 워크플로의 run 이라 인정하지 않은 경우 — 이전처럼 '결론없음' 으로 남아 FAIL 이 된다."""
        _, env, _, _ = self.run_main({"main.yml": ("", "워크플로 불일치", False),
                                      "ai_report.yml": self.OK, "earnings_collector.yml": self.OK})
        self.assertNotIn("main.yml", json.loads(env["WORKFLOW_STATES"]))
        self.assertEqual(json.loads(env["WORKFLOW_PENDING"]), [])

    def test_stdout_has_only_env_lines(self):
        _, env, stdout, _ = self.run_main({"main.yml": ("", "x", True), "ai_report.yml": self.OK,
                                           "earnings_collector.yml": self.OK})
        lines = stdout.splitlines()
        self.assertEqual(len(lines), 2, stdout)
        self.assertEqual([l.split("=", 1)[0] for l in lines], ["WORKFLOW_STATES", "WORKFLOW_PENDING"])

    def test_no_receipts_means_no_lookups_and_nothing_pending(self):
        rc, env, _, _ = self.run_main({}, with_receipts=False)
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(env["WORKFLOW_STATES"]), {})
        self.assertEqual(json.loads(env["WORKFLOW_PENDING"]), [])


class CliTests(unittest.TestCase):
    def test_cli_emits_both_env_lines_exactly_once(self):
        e = dict(os.environ)
        e["GH_TOKEN"] = ""
        p = subprocess.run([sys.executable, ".github/workflow_states.py",
                            "Hyeokgi/stock-omakase", "2026-09-18"],
                           cwd=str(ROOT), capture_output=True, text=True, timeout=120, env=e)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        keys = [l.split("=", 1)[0] for l in p.stdout.splitlines()]
        self.assertEqual(keys, ["WORKFLOW_STATES", "WORKFLOW_PENDING"], p.stdout)
        self.assertEqual(json.loads(p.stdout.splitlines()[1].split("=", 1)[1]), [])

    def test_finalizer_wiring_passes_the_pending_list_with_a_safe_default(self):
        import yaml
        d = yaml.safe_load((ROOT / ".github/workflows/stability_finalizer.yml")
                           .read_text(encoding="utf-8"))
        steps = d["jobs"]["finalize"]["steps"]
        record = next(s for s in steps if s.get("name", "").startswith("🚦 사이클 판정"))
        line = next(l for l in record["run"].splitlines() if l.startswith("python"))
        self.assertIn('--workflows "$WORKFLOW_STATES"', line)
        self.assertIn('--workflows-pending "${WORKFLOW_PENDING:-[]}"', line)
        self.assertEqual(d["jobs"]["finalize"]["timeout-minutes"], 10)     # 조회 상한 ≈4.5분 + 나머지 단계


if __name__ == "__main__":
    unittest.main()
