# -*- coding: utf-8 -*-
"""배지 수집 실패가 **주 산출물 판정과 분리**되는지 — 실패를 가정하고 확인한다.

왜 이 시험이 있나 (2026-09-20)
------------------------------
`16424d2` 가 `main.yml` 에 배지 보존 스텝을 넣으면서 `--upload` 가 실패 시 exit 1
이었다. 그 경로를 실측으로 따라가 보면:

    배지 업로드 실패 → 스텝 exit 1 → main.yml run = failure
      → workflow_states 가 "failure" 보고
      → evidence_builder.no_unexplained_failure = False
      → **그 거래일 Gate FAIL**

연구 자료 수집 실패가 생산 안정화 판정을 떨어뜨린다. `consensus_aux.yml` 를
분리한 이유와 **같은 범주**다 — "보조 자료의 실패가 주 산출물의 판정을 흐리지 않게".

게다가 닿지 않는 경로가 아니다. 스캐너 영수증은 EOD 창(15:00~15:30)의 회차가 내고
배지 관측 창은 14:20~15:10 이라 **15:00~15:10 에서 겹친다.**

그래서: 주 시스템에는 비차단, **자체 상태에는 실패로 기록.**
"""
import ast
import gzip
import json
import os
import pathlib
import subprocess
import sys
import unittest

import badge_observations as B
import evidence_builder as E

ROOT = pathlib.Path(__file__).resolve().parent.parent


def seed(folder, *, completed=True, intent=False, receipt=False):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "started.json.gz").write_bytes(gzip.compress(b'{"state":"STARTED"}'))
    if completed:
        (folder / "completed.json.gz").write_bytes(gzip.compress(b'{"state":"COMPLETE"}'))
    if intent:
        (folder / "upload_intent.json.gz").write_bytes(gzip.compress(b'{"filename":"x"}'))
    if receipt:
        (folder / "upload_receipt.json.gz").write_bytes(gzip.compress(b'{"state":"OK"}'))
    return folder


def status_of(folder):
    p = folder / "upload_status.json.gz"
    if not p.exists():
        return None
    return json.loads(gzip.decompress(p.read_bytes()))


class FailureIsRecordedInItsOwnStateTests(unittest.TestCase):
    """비차단이 '조용히 넘어간다' 는 뜻은 아니다. 실패는 자체 상태에 남는다."""

    def setUp(self):
        self.tmp = __import__("tempfile").TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_failed_upload_is_recorded(self):
        f = seed(self.root / "20260921T150200000000")
        n = B.archive(root=self.root, uploader=lambda name, data: None)   # Drive id 없음
        self.assertEqual(n, 1, "실패 건수는 여전히 센다")
        self.assertEqual(status_of(f)["state"], "FAILED")
        self.assertIn("no automatic retry", status_of(f)["reason"])

    def test_an_uncertain_upload_is_recorded(self):
        f = seed(self.root / "20260921T150200000000", intent=True)
        n = B.archive(root=self.root, uploader=lambda name, data: "id")
        self.assertEqual(n, 1)
        self.assertEqual(status_of(f)["state"], "UNCERTAIN")

    def test_an_incomplete_scan_is_recorded_even_when_the_upload_worked(self):
        """업로드가 성공해도 표본이 불완전하면 유효 표본이 아니다."""
        f = seed(self.root / "20260921T150200000000", completed=False)
        n = B.archive(root=self.root, uploader=lambda name, data: "driveid")
        self.assertEqual(n, 1)
        self.assertEqual(status_of(f)["state"], "INCOMPLETE")

    def test_a_clean_upload_records_no_failure(self):
        f = seed(self.root / "20260921T150200000000")
        n = B.archive(root=self.root, uploader=lambda name, data: "driveid")
        self.assertEqual(n, 0)
        self.assertIsNone(status_of(f), "성공했는데 실패 상태를 남기지 않는다")

    def test_nothing_collected_is_not_a_failure(self):
        """관측 창 밖 회차(하루 대부분)는 실패가 아니다."""
        self.assertEqual(B.archive(root=self.root, uploader=lambda n, d: "id"), 0)


class TheStepNeverBlocksTheMainSystemTests(unittest.TestCase):
    """CLI 가 실제로 exit 0 인가 — 워크플로가 읽는 것은 종료코드뿐이다."""

    def run_cli(self, seeded):
        tmp = __import__("tempfile").TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        # ROOT 는 상대경로다. cwd 를 옮기면 그 아래를 본다.
        target = pathlib.Path(tmp.name) / "data/research_private/badge_observations"
        if seeded:
            # intent-만-있는 상태: 업로드를 **시도하기 전에** 실패로 판정된다(네트워크 없음)
            seed(target / "20260921T150200000000", intent=True)
        else:
            target.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, PYTHONPATH=str(ROOT))
        return subprocess.run([sys.executable, str(ROOT / "badge_observations.py"), "--upload"],
                              cwd=tmp.name, capture_output=True, text=True, timeout=60, env=env)

    def test_exit_code_is_zero_when_the_upload_fails(self):
        p = self.run_cli(seeded=True)
        self.assertEqual(p.returncode, 0,
                         f"exit 1 이면 main.yml 이 빨개지고 그 거래일이 FAIL 이다:\n{p.stderr[-500:]}")

    def test_the_failure_is_still_announced(self):
        p = self.run_cli(seeded=True)
        out = p.stdout + p.stderr
        self.assertIn("배지 수집 저하", out, "비차단이 조용함을 뜻하지는 않는다")
        self.assertIn("::warning::", out)

    def test_it_does_not_emit_a_blocking_error_annotation(self):
        p = self.run_cli(seeded=True)
        self.assertNotIn("::error::", p.stdout + p.stderr,
                         "아무것도 막지 않으면서 error 로 표시하면 판독을 헷갈리게 한다")

    def test_exit_code_is_zero_when_there_is_nothing_to_upload(self):
        self.assertEqual(self.run_cli(seeded=False).returncode, 0)


class HonestReportingTests(unittest.TestCase):
    """2026-09-20 코덱스 지적 3건 — 말이 동작보다 앞서지 않게 한다.

    ① "항상 exit 0" 은 과한 표현이었다. 처리한 실패 경로만 비차단이었고
       미포착 예외는 그대로 올라갔다(실측 확인).
    ② "N건" 이 실제 건수가 아니었다. `archive()` 가 `1 if failures else 0` 이라
       **5건이 실패해도 1건으로 보고**됐다.
    ③ "주 산출물은 정상" 은 이 코드가 확인하지 않는 사실이다.
    """

    def setUp(self):
        self.tmp = __import__("tempfile").TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    # ② 건수
    def test_the_failure_count_is_the_real_count(self):
        for i in range(5):
            seed(self.root / f"2026092{i}T150000000000")
        self.assertEqual(B.archive(root=self.root, uploader=lambda n, d: None), 5,
                         "요약에 싣는 값이므로 실제 건수여야 한다")

    def test_a_single_failure_still_reports_one(self):
        seed(self.root / "20260921T150000000000")
        self.assertEqual(B.archive(root=self.root, uploader=lambda n, d: None), 1)

    def test_no_failure_is_falsy(self):
        seed(self.root / "20260921T150000000000")
        self.assertFalse(B.archive(root=self.root, uploader=lambda n, d: "id"))

    # ① 미포착 예외
    def test_an_unhandled_exception_inside_archive_does_not_block(self):
        """`archive()` 첫 줄의 `import hyeoks_run_freeze` 는 폴더 루프 **밖**이라
        per-bundle try/except 가 덮지 않는다. 실제로 미포착인 그 경로를 쓴다.

        (`__main__` 으로 돌아가는 모듈은 `import badge_observations` 와 다른 객체라
         모듈 속성 패치로는 이 경로에 닿지 못한다 — 첫 시도가 그래서 헛통과했다.)
        """
        tmp = __import__("tempfile").TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        work = pathlib.Path(tmp.name)
        # 깨진 대역 모듈을 **cwd** 에 둔다. `-c` 로 띄우면 sys.path[0] 이 cwd 라
        # 저장소본보다 먼저 잡힌다(스크립트를 직접 실행하면 스크립트 디렉터리가
        # sys.path[0] 이라 저장소본이 이긴다 — 두 번째 시도가 그래서 헛통과했고,
        # 그때 실제 네트워크 업로드까지 나갔다).
        (work / "hyeoks_run_freeze.py").write_text(
            "raise RuntimeError('freeze module broken')\n", encoding="utf-8")
        seed(work / "data/research_private/badge_observations" / "20260921T150200000000")
        env = dict(os.environ, PYTHONPATH=str(ROOT))
        launcher = ("import runpy, sys; sys.argv = ['badge_observations.py', '--upload']; "
                    f"runpy.run_path(r'{ROOT / 'badge_observations.py'}', run_name='__main__')")
        p = subprocess.run([sys.executable, "-c", launcher],
                           cwd=str(work), capture_output=True, text=True, timeout=60, env=env)
        out = p.stdout + p.stderr
        self.assertNotIn("ProxyError", out, "시험이 실제 네트워크를 타면 안 된다")
        self.assertEqual(p.returncode, 0, f"미포착 예외가 스텝을 죽이면 안 된다:\n{out[-600:]}")
        self.assertIn("처리되지 않은 예외", out, "조용히 삼키지 않는다")

    # ③ 확인하지 않은 것을 주장하지 않는다
    def test_it_does_not_claim_the_main_output_is_healthy(self):
        src = (ROOT / "badge_observations.py").read_text(encoding="utf-8")
        live = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
        self.assertNotIn("주 산출물은 정상이므로", live,
                         "이 코드는 주 산출물의 정상 여부를 확인하지 않는다")
        self.assertIn("배지 실패만으로 주 시스템을 실패 처리하지 않는다", live)


class ShellGuardTests(unittest.TestCase):
    """모듈 로드 실패는 파이썬 코드가 돌기 전이라 모듈 안에서 막을 수 없다.

    그 마지막 한 겹은 워크플로의 셸 가드가 맡는다. GitHub 은 `bash -e` 로 돌린다.
    """

    def run_block(self):
        import yaml
        d = yaml.safe_load((ROOT / ".github/workflows/main.yml").read_text(encoding="utf-8"))
        for s in d["jobs"]["build"]["steps"]:
            if "배지" in str(s.get("name", "")):
                return s["run"]
        self.fail("배지 스텝을 찾지 못했다")

    def test_the_step_survives_a_collector_that_cannot_start(self):
        script = self.run_block().replace("python badge_observations.py --upload",
                                          'python -c "import nope_xyz_module"')
        tmp = __import__("tempfile").NamedTemporaryFile("w", suffix=".sh", delete=False)
        self.addCleanup(os.unlink, tmp.name)
        tmp.write(script); tmp.close()
        p = subprocess.run(["bash", "-e", tmp.name], capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0,
                         f"bash -e 에서도 스텝이 초록이어야 한다:\n{p.stdout}{p.stderr}")
        self.assertIn("::warning::", p.stdout + p.stderr, "조용히 삼키지 않는다")


class SeparationFromTheMainJudgementTests(unittest.TestCase):
    """배지 상태가 Gate 판정의 **입력이 아니다** — 구조로 확인한다."""

    def judged(self, main_conclusion):
        ev, det = E.build("2026-09-21", "fp", root="/nonexistent-receipts",
                          workflow_states={"main.yml": main_conclusion,
                                           "ai_report.yml": "success",
                                           "earnings_collector.yml": "success"})
        return det["reasons"]["no_unexplained_failure"]

    def test_a_red_main_yml_does_fail_the_cycle(self):
        """이것이 위험의 근거다 — 그래서 스텝이 절대 빨개지면 안 된다."""
        self.assertIn("main.yml=failure", self.judged("failure"))

    def test_a_green_main_yml_carries_no_workflow_complaint(self):
        self.assertNotIn("main.yml=failure", self.judged("success"))

    def test_the_gate_never_imports_badge_observations(self):
        """판정 코드가 배지를 읽으면 분리가 아니다. AST 로 본다."""
        for name in ("evidence_builder.py", "stability_gate.py", "production_receipt.py"):
            tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    mods = [a.name.split(".")[0] for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    mods = [(node.module or "").split(".")[0]]
                else:
                    continue
                self.assertNotIn("badge_observations", mods,
                                 f"{name} 이 배지를 판정 입력으로 쓴다")

    def test_badge_criteria_are_not_among_the_gate_criteria(self):
        self.assertNotIn("badge", " ".join(E.build.__doc__ or ""))
        for key in E.build("2026-09-21", "fp", root="/nonexistent-receipts",
                           workflow_states={})[0]:
            self.assertNotIn("badge", key)

    def test_the_fingerprint_still_covers_the_module(self):
        """판정 입력은 아니지만 생산 코드이므로 지문에는 있어야 한다."""
        import stability_gate as G
        self.assertIn("badge_observations.py", G.FINGERPRINT_FILES)


class WorkflowWiringTests(unittest.TestCase):
    YML = ROOT / ".github" / "workflows" / "main.yml"

    def live(self):
        return "\n".join(l for l in self.YML.read_text(encoding="utf-8").splitlines()
                         if not l.lstrip().startswith("#"))

    def test_the_badge_step_is_present_and_always_runs(self):
        self.assertIn("badge_observations.py --upload", self.live())

    def test_the_badge_step_does_not_guard_the_scanner(self):
        """배지 스텝은 스캔 **뒤**에 온다. 앞에 오면 수집을 막을 수 있다."""
        live = self.live()
        self.assertLess(live.index("python omakase.py"),
                        live.index("badge_observations.py --upload"))
