# -*- coding: utf-8 -*-
"""
생산 진입점 스모크 (L4) — 2026-09-18.

🔴 `7f123c5` 에서 내가 `--phase` 분기를 넣으면서 `import sys` 를 빠뜨렸다.
   484건이 통과하고 CI 가 초록이었는데 **생산 진입점은 즉사했다.**

       NameError: name 'sys' is not defined   (hyeoks_earnings_collector.py:476)

   시험이 전부 소스 텍스트나 순수 함수만 봤기 때문이다. **아무도 스크립트를
   실제로 실행하지 않았다.** 그래서 두 층을 더한다.

   L4-a  진입점을 실제로 subprocess 로 돌려 인자 파싱 구간을 통과하는지 본다
   L4-b  모듈 전역에서 쓰는 표준 모듈이 실제로 import 돼 있는지 AST 로 본다
         (이 검사 하나면 이번 NameError 는 코드를 돌리지 않고도 잡힌다)
"""
import ast
import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent

# 이름만 봐도 표준 모듈인 것들 — 지역 변수로 쓰일 일이 거의 없다
STDLIB_HINTS = {
    "sys", "os", "re", "json", "csv", "time", "math", "random", "glob", "gzip",
    "shutil", "hashlib", "tempfile", "argparse", "subprocess", "pathlib",
    "datetime", "itertools", "collections", "statistics", "unittest", "zipfile",
    "textwrap", "traceback", "sqlite3", "base64", "warnings", "io",
}


def module_bound_names(tree):
    """이 파일에서 import 되었거나 대입된 최상위 이름 전부."""
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                names.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                names.add(a.asname or a.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    names.add(t.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.arg,)):
            names.add(node.arg)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
        elif isinstance(node, (ast.For, ast.comprehension)):
            tgt = getattr(node, "target", None)
            if isinstance(tgt, ast.Name):
                names.add(tgt.id)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
    return names


def scanned_sources():
    """AST 검사 대상. **워크플로가 부르는 `.github/*.py` 도 포함한다.**"""
    return (sorted(ROOT.glob("*.py"))
            + sorted((ROOT / "tests").glob("*.py"))
            + sorted((ROOT / ".github").glob("*.py")))


class UndefinedModuleUseTests(unittest.TestCase):
    """`sys.argv` 를 쓰면서 `import sys` 를 안 한 파일이 있는가.

    2026-09-18 확장 — 표준 모듈만 보다가 `hyeoks_tajeom.POLICY_ID` 를 import 없이
    쓴 것을 놓칠 뻔했다. **이 저장소의 모듈 이름**도 같은 방식으로 검사한다.
    """

    def scanned(self):
        return scanned_sources()

    def check(self, names, label):
        bad = []
        for path in self.scanned():
            tree = ast.parse(path.read_text(encoding="utf-8"))
            bound = module_bound_names(tree)
            for node in ast.walk(tree):
                # `sys.argv` 같은 **속성 접근**만 본다(지역 변수 오탐을 줄인다)
                if (isinstance(node, ast.Attribute)
                        and isinstance(node.value, ast.Name)
                        and node.value.id in names
                        and node.value.id != path.stem
                        and node.value.id not in bound):
                    bad.append(f"{path.name}:{node.lineno} {node.value.id}.{node.attr}")
        self.assertEqual(bad, [], f"import 없이 쓰는 {label}: {bad}")

    def test_no_stdlib_module_is_used_without_being_imported(self):
        self.check(STDLIB_HINTS, "표준 모듈")

    def test_no_local_module_is_used_without_being_imported(self):
        self.check({p.stem for p in ROOT.glob("*.py")}, "저장소 모듈")

    def test_github_helper_scripts_are_scanned_too(self):
        """🔴 2026-09-19 — `.github/*.py` 가 검사 범위 밖이었다.

        workflow_states.py 는 여기서 한 번도 안 읽혔고, 그래서 그 파일의
        import 문제가 생산 첫 실행까지 살아남았다.
        """
        scanned = {p.name for p in self.scanned()}
        self.assertIn("workflow_states.py", scanned)


class WorkflowStatesCliTests(unittest.TestCase):
    """🔴 2026-09-19 생산 P0 ① — finalizer run #1 이 여기서 즉사했다.

        ModuleNotFoundError: No module named 'evidence_builder'

    `python .github/workflow_states.py` 로 부르면 sys.path[0] 이 `.github/` 다.
    저장소 루트 모듈을 import 할 수 없다. 546건이 통과했는데도 못 잡았다 —
    이 파일에 대한 시험이 전부 AST·텍스트였고 **아무도 실행하지 않았기** 때문이다.
    `import sys` 누락 때와 같은 결함이라 같은 처방을 쓴다: 실제로 돌린다.
    """

    SCRIPT = ".github/workflow_states.py"

    def run_cli(self, *args):
        e = dict(os.environ)
        e["GH_TOKEN"] = ""            # 조회는 실패해도 된다. import 구간만 본다
        # 워크플로와 **똑같이** 부른다: 루트에서 상대 경로로
        return subprocess.run([sys.executable, self.SCRIPT, *args],
                              cwd=str(ROOT), capture_output=True, text=True, timeout=120, env=e)

    def test_it_survives_being_invoked_the_way_the_workflow_invokes_it(self):
        p = self.run_cli("Hyeokgi/stock-omakase", "2026-09-18")
        joined = p.stdout + p.stderr
        for fatal in ("ModuleNotFoundError", "ImportError", "NameError"):
            self.assertNotIn(fatal, joined, f"{self.SCRIPT} 가 {fatal} 로 죽는다:\n{joined[-800:]}")
        self.assertEqual(p.returncode, 0, joined[-800:])

    def test_it_emits_the_env_line_the_next_step_reads(self):
        """이 줄이 $GITHUB_ENV 로 가고 다음 스텝이 $WORKFLOW_STATES 로 읽는다."""
        p = self.run_cli("Hyeokgi/stock-omakase", "2026-09-18")
        lines = [l for l in p.stdout.splitlines() if l.startswith("WORKFLOW_STATES=")]
        self.assertEqual(len(lines), 1, p.stdout)
        json.loads(lines[0].split("=", 1)[1])          # 파싱되는 JSON 이어야 한다


class FinalizerCycleDateTests(unittest.TestCase):
    """🔴 2026-09-19 생산 P0 ② — 판정 대상 거래일을 벽시계로 정했다.

    예약 23:30 KST 가 3시간 19분 밀려 02:49 KST(토)에 돌았고, 대상이 토요일이
    되면서 금요일 9/18 은 아무도 판정하지 않았다. 수집기에서 방금 고친 것과
    같은 결함이 finalizer 에 남아 있었다.
    """

    YML = ROOT / ".github" / "workflows" / "stability_finalizer.yml"

    def src(self):
        return self.YML.read_text(encoding="utf-8")

    def live(self):
        """주석을 뺀 **실행되는** 줄만. 설명문에 적힌 금칙어를 위반으로 세지 않는다
        (예전에 설명 docstring 을 잡아 거짓 통과·거짓 실패를 낸 적이 있다)."""
        out = []
        for line in self.src().splitlines():
            body = line.split("#", 1)[0] if line.lstrip().startswith("#") else line
            out.append(body)
        return "\n".join(out)

    def test_the_wall_clock_date_is_gone(self):
        self.assertNotIn("date +%Y-%m-%d", self.live(),
                         "벽시계 날짜를 판정 대상으로 쓰면 cron 지연이 거래일을 삼킨다")

    def test_it_asks_the_calendar_instead(self):
        self.assertIn("--previous-trading-day", self.live())

    def test_it_runs_in_the_morning_after_the_trading_day(self):
        """23:30 KST 예약은 구조적으로 위험하다 — 실적이 00:17 에 끝난 적이 있다."""
        m = re.search(r"cron:\s*'(\d+)\s+(\d+)\s+\*\s+\*\s+([^']+)'", self.src())
        self.assertIsNotNone(m, "finalizer cron 을 읽지 못했다")
        minute, hour = int(m.group(1)), int(m.group(2))
        kst_hour = (hour + 9) % 24
        self.assertTrue(4 <= kst_hour <= 8,
                        f"판정은 거래일 **다음 날 이른 아침**이어야 한다 (지금 {kst_hour}:{minute:02d} KST)")

    def test_the_target_is_never_today(self):
        """달력이 답하는 값은 항상 오늘보다 앞선다 — 진행 중인 거래일을 판정하지 않는다."""
        p = subprocess.run([sys.executable, "production_receipt.py", "--previous-trading-day"],
                           cwd=str(ROOT), capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stderr)
        day = p.stdout.strip()
        self.assertRegex(day, r"^\d{4}-\d{2}-\d{2}$")
        self.assertLess(day, datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=9))).date().isoformat())


class EarningsCollectorCliTests(unittest.TestCase):
    """실제로 돌려 본다. 자격증명이 없어도 **인자 파싱 구간은 통과해야 한다.**"""

    SCRIPT = "hyeoks_earnings_collector.py"

    def run_phase(self, *args, env=None):
        e = dict(os.environ)
        e.pop("DART_API_KEY", None)
        e.update(env or {})
        return subprocess.run([sys.executable, str(ROOT / self.SCRIPT), *args],
                              cwd=str(ROOT), capture_output=True, text=True, timeout=120, env=e)

    def assertNoImportError(self, proc):
        joined = proc.stdout + proc.stderr
        for fatal in ("NameError", "ImportError", "ModuleNotFoundError",
                      "AttributeError: module"):
            self.assertNotIn(fatal, joined, f"진입점이 {fatal} 로 죽는다:\n{joined[-800:]}")

    def test_primary_gets_past_argument_parsing(self):
        p = self.run_phase("--phase", "primary")
        self.assertNoImportError(p)
        self.assertIn("phase=primary", p.stdout)

    def test_aux_gets_past_argument_parsing(self):
        p = self.run_phase("--phase", "aux")
        self.assertNoImportError(p)
        self.assertIn("phase=aux", p.stdout)

    def test_aux_does_not_require_the_dart_key(self):
        """⑩ — aux 는 DART 를 안 쓰는데 키가 없다고 죽고 있었다."""
        p = self.run_phase("--phase", "aux")
        self.assertNotIn("DART_API_KEY 환경변수가 없습니다", p.stdout)

    def test_primary_still_requires_the_dart_key(self):
        p = self.run_phase("--phase", "primary")
        self.assertIn("DART_API_KEY 환경변수가 없습니다", p.stdout)

    def test_bad_phase_is_rejected_with_exit_two(self):
        p = self.run_phase("--phase", "bogus")
        self.assertEqual(p.returncode, 2)
        self.assertIn("all|primary|aux", p.stdout)

    def test_default_phase_is_all(self):
        p = self.run_phase()
        self.assertNoImportError(p)
        self.assertIn("phase=all", p.stdout)


class DeadCodeTests(unittest.TestCase):
    def test_no_leftover_noop_argparse(self):
        """쓰지 않는 파서를 남겨 두면 다음 사람이 그게 인자를 판다고 믿는다."""
        src = (ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertNotIn("_ap.ArgumentParser(add_help=False)", src)


if __name__ == "__main__":
    unittest.main()
