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
import os
import pathlib
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


class UndefinedModuleUseTests(unittest.TestCase):
    """`sys.argv` 를 쓰면서 `import sys` 를 안 한 파일이 있는가."""

    def test_no_module_is_used_without_being_imported(self):
        bad = []
        for path in sorted(ROOT.glob("*.py")) + sorted((ROOT / "tests").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            bound = module_bound_names(tree)
            for node in ast.walk(tree):
                # `sys.argv` 같은 **속성 접근**만 본다(지역 변수 오탐을 줄인다)
                if (isinstance(node, ast.Attribute)
                        and isinstance(node.value, ast.Name)
                        and node.value.id in STDLIB_HINTS
                        and node.value.id not in bound):
                    bad.append(f"{path.name}:{node.lineno} {node.value.id}.{node.attr}")
        self.assertEqual(bad, [], f"import 없이 쓰는 표준 모듈: {bad}")


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
