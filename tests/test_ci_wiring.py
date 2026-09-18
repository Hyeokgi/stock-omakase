# -*- coding: utf-8 -*-
"""
CI 배선 회귀 — 2026-09-18.

두 사고를 같은 날 발견했다. 둘 다 "코드가 아니라 배선" 이 틀린 경우라
유닛테스트가 잡을 수 없었고, 그래서 며칠씩 조용히 굴러갔다.

  ① 오프라인 CI 가 9/16 부터 매 push 마다 실패하고 있었다.
     의존성 설치 단계가 없어서 requests 를 끌어오는 테스트 3개가 ImportError.
     368건만 돌고 28건은 실행조차 안 됐는데, 빨간불을 깔고 계속 push 했다.
  ② main.yml 의 `git add -- data/feature_store` 가 exit 128 로 죽었다.
     그 디렉터리가 저장소에 아직 없었기 때문이다. 9/18 14:41·14:51 두 회차가
     전수조사 한 줄을 커밋하지 못하고 워크플로째 실패했다. 기록 창은
     14:40~15:10 뿐이라 계속 실패했으면 그날치를 통째로 잃었다.

여기서는 워크플로 **텍스트** 를 본다. 실행해 볼 수 없는 것을 검사하는 유일한 방법이다.
"""
import pathlib
import re
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
WF = ROOT / ".github" / "workflows"
AUDIT = WF / "review_regressions.yml"


def load(path):
    # PyYAML 은 YAML 1.1 이라 `on:` 을 불리언 True 로 읽는다. 그대로 받는다.
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def triggers(doc):
    return doc.get("on") or doc.get(True) or {}


class OfflineAuditTests(unittest.TestCase):
    def setUp(self):
        self.doc = load(AUDIT)
        self.steps = self.doc["jobs"]["offline"]["steps"]
        self.runs = "\n".join(s.get("run", "") for s in self.steps)

    def test_dependencies_are_installed(self):
        """'offline' 은 자격증명이 없다는 뜻이지 의존성이 없다는 뜻이 아니다."""
        for pkg in ("requests", "beautifulsoup4", "gspread", "oauth2client"):
            with self.subTest(pkg):
                self.assertRegex(self.runs, rf"pip install[^\n]*\b{re.escape(pkg)}\b")

    def test_test_modules_import_under_ci_deps(self):
        """테스트가 import 하는 모듈의 **모듈 레벨** 서드파티가 CI 에 설치돼 있지도
        않고 그 테스트가 스텁으로 막지도 않으면, CI 는 ImportError 로 조용히 빨개진다.

        9/16 에 정확히 이 일이 났다. test_morning_fred 는 gspread·google·bs4 는
        스텁으로 막았는데 requests 는 안 막았고, CI 에는 설치돼 있지 않았다.

        한 홉만 본다: tests/*.py → 로컬 모듈 → 그 모듈의 top-level import.
        함수 안쪽 import 나 두 홉 뒤는 여기서 보지 않는다(실행이 증거다)."""
        import ast
        import sys

        installed = set()
        for line in self.runs.splitlines():
            m = re.search(r"pip install (.+)", line)
            if m and "--upgrade pip" not in m.group(1):
                installed.update(t.strip() for t in m.group(1).split())
        # import 이름 ≠ 배포 이름
        dist = {"bs4": "beautifulsoup4", "urllib3": "requests", "yaml": "pyyaml"}
        local = {q.stem for q in ROOT.glob("*.py")}
        siblings = {q.stem for q in (ROOT / "tests").glob("*.py")}   # 테스트끼리 재사용한다
        std = set(sys.stdlib_module_names)

        def top_imports(tree):
            out = set()
            for node in tree.body:
                if isinstance(node, ast.Import):
                    out.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    out.add(node.module.split(".")[0])
            return out

        missing = []
        for t in sorted((ROOT / "tests").glob("test_*.py")):
            src = t.read_text(encoding="utf-8")
            tree = ast.parse(src)
            # 스텁으로 막았는지: 그 이름이 이 테스트에 문자열로 등장하는가
            stubbed = {n.value for n in ast.walk(tree)
                       if isinstance(n, ast.Constant) and isinstance(n.value, str)}
            # 🔴 테스트 자신의 서드파티 import 도 본다. 이 파일이 yaml 을 쓰는데
            #    설치 목록에 없어서 방금 같은 사고를 한 번 더 낼 뻔했다.
            for mod in sorted(top_imports(tree) - std - local - siblings - stubbed):
                if dist.get(mod, mod) not in installed:
                    missing.append(f"{t.name} → {mod}")
            for name in sorted(top_imports(tree) & local):
                for mod in sorted(top_imports(ast.parse((ROOT / f"{name}.py").read_text(encoding="utf-8")))):
                    if mod in std or mod in local or mod in stubbed:
                        continue
                    if dist.get(mod, mod) not in installed:
                        missing.append(f"{t.name} → {name} → {mod}")
        self.assertEqual(missing, [], f"설치도 스텁도 안 된 import: {missing}")

    def test_paths_filter_is_not_a_hand_kept_list(self):
        """새 모듈이 목록에 없어서 CI 가 아예 안 돌던 구멍을 막는다."""
        for event in ("push", "pull_request"):
            with self.subTest(event):
                self.assertIn("**.py", triggers(self.doc)[event]["paths"])

    def test_workflow_changes_wake_the_wiring_tests(self):
        """🔴 2026-09-18 GPT §10 — 이 파일이 main.yml 등을 검사하는데,
        정작 그 워크플로를 고쳐도 audit CI 가 깨어나지 않았다.
        배선 시험을 만들어 놓고 배선 파일이 시험을 못 깨우는 구조였다."""
        for event in ("push", "pull_request"):
            with self.subTest(event):
                paths = triggers(self.doc)[event]["paths"]
                self.assertIn(".github/workflows/*.yml", paths,
                              f"워크플로 YAML 변경이 CI 를 깨우지 않는다: {paths}")
                # `**/` 는 디렉터리 0개를 매치한다는 보장이 없다 — 최상위 파일을 놓친다
                self.assertNotIn(".github/workflows/**/*.yml", paths,
                                 "`**/` 패턴은 최상위 main.yml 을 놓칠 수 있다")

    def test_every_workflow_this_suite_checks_is_covered(self):
        """시험이 읽는 워크플로 파일은 전부 트리거 범위 안이어야 한다."""
        import fnmatch
        checked = {"main.yml", "review_regressions.yml", "earnings_collector.yml"}
        paths = triggers(self.doc)["push"]["paths"]
        for wf in sorted(checked):
            with self.subTest(wf):
                target = f".github/workflows/{wf}"
                self.assertTrue(any(fnmatch.fnmatch(target, p) for p in paths),
                                f"{wf} 변경이 CI 를 깨우지 않는다")

    def test_self_tests_that_exist_are_actually_run(self):
        """자체검증을 만들어 놓고 CI 가 안 돌리면 회귀를 아무도 모른다."""
        has = {p.name for p in ROOT.glob("*.py")
               if "--self-test" in p.read_text(encoding="utf-8")}
        missing = sorted(n for n in has if n not in self.runs)
        self.assertEqual(missing, [], f"CI 가 돌리지 않는 자체검증: {missing}")


class GitAddPathTests(unittest.TestCase):
    """`git add -- <없는 경로>` 는 exit 128 이다. status 는 조용한데 add 는 죽는다."""

    ADD = re.compile(r"git add (?:-- )?(.+)")

    def test_every_added_path_is_guarded_or_created(self):
        bad = []
        for wf in sorted(WF.glob("*.yml")):
            doc = load(wf)
            for job in (doc.get("jobs") or {}).values():
                for step in job.get("steps", []):
                    body = step.get("run", "")
                    for line in body.splitlines():
                        stripped = line.strip()
                        if stripped.startswith("#") or "git add" not in stripped:
                            continue
                        # 같은 줄에 if [ -d ... ] / [ -f ... ] 가드가 있으면 안전
                        if re.search(r"\[\s*-[df]\s", stripped):
                            continue
                        m = self.ADD.search(stripped)
                        if not m:
                            continue
                        args = m.group(1).split("||")[0]
                        for path in args.split():
                            if path.startswith("-"):
                                continue
                            p = path.rstrip("/")
                            if (ROOT / p).exists():
                                continue
                            if re.search(rf"mkdir -p[^\n]*\b{re.escape(p)}\b", body):
                                continue
                            bad.append(f"{wf.name}: {p}")
        self.assertEqual(bad, [], f"없으면 워크플로를 죽이는 경로: {bad}")

    def test_main_yml_creates_the_dirs_it_adds(self):
        body = "\n".join(
            s.get("run", "") for s in load(WF / "main.yml")["jobs"]["build"]["steps"])
        self.assertIn("mkdir -p data/scanner_census data/feature_store", body)


class SourceErrorTests(unittest.TestCase):
    """실패 '이름' 만 남기면 DNS·TLS·타임아웃·차단을 구분할 수 없다."""

    def test_reason_survives(self):
        import socket
        import urllib.error
        import naver_sources as n
        self.assertIn("Name or service not known",
                      n.why(urllib.error.URLError(socket.gaierror("Name or service not known"))))
        self.assertIn("503", n.why(urllib.error.HTTPError("u", 503, "Service Unavailable", {}, None)))
        self.assertEqual(n.why(ValueError("")), "사유 없음")
        self.assertLessEqual(len(n.why(ValueError("x" * 500))), 120)


if __name__ == "__main__":
    unittest.main()
