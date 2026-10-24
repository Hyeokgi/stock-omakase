# 스캐너 증거 사본(A) 시험 — 파일 선택·manifest·비밀 제외·재실행·워크플로 비차단 배선을 확인한다
"""docs/collaboration/2026-10-13_Claude_스캐너영수증누락_설계.md §5-A (Codex 2026-10-13 검토 반영판) 의 구현 시험."""
import hashlib
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import yaml

import stability_gate as G

ROOT = pathlib.Path(__file__).resolve().parent.parent
SNAP_PATH = ROOT / ".github" / "evidence_snapshot.py"


def _load_snapshot():
    spec = importlib.util.spec_from_file_location("evidence_snapshot", SNAP_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


S = _load_snapshot()


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


class Snapshot(unittest.TestCase):
    """임시 git 저장소에서 '커밋 단계 직전' 상태를 흉내 낸다."""

    def setUp(self):
        self.repo = tempfile.mkdtemp()
        self.out = tempfile.mkdtemp()
        shutil.rmtree(self.out)
        _git(self.repo, "init", "-q")
        _git(self.repo, "config", "user.email", "t@t")
        _git(self.repo, "config", "user.name", "t")
        _git(self.repo, "config", "core.autocrlf", "false")
        self.write("data/scanner_census/rank_pool.csv", "date,channel\n2026-10-12,A\n")
        self.write("data/receipts/2026-10-12/scanner-1.json", "{}")
        self.write("README.md", "x")
        _git(self.repo, "add", "-A")
        _git(self.repo, "commit", "-q", "-m", "base")
        self.env = {"GITHUB_REPOSITORY": "o/r", "GITHUB_WORKFLOW": "Stock Omakase Auto Scraper",
                    "GITHUB_RUN_ID": "38210556325", "GITHUB_RUN_ATTEMPT": "2", "GITHUB_SHA": "abc123",
                    "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch"}

    def tearDown(self):
        shutil.rmtree(self.repo, ignore_errors=True)
        shutil.rmtree(self.out, ignore_errors=True)

    def write(self, rel, text):
        p = pathlib.Path(self.repo) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8", newline="")

    def run_snapshot(self):
        return S.snapshot(self.repo, self.out, env=self.env)

    def test_copies_only_changed_regular_files_in_the_three_dirs(self):
        self.write("data/scanner_census/rank_pool.csv", "date,channel\n2026-10-12,A\n2026-10-13,A\n")   # 수정
        self.write("data/receipts/2026-10-13/scanner-38210556325.json", '{"k":1}')                     # 새 파일
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")                                        # 새 파일
        self.write("secret.json", "TOP-SECRET")                                                         # 최상위 — 들어오면 안 된다
        self.write("data/other/x.csv", "nope")                                                          # 세 폴더 밖
        self.write("data/receipts/2026-10-13/scanner.json.tmp", "partial")                              # 임시 파일
        m = self.run_snapshot()
        got = sorted(f["path"] for f in m["files"])
        self.assertEqual(got, ["data/feature_store/2026-10-13.csv.gz",
                               "data/receipts/2026-10-13/scanner-38210556325.json",
                               "data/scanner_census/rank_pool.csv"])
        self.assertEqual([r["path"] for r in m["rejected"]], ["data/receipts/2026-10-13/scanner.json.tmp"])
        copied = sorted(str(p.relative_to(pathlib.Path(self.out) / "files")).replace("\\", "/")
                        for p in (pathlib.Path(self.out) / "files").rglob("*") if p.is_file())
        self.assertEqual(copied, got, "manifest 의 파일과 실제 복사본이 같아야 한다")
        everything = "".join(p.read_text(encoding="utf-8", errors="ignore")
                             for p in pathlib.Path(self.out).rglob("*") if p.is_file())
        self.assertNotIn("TOP-SECRET", everything, "비밀 파일 내용이 사본에 섞이면 안 된다")
        self.assertNotIn("secret.json", everything)

    def test_manifest_records_provenance_and_hashes(self):
        self.write("data/feature_store/2026-10-13.csv.gz", "gz-bytes")
        m = self.run_snapshot()
        saved = json.loads((pathlib.Path(self.out) / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(saved, m)
        self.assertEqual(m["schema"], "scanner-evidence-v1")
        self.assertEqual((m["repository"], m["workflow"], m["run_id"], m["run_attempt"], m["sha"]),
                         ("o/r", "Stock Omakase Auto Scraper", "38210556325", "2", "abc123"))
        self.assertEqual(m["name"], "scanner-evidence-38210556325-2", "재실행은 run_id 가 같으므로 run_attempt 를 붙인다")
        f = m["files"][0]
        self.assertEqual(f["sha256"], hashlib.sha256(b"gz-bytes").hexdigest())
        self.assertEqual(f["bytes"], 8)
        self.assertTrue(m["created_at"].endswith("+09:00"))

    def test_no_change_means_no_files_and_no_manifest(self):
        m = self.run_snapshot()
        self.assertEqual(m["files"], [])
        self.assertFalse((pathlib.Path(self.out) / "manifest.json").exists(), "빈 회차는 올릴 것이 없다")

    def test_symlink_is_rejected(self):
        target = pathlib.Path(self.repo) / "README.md"
        link = pathlib.Path(self.repo) / "data/receipts/2026-10-13/link.json"
        link.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(target, link)
        except (OSError, NotImplementedError):
            self.skipTest("이 환경은 심볼릭 링크를 만들 수 없다")
        m = self.run_snapshot()
        self.assertEqual([r["reason"] for r in m["rejected"]], ["링크"])
        self.assertEqual(m["files"], [])

    def test_deleted_file_is_listed_not_copied(self):
        os.remove(pathlib.Path(self.repo) / "data/receipts/2026-10-12/scanner-1.json")
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        m = self.run_snapshot()
        self.assertEqual(m["deleted"], ["data/receipts/2026-10-12/scanner-1.json"])
        self.assertEqual([f["path"] for f in m["files"]], ["data/feature_store/2026-10-13.csv.gz"])

    def test_out_dir_inside_the_repo_is_refused(self):
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        with self.assertRaises(ValueError):
            S.snapshot(self.repo, os.path.join(self.repo, "evidence"), env=self.env)

    def test_rerun_overwrites_the_old_copy(self):
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        self.run_snapshot()
        os.remove(pathlib.Path(self.repo) / "data/feature_store/2026-10-13.csv.gz")
        self.write("data/scanner_census/envelope_census.csv", "d\n")
        m = self.run_snapshot()
        copied = sorted(p.name for p in (pathlib.Path(self.out) / "files").rglob("*") if p.is_file())
        self.assertEqual(copied, ["envelope_census.csv"], "앞 실행의 사본이 남으면 안 된다")
        self.assertEqual(len(m["files"]), 1)

    def test_hidden_component_anywhere_is_rejected(self):
        """upload-artifact 는 숨김 폴더 안 파일을 기본으로 뺀다 — manifest 에 넣으면 artifact 와 어긋난다."""
        self.write("data/receipts/.staging/x.json", "{}")
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        m = self.run_snapshot()
        self.assertEqual(m["rejected"], [{"path": "data/receipts/.staging/x.json", "reason": "임시·숨김 파일"}])
        self.assertEqual([f["path"] for f in m["files"]], ["data/feature_store/2026-10-13.csv.gz"])

    def test_nonstandard_paths_are_rejected_not_normalized(self):
        """검사한 경로와 복사하는 경로가 같아야 한다 — 역슬래시·.. 를 고쳐 쓰지 않고 거부한다."""
        for rel in ("data/receipts/a\\b.json", "data/receipts/../receipts/x.json", "data/receipts//x.json",
                    "data/receipts/./x.json"):
            with self.subTest(rel):
                self.assertEqual(S.reject_reason(self.repo, rel), "비표준 경로")
        self.assertEqual(S.reject_reason(self.repo, "data/other/x.csv"), "폴더 밖")

    def test_rename_takes_the_new_path_and_keeps_two_char_status(self):
        _git(self.repo, "mv", "data/receipts/2026-10-12/scanner-1.json", "data/receipts/2026-10-12/scanner-9.json")
        self.write("data/scanner_census/rank_pool.csv", "date,channel\n2026-10-12,A\n2026-10-13,B\n")   # 추적 파일 수정(미스테이지)
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")                                          # 새 파일
        m = self.run_snapshot()
        got = {f["path"]: f["status"] for f in m["files"]}
        self.assertEqual(got, {"data/receipts/2026-10-12/scanner-9.json": "R ",
                               "data/scanner_census/rank_pool.csv": " M",
                               "data/feature_store/2026-10-13.csv.gz": "??"})

    def test_out_dir_not_made_by_this_script_is_not_deleted(self):
        os.makedirs(self.out)
        keep = os.path.join(self.out, "important.txt")
        pathlib.Path(keep).write_text("keep", encoding="utf-8")
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        with self.assertRaises(ValueError):
            self.run_snapshot()
        self.assertTrue(os.path.exists(keep), "소유 표식 없는 폴더는 지우지 않는다")

    def test_cli_never_fails_the_step_and_writes_outputs(self):
        self.write("data/feature_store/2026-10-13.csv.gz", "gz")
        gh_out = os.path.join(tempfile.mkdtemp(), "out.txt")
        env = dict(self.env, GITHUB_OUTPUT=gh_out)
        rc = S.main(["--root", self.repo, "--out", self.out], env=env)
        self.assertEqual(rc, 0)
        txt = pathlib.Path(gh_out).read_text(encoding="utf-8")
        self.assertIn("files=1", txt)
        self.assertIn("name=scanner-evidence-38210556325-2", txt)
        # 저장소가 아닌 곳 — 예외가 나도 exit 0, files=0
        gh_out2 = os.path.join(tempfile.mkdtemp(), "out.txt")
        rc = S.main(["--root", tempfile.mkdtemp(), "--out", self.out], env=dict(self.env, GITHUB_OUTPUT=gh_out2))
        self.assertEqual(rc, 0)
        self.assertIn("files=0", pathlib.Path(gh_out2).read_text(encoding="utf-8"))


class Workflow(unittest.TestCase):
    def setUp(self):
        self.steps = yaml.safe_load((ROOT / ".github/workflows/main.yml").read_text(encoding="utf-8"))["jobs"]["build"]["steps"]
        self.names = [s.get("name", "") for s in self.steps]

    def idx(self, part):
        return next(i for i, n in enumerate(self.names) if part in n)

    def test_copy_steps_run_before_the_commit_step(self):
        prep, up, commit = self.idx("증거 사본 준비"), self.idx("증거 사본 올리기"), self.idx("타점 전수조사·후보 동결 보존")
        self.assertLess(prep, up)
        self.assertLess(up, commit, "커밋이 실패해도 사본이 남도록 커밋 단계 앞에서 올린다")

    def test_copy_steps_can_never_fail_the_run(self):
        """Evidence Builder 는 run 결론으로 no_unexplained_failure 를 정한다 — 사본 실패가 FAIL 을 만들면 안 된다."""
        for part, limit in (("증거 사본 준비", 1), ("증거 사본 올리기", 2)):
            s = self.steps[self.idx(part)]
            with self.subTest(part):
                self.assertIs(s.get("continue-on-error"), True)
                self.assertTrue(str(s.get("if", "")).startswith("always()"))
                self.assertLessEqual(int(s.get("timeout-minutes", 99)), limit)

    def test_upload_is_scoped_named_and_short_lived(self):
        prep = self.steps[self.idx("증거 사본 준비")]
        up = self.steps[self.idx("증거 사본 올리기")]
        self.assertEqual(prep.get("id"), "evidence")
        self.assertIn(".github/evidence_snapshot.py", prep["run"])
        self.assertIn("$RUNNER_TEMP", prep["run"], "사본은 저장소 밖 임시 폴더에 만든다")
        self.assertTrue(up["uses"].startswith("actions/upload-artifact@"))
        w = up["with"]
        self.assertEqual(w["name"], "${{ steps.evidence.outputs.name }}")
        self.assertEqual(w["path"], "${{ runner.temp }}/scanner-evidence", "저장소 전체를 올리면 secret.json 이 섞인다")
        self.assertEqual(int(w["retention-days"]), 14)
        self.assertIn("steps.evidence.outputs.files != '0'", up["if"], "변경분이 없는 회차는 올리지 않는다")
        self.assertIn("steps.evidence.outputs.files != ''", up["if"], "준비 단계가 출력 없이 죽으면 올리지 않는다")

    def test_snapshot_script_is_in_the_fingerprint(self):
        self.assertIn(".github/evidence_snapshot.py", G.FINGERPRINT_FILES,
                      "생산 워크플로가 부르는 코드가 바뀌면 안정화 지문도 바뀌어야 한다")


if __name__ == "__main__":
    unittest.main()
