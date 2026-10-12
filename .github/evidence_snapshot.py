# -*- coding: utf-8 -*-
# 스캐너 회차가 커밋하려는 증거 파일(영수증·Feature Store·전수조사)을 artifact 용으로 한 벌 복사하고 출처 manifest 를 쓰는 스크립트
"""스캐너 증거 사본(A) — docs/collaboration/2026-10-13_Claude_스캐너영수증누락_설계.md §5-A.

main.yml 의 커밋 단계 바로 앞에서 돈다. 커밋·push 가 실패해도(9/18 14:41·14:51 유형) 그 회차가 만든 파일이
artifact 로 남게 하는 것이 목적이다. 판정에는 쓰지 않는다(사용자 결정 C-b). 되살리기(B)는 별도 도구가
manifest 로 출처를 검증한 뒤에만 한다.

지키는 것
  1. 세 폴더(data/receipts · data/feature_store · data/scanner_census)의 **변경된 일반 파일**만 복사한다.
     `git status` 를 세 폴더로 한정해 보므로 저장소 최상위의 secret.json 은 들어올 수 없다.
  2. 링크(경로 어디든)·임시 파일·숨김 이름(경로 어디든)·비표준 경로·폴더 밖 경로는 복사하지 않고 manifest 의
     rejected 에 사유와 함께 남긴다. 검사한 경로 그대로 복사한다(Codex 10/13 재검토).
  3. 사본 폴더는 저장소 밖이어야 한다(저장소 안이면 거부) — 저장소 전체가 올라가는 일을 막는다.
     앞 실행 사본은 소유 표식이 있을 때만 지운다.
  4. 이 스크립트 때문에 수집기가 실패하지 않는다 — CLI 는 예외를 잡아 경고만 남기고 exit 0 이다.
     워크플로 단계도 continue-on-error 다(Evidence Builder 는 run 결론으로 판정한다 — Codex 10/13 P2).
"""
import argparse
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys

DIRS = ("data/receipts", "data/feature_store", "data/scanner_census")
SCHEMA = "scanner-evidence-v1"
TEMP_SUFFIXES = (".tmp", ".temp", ".swp", ".part", "~")
OWNER = ".scanner-evidence-owner"   # 사본 폴더 소유 표식 — 숨김 파일이라 artifact 에는 올라가지 않는다
KST = datetime.timezone(datetime.timedelta(hours=9))


def _git(root, *args):
    return subprocess.run(["git", *args], cwd=root, capture_output=True, check=True).stdout


def changed(root):
    """세 폴더 안의 변경 항목 [(상태 두 글자, 경로)]. `-z` 라 경로가 따옴표로 바뀌지 않는다."""
    raw = _git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--", *DIRS)
    parts, out, i = raw.split(b"\0"), [], 0
    while i < len(parts):
        e = parts[i]
        i += 1
        if len(e) < 4:
            continue
        xy, path = e[:2].decode("ascii"), e[3:].decode("utf-8")
        if "R" in xy or "C" in xy:
            i += 1                      # 이름 바뀜 — 다음 칸은 옛 경로
        out.append((xy, path))
    return out


def reject_reason(root, rel):
    """복사하면 안 되는 이유. 빈 문자열이면 복사한다.

    🔴 Codex 10/13 재검토 — 검사한 경로와 복사하는 경로가 **같아야** 한다. 경로를 바꿔(정규화) 검사하면
    리눅스에서 `a\\b.json`(파일 이름)과 `a/b.json`(폴더 아래)이 다른 파일인데 하나를 검사하고 다른 하나를
    복사할 수 있다. 그래서 바꾸지 않고, 역슬래시·빈·`.`·`..` 구성요소가 있으면 비표준 경로로 거부한다.
    숨김 이름은 마지막 파일명뿐 아니라 **모든 구성요소**에서 거부한다 — upload-artifact 는 숨김 폴더 안
    파일을 기본으로 빼므로, 넣으면 manifest 와 artifact 의 파일 목록이 어긋난다.
    """
    parts = rel.split("/")
    if "\\" in rel or os.path.isabs(rel) or any(p in ("", ".", "..") for p in parts):
        return "비표준 경로"
    if not any(rel.startswith(d + "/") for d in DIRS):
        return "폴더 밖"
    if any(p.startswith(".") for p in parts) or parts[-1].endswith(TEMP_SUFFIXES):
        return "임시·숨김 파일"
    full = os.path.join(root, *parts)
    if any(os.path.islink(os.path.join(root, *parts[:i])) for i in range(1, len(parts) + 1)):
        return "링크"
    if not os.path.isfile(full):
        return "일반 파일 아님"
    return ""


def _inside(child, parent):
    child, parent = os.path.realpath(child), os.path.realpath(parent)
    try:
        return os.path.commonpath([child, parent]) == parent
    except ValueError:                  # 다른 드라이브
        return False


def _clear(out):
    """앞 실행의 사본을 지운다. 이 스크립트가 만든 폴더(소유 표식 있음)이거나 빈 폴더일 때만 지운다."""
    if not os.path.exists(out):
        return
    names = set(os.listdir(out))
    if not names:
        os.rmdir(out)
        return
    if OWNER not in names or names - {"files", "manifest.json", OWNER}:
        raise ValueError(f"이 스크립트가 만든 사본 폴더가 아니다 — 지우지 않는다: {sorted(names)}")
    shutil.rmtree(out)


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def artifact_name(env):
    return f"scanner-evidence-{env.get('GITHUB_RUN_ID') or 'local'}-{env.get('GITHUB_RUN_ATTEMPT') or '1'}"


def snapshot(root, out, env=None):
    """변경 파일을 `out/files/<경로>` 로 복사하고 `out/manifest.json` 을 쓴다. 변경이 없으면 아무것도 쓰지 않는다."""
    env = os.environ if env is None else env
    root = os.path.abspath(root)
    out = os.path.abspath(out)
    if _inside(out, root) or _inside(root, out):
        raise ValueError("사본 폴더는 저장소 밖이어야 하고 저장소를 품어도 안 된다")
    _clear(out)

    files, rejected, deleted = [], [], []
    for xy, rel in sorted(changed(root), key=lambda x: x[1]):
        if "D" in xy:
            deleted.append(rel)
            continue
        why = reject_reason(root, rel)
        if why:
            rejected.append({"path": rel, "reason": why})
        else:
            files.append((xy, rel))     # git 상태 두 글자 그대로(" M"·"??"·"R " …)

    manifest = {
        "schema": SCHEMA,
        "name": artifact_name(env),
        "repository": env.get("GITHUB_REPOSITORY", ""),
        "workflow": env.get("GITHUB_WORKFLOW", ""),
        "run_id": env.get("GITHUB_RUN_ID", ""),
        "run_attempt": env.get("GITHUB_RUN_ATTEMPT", ""),
        "sha": env.get("GITHUB_SHA", ""),
        "ref": env.get("GITHUB_REF", ""),
        "event": env.get("GITHUB_EVENT_NAME", ""),
        "head": _git(root, "rev-parse", "HEAD").decode("ascii").strip(),
        "created_at": datetime.datetime.now(KST).isoformat(timespec="seconds"),
        "files": [],
        "rejected": rejected,
        "deleted": deleted,
    }
    if not files:
        return manifest

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, OWNER), "w", encoding="utf-8") as f:
        f.write(SCHEMA + "\n")
    for status, rel in files:
        src = os.path.join(root, *rel.split("/"))      # 검사한 경로 그대로
        dst = os.path.join(out, "files", *rel.split("/"))
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, dst)
        manifest["files"].append({"path": rel, "status": status,
                                  "bytes": os.path.getsize(dst), "sha256": _sha256(dst)})
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    return manifest


def _output(env, **kv):
    path = env.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={v}\n")


def main(argv=None, env=None):
    env = os.environ if env is None else env
    ap = argparse.ArgumentParser(description="스캐너 증거 사본(artifact 용) 준비")
    ap.add_argument("--out", required=True, help="사본 폴더 — 저장소 밖 (CI 에서는 $RUNNER_TEMP 아래)")
    ap.add_argument("--root", default=".")
    a = ap.parse_args(argv)
    try:
        m = snapshot(a.root, a.out, env=env)
    except Exception as e:                       # 수집기를 막지 않는다
        print(f"::warning::증거 사본 준비 실패 — {type(e).__name__}: {e} (커밋 단계는 그대로 진행)")
        _output(env, files=0, name="")
        return 0
    n = len(m["files"])
    _output(env, files=n, name=m["name"] if n else "")
    print(f"🧳 증거 사본 — 파일 {n}개 · 거부 {len(m['rejected'])} · 삭제 {len(m['deleted'])}"
          + (f" · {m['name']}" if n else " · 올릴 것 없음"))
    for r in m["rejected"]:
        print(f"   · 거부 {r['reason']}: {r['path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
