# -*- coding: utf-8 -*-
"""required workflow 의 **실제** Actions 결론을 모은다 (2026-09-18 P1-3).

영수증의 `expected_state` 만 보면 영수증 **이후**의 실패를 못 본다 —
scanner 영수증 뒤에도 품질검사·git push 가 남아 있고, analyst 는 더 많이 남아 있다.

usage: workflow_states.py <repo> <YYYY-MM-DD>   (GH_TOKEN 환경변수 필요)
출력: WORKFLOW_STATES={"main.yml": "success", ...}
"""
import json
import os
import sys
import urllib.request

import evidence_builder

API = "https://api.github.com/repos/{repo}/actions/workflows/{wf}/runs?created={day}&per_page=30"


def conclusions(repo, day, token, wf):
    req = urllib.request.Request(
        API.format(repo=repo, wf=wf, day=day),
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "hyeoks-stability-gate"})
    with urllib.request.urlopen(req, timeout=20) as r:
        runs = json.load(r).get("workflow_runs") or []
    done = [x for x in runs if x.get("status") == "completed"]
    if not done:
        return ""                      # 결론 없음 — Builder 가 그대로 거짓으로 본다
    # 하나라도 실패했으면 실패다. 재시도로 덮지 않는다.
    return "success" if all(x.get("conclusion") == "success" for x in done) else "failure"


def main():
    repo, day = sys.argv[1], sys.argv[2]
    token = os.environ.get("GH_TOKEN", "")
    out = {}
    for wf in evidence_builder.REQUIRED_WORKFLOWS:
        try:
            c = conclusions(repo, day, token, wf)
        except Exception as e:                       # noqa: BLE001
            print(f"::warning::{wf} 결론 조회 실패: {type(e).__name__}", file=sys.stderr)
            c = ""
        if c:
            out[wf] = c
    print("WORKFLOW_STATES=" + json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
