# -*- coding: utf-8 -*-
"""required workflow 의 **실제** Actions 결론을 모은다 (2026-09-18 P1-3 · 개정).

🔴 개정 이유 — 이전 판은 그 날짜에 돈 **모든** completed run 을 보고 하나라도
   실패면 거래일 전체를 FAIL 로 만들었다. 그런데 main.yml 은 하루 144회,
   ai_report.yml 도 하루 여러 번 돈다. 오전의 무관한 재시도 하나가 실패해도
   그날이 통째로 떨어진다 — false-negative 가 과도해 3/3 이 불가능에 가까워진다.

   증거와 1:1 로 묶어야 한다. 영수증에는 이미 `run_id` 가 있다.
   **그 영수증을 만든 바로 그 run** 의 결론만 본다.

       scanner  receipt.run_id → 그 main.yml run
       analyst  receipt.run_id → 그 ai_report.yml run
       earnings receipt.run_id → 그 earnings_collector.yml run

🔴 2026-09-18 보완 — 위 개정에서 **지문 필터를 빠뜨렸다.**
   Evidence Builder 는 `latest(day, kind, fingerprint=현재지문)` 으로 고르는데
   여기는 `latest(day, kind)` 라 그냥 "그날 최신" 을 골랐다. 같은 날 두 지문의
   실행이 섞이면 이런 일이 가능하다:

       데이터 증거   → 현재 지문의 scanner receipt (Builder 가 고름)
       workflow 증거 → 늦게 끝난 **이전 지문**의 scanner receipt (여기서 고름)

   "1:1 로 묶었다" 고 했지만 **같은 지문** 이라는 조건이 이쪽에 없었다.
   Gate 의 철학은 같은 파이프라인·같은 지문·같은 실행의 증거다. 지문으로 거른다.

   한 겹 더 — run_id 를 조회한 뒤 그 run 의 실제 `path` 가 기대 워크플로와
   같은지도 본다. 잘못된 run_id 나 오염된 영수증이 **다른 워크플로의 성공**을
   가리켜도 통과하지 못하게 한다.

usage: workflow_states.py <repo> <YYYY-MM-DD>   (GH_TOKEN 환경변수 필요)
출력: WORKFLOW_STATES={"main.yml": "success", ...}
"""
import json
import os
import sys
import urllib.error
import urllib.request

import evidence_builder
import production_receipt
import stability_gate

# 영수증 종류 → 그 영수증을 만드는 워크플로
KIND_TO_WORKFLOW = {
    "scanner": "main.yml",
    "analyst": "ai_report.yml",
    "earnings": "earnings_collector.yml",
}
RUN_API = "https://api.github.com/repos/{repo}/actions/runs/{run_id}"


def run_conclusion(repo, run_id, token, expect_path):
    """그 run 하나의 결론. 없거나 미완료거나 **다른 워크플로면** 빈 문자열.

    반환 (결론, 사유) — 사유는 왜 인정하지 않았는지 말한다.
    """
    req = urllib.request.Request(
        RUN_API.format(repo=repo, run_id=run_id),
        headers={"Authorization": f"Bearer {token}",
                 "Accept": "application/vnd.github+json",
                 "User-Agent": "hyeoks-stability-gate"})
    with urllib.request.urlopen(req, timeout=20) as r:
        d = json.load(r)
    got_path = d.get("path") or ""
    if got_path != expect_path:
        # 오염된 영수증이나 잘못된 run_id 가 **다른 워크플로의 성공**을 가리킬 수 있다
        return "", f"워크플로 불일치 기대={expect_path} 실제={got_path or '없음'}"
    if d.get("status") != "completed":
        return "", f"미완료 status={d.get('status')}"
    return (d.get("conclusion") or ""), ""


def main():
    repo, day = sys.argv[1], sys.argv[2]
    token = os.environ.get("GH_TOKEN", "")
    # 🔴 Builder 와 **같은 지문**의 영수증만 본다. 안 그러면 데이터 증거와
    #    workflow 증거가 서로 다른 실행을 가리킬 수 있다.
    fp = stability_gate.fingerprint()
    print(f"지문 {fp} 의 영수증만 본다", file=sys.stderr)
    out = {}
    for kind, wf in KIND_TO_WORKFLOW.items():
        rec = production_receipt.latest(day, kind, fingerprint=fp)
        if not rec:
            print(f"::warning::{kind} 영수증이 없다(지문 {fp}) — {wf} 결론을 묶을 수 없다",
                  file=sys.stderr)
            continue                       # 결론 없음 — Builder 가 그대로 거짓으로 본다
        run_id = str(rec.get("run_id") or "").strip()
        if not run_id or not run_id.isdigit():
            print(f"::warning::{kind} 영수증에 쓸 수 있는 run_id 가 없다({run_id!r})",
                  file=sys.stderr)
            continue
        expect = f".github/workflows/{wf}"
        try:
            c, why = run_conclusion(repo, run_id, token, expect)
        except (urllib.error.URLError, ValueError, OSError) as e:
            print(f"::warning::{wf} run {run_id} 조회 실패: {type(e).__name__}", file=sys.stderr)
            c, why = "", type(e).__name__
        if c:
            out[wf] = c
            print(f"{wf} ← {kind} receipt(지문 {fp}) run {run_id}: {c}", file=sys.stderr)
        else:
            print(f"::warning::{wf} run {run_id} 인정 안 함: {why}", file=sys.stderr)
    # Builder 가 REQUIRED_WORKFLOWS 를 기준으로 빠진 것을 거짓으로 본다
    missing = [w for w in evidence_builder.REQUIRED_WORKFLOWS if w not in out]
    if missing:
        print(f"::warning::결론을 못 얻은 워크플로: {missing}", file=sys.stderr)
    print("WORKFLOW_STATES=" + json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
