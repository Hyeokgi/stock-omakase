# -*- coding: utf-8 -*-
# 드라이브에 못 올라가 봉인 사본(14일 artifact)으로만 남은 후보 자료를 찾아 만료 전에 복원하라고 알리는 점검기
"""봉인 사본 감시 (2026-11-01, 전략 평가 우선순위 3).

왜
  private_store.py 는 업로드에 실패한 원본을 봉인해 artifact 로 14일 남기고 색인에 '암호사본' 을 적는다.
  판정(evidence_builder)은 그 행을 보존으로 친다. 그런데 PC 에서 `--restore` 를 하지 않으면 14일 뒤 원본이 사라진다.
  또 artifact 올리기 단계는 실패를 허용하므로(main.yml continue-on-error) 색인에는 '암호사본' 인데 원격에 없을 수 있다
  (Codex · Gemini 평가 2026-10-31).
어떻게
  공개 색인에서 '암호사본' 인데 같은 (종류 · 날짜 · 채널) 의 '접수' 가 아직 없는 행을 모으고, 그 회차의 artifact 가
  실제로 있는지 · 언제 만료되는지 GitHub API 로 본다. 남은 것이 있으면 복원 명령을 적고 실패(종료 1)로 끝나
  깃허브 실패 메일이 가게 한다(사용자 결정 2026-11-01 — 지인과 같이 보는 텔레그램 채널에는 보내지 않는다).
  만료 뒤 GRACE_DAYS 가 지난 것은 이미 잃은 것이라 표에만 남기고 실패로 치지 않는다(매일 빨간불이 되지 않게).
하지 않는 것
  private_store.py · evidence_builder.py · main.yml(지문 대상)은 바꾸지 않는다. 복원은 키가 있는 PC 에서 사람이 한다.
"""
import datetime
import json
import os
import sys
import urllib.request

import private_store as P

REPO = "Hyeokgi/stock-omakase"
RETENTION_DAYS = 14          # main.yml retention-days 와 같다
GRACE_DAYS = 7               # 만료 뒤 이 기간까지는 실패로 띄운다


def unresolved(rows):
    """{run_id: [행]} — 봉인 사본인데 같은 (종류 · 날짜 · 채널) 의 접수가 아직 없는 것."""
    done = {(r.get("kind"), r.get("date"), r.get("channel") or "") for r in rows if r.get("status") == P.DONE}
    out = {}
    for r in rows:
        if r.get("status") == P.SEALED and (r.get("kind"), r.get("date"), r.get("channel") or "") not in done:
            out.setdefault(r.get("run_id") or "", []).append(r)
    return out


def github_artifacts(run_id, token, repo=REPO):
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts",
                                 headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as res:
        return json.load(res).get("artifacts") or []


def _ts(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))


def check(rows, artifacts, now):
    """[(run_id, 날짜들, 상태, 만료 시각 문자열, 급함, artifact 이름)] — artifacts(run_id) 는 그 회차의 artifact 목록."""
    out = []
    for run_id, rs in sorted(unresolved(rows).items()):
        days = sorted({r["date"] for r in rs})
        sealed_at = min(_ts(r["at"]) for r in rs)
        try:
            arts = [a for a in artifacts(run_id) if a.get("name", "").startswith(f"private-fallback-{run_id}-")]
        except Exception as e:                 # noqa: BLE001 — 조회 실패도 사람이 봐야 한다
            out.append((run_id, days, f"artifact 조회 실패 {type(e).__name__}", "", True, ""))
            continue
        live = [a for a in arts if not a.get("expired")]
        if live:
            art = max(live, key=lambda a: _ts(a["expires_at"]))
            exp = _ts(art["expires_at"])
            left = (exp - now).total_seconds() / 86400
            out.append((run_id, days, f"복원 필요 — 만료까지 {left:.1f}일", exp.isoformat(timespec="minutes"), True,
                        art["name"]))
        else:
            gone = now - sealed_at > datetime.timedelta(days=RETENTION_DAYS + GRACE_DAYS)
            out.append((run_id, days, "만료됨 — 원본 잃음" if arts else "원격 사본 없음 — 원본 잃음", "", not gone, ""))
    return out


def report(found):
    lines = ["## 🔒 봉인 사본 감시", ""]
    if not found:
        return "\n".join(lines + ["복원할 봉인 사본 없음."])
    lines += ["| 회차 | 날짜 | 상태 | 만료 |", "|---|---|---|---|"]
    lines += [f"| {r} | {' · '.join(d)} | {s} | {e} |" for r, d, s, e, _, _ in found]
    todo = [(r, n) for r, _, s, _, _, n in found if n]
    if todo:
        lines += ["", "PC(사용자 환경 변수 PRIVATE_STORE_KEY)의 주식 레포에서 PowerShell 로:", "```"]
        for r, n in todo:
            lines += [f"gh run download {r} -R {REPO} -n {n} -D $env:TEMP\\{n}",
                      f"python private_store.py --restore $env:TEMP\\{n}"]
        lines += ["git add data/scanner_census/private_index.csv", "```",
                  "복원하면 색인에 '접수' 행이 붙는다. 그 파일만 커밋해 올리면 다음 점검부터 빠진다."]
    return "\n".join(lines)


def main(argv=None, env=None, artifacts=None, now=None, index=None):
    env = os.environ if env is None else env
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if artifacts is None:
        token = env.get("GITHUB_TOKEN", "")
        artifacts = lambda run_id: github_artifacts(run_id, token)   # noqa: E731
    found = check(P.load(index), artifacts, now)
    text = report(found)
    print(text)
    if env.get("GITHUB_STEP_SUMMARY"):
        with open(env["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as fh:
            fh.write(text + "\n")
    urgent = [f for f in found if f[4]]
    if urgent:
        print(f"::error::봉인 사본 {len(urgent)}회차 — 만료 전에 PC 에서 복원해야 한다")
    return 1 if urgent else 0


if __name__ == "__main__":
    sys.exit(main())
