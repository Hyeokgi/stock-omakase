# -*- coding: utf-8 -*-
"""트랙 R 시도 기록 — "143개 중 골랐다" 를 사후에 세지 않기 위해.

`docs/사전등록_2026-09-17_트랙R_조합탐색.md` §4·§9 를 **강제한다.**
문서에만 적힌 상한은 지켜지지 않는다. 여기서 막는다.

  · 단계별 상한(1단계 15 · 2단계 45 · 3단계 20 · 총 80)을 넘으면 **거부한다**
  · 실패한 시도도 남긴다 — 기록되지 않은 시도의 결과는 인용할 수 없다
  · append-only. 지우거나 고치지 않는다

⚠️ 이 모듈은 **판정하지 않는다.** 무엇을 시험했는지만 센다.
"""
import csv
import datetime
import json
import os

KST = datetime.timezone(datetime.timedelta(hours=9))
TRIALS_PATH = "data/research/trials.csv"
PREREG = "사전등록_2026-09-17_트랙R_조합탐색"

# §4 — 이 숫자를 코드가 강제한다
STAGE_CAP = {1: 15, 2: 45, 3: 20}
TOTAL_CAP = 80

HEADER = ["trial_no", "stage", "features", "window_from", "window_to",
          "n_days", "ic_mean", "ic_p", "ic_boot_p", "lift_k2", "lift_k3",
          "lift_k5", "verdict", "note", "prereg", "code_sha", "recorded_at"]


class CapExceeded(RuntimeError):
    """상한을 넘은 시도. **막는 것이 목적이므로 예외를 밖으로 낸다.**"""


def _rows(path=TRIALS_PATH):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f)]


def counts(path=TRIALS_PATH):
    """{단계: 시도 수}. 총합도 함께."""
    out = {1: 0, 2: 0, 3: 0}
    for r in _rows(path):
        try:
            out[int(r["stage"])] = out.get(int(r["stage"]), 0) + 1
        except (ValueError, KeyError):
            continue
    out["total"] = sum(v for k, v in out.items() if isinstance(k, int))
    return out


def remaining(path=TRIALS_PATH):
    c = counts(path)
    return {s: STAGE_CAP[s] - c.get(s, 0) for s in STAGE_CAP} | {
        "total": TOTAL_CAP - c["total"]}


def norm_features(features):
    """조합은 **순서가 없다.** 같은 조합을 다른 순서로 두 번 세지 않는다."""
    if isinstance(features, str):
        features = [features]
    return "+".join(sorted(str(f).strip() for f in features if str(f).strip()))


def already_tried(features, path=TRIALS_PATH):
    key = norm_features(features)
    return any(r.get("features") == key for r in _rows(path))


def record(stage, features, *, window_from="", window_to="", n_days=None,
           ic_mean=None, ic_p=None, ic_boot_p=None, lift=None,
           verdict="", note="", path=TRIALS_PATH, now=None, sha=None,
           allow_duplicate=False):
    """시도 하나를 남긴다. 상한을 넘으면 `CapExceeded`.

    verdict 는 '생존'/'탈락'/'보류' 중 하나를 권하지만 강제하지 않는다 —
    **판정은 이 모듈의 일이 아니다.**
    """
    if stage not in STAGE_CAP:
        raise ValueError(f"단계는 1·2·3 중 하나다: {stage}")
    key = norm_features(features)
    if not key:
        raise ValueError("피처가 비어 있다")
    if not allow_duplicate and already_tried(key, path):
        raise ValueError(f"이미 시험한 조합이다: {key}")

    c = counts(path)
    if c.get(stage, 0) >= STAGE_CAP[stage]:
        raise CapExceeded(
            f"{stage}단계 상한 {STAGE_CAP[stage]} 도달 — 사전등록 §4. "
            "더 보려면 별도 사전등록이 필요하고, 그때까지의 결과는 탐색으로 표시된다.")
    if c["total"] >= TOTAL_CAP:
        raise CapExceeded(f"총 상한 {TOTAL_CAP} 도달 — 사전등록 §4.")

    now = now or datetime.datetime.now(KST)
    lift = lift or {}
    row = [c["total"] + 1, stage, key, window_from, window_to,
           n_days if n_days is not None else "",
           _f(ic_mean), _f(ic_p), _f(ic_boot_p),
           _f(lift.get(2)), _f(lift.get(3)), _f(lift.get(5)),
           verdict, note, PREREG,
           (os.environ.get("GITHUB_SHA") or "")[:8] if sha is None else sha,
           now.isoformat()]
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEADER)
        w.writerow(row)
    return row[0]


def _f(v):
    return "" if v is None else v


def summary(path=TRIALS_PATH):
    c, r = counts(path), remaining(path)
    L = [f"# 트랙 R 시도 기록 — {PREREG}", "",
         "| 단계 | 시도 | 상한 | 남음 |", "|---|---:|---:|---:|"]
    for s in (1, 2, 3):
        L.append(f"| {s}단계 | {c.get(s,0)} | {STAGE_CAP[s]} | {r[s]} |")
    L += [f"| **합계** | **{c['total']}** | **{TOTAL_CAP}** | **{r['total']}** |", "",
          "> 실패한 시도도 포함된 수다. 기록되지 않은 시도의 결과는 인용하지 않는다.", ""]
    rows = _rows(path)
    if rows:
        L += ["| # | 단계 | 조합 | 일수 | IC | 부트p | lift@2 | 판정 |",
              "|--:|--:|---|--:|--:|--:|--:|---|"]
        for r_ in rows[-20:]:
            L.append(f"| {r_['trial_no']} | {r_['stage']} | `{r_['features']}` | "
                     f"{r_['n_days']} | {r_['ic_mean']} | {r_['ic_boot_p']} | "
                     f"{r_['lift_k2']} | {r_['verdict']} |")
        L.append("")
    return "\n".join(L)
