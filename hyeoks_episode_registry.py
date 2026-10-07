# -*- coding: utf-8 -*-
# ==========================================================================
# 🗂️ 에피소드 등록부 `episode-registry-v1` — 대장 후보를 끝까지 추적하고 분모를 잃지 않는다
# --------------------------------------------------------------------------
# 근거: Codex 진행 계획(2026-10-05) §5 ②④ · Claude 반영 기록 §4 · 허브 §3 #14.
# 하는 것: 15:05 스냅샷(네이버 기준선)만으로 **구조** 등록부를 만든다.
#   · 에피소드 = 현행 대장 정의(hyeoks_theme_abc.build_B_current)에 나온 종목. 그 종목에 열린 에피소드가 없을 때만 새로 연다.
#     테마는 t0 의 topThemeNo 로 고정. 종목당 열린 에피소드는 하나 — 여러 테마 중복을 독립 표본으로 세지 않는다.
#   · 주 대조군 = t0 같은 날 **같은 테마**의 A 적격 비대장(상한가 근처 제외) 중 거래대금이 가장 비슷한 PRIMARY_K 종목.
#     등락률은 맞추지 않는다 — 대장 정의 자체가 등락률 1위라 맞출 수 없다(Codex: 정의 변수를 무리하게 맞추지 않음).
#   · 보조 대조군 = 그날 A 의 적격 비대장 전체에서 코드 정렬 후 고정 시드로 SECONDARY_K 종목. 그날 열린 모든 에피소드가 공유.
#   · 대조군이 나중에 대장이 돼도 교체·삭제하지 않고 사건(`대조군→대장`)으로 표시한다.
#   · 상태(관측일별): 진행 중 · 관측 결측(그날 스냅샷에 없음) · 상한 도달(행정 상한, 우측 검열) · 종료(소멸 추정).
#     배지 소실은 종료가 아니다. 결측일은 종료일이 아니다.
# 하지 않는 것: 가격 성과·경로 분류(수정주가 계열 확보 전)·미래 수익률 출력·운영 선정 변경·시트 쓰기.
#   그날의 상태표는 그날(과 이전) 스냅샷만 쓴다 — 뒤 날짜를 더해도 앞 날짜의 등록은 바뀌지 않는다(시험이 확인).
#   종목 단위 등록부는 공개 저장소에 쓰지 않는다(후보 목록이 되지 않게). 공개 출력은 건수·지문뿐.
# 지문 대상 파일이 아니다.
# ==========================================================================
import argparse
import hashlib
import json
import math
import os
import random
import sys

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, read_snapshot

VERSION = "episode-registry-v1"
SOURCE = "naver-topThemeNo-1505"      # stockinfo7 은 별도 출처 — 이 등록부에 합치지 않는다
RATE_CAP = 29.5                       # 상한가 근처(15:05 매수 불가 대용) — 대조군 후보에서 뺀다
PRIMARY_K = 2
SECONDARY_K = 3
SEED = 20261007
ADMIN_CAP_OBS = 60                    # 행정 상한(관측일). 수익 문턱이 아니다 — 상한에서 미종결이면 우측 검열
ABSENT_END = 10                       # 연속 10 관측일 스냅샷에 없으면 '종료(소멸 추정)'. 그 전까지는 관측 결측
STATUS = ("진행 중", "관측 결측", "상한 도달", "종료(소멸 추정)")


def _f(x):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return None


def load_obs(snap_dir=SNAP_DIR, until=None):
    """관측일(15:05 스냅샷이 있는 날) 오름차순. 결측일을 휴장으로 만들지 않는다 — 없는 날은 그냥 없다."""
    out = []
    for name in sorted(os.listdir(snap_dir)):
        if not name.endswith("_1505.csv.gz") or not name[:4].isdigit():
            continue
        d = name[:10]
        if until and d > until:
            continue
        _, rows, _ = read_snapshot(os.path.join(snap_dir, name))
        A, _ = T.build_A(rows)
        B, _ = T.build_B_current(A)
        out.append({"date": d, "codes": set(rows), "A": {x["code"]: x for x in A},
                    "B": {x["code"]: x["theme"] for x in B}})
    return out


def _eligible(x, leaders):
    return x["code"] not in leaders and x["rate"] < RATE_CAP


def primary_controls(day, leader):
    """같은 날 같은 테마의 적격 비대장 중 |log 거래대금 차| 가 작은 순, 동률은 코드순."""
    la = math.log(max(leader["amt"], 1.0))
    pool = [x for x in day["A"].values()
            if x["theme"] == leader["theme"] and x["code"] != leader["code"] and _eligible(x, day["B"])]
    pool.sort(key=lambda x: (abs(math.log(max(x["amt"], 1.0)) - la), x["code"]))
    return [x["code"] for x in pool[:PRIMARY_K]]


def secondary_controls(day):
    """그날 A 의 적격 비대장에서 코드 정렬 → random.Random(SEED + 날짜숫자) 로 SECONDARY_K 개."""
    pool = sorted(x["code"] for x in day["A"].values() if _eligible(x, day["B"]))
    seed = SEED + int(day["date"].replace("-", ""))
    return sorted(random.Random(seed).sample(pool, min(SECONDARY_K, len(pool)))), seed


def build(days):
    """등록부(에피소드·대조군·사건)와 관측일별 상태표. 가격 결과는 만들지 않는다."""
    episodes, open_by_code, status_rows, events, daily = [], {}, [], [], []
    for i, day in enumerate(days):
        # ① 기존 에피소드의 오늘 상태 — 오늘 스냅샷만 본다
        for ep in episodes:
            if ep["closed"] or ep["i0"] >= i:
                continue
            k = i - ep["i0"]
            present = ep["code"] in day["codes"]
            ep["absent_run"] = 0 if present else ep["absent_run"] + 1
            if ep["absent_run"] >= ABSENT_END:
                st = "종료(소멸 추정)"
            elif k >= ADMIN_CAP_OBS:
                st = "상한 도달"
            elif not present:
                st = "관측 결측"
            else:
                st = "진행 중"
            status_rows.append({"episode": ep["id"], "date": day["date"], "k": k, "status": st,
                                "in_snapshot": present, "in_A": ep["code"] in day["A"],
                                "leader_today": ep["code"] in day["B"],
                                "same_theme_today": day["B"].get(ep["code"]) == ep["theme"]})
            for c in ep["primary"]:
                if c in day["B"] and c not in ep["became_leader"]:      # 처음 한 번만 사건으로 남긴다
                    ep["became_leader"].add(c)
                    events.append({"episode": ep["id"], "date": day["date"], "code": c, "event": "대조군→대장"})
            if st in ("종료(소멸 추정)", "상한 도달"):
                ep["closed"], ep["end"], ep["end_status"] = True, day["date"], st
                open_by_code.pop(ep["code"], None)
        # ② 오늘 새 에피소드 — 그 종목에 열린 에피소드가 없을 때만
        sec, seed = secondary_controls(day)
        opened = 0
        for code, theme in sorted(day["B"].items()):
            if code in open_by_code:
                continue
            leader = day["A"][code]
            ep = {"id": f"{day['date']}_{code}", "code": code, "t0": day["date"], "i0": i, "theme": theme,
                  "source": SOURCE, "primary": primary_controls(day, leader), "secondary": sec,
                  "secondary_seed": seed, "t0_state": {"rate": leader["rate"], "amt": leader["amt"],
                                                        "alert": leader["alert"]},
                  # 대조군 중 t0 에 이미 열린 에피소드가 있던 종목(전에 대장이었다) — 빼지 않고 표시만
                  "primary_prior_episode": [c for c in primary_controls(day, leader) if c in open_by_code],
                  "closed": False, "end": "", "end_status": "", "absent_run": 0, "became_leader": set()}
            episodes.append(ep)
            open_by_code[code] = ep
            opened += 1
        daily.append({"date": day["date"], "leaders": len(day["B"]), "opened": opened,
                      "open_after": len(open_by_code), "secondary": len(sec)})
    return episodes, status_rows, events, daily


def digest(episodes):
    """등록 내용 지문 — t0 시점에 정해지는 필드만. 공개 기록에 남겨 나중의 재계산과 대조한다."""
    keep = [{k: ep[k] for k in ("id", "code", "t0", "theme", "source", "primary", "secondary", "secondary_seed")}
            for ep in episodes]
    return hashlib.sha256(json.dumps(keep, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def summary(days, episodes, status_rows, events, daily):
    """공개용 — 건수만. 종목 코드·이름·가격 없음."""
    last = days[-1]["date"] if days else ""
    cur = {}
    for r in status_rows:
        if r["date"] == last:
            cur[r["status"]] = cur.get(r["status"], 0) + 1
    return {"version": VERSION, "source": SOURCE, "obs_days": len(days),
            "first": days[0]["date"] if days else "", "last": last,
            "episodes": len(episodes), "open": sum(1 for e in episodes if not e["closed"]),
            "closed": {s: sum(1 for e in episodes if e["end_status"] == s) for s in ("상한 도달", "종료(소멸 추정)")},
            "primary_controls_per_episode": {n: sum(1 for e in episodes if len(e["primary"]) == n)
                                             for n in range(PRIMARY_K + 1)},
            "primary_control_with_prior_episode": sum(len(e["primary_prior_episode"]) for e in episodes),
            "primary_control_slots": sum(len(e["primary"]) for e in episodes),
            "control_became_leader_events": len(events),
            "status_on_last_day": cur, "opened_on_last_day": daily[-1]["opened"] if daily else 0,
            "registry_sha256": digest(episodes),
            "params": {"PRIMARY_K": PRIMARY_K, "SECONDARY_K": SECONDARY_K, "SEED": SEED,
                       "ADMIN_CAP_OBS": ADMIN_CAP_OBS, "ABSENT_END": ABSENT_END, "RATE_CAP": RATE_CAP}}


def tracked_codes(episodes, day_secondary):
    """연구 일봉 우선 구간 제안용 — 미종결 에피소드 종목 + 그 주 대조군 + 오늘 보조 대조군 (지수는 별도)."""
    s = set(day_secondary)
    for e in episodes:
        if not e["closed"]:
            s.add(e["code"])
            s.update(e["primary"])
    return sorted(s)


def main(argv=None):
    ap = argparse.ArgumentParser(description="에피소드 등록부 — 구조 건수만 출력(종목·가격 없음)")
    ap.add_argument("--until", default="", help="이 날짜까지의 관측일만 사용")
    a = ap.parse_args(argv)
    days = load_obs(until=a.until or None)
    eps, st, ev, daily = build(days)
    print(json.dumps(summary(days, eps, st, ev, daily), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
