# -*- coding: utf-8 -*-
# ==========================================================================
# 📚 테마·대장 경로 연구 — 자료 사전 (읽기 전용 · 구조 집계만 · 수익률 없음)
# --------------------------------------------------------------------------
# 목적: 테마 형성 → 대장 등장 → 조정·확산·교체 → 재상승/소멸 연구를 설계하기 전에,
#       저장된 테마·종목 스냅샷이 무엇을 담고 있고 어떤 정의가 흔들리는지 센다.
# 방화벽: 진입일 < `hyeoks_theme_abc.CONFIRM_FROM`(2026-10-06) 인 날만 읽는다. 잠긴 사전등록
#         `leader-hold-v1` 의 확증 구간을 보지 않는다. 익일 가격·수익률을 계산하지 않는다.
# 쓰지 않는 것: 파일·시트·네트워크.
# ==========================================================================
import collections
import csv
import glob
import gzip
import os
import statistics as S
import sys

import hyeoks_theme_abc as T
from hyeoks_closing_bet import SNAP_DIR, read_snapshot

PSEUDO_KEYS = ("지수", "S7", "밸류업", "그룹")   # 이름 휴리스틱 — 지수·그룹성 '테마' 표시용(확정 목록 아님)
TOPN = 20


def research_days(snap_dir=SNAP_DIR, cutoff=T.CONFIRM_FROM):
    """15:05 테마 파일이 있는 날 중 방화벽 이전 날만."""
    ds = {os.path.basename(p)[:10] for p in glob.glob(os.path.join(snap_dir, "2026-*_1505_theme.csv.gz"))}
    return sorted(d for d in ds if d < cutoff)


def theme_rows(snap_dir, day, slot="1505"):
    with gzip.open(os.path.join(snap_dir, f"{day}_{slot}_theme.csv.gz"), "rt", encoding="utf-8") as fh:
        return list(csv.DictReader(fh.read().splitlines()[1:]))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def is_pseudo(name):
    return any(k in (name or "") for k in PSEUDO_KEYS)


def collect(snap_dir=SNAP_DIR, cutoff=T.CONFIRM_FROM):
    days = research_days(snap_dir, cutoff)
    st = collections.defaultdict(list)
    cnt = collections.Counter()
    prev_lead = prev_rate = prev_val = None
    streak, runs = {}, collections.Counter()
    for d in days:
        tr = theme_rows(snap_dir, d)
        st["테마수"].append(len(tr))
        by_rate = sorted(tr, key=lambda r: -num(r["changeRate"]))
        st["ranking일치"].append(sum(1 for i, r in enumerate(by_rate) if r["ranking"] == str(i + 1)) / len(tr))
        _, rows, _ = read_snapshot(os.path.join(snap_dir, f"{d}_1505.csv.gz"))
        mem = collections.Counter(t for r in rows.values() for t in (r.get("themeNos") or "").split("|") if t)
        for r in tr:
            diff = int(num(r["risingCount"]) + num(r["fallingCount"]) + num(r["unchangedCount"])) - mem.get(r["code"], 0)
            cnt["규모일치" if diff == 0 else ("규모_테마>소속" if diff > 0 else "규모_테마<소속")] += 1
        name = {r["code"]: r["name"] for r in tr}
        A, _ = T.build_A(rows)
        for it in A:
            cnt["A"] += 1
            cnt["A_주테마지수그룹성"] += is_pseudo(name.get(it["theme"]))
            st["A_소속테마수"].append(len([t for t in (rows[it["code"]].get("themeNos") or "").split("|") if t]))
        Bc, _ = T.build_B_current(A)
        cnt["B"] += len(Bc)
        cnt["B_주테마지수그룹성"] += sum(is_pseudo(name.get(x["theme"])) for x in Bc)
        lead = {x["theme"]: x["code"] for x in Bc}
        if prev_lead is not None:
            codes_prev, codes_now = set(prev_lead.values()), set(lead.values())
            st["대장종목유지"].append(len(codes_prev & codes_now) / len(codes_prev) if codes_prev else 0.0)
            kept = [t for t in prev_lead if t in lead]
            st["대장테마유지"].append(len(kept) / len(prev_lead) if prev_lead else 0.0)
            st["대장교체"].append(sum(1 for t in kept if lead[t] != prev_lead[t]) / len(kept) if kept else 0.0)
        prev_lead = lead
        rate_top = {r["code"] for r in by_rate[:TOPN]}
        val_top = {r["code"] for r in sorted(tr, key=lambda r: -num(r["totalTradingValue"]))[:TOPN]}
        if prev_rate is not None:
            st["등락률상위겹침"].append(len(rate_top & prev_rate) / TOPN)
            st["거래대금상위겹침"].append(len(val_top & prev_val) / TOPN)
        prev_rate, prev_val = rate_top, val_top
        nxt = {c: streak.get(c, 0) + 1 for c in rate_top}
        for c, k in streak.items():
            if c not in rate_top:
                runs[k] += 1
        streak = nxt
        prof = os.path.join("data/intraday_profile", f"{d}.csv.gz")
        if os.path.exists(prof) and Bc:
            with gzip.open(prof, "rt", encoding="utf-8") as fh:
                pc = {r["code"] for r in csv.DictReader(fh.read().splitlines()[1:])}
            st["분봉포함"].append(len({x["code"] for x in Bc} & pc) / len(Bc))
    return days, st, cnt, runs, len(streak)


def report(snap_dir=SNAP_DIR):
    days, st, cnt, runs, ongoing = collect(snap_dir)
    med = lambda k: (round(S.median(st[k]), 3) if st[k] else None)
    L = ["# 📚 테마·대장 경로 연구 자료 사전 (구조 집계 · 수익률 없음)", "",
         f"- 분석 일수 {len(days)} ({days[0] if days else '—'} ~ {days[-1] if days else '—'}) — 방화벽: 진입일 < {T.CONFIRM_FROM}",
         f"- 테마 수/일 {min(st['테마수'], default=0)}~{max(st['테마수'], default=0)}",
         f"- `ranking` 이 등락률 내림차순 순위와 같은 비율(일 중앙) {med('ranking일치')}",
         f"- 테마 자체 규모(상승+하락+보합) vs 종목 스냅샷 소속 수: 일치 {cnt['규모일치']} · 테마>소속 {cnt['규모_테마>소속']} · 테마<소속 {cnt['규모_테마<소속']}",
         f"- A 종목의 소속 테마 수 중앙 {S.median(st['A_소속테마수']) if st['A_소속테마수'] else '—'} · 2개 이상 "
         f"{round(sum(1 for m in st['A_소속테마수'] if m >= 2) / max(1, len(st['A_소속테마수'])), 3)}",
         f"- 주테마(`topThemeNo`)가 지수·그룹성 이름: A {cnt['A_주테마지수그룹성']}/{cnt['A']} · B_현행 대장 {cnt['B_주테마지수그룹성']}/{cnt['B']}",
         f"- B_현행 대장 종목이 다음 관측일에도 대장(중앙) {med('대장종목유지')} · 대장 있던 테마가 다음날도 대장 있음 {med('대장테마유지')} · 이어진 테마 중 대장 교체 {med('대장교체')}",
         f"- 상위 {TOPN} 테마 전일 대비 겹침(중앙): 등락률 기준 {med('등락률상위겹침')} · 거래대금 기준 {med('거래대금상위겹침')}",
         f"- 등락률 상위 {TOPN} 연속 체류 일수(끝난 것): {dict(sorted(runs.items()))} · 마지막 날 진행 중 {ongoing}",
         f"- 분봉 프로파일에 B_현행 대장 포함 비율(일 중앙) {med('분봉포함')} · 최소 {round(min(st['분봉포함']), 3) if st['분봉포함'] else '—'}",
         "", "> 구조 집계다. 익일 가격·수익률을 읽지 않는다. 지수·그룹성 판별은 이름 휴리스틱이며 확정 목록이 아니다."]
    return "\n".join(L)


if __name__ == "__main__":
    print(report())
    sys.exit(0)
