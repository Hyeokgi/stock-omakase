# -*- coding: utf-8 -*-
# ==========================================================================
# 🧬 채널별 정책 코호트 경계 확인 — 읽기 전용. 아무것도 쓰지 않는다
# --------------------------------------------------------------------------
# 왜 만드나 — §3-5-7 U3 이 남긴 숙제
#   U3 은 "정책 동일성은 글로벌이 아니라 채널별" 로 정했다. 그러면 **채널마다
#   선정 정의가 언제 바뀌었는지** 를 알아야 코호트를 가를 수 있다.
#   그리고 그 경계는 **기억이나 성과가 아니라 코드 이력에서** 끌어내야 한다.
#
# 그런데 코드 이력만으로는 안 되는 구간이 있다
#   `수급TOP2` 의 위치요건(고가 근처 0.70~1.00) 제거는 **git 이전**에 일어났다.
#   저장소 최초 커밋이 f305c01(2026-08-04)이고 게이트 조건식은 그날 이미 현재 형태였다.
#   (59552c4 가 그 줄을 건드렸지만 `except:` → `except Exception:` 뿐이라 조건은 무변경.)
#   그러므로 "언제 바뀌었나" 는 이 저장소만으로 답할 수 없다.
#
# 🔑 그래서 질문을 바꾼다 — 답할 수 있는 형태로
#   git 은 **2026-08-04 이후 게이트가 안 바뀌었음을 증명한다.**
#   따라서 필요한 것은 변경일이 아니라 이 하나다:
#
#       "판정 표본 중 2026-08-04 보다 이른 진입일이 **하나라도 있는가**"
#
#   · 없다 → 모든 행이 git 이 보증하는 동일 게이트 아래 생성됐다. **단일 코호트.**
#     `수급TOP2` 의 N 은 줄지 않는다.
#   · 있다 → 그 행들은 게이트를 확인할 수 없는 구간이다. 섞지 않는다.
#     `channel_policy_unverified` 로 분리하고 그 사실을 산출물에 적는다.
#
#   이건 추론이 아니라 **원장을 읽으면 답이 나오는 사실 질문**이다.
#
# 무엇을 하지 않나
#   · 쓰지 않는다. update·clear·format 어떤 호출도 없다.
#   · 코호트를 **확정하지 않는다.** 경계 후보와 증거를 보여줄 뿐이고,
#     확정은 사전등록 문서 갱신으로 한다(사람의 결정).
#   · 수익률을 보지 않는다. **성과를 보고 경계를 정하면 그것이 사후 맞춤**이다.
#     이 프로브는 진입일·채널·행수만 읽는다. 수익률 열은 건드리지 않는다.
# ==========================================================================
import argparse
import datetime
import sys

KST = datetime.timezone(datetime.timedelta(hours=9))
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SHEET_NAME = "백테스트_로그"          # hyeoks_verdict.SHEET_NAME 과 같아야 한다
C_ENTRY_DATE, C_CHANNEL = 1, 2       # hyeoks_verdict 와 같은 열 규약

# git 이 게이트 무변경을 보증하기 시작하는 날 = 저장소 최초 커밋.
# 이 값은 **관측된 사실**이지 정책 선택이 아니다. 바꾸려면 근거가 필요하다.
GIT_FLOOR = "2026-08-04"
GIT_FLOOR_COMMIT = "f305c01"

# omakase.py:3142 가 각 행에 남기는 수급 게이트 플래그. 이 토큰이 기준선 **이전** 행에도
# 있다면, 그 행이 어떤 게이트를 통과했는지 **코드 없이** 알 수 있다.
GATE_TOKENS = ("GATE_PASS", "GATE_FAIL")
MAX_EXAMPLES = 6        # 열 하나당 보여줄 값 예시 수
EXAMPLE_CHARS = 40      # 예시 값 자르는 길이


def classify(rows, floor=GIT_FLOOR):
    """채널별로 (총행수, 최초 진입일, 최종 진입일, floor 이전 행수, 날짜불명 행수).

    시트 없이 검증되는 순수 함수다. 이 함수가 판정의 전부이고
    아래 `main` 은 원장을 읽어 여기에 넘기기만 한다.
    """
    out = {}
    for row in rows[1:] if rows else []:
        if len(row) <= C_CHANNEL:
            continue
        ch = str(row[C_CHANNEL]).strip()
        if not ch:
            continue
        d = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        try:
            d = datetime.date.fromisoformat(d).isoformat()
        except ValueError:
            d = ""                                  # 날짜 불명 — 조용히 통과시키지 않는다
        s = out.setdefault(ch, {"n": 0, "first": "", "last": "", "before": 0, "undated": 0})
        s["n"] += 1
        if not d:
            s["undated"] += 1
            continue
        if d < floor:
            s["before"] += 1
        if not s["first"] or d < s["first"]:
            s["first"] = d
        if d > s["last"]:
            s["last"] = d
    return out


def cohort_verdict(stat, floor=GIT_FLOOR):
    """(상태, 설명). 세 갈래뿐이고 **모호하면 확정하지 않는다.**"""
    if stat["n"] == 0:
        return "표본없음", "행이 없다"
    if stat["undated"]:
        return "확인불가", (f"진입일을 읽을 수 없는 행 {stat['undated']}건 — "
                        f"날짜 없이는 코호트를 가를 수 없다")
    if stat["before"]:
        return "코호트분리필요", (f"{floor} 이전 {stat['before']}건 — "
                             f"게이트를 코드 이력으로 확인할 수 없는 구간이다")
    return "단일코호트", (f"전 구간이 {floor} 이후 — git 이 보증하는 동일 게이트 아래 "
                     f"생성됐다")


def mapped_columns():
    """판정기가 이름을 아는 열. 나머지가 '본 적 없는 열' 이다."""
    try:
        import hyeoks_verdict as v
        return dict(v.EXPECTED_HEADER)
    except Exception:                               # noqa: BLE001
        return {}


def header_dump(rows):
    """원장 제목 행 전부. 이름만 본다 — 값이 아니라 **구조**를 보는 단계다."""
    if not rows:
        return []
    mapped = mapped_columns()
    out = []
    for i, name in enumerate(rows[0]):
        out.append({"i": i, "name": str(name).strip(),
                    "mapped": mapped.get(i), "known": i in mapped})
    return out


def unmapped_examples(rows, floor=GIT_FLOOR):
    """매핑 안 된 열의 값 예시를 기준선 **전/후로 나눠** 보여준다.

    왜 나누나 — 같은 열이라도 8/4 이전에는 비어 있고 이후에만 찼을 수 있다.
    그러면 그 열은 옛 구간의 증거가 못 된다. 나눠 보지 않으면 그걸 못 본다.
    """
    if not rows:
        return []
    mapped = mapped_columns()
    ncol = max((len(r) for r in rows), default=0)
    seen = {i: {"before": [], "after": []} for i in range(ncol) if i not in mapped}
    for row in rows[1:]:
        if len(row) <= C_CHANNEL:
            continue
        d = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        try:
            d = datetime.date.fromisoformat(d).isoformat()
        except ValueError:
            continue                                # 날짜 없는 행은 전/후를 못 가른다
        side = "before" if d < floor else "after"
        for i, bucket in seen.items():
            val = str(row[i]).strip() if i < len(row) else ""
            if val and val not in bucket[side] and len(bucket[side]) < MAX_EXAMPLES:
                bucket[side].append(val[:EXAMPLE_CHARS])
    head = rows[0]
    return [{"i": i,
             "name": str(head[i]).strip() if i < len(head) else "",
             "before": b["before"], "after": b["after"]}
            for i, b in sorted(seen.items())]


def gate_flag_coverage(rows, floor=GIT_FLOOR):
    """🔑 핵심 — `GATE_PASS`/`GATE_FAIL` 이 기준선 **이전** 행에도 있는가.

    있으면 옛 구간 행이 어떤 게이트를 통과했는지 **코드를 못 찾아도** 알 수 있다.
    없으면 이 경로는 막힌 것이고, 그 사실을 분명히 말해야 한다.
    돌려주는 값: 채널별 {이전 총/이전 플래그있음, 이후 총/이후 플래그있음}.
    """
    out = {}
    for row in rows[1:] if rows else []:
        if len(row) <= C_CHANNEL:
            continue
        ch = str(row[C_CHANNEL]).strip()
        if not ch:
            continue
        d = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        try:
            d = datetime.date.fromisoformat(d).isoformat()
        except ValueError:
            continue
        side = "before" if d < floor else "after"
        joined = " ".join(str(c) for c in row)
        has = any(t in joined for t in GATE_TOKENS)
        s = out.setdefault(ch, {"before": 0, "before_flag": 0,
                                "after": 0, "after_flag": 0})
        s[side] += 1
        if has:
            s[side + "_flag"] += 1
    return out


def gate_value_breakdown(rows, floor=GIT_FLOOR):
    """🔑🔑 플래그의 **값**을 본다. 이것이 선정 정의를 직접 시험한다.

    `omakase.py:3133·3159` — `supply_top2` 는 `gate_passed`(= `GATE_PASS` 인 행)
    에서만 뽑는다. 그러므로 **수급TOP2 행은 전부 `GATE_PASS` 여야 한다.**
    기준선 이전 수급TOP2 행에 `GATE_FAIL` 이 섞여 있다면, 그때의 선정 규칙이
    지금과 **달랐다**는 직접 증거다.

    반대로 `chart_top2` 는 `candidate_pool` 에서 뽑으므로(3132행) 게이트와 무관하다.
    차트TOP2 행에는 PASS 와 FAIL 이 **둘 다 있어야 정상**이다. 한쪽만 나오면
    그것도 규칙이 달랐다는 신호다. 두 채널이 서로의 대조군 노릇을 한다.

    돌려주는 값: 채널별 {before: {PASS, FAIL, 없음}, after: {...}}.
    """
    out = {}
    for row in rows[1:] if rows else []:
        if len(row) <= C_CHANNEL:
            continue
        ch = str(row[C_CHANNEL]).strip()
        if not ch:
            continue
        d = str(row[C_ENTRY_DATE]).strip()[:10] if len(row) > C_ENTRY_DATE else ""
        try:
            d = datetime.date.fromisoformat(d).isoformat()
        except ValueError:
            continue
        side = "before" if d < floor else "after"
        joined = " ".join(str(c) for c in row)
        key = ("GATE_PASS" if "GATE_PASS" in joined
               else "GATE_FAIL" if "GATE_FAIL" in joined else "없음")
        b = out.setdefault(ch, {"before": {"GATE_PASS": 0, "GATE_FAIL": 0, "없음": 0},
                                "after": {"GATE_PASS": 0, "GATE_FAIL": 0, "없음": 0}})
        b[side][key] += 1
    return out


def render_breakdown(bd, floor=GIT_FLOOR):
    """결론을 먼저 말한다. 이 표가 이 프로브에서 가장 결정적이다."""
    L = ["", "## 🔑🔑 게이트 플래그의 **값** — 선정 규칙이 같았는가", ""]
    if not bd:
        return "\n".join(L + ["행이 없다."])

    sup = bd.get("수급TOP2")
    if sup:
        bf = sup["before"]["GATE_FAIL"]
        bp = sup["before"]["GATE_PASS"]
        bn = sup["before"]["없음"]
        L.append("**수급TOP2 — `supply_top2` 는 `gate_passed` 에서만 뽑는다"
                 "(`omakase.py:3133·3159`). 전부 `GATE_PASS` 여야 한다.**")
        if bp + bf + bn == 0:
            L.append(f"· 기준선 `{floor}` 이전 행이 없다 — 시험할 것이 없다.")
        elif bf == 0 and bn == 0:
            L.append(f"· 이전 {bp}행이 **전부 `GATE_PASS`**. "
                     f"지금과 **같은 선정 규칙과 모순되지 않는다.**")
        else:
            L.append(f"· 이전 행에 `GATE_FAIL` **{bf}건** · 플래그 없음 **{bn}건**. "
                     f"**그때의 선정 규칙은 지금과 달랐다.**")
        L.append("")

    cht = bd.get("차트TOP2")
    if cht:
        b = cht["before"]
        L.append("**차트TOP2 — `chart_top2` 는 `candidate_pool` 에서 뽑는다"
                 "(`omakase.py:3132`). 게이트와 무관하므로 PASS·FAIL 이 섞여야 정상이다.**")
        if b["GATE_PASS"] and b["GATE_FAIL"]:
            L.append(f"· 이전 구간에 PASS {b['GATE_PASS']} · FAIL {b['GATE_FAIL']} — "
                     f"**섞여 있다. 정상이고, 수급TOP2 쪽 결과가 우연이 아님을 받쳐 준다.**")
        elif b["GATE_PASS"] + b["GATE_FAIL"]:
            L.append(f"· 이전 구간이 한쪽으로 쏠렸다 (PASS {b['GATE_PASS']} · "
                     f"FAIL {b['GATE_FAIL']}) — **규칙이 달랐다는 신호일 수 있다.**")
        L.append("")

    L += ["| 채널 | 이전 PASS | 이전 FAIL | 이전 없음 | 이후 PASS | 이후 FAIL | 이후 없음 |",
          "|---|--:|--:|--:|--:|--:|--:|"]
    for ch in sorted(bd):
        b, a = bd[ch]["before"], bd[ch]["after"]
        L.append(f"| {ch} | {b['GATE_PASS']} | {b['GATE_FAIL']} | {b['없음']} | "
                 f"{a['GATE_PASS']} | {a['GATE_FAIL']} | {a['없음']} |")
    L += ["",
          "> ⚠️ **이 시험이 통과해도 게이트의 *정의* 가 같았다는 증명은 아니다.** "
          "증명하려면 거래대금·전일비 거래량·250일 고가를 재계산해야 하는데 "
          "**원장에 그 열이 없고** freeze 파일은 2026-09-08 부터다.",
          "> 이 시험이 가리는 것은 **선정 규칙(무엇에서 뽑는가)** 이지 "
          "**게이트 조건(무엇을 통과시키는가)** 이 아니다. 둘을 섞지 않는다."]
    return "\n".join(L)


def render(stats, now, floor=GIT_FLOOR):
    L = [f"# 🧬 채널별 정책 코호트 경계 — {now.strftime('%Y-%m-%d %H:%M KST')}", ""]
    L.append(f"기준선 `{floor}` (최초 커밋 `{GIT_FLOOR_COMMIT}`) — "
             f"이 날부터 선정 게이트가 안 바뀌었음을 git 이 보증한다.")
    L.append("")
    L.append("| 채널 | 행수 | 최초 진입일 | 최종 진입일 | 기준선 이전 | 날짜불명 | 판정 |")
    L.append("|---|--:|---|---|--:|--:|---|")
    for ch in sorted(stats):
        s = stats[ch]
        v = cohort_verdict(s, floor)[0]
        L.append(f"| {ch} | {s['n']} | {s['first'] or '—'} | {s['last'] or '—'} | "
                 f"{s['before']} | {s['undated']} | {v} |")
    L.append("")
    for ch in sorted(stats):
        state, why = cohort_verdict(stats[ch], floor)
        L.append(f"- **{ch}** — {state}: {why}")
    L.append("")
    L.append("> 이 표는 **경계 후보와 증거**다. 코호트 확정은 사전등록 문서 갱신으로 한다.")
    L.append("> 위 판정은 `classify` 만 쓴다 — 진입일·채널만 본다. 아래 덤프는 참고 자료다.")
    return "\n".join(L)


def render_gate(cov, floor=GIT_FLOOR):
    """🔑 이 프로브에서 가장 중요한 표. 결론을 문장으로 먼저 말한다."""
    L = ["", "## 🔑 게이트 플래그 — 옛 구간에 증거가 있는가", ""]
    if not cov:
        return "\n".join(L + ["행이 없다."])
    before_rows = sum(c["before"] for c in cov.values())
    before_flag = sum(c["before_flag"] for c in cov.values())
    if before_rows == 0:
        L.append(f"기준선 `{floor}` 이전 행이 없다 — 이 질문 자체가 성립하지 않는다.")
    elif before_flag == 0:
        L.append(f"**막혔다.** 기준선 이전 {before_rows}행 중 `GATE_PASS`/`GATE_FAIL` 을 "
                 f"가진 행이 **0건**이다. 원장만으로는 옛 구간의 게이트를 알 수 없다.")
    elif before_flag == before_rows:
        L.append(f"**뚫렸다.** 기준선 이전 {before_rows}행 **전부**가 게이트 플래그를 갖고 있다. "
                 f"코드를 못 찾아도 옛 구간이 어떤 게이트를 통과했는지 알 수 있다.")
    else:
        L.append(f"**부분적이다.** 기준선 이전 {before_rows}행 중 {before_flag}행만 "
                 f"플래그를 갖는다. 나머지 {before_rows - before_flag}행은 여전히 확인 불가다.")
    L += ["", "| 채널 | 이전 행 | 이전 플래그有 | 이후 행 | 이후 플래그有 |",
          "|---|--:|--:|--:|--:|"]
    for ch in sorted(cov):
        c = cov[ch]
        L.append(f"| {ch} | {c['before']} | {c['before_flag']} | "
                 f"{c['after']} | {c['after_flag']} |")
    L.append("")
    L.append(f"> 찾는 토큰: {' · '.join('`%s`' % t for t in GATE_TOKENS)} "
             f"(`omakase.py:3142` 가 남기는 값). 행 전체에서 찾는다.")
    L.append("> **플래그가 있다는 것과 그것이 옳다는 것은 다르다.** 있으면 그때 대조한다.")
    return "\n".join(L)


def render_header(dump, examples):
    L = ["", "## 📋 원장 열 구조 — 판정기가 아는 열과 모르는 열", ""]
    if not dump:
        return "\n".join(L + ["제목 행이 없다."])
    L += [f"총 **{len(dump)}열**. "
          f"판정기가 이름을 아는 열 **{sum(1 for d in dump if d['known'])}개**, "
          f"본 적 없는 열 **{sum(1 for d in dump if not d['known'])}개**.", ""]
    L += ["| # | 원장 제목 | 판정기 매핑 |", "|--:|---|---|"]
    for d in dump:
        L.append(f"| {d['i']} | {d['name'] or '—'} | "
                 f"{'`' + d['mapped'] + '`' if d['known'] else '**미매핑**'} |")
    L += ["", "### 미매핑 열의 값 예시 — 기준선 전/후", ""]
    L += ["| # | 제목 | 기준선 이전 예시 | 기준선 이후 예시 |", "|--:|---|---|---|"]
    for e in examples:
        b = " · ".join(e["before"]) or "—"
        a = " · ".join(e["after"]) or "—"
        L.append(f"| {e['i']} | {e['name'] or '—'} | {b} | {a} |")
    L.append("")
    L.append("> 전/후로 나눈 이유 — 같은 열이라도 8/4 이후에만 찼을 수 있다. "
             "그러면 옛 구간의 증거가 못 된다.")
    return "\n".join(L)


def self_test():
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    H = ["trade_id", "진입일", "채널"]
    mk = lambda *rows: [H] + [list(r) for r in rows]

    print("🧪 분류 — 채널별 집계")
    st = classify(mk(("t1", "2026-08-10", "수급TOP2"),
                     ("t2", "2026-09-01", "수급TOP2"),
                     ("t3", "2026-08-20", "차트TOP2")))
    chk("채널별로 나뉜다", set(st) == {"수급TOP2", "차트TOP2"})
    chk("행수를 센다", st["수급TOP2"]["n"] == 2)
    chk("최초 진입일", st["수급TOP2"]["first"] == "2026-08-10")
    chk("최종 진입일", st["수급TOP2"]["last"] == "2026-09-01")

    print("🧪 기준선 — 이것이 이 프로브의 전부다")
    chk("기준선 이후만이면 단일코호트",
        cohort_verdict(st["수급TOP2"])[0] == "단일코호트")
    st2 = classify(mk(("t1", "2026-08-01", "수급TOP2"),      # 최초 커밋 3일 전
                      ("t2", "2026-09-01", "수급TOP2")))
    chk("기준선 이전이 하나라도 있으면 분리 필요",
        cohort_verdict(st2["수급TOP2"])[0] == "코호트분리필요")
    chk("몇 건인지 센다", st2["수급TOP2"]["before"] == 1)
    chk("기준선 당일은 이전이 아니다",
        classify(mk(("t", GIT_FLOOR, "수급TOP2")))["수급TOP2"]["before"] == 0)

    print("🧪 날짜 불명 — 조용히 통과시키지 않는다")
    st3 = classify(mk(("t1", "", "수급TOP2"), ("t2", "2026-09-01", "수급TOP2")))
    chk("날짜불명을 센다", st3["수급TOP2"]["undated"] == 1)
    chk("날짜불명이 있으면 확인불가 (단일코호트로 안 넘어간다)",
        cohort_verdict(st3["수급TOP2"])[0] == "확인불가")
    chk("깨진 날짜도 날짜불명이다",
        classify(mk(("t", "9월 1일", "차트TOP2")))["차트TOP2"]["undated"] == 1)

    print("🧪 경계가 성과와 무관한가 — 수익률 열을 아예 안 읽는다")
    # 소스에 "수익률" 이 있는지 보는 식의 자기참조 검사는 하지 않는다.
    # **동작으로** 건다 — 3열 뒤 내용이 무엇이든 결과가 한 글자도 안 달라져야 한다.
    base = [H + ["수익률", "알파"], ["t1", "2026-09-01", "수급TOP2", "0.0", "0.0"]]
    huge = [H + ["수익률", "알파"], ["t1", "2026-09-01", "수급TOP2", "999.9", "-999.9"]]
    chk("3열 뒤 값이 달라도 집계가 동일하다", classify(base) == classify(huge))
    chk("열이 더 붙어도 판정이 같다",
        cohort_verdict(classify(huge)["수급TOP2"])[0] == "단일코호트")
    # 성과가 경계를 못 움직인다는 것을 가장 센 형태로 — 최고 수익 행과 최악 수익 행의
    # 진입일이 서로 바뀌어도, 경계 판정은 **진입일 분포만** 따른다.
    swapped = [H + ["수익률"], ["t1", "2026-09-01", "수급TOP2", "-99"],
               ["t2", "2026-08-05", "수급TOP2", "+99"]]
    flipped = [H + ["수익률"], ["t1", "2026-09-01", "수급TOP2", "+99"],
               ["t2", "2026-08-05", "수급TOP2", "-99"]]
    chk("수익률을 뒤집어도 경계 판정이 같다", classify(swapped) == classify(flipped))
    src = open(__file__, encoding="utf-8").read()
    body = src.split("def self_test")[0]

    print("🧪 빈 원장·짧은 행")
    chk("빈 원장은 빈 결과", classify([]) == {})
    chk("제목만 있으면 빈 결과", classify([H]) == {})
    chk("열이 모자란 행은 건너뛴다", classify([H, ["t"]]) == {})
    chk("채널이 비면 건너뛴다", classify(mk(("t", "2026-09-01", "  "))) == {})

    print("🧪 게이트 플래그 탐지 — 이 프로브의 핵심")
    G = ["trade_id", "진입일", "채널", "v2게이트"]
    gk = lambda *r: [G] + [list(x) for x in r]
    cov = gate_flag_coverage(gk(("t1", "2026-07-10", "수급TOP2", "GATE_PASS"),
                                ("t2", "2026-07-11", "수급TOP2", ""),
                                ("t3", "2026-09-01", "수급TOP2", "GATE_FAIL")))
    chk("기준선 전/후를 나눠 센다",
        (cov["수급TOP2"]["before"], cov["수급TOP2"]["after"]) == (2, 1))
    chk("이전 구간의 플래그 보유 행만 센다", cov["수급TOP2"]["before_flag"] == 1)
    chk("GATE_FAIL 도 플래그로 센다 — 통과 여부가 아니라 **기록 유무**가 질문이다",
        cov["수급TOP2"]["after_flag"] == 1)
    chk("플래그가 어느 열에 있든 찾는다 (행 전체 검색)",
        gate_flag_coverage([G, ["GATE_PASS", "2026-07-01", "차트TOP2", ""]]
                           )["차트TOP2"]["before_flag"] == 1)
    chk("플래그가 전무하면 0", gate_flag_coverage(gk(("t", "2026-07-01", "수급TOP2", "x"))
                                          )["수급TOP2"]["before_flag"] == 0)
    chk("날짜불명 행은 전/후를 못 가르므로 제외",
        gate_flag_coverage(gk(("t", "", "수급TOP2", "GATE_PASS"))) == {})

    print("🧪 결론 문장 — 세 경우를 구분해 말하는가")
    _mk = lambda b, bf: {"수급TOP2": {"before": b, "before_flag": bf,
                                    "after": 0, "after_flag": 0}}
    chk("이전 행에 플래그 0 이면 '막혔다'", "막혔다" in render_gate(_mk(5, 0)))
    chk("이전 행 전부에 플래그면 '뚫렸다'", "뚫렸다" in render_gate(_mk(5, 5)))
    chk("일부만이면 '부분적'", "부분적" in render_gate(_mk(5, 2)))
    chk("이전 행이 없으면 질문이 성립 안 함을 밝힌다",
        "성립하지 않는다" in render_gate(_mk(0, 0)))

    print("🧪 플래그 값 분해 — 선정 규칙 시험")
    B = ["trade_id", "진입일", "채널", "v2게이트"]
    bk = lambda *r: [B] + [list(x) for x in r]
    bd = gate_value_breakdown(bk(("t1", "2026-07-01", "수급TOP2", "GATE_PASS"),
                                 ("t2", "2026-07-02", "수급TOP2", "GATE_FAIL"),
                                 ("t3", "2026-09-01", "수급TOP2", "GATE_PASS"),
                                 ("t4", "2026-07-03", "차트TOP2", "")))
    chk("PASS·FAIL·없음을 따로 센다",
        bd["수급TOP2"]["before"] == {"GATE_PASS": 1, "GATE_FAIL": 1, "없음": 0})
    chk("기준선 이후도 따로", bd["수급TOP2"]["after"]["GATE_PASS"] == 1)
    chk("플래그 없는 행은 '없음'", bd["차트TOP2"]["before"]["없음"] == 1)

    print("🧪 결론 문장 — 수급TOP2 는 전부 PASS 여야 한다")
    _clean = bk(("t1", "2026-07-01", "수급TOP2", "GATE_PASS"),
                ("t2", "2026-07-02", "수급TOP2", "GATE_PASS"))
    chk("전부 PASS 면 '모순되지 않는다'",
        "모순되지 않는다" in render_breakdown(gate_value_breakdown(_clean)))
    _dirty = bk(("t1", "2026-07-01", "수급TOP2", "GATE_PASS"),
                ("t2", "2026-07-02", "수급TOP2", "GATE_FAIL"))
    chk("FAIL 이 섞이면 '달랐다' 고 단정",
        "달랐다" in render_breakdown(gate_value_breakdown(_dirty)))
    _none = bk(("t1", "2026-07-01", "수급TOP2", ""))
    chk("플래그 없는 행도 '달랐다' 로 잡는다 — 조용히 통과시키지 않는다",
        "달랐다" in render_breakdown(gate_value_breakdown(_none)))
    chk("이전 행이 없으면 시험할 것이 없다고 밝힌다",
        "시험할 것이 없다" in render_breakdown(gate_value_breakdown(
            bk(("t", "2026-09-01", "수급TOP2", "GATE_PASS")))))

    print("🧪 차트TOP2 는 반대 — 섞여야 정상이다")
    _mix = bk(("a", "2026-07-01", "차트TOP2", "GATE_PASS"),
              ("b", "2026-07-02", "차트TOP2", "GATE_FAIL"))
    chk("섞여 있으면 정상이라고 말한다",
        "정상이고" in render_breakdown(gate_value_breakdown(_mix)))
    _skew = bk(("a", "2026-07-01", "차트TOP2", "GATE_PASS"),
               ("b", "2026-07-02", "차트TOP2", "GATE_PASS"))
    chk("한쪽으로 쏠리면 신호일 수 있다고 말한다",
        "쏠렸다" in render_breakdown(gate_value_breakdown(_skew)))

    print("🧪 과대주장 방지 — 정의와 규칙을 구분해 적는가")
    _txt = render_breakdown(gate_value_breakdown(_clean))
    chk("게이트 '정의' 증명이 아님을 밝힌다", "증명은 아니다" in _txt)
    chk("원장에 재계산 입력이 없음을 밝힌다", "원장에 그 열이 없고" in _txt)

    print("🧪 열 구조 덤프")
    dump = header_dump(gk(("t", "2026-09-01", "수급TOP2", "GATE_PASS")))
    chk("열 수만큼 나온다", len(dump) == 4)
    chk("판정기가 아는 열은 매핑 표시", dump[1]["mapped"] == "진입일" and dump[1]["known"])
    chk("모르는 열은 미매핑 표시", not dump[3]["known"])
    ex = unmapped_examples(gk(("t1", "2026-07-01", "수급TOP2", "옛값"),
                              ("t2", "2026-09-01", "수급TOP2", "새값")))
    got = {e["i"]: e for e in ex}
    chk("매핑된 열은 예시에서 빠진다", 1 not in got and 2 not in got)
    chk("기준선 이전 예시를 따로 모은다", got[3]["before"] == ["옛값"])
    chk("기준선 이후 예시를 따로 모은다", got[3]["after"] == ["새값"])
    chk("빈 값은 예시로 안 넣는다",
        unmapped_examples(gk(("t", "2026-09-01", "수급TOP2", "")))[0]["after"] == [])
    chk("같은 값은 한 번만", len(unmapped_examples(
        gk(("a", "2026-09-01", "수급TOP2", "같음"),
           ("b", "2026-09-02", "수급TOP2", "같음")))[0]["after"]) == 1)

    print("🧪 덤프가 코호트 판정을 못 흔든다")
    _r = gk(("t1", "2026-07-01", "수급TOP2", "GATE_PASS"))
    chk("게이트 플래그가 있어도 판정은 여전히 진입일 기준이다",
        cohort_verdict(classify(_r)["수급TOP2"])[0] == "코호트분리필요")

    print("🧪 쓰기 호출이 없는가 (읽기 전용 보장)")
    for bad in (".update(", ".append_row(", ".batch_clear(", ".clear(",
                ".add_worksheet(", ".batch_update("):
        chk(f"{bad} 없음", bad not in body)

    print("🧪 열 규약이 판정기와 같은가")
    import hyeoks_verdict as v
    chk("SHEET_NAME 일치", SHEET_NAME == v.SHEET_NAME, SHEET_NAME)
    chk("진입일·채널 열 일치",
        (C_ENTRY_DATE, C_CHANNEL) == (v.C_ENTRY_DATE, v.C_CHANNEL))

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="시트 없이 판정 로직만 검증")
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(SHEET_URL)
    rows = doc.worksheet(SHEET_NAME).get_all_values()

    print(render(classify(rows), datetime.datetime.now(KST)))
    print(render_gate(gate_flag_coverage(rows)))
    print(render_breakdown(gate_value_breakdown(rows)))
    print(render_header(header_dump(rows), unmapped_examples(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
