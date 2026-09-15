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
    L.append("> 수익률은 읽지 않았다 — 성과를 보고 경계를 정하면 사후 맞춤이 된다.")
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
    return 0


if __name__ == "__main__":
    sys.exit(main())
