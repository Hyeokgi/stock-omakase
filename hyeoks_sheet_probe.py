# -*- coding: utf-8 -*-
# ==========================================================================
# 🔍 HYEOKS 시트 반영 확인 — 읽기 전용. 아무것도 쓰지 않는다
# --------------------------------------------------------------------------
# 왜 만드나
#   ChatGPT 가 반복해서 옳게 지적한 것이 있다 —
#     "테스트 성공과 시트 배포 성공은 구분한다."
#     "코드 push, CI 성공, 실제 장중 수집 성공, 동결 파일 push 성공은 서로 다른 증거다."
#   그런데 지금까지 그 구분을 확인할 **수단이 없었다.** 시트에 실제로 무엇이 적혔는지
#   보려면 자격증명이 필요하고, 그건 액션에만 있다. 그래서 로그만 보고
#   "반영됐을 것이다" 라고 추정해 왔다. 추정은 증거가 아니다.
#
# 무엇을 보나 — 2026-09-14 시간외 표시 교정(119f9aa)이 실제로 시트에 닿았는가
#   · `DB_스캐너`      Q1:R1 제목 + Q/R 값 몇 줄
#   · `주가데이터_보조` AA1:AC1 제목 + AA/AB/AC 값 몇 줄
#
# ⚠️ 장중에는 값이 비어 있는 것이 **정상**이다
#   `omakase.py:2685` 가 정규장(09:00~15:40)에는 r[26]·r[27] 을 비우고
#   r[28] 에 '정규장 진행중' 을 넣는다. 그러니 15:30 에 값이 비었다고 실패가 아니다.
#   **제목은 매 회차 갱신**되므로 장중에도 확인된다. 값은 15:41 이후에 본다.
#
# 무엇을 하지 않나
#   · 쓰지 않는다. update·clear·format 어떤 호출도 없다.
#   · 판정하지 않는다. 무엇이 적혀 있는지 보여줄 뿐이다.
#   · 값의 정확성을 인증하지 않는다. "그 칸에 무엇이 있나" 까지다.
# ==========================================================================
import argparse
import datetime
import sys

from after_market_quotes import AFTER_HEADER, NXT_HEADER

KST = datetime.timezone(datetime.timedelta(hours=9))
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"

# (시트, 제목범위, 값범위, 기대 제목)  — 기대 제목이 None 이면 대조하지 않고 보여만 준다
TARGETS = [
    ("DB_스캐너", "Q1:R1", "Q2:R6", [AFTER_HEADER, NXT_HEADER]),
    ("주가데이터_보조", "AA1:AC1", "AA2:AC6", None),
]


def is_regular_market(now):
    """omakase.py:1637 과 같은 규약. 여기서 새로 정하지 않는다."""
    return (9 <= now.hour < 15) or (now.hour == 15 and now.minute <= 40)


def header_verdict(got, expected):
    """(상태, 설명). 제목 대조는 순수 함수라 시트 없이 검증된다."""
    if expected is None:
        return "참고", "기대 제목을 정하지 않은 범위"
    got = [str(x).strip() for x in (got or [])]
    exp = [str(x).strip() for x in expected]
    if got == exp:
        return "일치", "새 제목이 시트에 반영됐다"
    if not any(got):
        return "빈칸", "제목이 비어 있다 — 아직 한 번도 안 쓰였거나 범위가 다르다"
    return "불일치", f"시트={got} / 기대={exp}"


def value_note(rows, now):
    """값이 비어 있는 것이 정상인 시각인지 함께 알려준다."""
    flat = [str(c).strip() for r in (rows or []) for c in r]
    filled = sum(1 for c in flat if c)
    if filled:
        return f"값 {filled}칸 채워짐"
    if is_regular_market(now):
        return ("값 전부 비어 있음 — **정규장이라 정상**이다"
                " (omakase.py:2685 가 장중에는 시간외 칸을 비운다). 15:41 이후에 다시 볼 것")
    return "값 전부 비어 있음 — 장 마감 후인데 비었다. 확인 필요"


# ── 리포트 분석 백로그 (2026-09-15 추가) ────────────────────────────────
# 왜 여기 붙이나 — `docs/리포트수집_설계_2026-09-15.md` §2 가 딱 한 숫자에 걸려 있다.
#   "Drive 의 PDF 중 몇 건이 이미 DB_중장기 에 적재됐는가."
#   ≈0 이면 LIFO 는 무해하고 가동 감시만 있으면 된다.
#   300+ 면 4~8월 산업리포트가 **영구 미분석**이고 우선순위 정책이 필요하다.
# 새 도구를 만들지 않고 이미 있는 읽기 전용 프로브에 붙인다.
TREND_SHEET = "DB_중장기"
TREND_FILE_COL = 6          # G열 — hyeoks_trend.py 가 리포트 파일명을 적는 자리
NEW_NAMING = "_hana_industry_"   # 2026-09-13 이후 수집기 규약
OLD_NAMING = "_industry_"        # 그 이전 수동/구 수집분


def backlog_stats(rows):
    """`DB_중장기` 전체 값 → 분석된 리포트 집계. 시트 없이 검증되는 순수 함수다.

    파일명 규약으로 신/구를 가른다. 구 규약(4~8월분)이 0 에 가까우면
    **LIFO 가 실제로 옛 자료를 굶기고 있다**는 직접 증거가 된다.
    """
    if not rows:
        return {"헤더없음": True}
    body = rows[1:]
    names = []
    for r in body:
        v = str(r[TREND_FILE_COL]).strip() if len(r) > TREND_FILE_COL else ""
        if v:
            names.append(v)
    uniq = set(names)
    new = {n for n in uniq if NEW_NAMING in n}
    old = {n for n in uniq if NEW_NAMING not in n and OLD_NAMING in n}
    return {"데이터행": len(body), "파일명있는행": len(names),
            "고유파일명": len(uniq), "중복적재": len(names) - len(uniq),
            "신규규약": len(new), "구규약": len(old),
            "분류불가": len(uniq) - len(new) - len(old), "헤더없음": False}


def render_backlog(stat, drive_total=None):
    L = ["", f"## 📚 리포트 분석 백로그 — `{TREND_SHEET}`", ""]
    if stat.get("오류"):
        return "\n".join(L + [f"- ❌ 읽기 실패 — {stat['오류']}"])
    if stat.get("헤더없음"):
        return "\n".join(L + ["- ⚠️ 시트가 비어 있다"])
    L += [f"- 데이터 행 **{stat['데이터행']}** · 파일명 있는 행 {stat['파일명있는행']}",
          f"- **고유 리포트 {stat['고유파일명']}건** (중복 적재 {stat['중복적재']}건)",
          f"  - 신규 규약(`{NEW_NAMING}`, 2026-09-13~) **{stat['신규규약']}건**",
          f"  - 구 규약(`{OLD_NAMING}`, ~2026-08) **{stat['구규약']}건**",
          f"  - 분류 불가 {stat['분류불가']}건"]
    if drive_total:
        left = drive_total - stat["고유파일명"]
        L.append(f"- Drive PDF {drive_total}건 대비 **미분석 {left}건**")
    L += ["",
          "> 이 수치는 **시트에 적힌 파일명 기준**이다. 실제 PDF 와 1:1 대응하는지까지"
          " 인증한 것은 아니다(같은 이름의 다른 내용, 이름 변경 등).",
          "> 구 규약 건수가 0 에 가까우면 `hyeoks_trend.py:61` 의 최신 우선 정렬이"
          " 옛 자료를 굶기고 있다는 직접 증거다."]
    return "\n".join(L)


def render(results, now):
    L = [f"# 🔍 시트 반영 확인 — {now.strftime('%Y-%m-%d %H:%M')} KST",
         "",
         f"정규장 여부: **{'정규장 진행중' if is_regular_market(now) else '장 마감 후'}**",
         "",
         "> 읽기만 했다. 이 스크립트는 시트에 쓰지 않는다.",
         "> 값의 정확성을 인증하지 않는다 — 그 칸에 무엇이 있는지까지다.",
         ""]
    for r in results:
        L.append(f"## {r['sheet']}")
        L.append("")
        if r.get("error"):
            L.append(f"- ❌ 읽기 실패 — {r['error']}")
            L.append("")
            continue
        status, why = r["header_verdict"]
        mark = {"일치": "✅", "불일치": "❌", "빈칸": "⚠️", "참고": "·"}[status]
        L.append(f"- 제목 `{r['header_range']}` — {mark} **{status}** · {why}")
        L.append(f"  - 실제: `{r['header']}`")
        L.append(f"- 값 `{r['value_range']}` — {value_note(r['values'], now)}")
        for row in (r["values"] or [])[:5]:
            L.append(f"  - `{row}`")
        L.append("")
    return "\n".join(L)


def self_test():
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    print("🧪 정규장 경계 (omakase 와 같은 규약)")
    at = lambda h, m: datetime.datetime(2026, 9, 15, h, m, tzinfo=KST)
    chk("08:59 는 장 전", not is_regular_market(at(8, 59)))
    chk("09:00 는 장중", is_regular_market(at(9, 0)))
    chk("15:40 은 아직 장중", is_regular_market(at(15, 40)))
    chk("15:41 부터 장 마감 후", not is_regular_market(at(15, 41)))

    print("🧪 제목 대조")
    chk("정확히 같으면 일치", header_verdict([AFTER_HEADER, NXT_HEADER],
                                        [AFTER_HEADER, NXT_HEADER])[0] == "일치")
    chk("앞뒤 공백은 무시", header_verdict([f" {AFTER_HEADER} ", NXT_HEADER],
                                      [AFTER_HEADER, NXT_HEADER])[0] == "일치")
    chk("옛 제목이면 불일치", header_verdict(["시간외 종가", "NXT 종가"],
                                       [AFTER_HEADER, NXT_HEADER])[0] == "불일치")
    chk("비어 있으면 빈칸", header_verdict(["", ""], [AFTER_HEADER, NXT_HEADER])[0] == "빈칸")
    chk("범위가 짧아도 불일치로 잡는다",
        header_verdict([AFTER_HEADER], [AFTER_HEADER, NXT_HEADER])[0] == "불일치")
    chk("기대 없으면 참고", header_verdict(["뭐든"], None)[0] == "참고")

    print("🧪 값 해석 — 장중 빈칸을 실패로 읽지 않는다")
    chk("장중 빈칸은 정상이라고 밝힌다", "정상" in value_note([["", ""]], at(14, 0)))
    chk("마감 후 빈칸은 확인 필요", "확인 필요" in value_note([["", ""]], at(18, 0)))
    chk("값이 있으면 개수를 센다", "2칸" in value_note([["a", "b"]], at(18, 0)))

    print("🧪 백로그 집계 — 설계서 §2 의 한 숫자")
    H = ["분석일자", "섹터/테마명", "핵심 상승 논리", "Top Pick 1", "Top Pick 2",
         "추세추종 진입 전략", "리포트 출처(파일명)"]
    mk = lambda *names: [H] + [["", "", "", "", "", "", n] for n in names]
    st = backlog_stats(mk("20260913_hana_industry_2220_1_a.pdf",
                          "20260901_hana_industry_2220_2_b.pdf",
                          "20260810_industry_671677000.pdf"))
    chk("데이터 행을 센다", st["데이터행"] == 3)
    chk("신규 규약을 가른다", st["신규규약"] == 2, str(st["신규규약"]))
    chk("구 규약을 가른다", st["구규약"] == 1, str(st["구규약"]))
    chk("신규 규약이 구 규약에 이중 계수되지 않는다",
        st["신규규약"] + st["구규약"] + st["분류불가"] == st["고유파일명"])

    dup = backlog_stats(mk("같은.pdf", "같은.pdf", "다른.pdf"))
    chk("중복 적재를 센다", dup["중복적재"] == 1 and dup["고유파일명"] == 2)
    chk("규약에 안 맞으면 분류불가", dup["분류불가"] == 2, str(dup["분류불가"]))

    chk("빈 시트는 헤더없음", backlog_stats([])["헤더없음"])
    chk("헤더만 있으면 0행", backlog_stats([H])["데이터행"] == 0)
    chk("G열이 없는 행은 세지 않는다",
        backlog_stats([H, ["a", "b"]])["파일명있는행"] == 0)
    chk("G열이 공백이면 세지 않는다",
        backlog_stats([H, ["", "", "", "", "", "", "   "]])["파일명있는행"] == 0)

    print("🧪 백로그 출력 — 과대주장 방지")
    txt = render_backlog(st, drive_total=390)
    chk("Drive 대비 미분석 건수를 낸다", "미분석 387건" in txt, txt[-200:][:60])
    chk("1:1 대응을 인증한 것이 아니라고 밝힌다", "인증한 것은 아니다" in txt)
    chk("구 규약 0 의 의미를 적어 둔다", "굶기고 있다는 직접 증거" in txt)
    chk("읽기 실패를 조용히 넘기지 않는다", "읽기 실패" in render_backlog({"오류": "x"}))

    print("🧪 쓰기 호출이 없는가 (읽기 전용 보장)")
    src = open(__file__, encoding="utf-8").read()
    body = src.split("def self_test")[0]
    for bad in (".update(", ".append_row(", ".batch_clear(", ".clear(", ".add_worksheet("):
        chk(f"{bad} 없음", bad not in body)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="시트 없이 해석 로직만 검증")
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(SHEET_URL)

    now = datetime.datetime.now(KST)
    results = []
    for sheet, hrange, vrange, expected in TARGETS:
        item = {"sheet": sheet, "header_range": hrange, "value_range": vrange}
        try:
            ws = doc.worksheet(sheet)
            head = (ws.get(hrange) or [[]])[0]
            item["header"] = head
            item["header_verdict"] = header_verdict(head, expected)
            item["values"] = ws.get(vrange)
        except Exception as e:                     # noqa: BLE001
            item["error"] = f"{type(e).__name__}: {e}"
        results.append(item)

    print(render(results, now))

    # 백로그 — 실패해도 위 결과를 막지 않는다. 다만 조용히 넘기지도 않는다.
    try:
        stat = backlog_stats(doc.worksheet(TREND_SHEET).get_all_values())
    except Exception as e:                          # noqa: BLE001
        stat = {"오류": f"{type(e).__name__}: {e}"}
    print(render_backlog(stat))
    return 0


if __name__ == "__main__":
    sys.exit(main())
