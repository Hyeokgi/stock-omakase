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
    return 0


if __name__ == "__main__":
    sys.exit(main())
