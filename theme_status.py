# -*- coding: utf-8 -*-
# 스캐너의 테마 수집 결과를 사람이 보는 표시 전용 탭 '테마_현황'에 쓰는 모듈 — 어떤 코드도 이 탭을 읽지 않는다
"""테마 현황 표시 (2026-10-13 사용자 요청).

왜 따로 두나
  `수급_실시간` 은 화면이 아니라 **입력**이다. 스캐너(`update_technical_data`)가 다시 읽어 테마 순위·대장 여부를
  정하고, 리포트 AI(`hyeoks_analyst.py`)가 테마별 거래대금을 시황 근거로 쓴다. 그 탭의 내용을 바꾸면 선정 정책이
  바뀐다(블록 A 정책 불변 기간 ~11/06). 그래서 사람이 보는 정보는 이 표시 전용 탭에만 쓴다.

보여 주는 것
  1. 갱신 시각과 상태 — 통과 테마가 없으면 '지금 통과 테마 없음' 을 분명히 쓴다. `수급_실시간` 은 그때 갱신을
     건너뛰어 앞선 결과가 남는다는 점, 그 결과의 마지막 갱신 시각을 함께 적는다.
  2. 통과 테마 — `수급_실시간` 에 쓴 것과 같은 행.
  3. 1등 독식으로 빠진 묶음 — 1등 거래대금이 2등의 5배 이상이라 '개별주 급등' 으로 보고 테마에서 뺀 것.
     대장·2등과 배수를 보여 준다. **선정에는 쓰지 않는다**(규칙은 그대로다).

지키는 것
  - 시트 쓰기가 실패해도 예외를 밖으로 내지 않는다(수집기를 막지 않는다).
  - 로그에 종목명을 찍지 않는다(공개 실행 로그, 허브 #18).
"""

TAB = "테마_현황"
HEADER = ["구분", "순위", "테마명", "종목명", "종목코드", "등락률(%)", "거래대금(억원)", "비고"]
TITLE = "테마 현황 — 표시 전용 (어떤 코드도 이 탭을 읽지 않는다. 선정·리포트 입력은 수급_실시간)"


def build(status, theme_rows, now_str):
    """(머리 4줄, 본문 행들). `theme_rows` 는 수급_실시간에 쓴 행(dict) 목록, `status` 는 스캐너가 모은 정보."""
    passed = len({r.get("순위") for r in theme_rows})
    note = status.get("note", "")
    written = status.get("input_written", "")
    if passed and written:
        state = f"통과 테마 {passed}개 — 수급_실시간 갱신함({written})"
    elif passed:
        # 🔴 Codex 10/13 — 원본 탭 쓰기가 실패했는데 '갱신함' 이라고 쓰면 안 된다.
        state = f"통과 테마 {passed}개 — 그러나 수급_실시간 쓰기 실패 또는 미확인(로그 확인). 그 탭은 앞선 결과일 수 있다"
    else:
        state = ("지금 통과 테마 없음 — 수급_실시간은 갱신을 건너뛰어 앞선 결과가 남아 있다"
                 + (f" ({note})" if note else ""))
    head = [[TITLE, ""],
            ["갱신", now_str],
            ["상태", state],
            ["1등 독식으로 빠진 묶음", f"{len(status.get('dominated', []))}개 (1등 거래대금 ≥ 2등 × 5 → 개별주로 보고 테마에서 뺌)"]]
    body = [HEADER]
    for r in theme_rows:
        body.append(["통과", r.get("순위", ""), r.get("테마명", ""), r.get("종목명", ""), r.get("종목코드", ""),
                     r.get("등락률(%)", ""), r.get("거래대금(억원)", ""), ""])
    for d in status.get("dominated", []):
        ratio = d["top"]["value"] / d["second"]["value"] if d["second"]["value"] else 0
        for label, s in (("1등 독식 — 대장", d["top"]), ("1등 독식 — 2등", d["second"])):
            body.append([label, "", d["theme"], s["name"], s["code"], s["rate"], int(s["value"] / 100),
                         f"1등/2등 거래대금 {ratio:.1f}배" if label.endswith("대장") else ""])
    return head, body


def update(doc, theme_rows, status, now_str):
    """표시 탭을 다시 쓴다. 성공하면 True. 실패해도 예외를 내지 않는다.

    `수급_실시간` 쓰기가 실제로 성공했을 때만 '수급_실시간 마지막 갱신' 칸(B5)을 **그 쓰기 시각**
    (`status["input_written"]`, 날짜 포함)으로 바꾼다 — 표시를 쓰는 지금 시각이 아니다(Codex 10/13).
    아니면 그대로 두어 앞선 결과가 언제 것인지 보이게 한다.
    """
    try:
        try:
            ws = doc.worksheet(TAB)
        except Exception:
            ws = doc.add_worksheet(title=TAB, rows=200, cols=len(HEADER))
        head, body = build(status, theme_rows, now_str)
        ws.update(range_name="A1", values=head, value_input_option="USER_ENTERED")
        if theme_rows and status.get("input_written"):
            ws.update(range_name="A5", values=[["수급_실시간 마지막 갱신", status["input_written"]]],
                      value_input_option="USER_ENTERED")
        ws.batch_clear(["A7:Z"])
        ws.update(range_name="A7", values=body, value_input_option="USER_ENTERED")
        print(f"🖥️ [{TAB}] 표시 갱신 — 통과 {len({r.get('순위') for r in theme_rows})} · "
              f"1등 독식 {len(status.get('dominated', []))}")
        return True
    except Exception as e:
        print(f"⚠️ [{TAB}] 표시 갱신 실패(선정·리포트와 무관) — {type(e).__name__}")
        return False
