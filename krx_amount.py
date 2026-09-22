# -*- coding: utf-8 -*-
"""거래대금 칸 하나를 읽는 **단 하나의 규칙**.

왜 있나
-------
시트를 `get_all_values()` 로 읽으면 저장된 숫자가 아니라 **표시 형식이 적용된
문자열**이 온다. `거래대금(억원)` 칸은 `omakase.py:666` 에서 `int()` 로 쓰이지만
읽을 때는 `1,938억원` 으로 돌아온다. 쉼표만 지우고 `int()` 하면 전부 실패한다.

2026-09-22 analyst 최종 영수증의 `theme_money` ValueError **4,132건**이 이것이다.
빈 칸 문제가 아니라 **정상적인 표시 형식을 읽지 못한 것**이다.

같은 실수가 이미 같은 파일 안에서 두 번 따로 났다.
  - `hyeoks_analyst.py:759`  수급_실시간 — `re.sub(r'[^0-9]', '', ...)` 로 때웠다.
  - `hyeoks_analyst.py:788`  수급_Raw    — 쉼표만 지워 전부 실패했다.

`[^0-9]` 를 전부 지우는 것은 **단위를 해석한 것이 아니다.** `1.2조원` 을 12 로
만들고 음수 부호도 지운다. 그래서 규칙을 한 곳에 두고 단위를 명시적으로 읽는다.
`krx_code.py` 와 같은 이유, 같은 방식이다.

무엇을 구분하나
---------------
셋을 구분한다. **모르는 것을 0 으로 만들지 않는다.**

  ``ok``       숫자로 읽었다
  ``missing``  칸이 비었다 — 오류가 아니라 결측이다
  ``bad``      값이 있는데 이 규칙으로 읽을 수 없다 — 오류다

단위는 **열이 선언한 단위(억원)만** 받는다. `원`·`조`·`조원` 은 받지 않는다.
받아 주면 1억 배 틀린 값을 조용히 더하게 된다. 모르는 단위는 `bad` 다.
단위 환산은 하지 않는다 — 숫자 부분을 열의 단위 그대로 돌려준다.
"""
import re

RULE_VERSION = "krx-amount-v1"

# 열이 선언한 단위. 이것 말고는 받지 않는다.
ALLOWED_SUFFIXES = ("억원", "억")

# 부호 · 쉼표 낀 정수부 · 선택적 소수부. 그 뒤에 허용 단위만 올 수 있다.
_NUMBER = re.compile(r'^(?P<sign>-?)(?P<int>\d{1,3}(?:,\d{3})*|\d+)(?:\.(?P<frac>\d+))?$')

OK, MISSING, BAD = "ok", "missing", "bad"


def parse(raw):
    """(값, 상태) 를 돌려준다. 값은 상태가 ``ok`` 일 때만 의미가 있다.

    값은 **열의 단위 그대로**다(거래대금(억원) 이면 억원 단위의 정수).
    소수는 버림한다 — 쓰는 쪽이 `int()` 로 쓰므로 소수는 표시 반올림의 흔적이다.
    """
    text = str(raw or "").strip()
    if not text:
        return None, MISSING

    body = text
    for suffix in ALLOWED_SUFFIXES:          # 긴 것부터 — "억원" 이 "억" 보다 먼저다
        if body.endswith(suffix):
            body = body[: -len(suffix)].strip()
            break

    m = _NUMBER.fullmatch(body)
    if not m:
        return None, BAD

    value = int(m.group("int").replace(",", ""))
    return (-value if m.group("sign") else value), OK


def _selftest():
    ok = 0

    def chk(label, cond):
        nonlocal ok
        assert cond, f"❌ {label}"
        ok += 1
        print(f"  ✅ {label}")

    print(f"[{RULE_VERSION}] 자체 검증")

    # ── 실제로 터진 그 형식
    chk("'1,938억원' 을 읽는다 (9/22 theme_money 4,132건의 정체)",
        parse("1,938억원") == (1938, OK))
    chk("쉼표만 지우는 옛 방식은 실패했다",
        not str("1,938억원").replace(",", "").strip().isdigit())

    # ── 정상 표기의 변형
    chk("단위 없는 숫자", parse("1938") == (1938, OK))
    chk("쉼표만", parse("1,938") == (1938, OK))
    chk("'억' 만 붙은 경우", parse("1,938억") == (1938, OK))
    chk("앞뒤 공백", parse("  1,938억원  ") == (1938, OK))
    chk("0 은 값이다", parse("0억원") == (0, OK))
    chk("음수 부호를 지우지 않는다([^0-9] 방식의 결함)",
        parse("-1,938억원") == (-1938, OK))
    chk("소수는 버림", parse("1,938.7억원") == (1938, OK))

    # ── 결측: 오류가 아니다
    chk("빈 칸은 결측", parse("") == (None, MISSING))
    chk("공백만도 결측", parse("   ") == (None, MISSING))
    chk("None 도 결측", parse(None) == (None, MISSING))

    # ── 오류: 0 으로 바꾸지 않는다
    chk("모르는 단위는 받지 않는다 — 1억 배 틀린 값을 더하게 된다",
        parse("1.2조원")[1] == BAD and parse("193800000000원")[1] == BAD)
    chk("[^0-9] 방식이었다면 '1.2조원' 은 12 가 됐다",
        re.sub(r'[^0-9]', '', "1.2조원") == "12")
    chk("문자열은 오류", parse("집계없음")[1] == BAD)
    chk("하이픈은 오류(0 이 아니다)", parse("-")[1] == BAD and parse("—")[1] == BAD)
    chk("숫자 사이 공백은 오류", parse("1 938억원")[1] == BAD)
    chk("단위가 앞에 오면 오류", parse("억원1938")[1] == BAD)
    chk("쉼표 자리가 틀리면 오류", parse("1,93,8억원")[1] == BAD)

    # ── 어떤 입력에도 예외를 내지 않는다(계측이 죽으면 안 된다)
    for weird in (object(), [], {}, 3.14, True):
        parse(weird)
    chk("이상한 입력에도 예외를 내지 않는다", True)

    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
