# -*- coding: utf-8 -*-
"""KRX 단축코드 규칙 — **저장소에 하나만 둔다.**

왜 이 파일이 생겼나 (2026-09-20)
--------------------------------
같은 결함을 **세 번** 고쳤다. 매번 다른 파일에서.

    2026-08-28  hyeoks_market_snapshot._is_code   `c.isdigit()` → 6자리 영숫자
                (스냅샷 2,877 중 84개 = 2.9% 가 테마 매핑에서 조용히 빠졌다)
    2026-09-15  hyeoks_account / account_source_adapter
                둘 다 `isdigit()` 이라 `0155E0` 을 거부 → 계좌 재구성 첫 실행이 죽었다
    2026-09-20  naver_sources.consensus_estimates  `\\d{6}` 이라 `0015N0` 을 거부
                ← 생산 로그가 찾았다: `컨센서스 원천 실패 0015N0: Invalid stock code`

세 번째가 나온 이유는 분명하다. **규칙이 복사돼 있었다.** 고친 곳은 고쳐졌고
안 고친 곳은 남았다. `tests/test_krx_code.py` 가 계좌 두 모듈만 묶어 뒀으므로
`naver_sources` 는 아무도 보지 않았다. 그래서 정본을 여기 하나 둔다.

규칙
----
`[0-9A-Z]{6}` — 6자리, 숫자와 **대문자 ASCII 영문**만.

⚠️ `str.isalnum()` 을 쓰지 않는다. 그건 한글(`가나다라마바`)과 아라비아-인도
   숫자(`٠١٢٣٤٥`)까지 참이다. 종목코드가 아닌 것을 종목코드로 받으면
   조용히 엉뚱한 행이 섞인다. 넓히는 것과 아무거나 받는 것은 다르다.
   (`hyeoks_market_snapshot._is_code` 는 아직 `isalnum()` 이다 — 아래 주의 참조.)

`000000` 은 형식은 맞지만 실재하지 않는다. 빈 칸·자리표시자로 자주 들어온다.
"""
import re

RULE_VERSION = "krx-code-v1"

#: 정본. 6자리 · 숫자와 대문자 ASCII 영문.
PATTERN = re.compile(r'[0-9A-Z]{6}')

#: 형식은 맞지만 실재하지 않는 코드(자리표시자로 흔히 들어온다).
NOT_A_CODE = frozenset({'000000'})


def normalize(raw):
    """표기를 하나로 맞춘다. 종목코드가 아니면 **빈 문자열**.

    계좌 쪽 `normalize_code` 와 **같은 규칙이어야 한다.**
    어긋나면 `tests/test_krx_code.py` 가 깨진다.
    """
    c = str(raw or "").replace("'", "").strip().upper()
    if len(c) < 6 and c.isdigit():
        c = c.zfill(6)               # 앞자리 0 이 잘려 들어온 숫자코드
    if not PATTERN.fullmatch(c) or c in NOT_A_CODE:
        return ""
    return c


def is_code(raw):
    """종목코드로 볼 수 있는가. 정규화한 뒤 판정한다."""
    return bool(normalize(raw))


def _selftest():
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    print(f"🧪 KRX 단축코드 규칙 ({RULE_VERSION})")
    chk("평범한 숫자코드", normalize("005930") == "005930")
    # 🔴 세 번의 사고가 전부 이 줄이다
    chk("영문 섞인 실제 코드를 받는다", all(
        is_code(c) for c in ("0155E0", "0220W0", "00680K", "0015N0")))
    chk("앞자리 0 이 잘린 숫자코드를 복원", normalize("5930") == "005930")
    chk("따옴표·공백 표기를 벗긴다", normalize("'005930 ") == "005930")
    chk("소문자는 대문자로", normalize("0155e0") == "0155E0")

    # ⚠️ 넓히는 것과 아무거나 받는 것은 다르다
    chk("한글은 코드가 아니다(isalnum() 이 참이라 특히 위험)", not is_code("가나다라마바"))
    chk("아라비아-인도 숫자도 아니다", not is_code("٠١٢٣٤٥"))
    # ⚠️ 짧은 **순수 숫자**는 거부가 아니라 zfill 이다(계좌 모듈의 기존 동작).
    #    따라서 "길이 오류" 는 ① 너무 길다 ② 짧지만 숫자가 아니다 두 경우다.
    chk("너무 길면 거부", not any(
        is_code(c) for c in ("0059300", "005930A", "0155E00")))
    chk("짧고 숫자가 아니면 거부(zfill 대상이 아니다)", not any(
        is_code(c) for c in ("155E0", "E0", "A1B2C")))
    chk("짧은 순수 숫자는 zfill 된다(거부가 아니다)",
        normalize("00593") == "000593" and normalize("593") == "000593")
    chk("특수문자 거부", not any(
        is_code(c) for c in ("00-930", "005_30", "00593.", "0059 0", "00593$")))
    chk("빈 값·None 거부", not any(is_code(v) for v in ("", None, "   ")))
    chk("000000 은 형식은 맞지만 코드가 아니다", not is_code("000000"))

    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
