# -*- coding: utf-8 -*-
"""
텔레그램 목적지 한 곳 — 8개 모듈이 같은 상수를 본다.

🔴 2026-09-18 되돌림 기록 (이걸 안 남기면 또 같은 실수를 한다)
   어제 나는 채널 ID 하드코딩 5곳을 `os.environ.get("TELEGRAM_CHAT_ID")` 로
   바꿨다. **방향이 반대였다.**

     - 하드코딩 "-1003778485916" = 리포트·브리핑이 **지금 정상 도착하는** 채널
     - secret  TELEGRAM_CHAT_ID  = 오마카세레이더, **더는 안 쓰는 옛 채널**

   그 변경을 그대로 두면 다음 모닝 브리핑(07:45 KST)부터 리포트·브리핑이
   **옛 채널로 갔다.** 사용자가 알아채고 막았다.

   그래서 목적지는 다시 상수로 고정한다. 단 사본 5개가 아니라 여기 한 곳이다.
   secret 은 읽지 않는다 — 그 secret 은 지금 **옛 채널 값**을 들고 있고,
   코드가 그걸 읽는 순간 같은 사고가 재발한다.

   채널을 진짜로 옮길 때는 아래 CHAT_ID 를 바꾸는 것이 유일한 절차다.
   (임시 시험 발송만 TELEGRAM_CHAT_ID_OVERRIDE 로 우회한다. 상시 배선 금지.)
"""
import os

# 리포트·브리핑·스캐너 알림이 실제로 도착하고 있는 채널.
CHAT_ID = "-1003778485916"

OVERRIDE_ENV = "TELEGRAM_CHAT_ID_OVERRIDE"


def chat_id():
    """상시 목적지는 CHAT_ID. 명시적 override 가 있을 때만 그쪽으로 보낸다."""
    return (os.environ.get(OVERRIDE_ENV) or "").strip() or CHAT_ID


def _selftest():
    ok = 0

    def eq(a, b, name):
        nonlocal ok
        assert a == b, f"{name}: {a!r} != {b!r}"
        ok += 1

    saved = {k: os.environ.get(k) for k in (OVERRIDE_ENV, "TELEGRAM_CHAT_ID")}
    try:
        os.environ.pop(OVERRIDE_ENV, None)
        os.environ.pop("TELEGRAM_CHAT_ID", None)
        eq(chat_id(), CHAT_ID, "기본값은 상수")

        # 핵심 회귀: 옛 채널 값이 든 secret 이 배선돼 있어도 **끌려가지 않는다**
        os.environ["TELEGRAM_CHAT_ID"] = "-1001111111111"
        eq(chat_id(), CHAT_ID, "TELEGRAM_CHAT_ID 는 목적지를 바꾸지 못한다")

        os.environ[OVERRIDE_ENV] = "-1002222222222"
        eq(chat_id(), "-1002222222222", "명시적 override 만 우회 가능")

        os.environ[OVERRIDE_ENV] = "   "
        eq(chat_id(), CHAT_ID, "공백 override 는 무시")

        eq(CHAT_ID.startswith("-100"), True, "채널 ID 형식")
        eq(CHAT_ID, "-1003778485916", "채널 값 고정")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return ok


if __name__ == "__main__":
    print(f"✅ telegram_target 자체검증 {_selftest()}건 통과 → {CHAT_ID}")
