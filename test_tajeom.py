# -*- coding: utf-8 -*-
"""F01 회귀 테스트 — 실제 실행 경로(hyeoks_tajeom)를 그대로 시험한다.

2026-09-07 외부 감사가 찾아낸 결함 두 개를 고정한다.
  F01   중기 채널의 유일한 모수인 '📉 과매도 · 역배팅' 이 위험 분류에 걸려
        프롬프트 규칙 2(이 태그만 골라라)와 규칙 4(📉 는 거부하라)가 배타적이 됐다.
  F01-b '🎯 종베 · 관성파동' 처럼 태그가 맨 앞이면 split('🎯')[0] 이 빈 문자열이 됐다.

실행: python test_tajeom.py
"""
import sys
from hyeoks_tajeom import (clean_tajeom, trend_phase, is_downtrend_risk,
                           OVERSOLD_TAG, POLICY_ID)

FAIL = []


def chk(name, got, want):
    ok = got == want
    print(("  ✅ " if ok else "  ❌ ") + name + ("" if ok else f"\n        기대={want!r}\n        실제={got!r}"))
    if not ok:
        FAIL.append(name)


print(f"🧪 F01 — 과매도 태그가 더 이상 위험으로 분류되지 않는다  (정책 {POLICY_ID})")
chk("과매도 태그 단독 → 위험 아님", is_downtrend_risk(OVERSOLD_TAG), False)
chk("과매도 태그 → 추세 '중립'", trend_phase(OVERSOLD_TAG), "중립")
chk("과매도 태그 + 윗꼬리 주석 → 위험 아님",
    is_downtrend_risk(f"{OVERSOLD_TAG} ⚠️(윗꼬리/이격)"), False)

print("\n🧪 F01 — 진짜 위험 문구는 그대로 거부한다 (떨어지는 칼 회피 유지)")
for tag in ["⏸ 관망 · 과매도 반등 미확인 (칼날 회피)",
            "⏸ 관망 · 바닥권 반등 미확인 (칼날 회피)",
            "🌱 바닥 · 분할매수 · 하락 전환",
            "🚀 대장 · 당일단타 · 3파 익절",
            "🎯 종베 · 관성파동 · 고점 리스크",
            "⏸ 관망 · 하락장 돌파매매 금지 조항 적용"]:
    chk(f"거부 유지: {tag[:28]}", is_downtrend_risk(tag), True)

print("\n🧪 F01 — 과매도 태그라도 위험 문구를 함께 달면 거부한다")
chk("과매도 + 하락 전환 → 위험", is_downtrend_risk(f"{OVERSOLD_TAG} · 하락 전환"), True)
chk("과매도 + 3파 익절 → 위험", is_downtrend_risk(f"{OVERSOLD_TAG} · 3파 익절"), True)
chk("과매도 + 다른 📉 문구 → 위험", is_downtrend_risk(f"{OVERSOLD_TAG} / 📉 급락"), True)

print("\n🧪 F01-b — 종베 태그가 통째로 사라지지 않는다")
chk("맨 앞 🎯 는 보존", clean_tajeom("🎯 종베 · 관성파동"), "🎯 종베 · 관성파동")
chk("맨 앞 🎯 + ⚠️ 주석", clean_tajeom("🎯 종베 · 관성파동 ⚠️(윗꼬리/이격)"), "🎯 종베 · 관성파동")
chk("후행 🎯 는 잘라낸다", clean_tajeom("💎 외인 역발상 매집 🎯참고"), "💎 외인 역발상 매집")
chk("과매도 태그도 보존", clean_tajeom(OVERSOLD_TAG), OVERSOLD_TAG)
chk("빈 입력", clean_tajeom(""), "")
chk("None 입력", clean_tajeom(None), "")

print("\n🧪 회귀 — 나머지 위상 분류가 그대로다")
chk("가속", trend_phase("🚀 대장 · 당일단타 · 2파 가속"), "상승가속")
chk("전환", trend_phase("🔍 칼만 전환 · 관심"), "상승전환초기")
chk("유지", trend_phase("📦 박스 돌파 · 스윙 · 추세 유지"), "상승유지")
chk("중립", trend_phase("⏸ 관망 · 조건미달"), "중립")

print("\n🧪 모순 재현 — 수정 전 로직이었다면 실패했을 검사")
# 수정 전: "📉" in raw → True. 즉 중기 모수가 통째로 거부됐다.
before = "📉" in OVERSOLD_TAG
chk("과매도 태그는 실제로 📉 를 포함한다(모순의 원인)", before, True)
chk("그럼에도 지금은 위험이 아니다", is_downtrend_risk(OVERSOLD_TAG), False)

print("\n" + ("❌ 실패 " + str(len(FAIL)) + "건: " + ", ".join(FAIL) if FAIL else "✅ 전부 통과"))
sys.exit(1 if FAIL else 0)
