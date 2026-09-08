# -*- coding: utf-8 -*-
# ==========================================================================
# 🏷️ HYEOKS 타점 문자열 해석 (순수 함수 · 의존성 없음)
# --------------------------------------------------------------------------
# 왜 별도 모듈인가
#   이 두 함수는 원래 hyeoks_analyst.py 안에 인라인으로 있었다. 그런데 그 파일은
#   gspread·pdfkit·genai 등을 모듈 최상단에서 import 하므로, 로직만 시험하려 해도
#   무거운 패키지가 전부 필요하다. 실제로 2026-09-07 외부 감사자도, 그리고 우리도
#   "원본에서 함수만 떼어내 합성 테스트" 하는 우회를 해야 했다.
#   **떼어낸 사본을 시험하면 원본이 맞다는 보장이 없다.** 그래서 원본을 여기로 옮기고
#   analyst 가 이걸 import 하게 한다. 이제 테스트가 진짜 실행 경로를 시험한다.
#
# ⚠️ 여기에는 절대 무거운 import 를 추가하지 말 것. 순수 문자열 함수만 둔다.
# ==========================================================================

# 스캐너(omakase)가 붙이는 과매도 반등 태그. 중기 채널의 **유일한 모수**다.
OVERSOLD_TAG = "📉 과매도 · 역배팅"

# 실제 위험 문구. 이건 태그 종류와 무관하게 거부 사유다.
#   · '반등 미확인' 이 들어 있으므로 '⏸ 관망 · 과매도 반등 미확인' 은 계속 거부된다
#     (떨어지는 칼 회피 장치 유지).
RISK_WORDS = ("하락 전환", "3파 익절", "고점 리스크", "반등 미확인", "하락장")

# 이 파일이 구현하는 선정 정책의 식별자. 표본을 정책별로 가를 때 쓴다.
POLICY_ID = "oversold-veto-v2"
POLICY_SINCE = "2026-09-07"


def clean_tajeom(tajeom_raw):
    """AI 입력용으로 타점 문자열에서 후행 주석을 떼어낸다.

    🚨 [F01-b] 원래는 `split('⚠️')[0].split('🎯')[0]` 이었다. 그런데
       '🎯 종베 · 관성파동' 처럼 **태그가 맨 앞에서 시작하면** `[0]` 이 빈 문자열이 되어
       타점 설명이 통째로 사라졌다. AI 는 타점을 못 본 채 판단하게 된다.
       후행 주석을 자르려던 의도이므로 **0번 위치가 아닐 때만** 자른다.
    """
    s = (tajeom_raw or "").split("⚠️")[0].strip()
    i = s.find("🎯")
    if i > 0:
        s = s[:i].strip()
    return s


def is_downtrend_risk(tajeom_raw):
    """이 타점을 '하락/고점주의'로 볼 것인가.

    🚨 [F01 · 2026-09-07 외부 감사] 원래 조건은 `"📉" in tajeom_raw` 였다.
       그런데 중기 채널의 유일한 모수인 `📉 과매도 · 역배팅` 태그가 📉 로 시작한다. 그래서
         · 이 함수가 그 종목을 '하락/고점주의' 로 분류하고
         · 프롬프트 규칙 4(최우선 거부권)가 그것을 거부하고
         · 프롬프트 규칙 2 는 바로 그 태그만 고르라고 요구한다
       → **서로 배타적인 지시**가 되어 중기는 구조적으로 000000 만 나왔다.

       수정: 태그 **그 자체**만 위험 판정에서 뺀다. 태그를 지운 나머지에 📉 가 또 있거나
       RISK_WORDS 가 하나라도 있으면 여전히 위험이다.

    ⚠️ 이 결함이 중기 0건의 **유일한** 원인이라는 뜻은 아니다. −20% 엔벨로프 문턱도
       같이 걸려 있었고(로드맵 §3-2-1), 둘의 기여도는 아직 분리되지 않았다.
    """
    raw = tajeom_raw or ""
    if any(k in raw for k in RISK_WORDS):
        return True
    return "📉" in raw.replace(OVERSOLD_TAG, "")


def trend_phase(tajeom_raw):
    """추세 위상 문자열. analyst 가 AI 입력에 그대로 실어 보낸다."""
    raw = tajeom_raw or ""
    if is_downtrend_risk(raw):
        return "하락/고점주의"
    if any(k in raw for k in ("가속", "2파")):
        return "상승가속"
    if any(k in raw for k in ("전환", "1파")):
        return "상승전환초기"
    if "추세 유지" in raw:
        return "상승유지"
    return "중립"


# ── 단계별 후보 계측 (재검증 Q1 권고 · 2026-09-08) ────────────────────────
# 왜 필요한가 — 9/8 첫 신정책 실행에서 중기 픽 근거가 이렇게 나왔다:
#   "유형:SEED 중 타점이 '과매도 · 역배팅'인 종목이 리스트에 존재하지 않아"
# 모순은 사라졌지만(거부권에 걸린 게 아니다) **이 문장만으로는 어디서 막혔는지 모른다.**
# SEED 가 없었나 · 과매도 태그가 없었나 · 둘의 교집합이 없었나가 전부 같은 문장이 된다.
# 그래서 각 관문을 따로 센다. 문턱을 건드릴지 말지는 이 숫자를 보고 정할 문제다.
FUNNEL_STAGES = ("풀", "SEED", "과매도태그", "교집합", "위험문구거부", "최종적격")


def channel_funnel(pool, tajeom_key="tajeom_raw", type_key="type"):
    """중기(과매도 역배팅) 채널의 관문별 잔존 수를 센다.

    pool 은 {type, tajeom_raw} 를 가진 dict 들의 리스트다.
    반환하는 '최종적격'은 **AI 에게 넘어가기 전 파이썬이 아는 적격 후보 수**다.
    AI 가 그중 하나를 고르는지는 별개이고, 그 차이가 바로 '모델 판단' 몫이다.
    """
    def raw(c):
        return c.get(tajeom_key) or ""

    seed = [c for c in pool if str(c.get(type_key, "")).strip().upper() == "SEED"]
    oversold = [c for c in pool if OVERSOLD_TAG in raw(c)]
    both = [c for c in seed if OVERSOLD_TAG in raw(c)]
    vetoed = [c for c in both if is_downtrend_risk(raw(c))]
    eligible = [c for c in both if not is_downtrend_risk(raw(c))]
    return {
        "풀": len(pool),
        "SEED": len(seed),
        "과매도태그": len(oversold),
        "교집합": len(both),
        "위험문구거부": len(vetoed),
        "최종적격": len(eligible),
        "적격종목": [c.get("name") or c.get("code") or "?" for c in eligible][:5],
    }


def funnel_line(f):
    """계측을 로그 한 줄로. 액션 로그는 90일 뒤 사라지므로 문서로 옮길 것."""
    body = " → ".join(f"{s} {f[s]}" for s in FUNNEL_STAGES)
    tail = ""
    if f["최종적격"]:
        tail = f"  [적격: {', '.join(f['적격종목'])}]"
    elif f["교집합"] and f["위험문구거부"]:
        tail = "  ⚠️ 교집합은 있었는데 전부 위험문구로 거부됐다 — F01 모순 재발 신호"
    elif f["SEED"] and not f["과매도태그"]:
        tail = "  → SEED 는 있는데 과매도 태그가 0. 스캐너 문턱(엔벨로프) 문제다"
    elif not f["SEED"]:
        tail = "  → SEED 자체가 0. 중기 채널의 상위 관문에서 막힌 것이다"
    return f"📊 [중기 채널 깔때기] {body}{tail}"
