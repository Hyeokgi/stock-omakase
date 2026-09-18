# 🔇 조용한 예외 12곳 — 영향도 분류표

2026-09-18 사용자 지시 5번. **일괄 `raise` 로 바꾸지 않는다.**
12곳을 raise 로 바꾸면 종목 하나가 이상할 때 그날 수집 전체를 잃는다. 그건
"조용히 틀림" 을 "요란하게 아무것도 못함" 으로 바꾸는 것일 뿐이다.
원하는 것은 **그 값만 쓰지 않고, 그 사실이 보이는 것**이다.

분류 기준(`feature_telemetry.py`):

| 분류 | 뜻 | Gate |
|---|---|---|
| `CRITICAL` | 핵심 Feature·가격·정책·후보선정 | **떨어뜨린다** |
| `OBSERVE` | 관측 저장 실패 — 생산은 살리되 | **떨어뜨린다** |
| `DISPLAY` | UI·표시·부가 설명 | 영향 없음 |
| `RESEARCH` | 연구 전용 — 생산 유지, 연구 표본 fail-closed | 영향 없음 |

---

## 분류표

| # | 위치 | 현재 fallback | 영향받는 값 | 투자/연구 영향 | 분류 | 새 동작 | Gate | 회귀시험 |
|---|---|---|---|---|---|---|---|---|
| 1 | `omakase.py:607` | `continue` (종목 1개 건너뜀) | 급등주 후보 목록 | **후보 발굴에서 종목이 조용히 빠진다** | `CRITICAL` | 세고 사유 남김. 종목은 여전히 건너뜀(그 값을 못 믿으므로) | ✅ | `test_silent_exceptions` |
| 2 | `omakase.py:1003` | 진입가를 기준종가로 폴백 | 트레일링 손절 기준가 | **손절 발동가가 틀어진다**(실측 5/14건 1%+ 괴리) | `CRITICAL` | 세고 사유 남김. 폴백은 유지하되 **폴백했다는 사실이 보인다** | ✅ | 〃 |
| 3 | `omakase.py:451` | 다음 원천으로 넘어감 | 종목코드 조회 | 다중 원천 폴백 — 설계된 동작 | `DISPLAY` | 세기만 한다 | — | 〃 |
| 4 | `omakase.py:2529` | `continue` | 과거 테마 매핑(3개월) | 테마 귀속 연구용 | `RESEARCH` | 세고, 해당 연구 표본 fail-closed | — | 〃 |
| 5 | `hyeoks_analyst.py:770` | `pass` | 테마 일별 대금 보조맵 | **대장 판정 근거** | `CRITICAL` | 세고 사유 남김 | ✅ | 〃 |
| 6 | `hyeoks_analyst.py:846` | 현재가 정수 변환 실패 | 프롬프트 표시용 현재가 | 표시 | `DISPLAY` | 세기만 한다 | — | 〃 |
| 7 | `hyeoks_morning.py:143` | `except: pass` (bare) | PER/PBR 펀더멘털 | 브리핑 표시 | `DISPLAY` | bare except 제거 + 세기 | — | 〃 |
| 8 | `hyeoks_nightly.py:178` | `pass` | MA20·60일 고가 텍스트 | 시간외 표시 | `DISPLAY` | 세기만 한다 | — | 〃 |
| 9 | `hyeoks_backfill_targets.py:159` | `continue` | 과거 목표가·손절가 백필 | 1회성 유틸리티 | `RESEARCH` | 세고, 해당 행 fail-closed | — | 〃 |
| 10 | `feature_store.py:173` | `continue` (행 1개 버림) | **연구 표본 행** | 그 종목이 그날 표본에서 사라진다 | `OBSERVE` | 세고, **버린 행 수를 영수증에 싣는다** | ✅ | 〃 |
| 11 | `rank_pool.py:106` | `continue` | **순위 풀 행** | 〃 | `OBSERVE` | 〃 | ✅ | 〃 |
| 12 | `research_trials.py:48` | `continue` | 시도 횟수 집계 | **상한 집계가 새면 상한이 무의미** | `RESEARCH`→`CRITICAL` | 깨진 행이 있으면 **집계를 거부**한다 | — | 〃 |

---

## 왜 12번만 동작을 바꾸는가

`research_trials.counts()` 는 탐색 시도 **상한**(단계 15/45/20 · 총 80)을 세는 함수다.
깨진 행을 조용히 건너뛰면 **실제 시도가 상한보다 많아도 통과**한다.
상한을 코드로 막아 놓은 의미가 사라진다. 그래서 여기서는 세는 것으로 부족하고,
**집계 자체를 거부**한다(fail-closed).

나머지 11곳은 fallback 동작을 바꾸지 않는다. 바꾸면 그날 수집을 잃거나
선정 결과가 달라진다 — 이번 안정화에서 **선정은 건드리지 않는다**는 전제에 어긋난다.
바뀌는 것은 **보이느냐**다.

## 표 밖에서 추가로 건 계측 3곳

12곳 분류와 별개로, 사용자 지시 ⑤·⑦ 을 만족하려면 계측이 더 필요했다.

| 위치 | 이유 | 분류 |
|---|---|---|
| `omakase` V2 게이트 판정 실패 | ⑤ — V3 요약만으로 `no_silent_parse_error` 를 참으로 만들지 않는다 | `CRITICAL` |
| `omakase` 원장 read-after-write 누락 | ⑦ — append **호출 성공**은 적재의 증거가 아니다 | `CRITICAL` |
| `omakase` 순위 풀 기록 실패 | ⑪ 과 짝 — 관측 저장 실패는 생산을 살리되 cycle 을 떨어뜨린다 | `OBSERVE` |

## Gate 와의 연결

- `CRITICAL`·`OBSERVE` 합계가 0 이 아니면 `no_silent_parse_error` 가 **거짓**이다.
- 즉 이 12곳 중 하나라도 조용히 실패하면 **그 거래일은 Stability Gate 를 통과하지 못한다.**
- `DISPLAY`·`RESEARCH` 는 세기만 하고 Gate 를 막지 않는다. 다만 영수증에는 남는다.
