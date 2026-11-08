# 중장기 분석 google-genai 2.x 고정 (2026-11-08, PC Claude)

## 무엇이 깨졌나
- 📈 Mid-Term Trend Analyzer(`trend.yml` → `hyeoks_trend.py`, 토 · 일 22:00 KST)의 11/07 실행이 실패했다. 마지막 성공은 11/01.
- 오류: `400 INVALID_ARGUMENT — JSON mode is not enabled for api version v1. Use api version v1beta instead.` (Gemini `generate_content` 첫 호출)
- 원인: 워크플로가 `google-genai` 를 버전 없이 설치한다. 11/06 에 3.0.0 이 나왔다(11/01 은 2.34.0, 11/07 은 3.0.0 설치).
  - 2.34.0: Gemini API 키 클라이언트의 기본 API 버전이 `v1beta`.
  - 3.0.0: 기본이 `GOOGLE_AI_API_DEFAULT_VERSION = 'v1'`(`_api_client.py`). v1 은 `response_mime_type: application/json` 을 받지 않는다.
  - `hyeoks_trend.py` 는 11/01 이후 바뀌지 않았다. 저장소에서 `response_mime_type` 을 쓰는 파일은 이것 하나다.

## 바꾼 것
- `.github/workflows/trend.yml` — `pip install "google-genai<3"` 로 고정. `hyeoks_trend.py` · 프롬프트 · 모델은 그대로다. 지문 대상 아님.

## 영향 · 블록 B
- `DB_중장기` 는 `hyeoks_analyst.py` 가 읽는다(리포트 선정 입력). 11/07 실패로 이번 주말 갱신이 없었다. 11/08 밤 실행도 실패하면 11/09(블록 B 시작) 리포트가 11/01 갱신본으로 돈다.
- 이 변경은 생성 규칙을 바꾸지 않고 평소의 주말 갱신을 되살린다. 11/08 22:00 실행 전 병합이 목표.

## 남은 것
- `ai_report.yml` · `calendar_ingest.yml` · `fred_check.yml` · `morning_briefing.yml` · `scanner_run.yml` 도 `google-genai` 를 고정 없이 설치한다. JSON 모드를 쓰지 않아 같은 오류는 아니지만, 3.0 의 다른 변경 영향은 확인하지 않았다(3.0 은 Python 3.11 이상만 받으므로 3.10 러너는 2.x 에 머문다).

## 확인
- 두 버전의 휠 소스에서 기본 API 버전 비교(위). `tests.test_trend_safety` · `tests.test_ci_wiring` 16건 통과. YAML 파싱 확인.
- 실제 Gemini 호출은 하지 않았다(키를 PC 에서 쓰지 않음). 병합 뒤 다음 예약 실행 결과로 확인한다.

## 사용자 승인 필요 사항
병합 — 사용자. 타인의 검토 · 승인은 없다.
