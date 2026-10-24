# Codex 검토(2026-10-25) — PR #1~#5 반영 · 결정 대기 (2026-10-25)

| 항목 | 내용 |
|---|---|
| 기준 커밋 | main `fe9a01f` · 브랜치 `fix/codex-review-1025` |
| 담당자 | 검토 Codex(읽기 전용, 사용자 지시 2026-10-25 "코덱스에게 오류확인 및 내용검토를 지시해줘") · 반영 Claude (PC 세션) |
| 지문 | 바뀌지 않음 — 이번 반영은 지문 대상 파일을 건드리지 않았다. #2 지적은 지문 대상이라 사용자 결정 대기 |

## 판정 요약

| PR | Codex 판정 | 처리 |
|---|---|---|
| #1 금요일 밤 하락 알림 | 고칠 것 있음(P2 4) | **반영** — 아래 표 |
| #2 지문 변경 묶음 | 고칠 것 있음(P1 1 · P3 1) | **사용자 결정 대기** — 지문 대상 · 공개 범위 결정 |
| #3 업종 구성 PARTIAL | 그대로 좋음 | — |
| #4 분봉 자동 메우기 | 고칠 것 있음(P2 2) | **반영** |
| #5 결측 감시 알려진 구멍 | 고칠 것 있음(P2 2 · P3 1) | **반영**(P3 은 참고) |
| 원격 브랜치 보관 · 삭제 | 동의 | — |

## 반영한 것

| Codex 지적 | 확인 | 처리(커밋) |
|---|---|---|
| #1 같은 등락률이 30개 넘는 구간(보합 0.00%)을 잘린 채 넘김 — 저녁에 크게 빠진 알림 대상이 숨을 수 있다 | 맞다. 가격 필터(`fid_input_price_1/2`, 양끝 포함)가 동작함을 실측 | 등락률로 못 나누면 가격 구간으로 나눈다. 그래도 남으면 '조회불완전'(대상없음 · 해당없음으로 확정하지 않음). 10/23 실측 잘린 구간 0 · 2,524종목 · 386회 (`5855e19`) |
| #1 시작 때만 19:58 검사 — 조회가 늦으면 20시 뒤 발송 | 맞다 | 발송 직전 19:59 검사 (`5855e19`) |
| #1 거래량 기준 시각에 따라 1~2위가 달라짐(18:00 백업 vs 19:40) | 맞다. 확정 일봉 거래대금은 애프터 포함(10/22 실측: 일봉 = 19:59 봉 누적) | 후보를 19:40 에 고른다 — 관측 정의(확정 일봉)에 가장 가깝다. 19:40~20:00 거래분 차이는 남는다 (`5855e19`) |
| #1 상태 동기화 실패 시 중복 발송 가능 | 맞다(발송 뒤 기록 push 실패 등) | **바꾸지 않음** — 알림이 빠지는 것보다 두 번 가는 쪽이 낫다고 판단. 기록 push 는 3회 재시도 |
| #1 '발송 실패 뒤 재시도' 가 실제 재발송까지 시험되지 않음 | 맞다 | 실패 → 다음 실행이 실제로 보내는 시험 추가 |
| #4 날짜 없는 `--force` 회귀 | 맞다 | 예전처럼 오늘 파일 재생성 (`d9e3a69`) |
| #4 자동 메우기가 그날 스냅샷이 없으면 '오늘 순위' 대용을 씀(미래 정보로 표본 선택) | 맞다(스냅샷은 8/28 이후 매일 있어 실제로는 드묾) | 자동 메우기에서는 저장하지 않고 실패로 알림. `--date` 직접 실행은 예전처럼 허용 (`d9e3a69`) |
| #5 사유 없는 줄 · reason 열 없는 목록도 면제 | 맞다 | 열 없으면 목록 무효, 사유 빈 줄은 면제 안 함 (`711d8b5`) |
| #5 지난 날짜 파일 손상을 못 잡음(마지막 거래일만) | 맞다 | 날짜마다 '파일 손상' 을 구멍으로 셈 (`711d8b5`) |
| #5 같은 날 파일이 바뀌어 결손이 커져도 같은 문구면 흡수(P3) | 맞다 | 참고 — 과거 스냅샷 파일은 고치지 않는다는 전제를 유지 |

시험: #1 16건 · #4 11건 · #5 9건, 변이 7종 검출. 전체 1,216건 — 실패 3건은 기존 Windows 권한 시험.

## 사용자 결정 대기 — #2

1. **P1 · `data/feature_store` 공개.** 종목별 `name` · `code` · `price` · 점수 · 후보 여부 · 선정 채널이 담긴 파일이 **2026-09-18 부터 매일 공개 커밋**되고 있다(`main.yml` 의 커밋 단계, 지금 22일치). 허브 §8 '후보 풀 사본 공개 금지' 와 충돌할 수 있다. 10/24 PR #2 의 증거 사본(artifact)도 같은 파일을 담는다.
   이번 PR 이 처음 만든 문제는 아니지만, 기록 문서의 "새로 드러나는 정보는 없다" 는 정정한다 — 커밋이 실패한 회차의 파일은 artifact 로만 남는다.
   선택지: (가) 공개 유지(연구 재현용) · (나) 비공개 드라이브 보관으로 전환하고 공개에는 건수만 — `main.yml` · `.github/evidence_snapshot.py` · `feature_store.py` 등 **지문 대상 변경**, 과거 파일 처리(남김/삭제)도 함께 결정.
2. **P3 · `evidence_builder.pool_rows_ok` 입력 형식.** `rows=False` · `0.0` · 숫자 사유도 통과한다. 정상 생산 경로의 오판은 아니다. 지문 대상이라 위 결정과 같이 묶는 것을 제안한다.

## 기록 문서 정정

- `2026-10-23_Claude_금요일밤하락알림.md` — "이 작업은 19시 이후에만 돈다" 는 18:00 백업과 맞지 않았다 → 이제 백업도 19:40 까지 기다린 뒤 고른다.
- `2026-10-24_Claude_지문묶음_18_20_증거사본.md` — "새로 드러나는 정보는 없다" → 위 #2-1.
- `2026-10-24_Claude_분봉_빠진날_자동메우기.md` — "하루치 동작은 같다" 에 `--force` 회귀가 빠졌다 → 고침.
- `2026-10-24_Claude_결측감시_새구멍만실패.md` — "형식이 깨지면 모두 새 구멍" 이 사유 빈 줄에는 성립하지 않았다 → 고침.

## 타인의 검토·승인 상태

- Codex: 검토함(읽기 전용, 2026-10-25) — 아래 원문(PC Claude 전달본, `-o` 출력 그대로, 끝 표의 중복 세 줄만 뺌). 반영분 재검토는 받지 않았다.

## Codex 원문

**PR #3은 그대로 좋고, #1·#2·#4·#5는 보완이 필요합니다.** 가장 먼저 정리할 사항은 #2의 종목별 원자료 공개 경로입니다.

기준은 로컬 [`fe9a01f`](https://github.com/Hyeokgi/stock-omakase/commit/fe9a01f2eb8284cc5d2c823ee076a5ba0ce14632)입니다. 지정한 비교 구간과 병합 결과를 대조했습니다. 파일 생성·수정, Python·시험 실행, 원격 접근은 하지 않았습니다.

**1. PR별 발견 사항입니다.**

**PR #1. 금요일 밤 하락 알림.**

- **P2. 불완전한 후보 조회를 정상 완료로 확정합니다.**  
  같은 등락률에 30종목 이상이 몰리면 구간을 더 나눠도 일부가 빠집니다. `cut`을 기록하지만 그대로 1~2위를 선정하고, `대상없음`·`해당없음`이면 이후 실행도 막습니다. 빠진 종목이 실제 거래대금 1위일 수 있습니다. 특히 보합 구간에서 빠지는 종목은 종가 대비 크게 하락한 알림 대상일 수도 있습니다. 조회 불완전과 정상적인 대상 없음을 구분해야 합니다.  
  근거는 [hyeoks_friday_dip_alert.py:93](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L93), [동일 파일:257](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L257), [동일 파일:40](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L40)입니다.

- **P2. 시작한 뒤 마감 시각을 넘기는 경우를 막지 않습니다.**  
  `19:58` 검사는 시작 시 한 번뿐입니다. 그 전에 시작했어도 순위·일봉 조회가 지연되면 20시 이후에 발송할 수 있습니다. KRX 조회 시각만 `200000`으로 제한하며, NXT 조회와 발송은 계속합니다. 가격 조회 전과 발송 직전에 시각을 다시 검사해야 합니다.  
  근거는 [동일 파일:234](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L234), [동일 파일:268](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L268), [동일 파일:289](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L289)입니다.

- **P2. 상태 동기화에 실패해도 발송하므로 중복 방지가 깨질 수 있습니다.**  
  최신 main을 받지 못하면 옛 상태로 계속합니다. 앞 실행이 이미 발송했어도 옛 체크아웃에는 성공 기록이 없어 다시 보낼 수 있습니다. 발송 후 기록 push가 실패한 경우도 같습니다. 현재 중복 방지는 **성공 기록이 저장소에 반영되고 다음 실행이 읽었을 때만** 성립합니다.  
  근거는 [.github/workflows/friday_dip_alert.yml:38](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/.github/workflows/friday_dip_alert.yml#L38), [동일 파일:60](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/.github/workflows/friday_dip_alert.yml#L60), [hyeoks_friday_dip_alert.py:186](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L186)입니다.

- **P2. 거래량 기준 시각에 따라 대상 1~2위가 달라집니다.**  
  종가는 15:30으로 고정했지만 거래량은 조회 당시의 애프터 포함 누적값입니다. 18시 백업이 선정하고 기다리는 경우와 19시 이후 실행이 다른 대상을 고를 수 있습니다. 확정 일봉으로 나중에 계산하는 관측과도 항상 같다고 보장할 수 없습니다. 거래량 기준 시각을 고정하거나 이러한 차이를 명시해야 합니다.  
  근거는 [동일 파일:126](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L126), [동일 파일:135](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L135), [동일 파일:251](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_friday_dip_alert.py#L251)입니다.

15:30 분봉에서 종가를 읽는 구현은 의도대로입니다. 다만 그 봉이 빠지면 오류가 아니라 후보 탈락으로 처리합니다. 또한 `SCAN_LO=-5%`보다 더 하락한 종목은 탐색 밖이므로 전체 후보의 완전성은 보장하지 못합니다.

텔레그램은 **실패 뒤 다음 실행을 허용**할 뿐, 같은 실행 안에서 재시도하지 않습니다. `ReadTimeout` 같은 응답불명은 중복 우려 때문에 재발송을 막는 구조이며 의도와 맞습니다. 근거는 [hyeoks_research_daily.py:642](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_research_daily.py#L642), [동일 파일:693](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_research_daily.py#L693)입니다.

**PR #2. 지문 변경 묶음.**

- **P1. 현재 공개 금지 기준과 충돌하는 원자료 공개 경로가 있습니다.**  
  새 artifact는 `data/feature_store` 변경 파일을 가공 없이 복사합니다. 해당 파일에는 `name`, `code`, `price`, 점수, 후보 여부, 선정 채널이 들어갑니다. 경로 제한은 자격증명 혼입 방지에는 유효하지만 종목별 후보 자료를 보호하지는 않습니다. 현재 요청의 공개 금지 기준을 적용하면 비공개 보관으로 전환하거나 공개 사본에서 제외해야 합니다.  
  기존 커밋 경로도 같은 자료를 공개하므로 **이번 PR만의 신규 문제라고 보지는 않습니다.** 다만 새 배포 경로가 추가됐으며, “이미 공개된 것과 같아 새 정보가 없다”는 설명은 커밋 실패 회차의 미공개 자료까지 보존한다는 목적과 맞지 않습니다.  
  근거는 [.github/evidence_snapshot.py:28](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/.github/evidence_snapshot.py#L28), [동일 파일:158](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/.github/evidence_snapshot.py#L158), [feature_store.py:66](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/feature_store.py#L66), [.github/workflows/main.yml:134](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/.github/workflows/main.yml#L134)입니다.

- **P3. `pool_rows_ok`의 입력 형식 검사가 일부 느슨합니다.**  
  `rows=False`나 `0.0`도 `rows == 0`을 통과하며, 사유는 숫자라도 문자열로 변환되어 인정됩니다. 정상 생산자는 정수와 정해진 사유를 쓰므로 현재 정상 경로의 오판을 발견한 것은 아닙니다. 그래도 판정 입력에서는 정확한 형식을 요구하는 편이 맞습니다.  
  근거는 [evidence_builder.py:48](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/evidence_builder.py#L48)입니다.

핵심 질문에 대한 판단은 다음과 같습니다.

- **10/20 네이버 복구 의도를 깨뜨린 충돌 해결은 발견하지 못했습니다.** `THEME_SOURCE_STATE`와 표시 상태를 모두 초기화하고, 조회 불완전 시 입력 보류·CRITICAL 계측·영수증 상태를 유지합니다. [omakase.py:553](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/omakase.py#L553), [동일 파일:650](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/omakase.py#L650), [동일 파일:714](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/omakase.py#L714)입니다.
- **정상 생산 경로에서 0행 허용 범위가 지나치게 넓지는 않습니다.** 두 채널 후보가 모두 0이어야 하며, Builder의 날짜·지문·Feature Store 최소 행수·버림 0 검사도 유지됩니다. 오류 계측이 있으면 `expected_state=degraded`입니다. [evidence_builder.py:168](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/evidence_builder.py#L168), [omakase.py:3680](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/omakase.py#L3680)입니다.
- 보호 종목명 출력은 개수로 바뀌었고, 선정 조건·점수·`수급_실시간` 데이터 쓰기 규칙·주문 경로 변경은 발견하지 못했습니다.
- **10/23 FAIL 행은 현재 main에 보존되어 있습니다.** 지정한 두 끝점 비교의 삭제 표시는 분기 시점 차이였습니다. 앞선 중간 보고를 정정하며 실제 결함에서 제외합니다.

**PR #3. 업종 구성 PARTIAL.**

**그대로 좋습니다.** 구성 완전성과 가격 연결을 분리하면서 불완전 집계는 빈칸으로 유지합니다. 추가 콘솔 수정도 빈 집계를 순위에서 제외하고, 전부 보류일 때 경고합니다. 저장 파일의 연결과 watch까지 확인하는 시험도 추가되었습니다.

근거는 [hyeoks_market_snapshot.py:420](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_market_snapshot.py#L420), [동일 파일:789](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_market_snapshot.py#L789), [tests/test_upjong_link_20261024.py:121](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/tests/test_upjong_link_20261024.py#L121)입니다. 새 P1·P2는 발견하지 못했습니다.

**PR #4. 분봉 빠진 날 자동 메우기.**

- **P2. 날짜 없는 `--force`가 기존 파일을 재생성하지 않는 회귀입니다.**  
  `catchup_days()`가 기존 파일을 먼저 제외하므로 `collect_day(..., force=True)`까지 도달하지 않습니다. 변경 전에는 `--force`만 줘도 오늘 파일을 다시 만들었습니다.  
  근거는 [hyeoks_intraday_collector.py:250](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_intraday_collector.py#L250), [동일 파일:265](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_intraday_collector.py#L265)입니다.

- **P2. 자동 복원에도 기존의 대용 순위 경로가 적용됩니다.**  
  과거 날짜는 15:05, 없으면 13:00 스냅샷을 사용하며 둘 다 없으면 **오늘 순위**를 사용합니다. 따라서 당일 장 마감 순위와 다른 표본이 만들어질 수 있고, 오늘 순위 대용은 미래 정보를 이용한 표본 선택입니다. 이 경로는 기존 코드지만 이번 기본 자동 메우기가 자동으로 호출하게 됩니다. `rankSource`는 남지만 정상 복원과 구분해 완료를 판단하는 보호는 없습니다.  
  근거는 [동일 파일:138](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_intraday_collector.py#L138), [동일 파일:156](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_intraday_collector.py#L156), [동일 파일:323](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_intraday_collector.py#L323)입니다.

자정 뒤 실행이 전날을 메우는 흐름은 맞습니다. 참고로 실제 10/23 스냅샷 2,872행의 거래대금은 모두 숫자로 해석 가능했습니다. 앞서 의심했던 숫자 형식은 결함에서 제외했습니다.

**PR #5. 알려진 구멍 목록.**

- **P2. 사유가 빠진 목록도 실패 면제에 사용됩니다.**  
  `date,item`만 있는 CSV나 `reason`이 빈 행도 정상 등록됩니다. 따라서 “형식이 깨지면 모든 구멍을 새 구멍으로 본다”는 설명은 보장되지 않습니다. 필수 열과 비어 있지 않은 사유를 검사해야 합니다.  
  근거는 [hyeoks_snapshot_watch.py:182](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_snapshot_watch.py#L182), [동일 파일:199](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_snapshot_watch.py#L199)입니다.

- **P2. 과거 파일의 새로운 손상을 놓치는 기존 사각지대가 남습니다.**  
  매일 파일을 읽지만 `_error`는 `membership_issues()`에서 문제로 변환하지 않습니다. 파일 손상을 `lost`에 추가하는 별도 검사는 마지막 거래일에만 적용됩니다. 알려진 과거 구멍의 파일이 나중에 손상되면 기존 구성 경고가 사라지고 새 손상도 실패로 잡히지 않을 수 있습니다.  
  근거는 [동일 파일:119](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_snapshot_watch.py#L119), [동일 파일:251](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_snapshot_watch.py#L251), [동일 파일:294](https://github.com/Hyeokgi/stock-omakase/blob/fe9a01f/hyeoks_snapshot_watch.py#L294)입니다.

- **P3. 예외는 날짜·항목 문자열만 식별합니다.**  
  다른 날짜·슬롯·항목은 계속 실패하므로 미래 날짜 전체를 숨기는 와일드카드는 없습니다. 다만 같은 날짜 파일이 바뀌어 결손이 확대돼도 동일한 `upjong 구성 PARTIAL`이면 기존 예외에 흡수됩니다. 과거 파일 불변을 지킨다는 전제가 필요합니다.

**2. 기록 문서와 코드·시험의 대조 결과입니다.**

- #1의 시험 12건은 소스와 맞습니다. 다만 “19시 이후에만 돈다”는 설명은 18시 백업과 모순입니다. “발송 실패 뒤 재시도”는 재실행 허용이며 실제 재발송까지 시험하지 않습니다.
- #2의 네이버 충돌 해결·#18·#20·정렬 예외 격리 설명은 코드와 맞습니다. **“artifact로 새 정보가 드러나지 않는다”는 설명은 정정해야 합니다.**
- #3의 최초 4건에 저장·콘솔 시험 2건을 더한 설명은 맞습니다.
- #4의 시험 9건은 맞지만 “하루치 동작은 같다”는 설명에는 `--force` 회귀가 빠졌습니다.
- #5의 목록은 실제 25행이며 시험 7건도 맞습니다. 다만 형식 오류 시 안전하게 실패한다는 주장은 과합니다.

전체 시험 수의 증가인 `1,186 → 1,192 → 1,201 → 1,208`은 추가 시험 수와 일치합니다. **통과 여부, 기존 Windows 실패의 동일성, CI 결과는 이번 검토에서 재실행·독립 확인하지 않았습니다.**

**3. 보강할 시험입니다.**

| PR | 필요한 구체 시험 |
|---|---|
| #1 | 같은 등락률 31종목 중 숨은 거래대금 1위가 있어도 정상 완료하지 않는지. 조회 중 20시를 넘으면 발송하지 않는지. 최신 상태 받기 실패 시 중복 발송을 막는지. HTTP 실패 뒤 실제 두 번째 실행이 발송하는지. |
| #2 | 종목별 이름·코드·가격·점수를 넣은 합성 자료가 공개 artifact에 들어가지 않는지. `rows=False`, `0.0`, 숫자 사유를 거부하는지. |
| #3 | 집계 보류 업종이 정상 거래대금 0인 업종보다 앞에 있어도 정상 업종만 콘솔 순위에 쓰는지. 현재 혼합·전부 보류 시험은 적절합니다. |
| #4 | 오늘 파일이 존재할 때 날짜 없는 `--force`가 다시 수집하는지. 과거 스냅샷이 모두 없을 때 오늘 순위 대용을 정상 복원으로 확정하지 않는지. |
| #5 | `reason` 열 누락·빈 사유를 면제하지 않는지. 마지막 거래일이 아닌 알려진 날짜의 gzip 손상도 종료 코드 2가 되는지. |

**4. 브랜치 보관·삭제 판단에 동의합니다.**

로컬 보관 태그는 `c5f069b`를 가리키고 그 부모가 `b2a519e`이므로 두 작업을 함께 보존합니다. `b2a519e`의 어댑터·워크플로 수정은 `a77348e`에 반영된 내용을 확인했습니다.

미반영 `priority_trials.py`는 종베 진입 다음 거래일의 **종가**를 청산 시각으로 사용합니다. 현재 운영대전제와 달라 그대로 main에 넣지 않은 판단이 맞습니다. 근거는 [보관 코드:118](https://github.com/Hyeokgi/stock-omakase/blob/c5f069b/priority_trials.py#L118)입니다. 현재 생산 연결은 발견하지 못했으며, 과거에도 연결된 적이 전혀 없다는 전체 이력 주장과 원격 삭제 상태까지 확인한 것은 아닙니다.

**5. 최종 판정입니다.**

| PR | 판정 | 고칠 것 요약 |
|---|---|---|
| #1 | **고칠 것 있음** | 후보 불완전 처리, 실행 중 마감 검사, 상태 동기화 실패 시 중복 방지, 거래량 기준 시각. |
| #2 | **고칠 것 있음** | 공개 원자료 사본 경로 정리. 0행 입력 형식 보강은 P3. 네이버 복구 의도는 유지됨. |
