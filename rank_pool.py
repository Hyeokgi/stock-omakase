# -*- coding: utf-8 -*-
"""순위 풀 적재 — "3~5위였다면" 을 **나중에** 잴 수 있게 지금 남긴다.

왜 필요한가 (2026-09-16)
------------------------
1.5단계에서 "픽을 3·5개로 늘렸다면 알파가 얼마였나"를 재려 했는데 **잴 수 없었다.**
뽑히지 않은 후보의 수익률이 필요한데 그게 어디에도 없다:

  · `omakase.py` 의 `candidate_pool` 은 메모리에서 계산되고 **TOP2 만 원장에 남는다**
  · 과거 순위 재구성도 불가능하다 — 점수가 그날의 테마 순위·뉴스·정적DB 등
    재현 불가능한 상태에서 계산된다
  · `scanner_census` 는 퍼널 **카운트**만 있고 순위 목록이 없다

그래서 지금부터 남긴다. 몇 주 뒤에 진짜로 잴 수 있다.

무엇을 남기고 무엇을 남기지 않나
--------------------------------
남기는 것: 날짜·채널·순위·종목코드·종목명·점수·뽑혔는지·풀 크기.
**수익률은 남기지 않는다.** 나중에 코드+날짜로 일봉에서 계산하면 된다
(원장과 같은 규약: T+1 시가 진입 → T+N 종가). 지금 계산하면 스캔이 느려지고
그 시점 일봉은 아직 T+1 도 없다.

⚠️ 이 모듈은 **관측만 추가한다. 선정을 바꾸지 않는다.**
   호출부는 `chart_top2`·`supply_top2` 가 이미 정해진 **뒤에** 읽기만 한다.
   그래야 기존 표본의 성격이 유지되고 변경 전/후 비교가 성립한다.

⚠️ **절대 예외를 밖으로 내보내지 않는다.** 이 기록 때문에 스캐너가 죽으면
   관측을 지키려다 수집을 잃는다 — `scanner_census` 와 같은 규율이다.
"""
import csv
import datetime
import os

KST = datetime.timezone(datetime.timedelta(hours=9))
POOL_PATH = "data/scanner_census/rank_pool.csv"
TOP_N = 10          # 3~5위 질문에 답하려면 여유 있게. 풀이 작으면 있는 만큼만.

# ⑪ build_rows 가 버린 행의 사유 목록. 호출부가 읽어 영수증에 싣는다.
DROPPED = []

HEADER = ["date", "channel", "rank", "code", "name", "score", "picked",
          "eligible", "exclusion", "pool_size", "eligible_size",
          "policy_id", "code_sha", "run_id", "captured_at"]

# 채널이 무엇으로 순위를 매기는가 — omakase.py:3132(차트=r[29]) · 3163(수급=r[31])
IDX_NAME, IDX_CODE, IDX_V1, IDX_V2 = 0, 1, 29, 31


def _num(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def policy_id(channel, band=None):
    """**채널별** 선정 정책 식별자.

    🔴 2026-09-16 정정 — 처음에는 `hyeoks_tajeom.POLICY_ID`(= `oversold-veto-v2`)를
       모든 채널에 박았다. **그건 범주 오류다.** 그 상수는 과매도 태그를 다루는
       **리포트 중기 채널의 모수**이고(hyeoks_tajeom 모듈 설명), 차트TOP2·수급TOP2 의
       선정과는 아무 상관이 없다. U3 에서 "정책 동일성은 글로벌이 아니라 채널별"이라고
       정해 놓고 하루 뒤에 스스로 어겼다.

    있지도 않은 정책 레지스트리 버전을 지어내지 않는다. 대신 **그 채널의 선정을
    실제로 정하는 요소**를 조합해 만든다 — 순위열 + 후보 규칙 + 켜진 스위치.
    스위치를 켜고 이 값이 안 바뀌면 그게 조용한 정책 변경이다.
    """
    if channel == "차트TOP2":
        return "chart-top2/v1/badge-pool"
    if channel == "수급TOP2":
        base = "supply-top2/v2/gate-pass"
        return f"{base}+band{band}" if band else base
    return f"{channel}/unspecified"


def code_sha():
    """그날 어떤 코드였나를 나중에 복원하려면 run_id 만으로는 부족하다."""
    return (os.environ.get("GITHUB_SHA") or "")[:8]


def norm_code(v):
    return str(v).replace("'", "").strip().zfill(6)


def build_rows(day, channel, ranked, picked_codes, score_idx,
               top_n=TOP_N, policy="", run_id="", captured_at="",
               eligible_codes=None, exclusion="", sha=""):
    """정렬된 후보 목록 → 기록할 행들. **순수 함수라 오프라인 검증된다.**

    `ranked` 는 **이미 정렬된** 목록이다. 여기서 다시 정렬하지 않는다 —
    호출부가 쓴 바로 그 순서를 남겨야 '그때 무엇을 봤는가' 가 보존된다.
    """
    DROPPED.clear()                 # 호출마다 새로 센다
    out, pool = [], len(ranked)
    picked = {norm_code(c) for c in picked_codes}
    # eligible_codes 가 None 이면 필터가 없다는 뜻 — 전부 적격이다.
    elig = None if eligible_codes is None else {norm_code(c) for c in eligible_codes}
    n_elig = pool if elig is None else sum(
        1 for r in ranked if _safe_code(r) in elig)
    for i, r in enumerate(ranked[:top_n], start=1):
        try:
            code = norm_code(r[IDX_CODE])
            ok = True if elig is None else code in elig
            out.append([day, channel, i, code, str(r[IDX_NAME]).strip(),
                        _num(r[score_idx]) if len(r) > score_idx else None,
                        "Y" if code in picked else "N",
                        "Y" if ok else "N", "" if ok else exclusion,
                        pool, n_elig, policy, sha, run_id, captured_at])
        except (IndexError, TypeError) as e:
            # 🔇 ⑪ OBSERVE — 버린 행 수를 남긴다(feature_store 와 같은 규율).
            #    연구 표본에서 종목이 조용히 사라지는 것을 막을 수는 없어도
            #    사라졌다는 사실은 보이게 한다. 0 이 아니면 Gate 가 떨어진다.
            DROPPED.append(type(e).__name__)
            continue          # 행 하나가 깨져도 나머지는 남긴다
    return out


def _safe_code(r):
    try:
        return norm_code(r[IDX_CODE])
    except (IndexError, TypeError):
        return None


def already_recorded(day, channel, path=POOL_PATH):
    """같은 날 같은 채널이 이미 있나. 재실행이 중복 적재하지 않게."""
    if not os.path.exists(path):
        return False
    try:
        with open(path, encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) >= 2 and row[0] == day and row[1] == channel:
                    return True
    except OSError:
        return False
    return False


def append(rows, path=POOL_PATH):
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(HEADER)
        w.writerows(rows)


def record(day, channel, ranked, picked_codes, score_idx,
           policy=None, run_id="", now=None, path=POOL_PATH, top_n=TOP_N,
           eligible_codes=None, exclusion="", band=None, sha=None):
    """스캐너가 부르는 입구. (기록됨, 메시지).

    **절대 예외를 밖으로 내보내지 않는다.** scanner_census 와 같은 규율이다.
    """
    try:
        if not ranked:
            return False, "후보 없음"
        if already_recorded(day, channel, path):
            return False, f"{day} {channel} 이미 기록됨"
        now = now or datetime.datetime.now(KST)
        rows = build_rows(day, channel, ranked, picked_codes, score_idx,
                          top_n=top_n,
                          policy=policy if policy is not None
                          else policy_id(channel, band),
                          run_id=run_id, captured_at=now.isoformat(),
                          eligible_codes=eligible_codes, exclusion=exclusion,
                          sha=code_sha() if sha is None else sha)
        if not rows:
            return False, "기록할 행이 없다"
        append(rows, path)
        return True, f"{channel} {len(rows)}행 (풀 {len(ranked)})"
    except Exception as e:                     # noqa: BLE001 — 스캐너를 죽이지 않는다
        return False, f"기록 실패: {e}"
