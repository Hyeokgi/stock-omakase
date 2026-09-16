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

HEADER = ["date", "channel", "rank", "code", "name", "score", "picked",
          "pool_size", "policy_id", "run_id", "captured_at"]

# 채널이 무엇으로 순위를 매기는가 — omakase.py:3132(차트=r[29]) · 3163(수급=r[31])
IDX_NAME, IDX_CODE, IDX_V1, IDX_V2 = 0, 1, 29, 31


def _num(v):
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def policy_id():
    """선정 정책 식별자. **호출부가 import 를 신경 쓰지 않게** 여기서 찾는다.

    scanner_census.policy_fields 와 같은 패턴이다. omakase 가
    `hyeoks_tajeom.POLICY_ID` 를 직접 쓰면 그 모듈을 import 하지 않아
    NameError 로 스캐너가 죽는다 — 실제로 그렇게 쓸 뻔했다.
    """
    try:
        from hyeoks_tajeom import POLICY_ID
        return str(POLICY_ID)
    except Exception:                          # noqa: BLE001
        return ""


def norm_code(v):
    return str(v).replace("'", "").strip().zfill(6)


def build_rows(day, channel, ranked, picked_codes, score_idx,
               top_n=TOP_N, policy_id="", run_id="", captured_at=""):
    """정렬된 후보 목록 → 기록할 행들. **순수 함수라 오프라인 검증된다.**

    `ranked` 는 **이미 정렬된** 목록이다. 여기서 다시 정렬하지 않는다 —
    호출부가 쓴 바로 그 순서를 남겨야 '그때 무엇을 봤는가' 가 보존된다.
    """
    out, pool = [], len(ranked)
    picked = {norm_code(c) for c in picked_codes}
    for i, r in enumerate(ranked[:top_n], start=1):
        try:
            code = norm_code(r[IDX_CODE])
            out.append([day, channel, i, code, str(r[IDX_NAME]).strip(),
                        _num(r[score_idx]) if len(r) > score_idx else None,
                        "Y" if code in picked else "N",
                        pool, policy_id, run_id, captured_at])
        except (IndexError, TypeError):
            continue          # 행 하나가 깨져도 나머지는 남긴다
    return out


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
           policy=None, run_id="", now=None, path=POOL_PATH, top_n=TOP_N):
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
                          policy_id=policy if policy is not None else policy_id(),
                          run_id=run_id, captured_at=now.isoformat())
        if not rows:
            return False, "기록할 행이 없다"
        append(rows, path)
        return True, f"{channel} {len(rows)}행 (풀 {len(ranked)})"
    except Exception as e:                     # noqa: BLE001 — 스캐너를 죽이지 않는다
        return False, f"기록 실패: {e}"
