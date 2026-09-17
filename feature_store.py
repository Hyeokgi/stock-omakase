# -*- coding: utf-8 -*-
"""Feature Store — 그날 **알고 있던 것 전부**를 미래수익을 알기 전에 동결한다.

왜 필요한가 (2026-09-17)
------------------------
1.5단계에서 "픽을 3·5개로 늘렸다면?" 을 재려다 **잴 수 없었다.** 뽑히지 않은
후보의 정보가 어디에도 없었기 때문이다. `rank_pool` 이 상위 10개를 남기기 시작했지만
그것만으로는 "V1 75+ 이면서 V2 40~65 이고 주도테마인 종목" 같은 조합을 못 만든다.

**그런데 이건 새 수집기를 만드는 일이 아니다.** `omakase.analyze_single_stock` 이
이미 종목당 35개 피처를 계산하고(`result_row`), TOP2 만 빼고 **매일 버린다.**
이 모듈은 그걸 버리지 않고 떨군다.

설계 원칙
---------
1. **미래수익을 담지 않는다.** 선정 시점에 존재할 수 없는 값은 한 칸도 없다.
   수익률은 나중에 코드+날짜로 일봉에서 계산한다(원장과 같은 규약: T+1 시가 → T+N 종가).
2. **나중에 조인할 수 없는 것은 지금 박는다.** 아래 넷이 그렇다 —
   · `is_junk`(관리종목류)  : `DB_정적데이터` 가 **매일 덮어쓰인다**
   · `theme_hist_max`       : 누적 최대값이라 과거 시점 값을 복원할 수 없다
   · `rs_score`             : 스캔 완료 후 **백분위로 덮어써진다**(omakase.py:2348 주석)
   · `theme_rank`           : 그날의 테마 랭킹은 보존되지 않는다
3. **선정을 바꾸지 않는다.** 읽기만 한다. 관측 추가일 뿐이다.
4. **절대 예외를 밖으로 내지 않는다.** 관측을 지키려다 수집을 잃으면 안 된다.

왜 하루 한 파일(gzip)인가
-------------------------
한 CSV 에 append 하면 git 이 **매일 파일 전체를 새로 저장**해 저장소가 부푼다.
`data/market_snapshot` 이 이미 쓰는 방식대로 날짜별 gzip 으로 떨군다.
실측 규모: 스캔 715행 × ~45열 ≈ 압축 전 340KB/일, 연 86MB. 비용은 문제가 아니다.
"""
import csv
import datetime
import gzip
import os

KST = datetime.timezone(datetime.timedelta(hours=9))
STORE_DIR = "data/feature_store"
STORE_VERSION = "feature-store-v1"

# omakase.py:2348 `result_row` 의 순서 그대로. 이름은 우리가 붙인다.
# ⚠️ 이 목록의 순서를 바꾸면 과거 파일과 어긋난다. 추가는 **뒤에만** 한다.
RESULT_FIELDS = [
    "name", "code", "price", "change_rate", "ma5", "ma20", "vol_ratio", "signal",
    "tajeom", "ai_brief", "high_today", "low_today", "high_60d",
    "market_cap", "shadow", "dist_high", "disparity", "leader", "vol_status", "theme",
    "program", "high_250d", "supply_status",
    "target", "stop", "seed_tag",
    "krx_after", "nxt_after", "market_type",
    "v1_score", "v1_display",
    "v2_score", "v2_display",
    "rs_score", "v2_gate",
]

# 그날의 시장 상태 + 나중에 조인 불가능한 point-in-time 값
CONTEXT_FIELDS = [
    "date", "captured_at", "kospi_rate", "warning_market", "index_above_ma5",
    "is_junk", "theme_rank", "theme_hist_max", "rs_is_percentile",
    "picked_by", "in_candidate_pool", "gate_passed",
    "store_version", "code_sha", "run_id",
]

HEADER = CONTEXT_FIELDS + RESULT_FIELDS


def norm_code(v):
    return str(v).replace("'", "").strip().zfill(6)


def path_for(day, root=STORE_DIR):
    return os.path.join(root, f"{day}.csv.gz")


def code_sha():
    return (os.environ.get("GITHUB_SHA") or "")[:8]


def build_rows(day, results, *, kospi_rate=None, warning_market=None,
               index_above_ma5=None, static_db=None, theme_rank=None,
               theme_hist_max=None, picked=None, candidate_codes=None,
               gate_codes=None, captured_at="", run_id="", sha="",
               rs_is_percentile=True):
    """`results` → 저장할 행들. **순수 함수라 오프라인 검증된다.**

    picked          : {종목코드: "차트TOP2|수급TOP2|..."} — 그날 실제로 원장에 들어간 것
    candidate_codes : 배지 필터 통과 풀(차트TOP2 모집단)
    gate_codes      : V2 게이트 통과 풀(수급TOP2 모집단)
    """
    static_db = static_db or {}
    theme_rank = theme_rank or {}
    theme_hist_max = theme_hist_max or {}
    # 🔴 키를 정규화한다. 호출부(omakase)는 원장 행에서 `'000001` 형태로 준다 —
    #    정규화하지 않으면 picked_by 가 **전 행에서 빈칸**이 된다.
    picked = {norm_code(k): v for k, v in (picked or {}).items()}
    static_db = {norm_code(k) for k in static_db}
    cand = {norm_code(c) for c in (candidate_codes or [])}
    gate = {norm_code(c) for c in (gate_codes or [])}
    rows = []
    for r in results:
        try:
            code = norm_code(r[1])
            theme = str(r[19]).strip() if len(r) > 19 else ""
            ctx = [
                day, captured_at, kospi_rate, warning_market, index_above_ma5,
                # 🔴 나중에 조인 불가 — 지금 박는다
                "Y" if code in static_db else "N",
                theme_rank.get(theme, ""),
                theme_hist_max.get(theme, ""),
                "Y" if rs_is_percentile else "N",
                picked.get(code, ""),
                "Y" if code in cand else "N",
                "Y" if code in gate else "N",
                STORE_VERSION, sha, run_id,
            ]
            vals = [r[i] if i < len(r) else "" for i in range(len(RESULT_FIELDS))]
            vals[1] = code                      # 아포스트로피 제거한 코드로 통일
            rows.append(ctx + vals)
        except (IndexError, TypeError):
            continue                            # 한 행이 깨져도 나머지는 남긴다
    return rows


def write(day, rows, root=STORE_DIR):
    """날짜별 gzip 으로 떨군다. 이미 있으면 덮어쓰지 않는다."""
    p = path_for(day, root)
    if os.path.exists(p):
        return False, f"{day} 이미 있음"
    os.makedirs(root, exist_ok=True)
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        w.writerows(rows)
    os.replace(tmp, p)                          # 반쯤 쓴 파일이 남지 않게
    return True, p


def record(day, results, **kw):
    """스캐너가 부르는 입구. (기록됨, 메시지). **절대 예외를 밖으로 내지 않는다.**"""
    try:
        if not results:
            return False, "결과 없음"
        if os.path.exists(path_for(day, kw.get("root", STORE_DIR))):
            return False, f"{day} 이미 기록됨"
        now = kw.pop("now", None) or datetime.datetime.now(KST)
        root = kw.pop("root", STORE_DIR)
        kw.setdefault("captured_at", now.isoformat())
        kw.setdefault("sha", code_sha())
        rows = build_rows(day, results, **kw)
        if not rows:
            return False, "기록할 행이 없다"
        ok, msg = write(day, rows, root)
        return (True, f"{len(rows)}행 → {msg}") if ok else (False, msg)
    except Exception as e:                      # noqa: BLE001 — 스캐너를 죽이지 않는다
        return False, f"기록 실패: {e}"
