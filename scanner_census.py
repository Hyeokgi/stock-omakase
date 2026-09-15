# -*- coding: utf-8 -*-
# ==========================================================================
# 📊 스캐너 타점 전수조사 보존 — 켜기 전 기준선을 남긴다
# --------------------------------------------------------------------------
# 왜 만드나
#   2026-09-14 스캐너 실측: **스캔 710종목 · 엔벨로프(-20%) 통과 0종목.**
#   중기 채널이 굶는 원인은 태그 생성 오류도 반등 게이트도 아니고
#   **관문①(엔벨로프 밴드)에서 후보가 0** 이라는 것이었다.
#   그런데 이 숫자는 **워크플로 로그에만** 있고 로그는 만료된다. 어디에도 안 남는다.
#   §4-5 '실행 입력 동결'은 analyst 만 덮고 scanner 는 안 덮는 구멍이 여기다.
#
# 왜 지금이 중요한가 — **순서가 결과를 정한다**
#   로드맵 §3-2-1 이 `ENVELOPE_BAND` ON 을 사전등록해 뒀다. 그걸 켜는 순간
#   이 숫자는 바뀐다. **켜기 전에 기준선을 안 남기면 "원래 몇 개였는지"를
#   영영 못 센다.** 전/후 비교가 성립하지 않는다. 그래서 켜기 전에 이것부터 한다.
#
# 설계
#   · 하루 **한 줄**만 남긴다. 스캐너는 10분마다 돌지만 기록은 고정 창에서 한 번.
#     §6-12 가 15:05 를 못박은 것과 같은 논리다 — 관측 시각을 먼저 정한다.
#   · 창은 **14:40~15:10 KST**. 15:00 리포트가 읽는 풀을 만든 회차에 맞춘다.
#   · 날짜 기준 **멱등**. 같은 날 두 번 돌아도 한 줄이다.
#   · 휴장일은 건너뛴다. 달력은 `data/market_snapshot/nontrading.txt` 를 그대로 쓴다
#     (GPT 가 출처와 함께 확정한 그 파일). 없으면 주말만 거른다.
#   · **밴드 설정을 행에 같이 적는다.** 스위치가 켜지는 순간의 경계가 데이터 안에
#     남아야 나중에 기억에 의존하지 않고 전/후를 가를 수 있다.
#
# 이 파일이 하지 않는 것
#   · 문턱을 바꾸지 않는다. 읽고 적기만 한다.
#   · 판정하지 않는다. 이건 관측 아카이브이지 연구 표본이 아니다.
#     연구에 쓰려면 별도 사전등록이 필요하다.
# ==========================================================================
import os, csv, datetime

KST = datetime.timezone(datetime.timedelta(hours=9))
CENSUS_PATH   = os.path.join("data", "scanner_census", "envelope_census.csv")
NONTRADING    = os.path.join("data", "market_snapshot", "nontrading.txt")
WINDOW_OPEN   = (14, 40)      # KST. 15:00 리포트가 쓰는 풀을 만든 회차 부근
WINDOW_CLOSE  = (15, 10)
HEADER = ["date", "captured_at", "scanned",
          # 관문① 을 **단계별**로 센다. 복합 조건 통과 수 하나로는 어디서 막혔는지 모른다.
          "price_pass",       # 이격만 (current_price <= ma20 × (1-band_pct))
          "price_tv_pass",    # + 거래대금 문턱
          "envelope_pass",    # + 상한가 아님 + 등락률 ≤10% (= 복합 전체)
          "oversold", "knife_wait",
          # 이 줄이 **어떤 설정·어떤 코드·어떤 실행**에서 나왔는지 묶는다.
          # 날짜 상수 하나만으로는 "이 표본이 어느 정책에서 나왔나"를 인증하지 못한다.
          "band_mode", "band_pct", "min_turnover", "warning_market", "kospi_rate",
          "policy_id", "policy_since", "code_sha", "run_id",
          "calendar_ok", "source"]

# `source` 는 이 줄이 **어떻게 들어왔는지**다. 스캐너가 직접 쓴 줄은 "scanner",
# 만료 전에 워크플로 로그에서 옮겨 적은 줄은 "log:<run id>" 로 둔다.
# 옮겨 적은 줄을 직접 관측과 섞으면 나중에 둘을 구별할 방법이 없다.
SOURCE_LIVE = "scanner"


def band_settings(warning_market, env=None):
    """omakase 가 실제로 쓰는 밴드 설정을 **같은 환경변수에서** 읽는다.

    값을 여기서 새로 정하지 않는다. 스캐너가 -12% 로 돌았는데 기록이 -20% 면
    그 기록은 거짓말이다. 그래서 omakase.py:1697~1706 과 같은 규칙을 그대로 읽는다.
    돌려주는 값: (모드, 실제 적용된 하단 %)
    """
    env = os.environ if env is None else env
    on = str(env.get("ENVELOPE_BAND", "off")).strip().lower() in ("on", "true", "1")
    if not on:
        return "off", 20.0                      # 현행 고정값
    try:
        normal = float(env.get("ENVELOPE_PCT_NORMAL", "12"))
    except (TypeError, ValueError):
        normal = 12.0
    try:
        warn = float(env.get("ENVELOPE_PCT_WARNING", "20"))
    except (TypeError, ValueError):
        warn = 20.0
    return "on", (warn if warning_market else normal)


def load_nontrading_checked(path=NONTRADING):
    """(휴장일 집합, 읽기 성공 여부).

    ⚠️ 파일을 못 읽으면 주말만 걸러진다. 그 상태를 **정상 관측처럼 취급하지 않는다** —
    행에 `calendar_ok=N` 을 박아 나중에 그 줄을 의심할 수 있게 한다.
    비어 있는 파일도 실패로 본다(달력이 있는데 비어 있을 이유가 없다).
    """
    out = set()
    try:
        with open(path, encoding="utf-8") as fp:
            for line in fp:
                s = line.split("#")[0].strip()
                if len(s) == 10:
                    out.add(s)
    except OSError:
        return set(), False
    return out, bool(out)


def load_nontrading(path=NONTRADING):
    """휴장일 목록. 없으면 빈 집합 — 그러면 주말만 걸러진다.

    `hyeoks_closing_bet.py` 가 같은 파일을 읽는다. 파서를 공유하지 않는 이유는
    스캐너(운영)가 분석기(연구)를 import 하면 의존 방향이 뒤집히기 때문이다.
    대신 **같은 파일을 같은 규칙으로** 읽는지 자기검증이 대조한다.
    """
    out = set()
    try:
        with open(path, encoding="utf-8") as fp:
            for line in fp:
                s = line.split("#")[0].strip()
                if len(s) == 10:
                    out.add(s)
    except OSError:
        pass
    return out


def code_sha(path="omakase.py"):
    """스캐너 코드의 내용 해시 8자. 커밋 없이 바뀐 실행도 구분된다."""
    try:
        import hashlib
        with open(path, "rb") as fp:
            return hashlib.sha256(fp.read()).hexdigest()[:8]
    except OSError:
        return ""


def policy_fields():
    """선정 정책 식별자. 스위치를 켜고 이걸 안 올리면 조용한 정책 변경이 된다."""
    try:
        from hyeoks_tajeom import POLICY_ID, POLICY_SINCE
        return str(POLICY_ID), str(POLICY_SINCE)
    except Exception:
        return "", ""


def in_window(now):
    hm = (now.hour, now.minute)
    return WINDOW_OPEN <= hm < WINDOW_CLOSE


def is_trading_day(date_str, nontrading):
    try:
        d = datetime.date.fromisoformat(date_str)
    except ValueError:
        return False
    return d.weekday() < 5 and date_str not in nontrading


def read_dates(path=CENSUS_PATH):
    """이미 기록된 날짜들. 파일이 없거나 깨졌으면 빈 집합."""
    out = set()
    try:
        with open(path, encoding="utf-8", newline="") as fp:
            for row in csv.reader(fp):
                if row and row[0] != "date":
                    out.add(row[0])
    except OSError:
        pass
    return out


def should_record(now, path=CENSUS_PATH, nontrading=None):
    """(기록할까, 사유). 사유는 안 적을 때도 로그에 남겨 침묵하지 않게 한다."""
    nt = load_nontrading_checked()[0] if nontrading is None else nontrading
    date_str = now.strftime("%Y-%m-%d")
    if not is_trading_day(date_str, nt):
        return False, "휴장일"
    if not in_window(now):
        return False, (f"기록 창({WINDOW_OPEN[0]}:{WINDOW_OPEN[1]:02d}~"
                       f"{WINDOW_CLOSE[0]}:{WINDOW_CLOSE[1]:02d}) 밖")
    if date_str in read_dates(path):
        return False, "오늘치 이미 기록됨"
    return True, ""


def build_row(now, scanned, price_pass, price_tv_pass, envelope_pass,
              oversold, knife_wait, warning_market, kospi_rate,
              env=None, calendar_ok=True):
    env = os.environ if env is None else env
    mode, pct = band_settings(warning_market, env)
    pid, psince = policy_fields()

    def _n(v):
        return "" if v is None else str(int(v))

    return {
        "date": now.strftime("%Y-%m-%d"),
        "captured_at": now.isoformat(timespec="seconds"),
        "scanned": _n(scanned),
        "price_pass": _n(price_pass),
        "price_tv_pass": _n(price_tv_pass),
        "envelope_pass": _n(envelope_pass),
        "oversold": _n(oversold),
        "knife_wait": _n(knife_wait),
        "band_mode": mode,
        "band_pct": f"{pct:.1f}",
        "min_turnover": 10_000_000_000 if warning_market else 5_000_000_000,
        "warning_market": "Y" if warning_market else "N",
        "kospi_rate": "" if kospi_rate is None else f"{float(kospi_rate):.2f}",
        "policy_id": pid,
        "policy_since": psince,
        "code_sha": code_sha(),
        "run_id": str(env.get("GITHUB_RUN_ID", "")),
        "calendar_ok": "Y" if calendar_ok else "N",
        "source": SOURCE_LIVE,
    }


def append_row(row, path=CENSUS_PATH):
    """append 전용. 기존 줄을 고치지 않는다."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    new = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=HEADER)
        if new:
            w.writeheader()
        w.writerow(row)


def record(scanned, price_pass, price_tv_pass, envelope_pass, oversold, knife_wait,
           warning_market, kospi_rate=None, now=None, path=CENSUS_PATH,
           env=None, nontrading=None):
    """스캐너가 부르는 입구. (기록됨, 메시지).

    **절대 예외를 밖으로 내보내지 않는다.** 이 기록 때문에 스캐너가 죽으면
    관측을 지키려다 수집을 잃는다. 실패는 "기록 실패: ..." 메시지로만 돌려주고,
    부르는 쪽이 그 접두사를 보고 눈에 띄게 남긴다.
    """
    try:
        now = now or datetime.datetime.now(KST)
        if nontrading is None:
            nt, cal_ok = load_nontrading_checked()
        else:
            nt, cal_ok = nontrading, True
        ok, why = should_record(now, path, nt)
        if not ok:
            return False, why
        append_row(build_row(now, scanned, price_pass, price_tv_pass, envelope_pass,
                             oversold, knife_wait, warning_market, kospi_rate,
                             env, cal_ok), path)
        return True, path + ("" if cal_ok else "  ⚠️ 휴장 달력을 못 읽었다(calendar_ok=N)")
    except Exception as e:                     # noqa: BLE001 — 스캐너를 죽이지 않는다
        return False, f"기록 실패: {e}"


def gaps(path=CENSUS_PATH, nontrading=None, today=None):
    """기록돼야 했는데 비어 있는 거래일. 결측을 사람이 볼 수 있게 한다.

    보존이 조용히 실패하면 파일은 그냥 짧아질 뿐 아무도 모른다. 그래서 센다.
    """
    dates = read_dates(path)
    if not dates:
        return []
    nt = load_nontrading_checked()[0] if nontrading is None else nontrading
    start = datetime.date.fromisoformat(min(dates))
    end = today or datetime.datetime.now(KST).date()
    out, d = [], start
    while d <= end:
        iso = d.isoformat()
        if is_trading_day(iso, nt) and iso not in dates:
            out.append(iso)
        d += datetime.timedelta(days=1)
    return out
