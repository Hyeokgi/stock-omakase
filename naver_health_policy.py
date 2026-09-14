"""Function-level health, not an AND of every historical endpoint."""
import datetime as dt

KST = dt.timezone(dt.timedelta(hours=9))
# Any one source in each group can support this function; missing probes are not passes.
GROUPS = {
    "수급": ("치명적", ("frgn_html", "frgn_detail", "frgn_json")),
    "위험종목": ("치명적", ("risk_pages", "risk_json")),
    "테마목록": ("중요", ("theme_html", "theme_json")),
    "테마구성": ("중요", ("theme_detail", "themestk_json")),
    "시가총액": ("중요", ("market_sum", "marketsum_json")),
    "주요뉴스": ("중요", ("mainnews", "news_json")),
    "시황뉴스": ("중요", ("newslist", "newslist_json")),
    "종목뉴스": ("중요", ("itemnews", "itemnews_prod", "itemnews_json")),
    "검색상위": ("중요", ("lastsearch", "rank_json")),
}
RETIRED = {"frgn_html", "risk_pages", "theme_html", "theme_detail", "market_sum",
           "mainnews", "newslist", "itemnews", "itemnews_prod", "lastsearch"}
OPTIONAL = {"sise_bulk", "overtime"}  # Candidate source / optional display field, not primary input.
STANDALONE = {"morning_news_v2", "morning_search_v2", "consensus_v2", "fchart_stock",
              "fchart_index", "stock_basic", "index_basic", "index_price", "search_code",
              "upjong_json", "group_json", "price_fresh", "flow_fresh"}
REQUIRED = STANDALONE | OPTIONAL | {k for _, keys in GROUPS.values() for k in keys}


def evaluate(results):
    alerts, transitions, advisory = [], [], []
    missing = REQUIRED - results.keys()
    if missing:
        alerts.append({"name": "진단 미완료", "severity": "치명적", "detail": ", ".join(sorted(missing))})
    def ok(key):
        return results.get(key, {}).get("ok") is True
    grouped = set()
    for name, (severity, keys) in GROUPS.items():
        grouped.update(keys)
        failed = [k for k in keys if not ok(k)]
        if not any(ok(k) for k in keys):
            alerts.append({"name": name, "severity": severity, "detail": "사용 가능한 원천 없음: " + ", ".join(keys)})
        elif failed:
            transitions.append({"name": name, "failed_sources": failed,
                                "working_sources": [k for k in keys if ok(k)]})
    for key, value in results.items():
        if key in grouped or ok(key):
            continue
        entry = {"name": value.get("desc", key), "severity": value.get("severity", "중요"),
                 "detail": value.get("error") or value.get("detail") or "검사 실패"}
        (advisory if key in OPTIONAL else alerts).append(entry)
    return {"schema": "naver-health-v2", "complete": not missing, "alerts": alerts,
            "transitions": transitions, "advisory": advisory,
            "status": "failed" if alerts else "sources_available",
            "sheet_write_verification": "not_checked"}


def expected_market_date(now, is_session):
    now = now.astimezone(KST)
    day = now.date()
    if now.time() < dt.time(9):
        day -= dt.timedelta(days=1)
    for _ in range(15):
        if is_session(day.isoformat()):
            return day
        day -= dt.timedelta(days=1)
    raise ValueError("No reviewed trading date")


def price_fresh(timestamp, now, is_session):
    now = now.astimezone(KST)
    seen = dt.datetime.fromisoformat(timestamp)
    if seen.tzinfo is None:
        return False, "시세 시각 timezone 없음"
    seen = seen.astimezone(KST)
    if seen > now + dt.timedelta(minutes=2):
        return False, "미래 시세 시각"
    expected = expected_market_date(now, is_session)
    if seen.date() < expected:
        return False, f"시세 날짜 지연: {seen.date()} < {expected}"
    active = is_session(now.date().isoformat()) and dt.time(9, 10) <= now.time() <= dt.time(15, 20)
    if active and now - seen > dt.timedelta(minutes=10):
        return False, "장중 시세 10분 초과 지연"
    return True, f"시세 시각 {seen.isoformat()} (대표종목 표본 검사)"


def flow_fresh(dates, now, is_session):
    # Daily investor data need not contain today's unsettled flow.
    previous = now.astimezone(KST).date() - dt.timedelta(days=1)
    for _ in range(15):
        if is_session(previous.isoformat()):
            break
        previous -= dt.timedelta(days=1)
    else:
        raise ValueError("No reviewed prior trading date")
    if not dates:
        return False, "수급 날짜 없음"
    parsed = [dt.datetime.strptime(x, "%Y%m%d").date() for x in dates]
    if max(parsed) > now.astimezone(KST).date():
        return False, "미래 수급 날짜"
    return max(parsed) >= previous, f"최신 수급일 {max(parsed)} / 최소 기대일 {previous}"
