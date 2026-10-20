# 네이버 테마·업종·그룹의 커서형 구성종목 응답을 공통 형식으로 읽는다.
from urllib.parse import urlencode
import math

import krx_code


class MembershipError(RuntimeError):
    """구성 조회 실패는 정상적인 구성 0건과 구분한다."""


def fetch_members(kind, no, session, timeout=10, max_pages=40):
    """전체 시장을 끝까지 읽거나 실패한다. 부분 목록을 완성본으로 반환하지 않는다.

    2026-10-20 실측: marketType=ALL 은 400. 생략하면 두 시장을 포함한다.
    size/cursor 페이지네이션, itemCode 및 krx 중첩 필드를 사용한다.
    가격 단위는 원 그대로이며 NXT 값을 KRX 대용으로 쓰지 않는다.
    """
    if kind not in {"theme", "upjong", "group"} or not str(no).isdigit():
        raise MembershipError("Invalid membership endpoint")
    base = f"https://stock.naver.com/api/domestic/market/{kind}/{no}/stocklist"
    rows, codes, cursors, cursor = [], set(), set(), None
    for page in range(max_pages):
        params = {"size": 100, "sortType": "changeRate"}
        if cursor:
            params["cursor"] = cursor
        try:
            response = session.get(base + "?" + urlencode(params), timeout=timeout)
            if response.status_code != 200:
                raise MembershipError(f"{kind} constituents page={page + 1} HTTP {response.status_code}")
            data = response.json()
        except MembershipError:
            raise
        except Exception as error:
            # URL·원본문을 기록하지 않는다.
            raise MembershipError(f"{kind} constituents page={page + 1} {type(error).__name__}") from error
        if (not isinstance(data, dict) or not isinstance(data.get("items"), list)
                or type(data.get("hasNext")) is not bool):
            raise MembershipError(f"{kind} constituents schema changed")
        for item in data["items"]:
            code = krx_code.normalize(item.get("itemCode")) if isinstance(item, dict) else None
            if not code or code in codes:
                raise MembershipError(f"{kind} constituents invalid/duplicate code")
            codes.add(code)
            quote = item.get("krx") or {}
            if not isinstance(quote, dict):
                raise MembershipError(f"{kind} constituents KRX schema changed")
            rows.append({"itemcode": code, "itemname": item.get("itemName"),
                         "prevChangeRate": quote.get("changeRate"),
                         "tradeAmount": quote.get("tradingValue")})
        if not data["hasNext"]:
            return rows
        cursor = data.get("cursor")
        if not data["items"] or not isinstance(cursor, str) or not cursor or cursor in cursors:
            raise MembershipError(f"{kind} constituents invalid/repeated cursor")
        cursors.add(cursor)
    raise MembershipError(f"{kind} constituents page budget exceeded")


def membership_status(rows):
    """구성 목록의 완전성만 집계한다. 가격 자료의 품질과는 별개다."""
    if not rows:
        return "FAILED"
    good = sum(row.get("membershipStatus") == "OK" for row in rows)
    return "OK" if good == len(rows) else "PARTIAL" if good else "FAILED"


def scanner_rows(rows):
    """KRX 원 단위를 스캐너의 백만원으로 옮긴다. 미확인 가격은 0으로 대체하지 않는다."""
    if not rows:
        raise MembershipError("theme constituents empty")
    out = []
    for row in rows:
        try:
            rate, amount = float(row["prevChangeRate"]), float(row["tradeAmount"])
            if not math.isfinite(rate) or not math.isfinite(amount) or amount < 0:
                raise ValueError("invalid number")
            if not row.get("itemname"):
                raise ValueError("missing name")
        except (KeyError, TypeError, ValueError) as error:
            raise MembershipError("theme KRX quote missing/invalid") from error
        out.append({"name": row["itemname"], "code": "'" + row["itemcode"],
                    "rate": rate, "value": int(amount / 1_000_000)})
    return out


def theme_membership_ok(meta, rows):
    """새 파일은 명시적 상태, 과거 파일은 전량 연결 0 여부로 품질을 확인한다."""
    status = meta.get("themeMembershipStatus")
    if status is not None:
        return status == "OK"
    return any(row.get("themeNos") or row.get("topThemeNo") for row in rows.values())
