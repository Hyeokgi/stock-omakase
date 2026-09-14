"""Read-only adapters for Naver's migrated pages. No credentials or paid APIs."""
import datetime as dt
import html
import json
import math
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class SourceError(RuntimeError):
    pass


def read(url, referer="https://stock.naver.com/"):
    try:
        with urlopen(Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": referer}), timeout=12) as response:
            if response.status != 200:
                raise SourceError("Non-200 response")
            return response.read().decode("utf-8")
    except Exception as error:
        raise SourceError(f"Source unavailable: {type(error).__name__}") from error


def get_json(url, referer="https://stock.naver.com/"):
    try:
        return json.loads(read(url, referer))
    except (ValueError, TypeError) as error:
        raise SourceError("Invalid JSON") from error


def exact_code(payload, name):
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise SourceError("Search schema changed")
    codes = {str(i.get("code", "")) for i in payload["items"]
             if isinstance(i, dict) and i.get("name") == name
             and re.fullmatch(r"\d{6}", str(i.get("code", "")))}
    return next(iter(codes)) if len(codes) == 1 else None


def search_code(name):
    return exact_code(get_json("https://ac.stock.naver.com/ac?" + urlencode({"q": name, "target": "stock"})), name)


def news_titles(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("articles"), list):
        raise SourceError("News schema changed")
    titles = list(dict.fromkeys(html.unescape(a["title"]).strip()
        for a in payload["articles"] if isinstance(a, dict) and isinstance(a.get("title"), str) and a["title"].strip()))
    if not titles:
        raise SourceError("News list empty")
    return titles[:15]


def main_news():
    return news_titles(get_json("https://stock.naver.com/api/domestic/news/list?category=MAINNEWS&page=1&pageSize=15"))


def parse_consensus(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("JsonData"), list):
        raise SourceError("Consensus schema changed")
    result = {}
    for row in payload["JsonData"]:
        if not isinstance(row, dict) or not re.fullmatch(r"\d{4}\.(03|06|09|12)\([AE]\)", str(row.get("YYMM", ""))):
            raise SourceError("Invalid quarter label")
        if not row["YYMM"].endswith("(E)"):
            continue
        if row.get("MAIN") != "IFRS연결":
            raise SourceError("Unexpected financial basis")
        if row["YYMM"] in result:
            raise SourceError("Duplicate estimate quarter")
        values = {}
        for source, target in (("SALES", "매출액"), ("OP", "영업이익"), ("NP", "당기순이익")):
            if source not in row:
                raise SourceError("Missing financial field")
            raw = row[source]
            if raw is None or str(raw).strip() in ("", "-", "N/A"):
                continue
            try:
                number = float(str(raw).replace(",", ""))
            except ValueError as error:
                raise SourceError("Invalid financial number") from error
            if not math.isfinite(number):
                raise SourceError("Nonfinite financial number")
            values[target] = number  # Provider table explicitly labels these as 억원.
        if values:
            result[row["YYMM"]] = values
    return result


def consensus_estimates(code):
    if not re.fullmatch(r"\d{6}", code):
        raise SourceError("Invalid stock code")
    page = f"https://navercomp.wisereport.co.kr/v3/company/c1050001.aspx?cmp_cd={code}&theme=light&cn="
    source = read(page)
    match = re.search(r"sDT:\s*'([0-9]{8})'", source)
    if not match:
        raise SourceError("Provider reference date missing")
    dt.datetime.strptime(match[1], "%Y%m%d")
    params = dict(flag="2", cmp_cd=code, finGubun="IFRSL", frq="1", sDT=match[1], chartType="svg")
    url = "https://navercomp.wisereport.co.kr/v3/company/ajax/c1050001_data.aspx?" + urlencode(params)
    return parse_consensus(get_json(url, page))


def merge_consensus_rows(old, new):
    if not new or len(new[0]) != 7:
        raise SourceError("Invalid output schema")
    if old and old[0][:7] != new[0]:
        raise SourceError("Existing consensus header mismatch")
    refreshed = {str(r[0]).lstrip("'") for r in new[1:]}
    preserved = [r[:7] for r in old[1:] if r and str(r[0]).lstrip("'") not in refreshed]
    return new + preserved
