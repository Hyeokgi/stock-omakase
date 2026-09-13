"""Public Hana industry archive collector. No LLM calls or Drive credentials.

Downloads are private runtime artifacts. A connected Drive uploader consumes manifest.json.
Publication time is from the listing, not the (occasionally future-dated) PDF filename.
"""
import argparse
import datetime as dt
import hashlib
import html
import json
from pathlib import Path
import re
import time
from urllib.parse import urljoin
from urllib.request import Request, urlopen

BASE = "https://www.hanaw.com"
LIST = BASE + "/main/research/research/list.cmd?pid=3&cid=1&curPage="
KST = dt.timezone(dt.timedelta(hours=9))


def fetch(url):
    if not url.startswith(BASE + "/"):
        raise ValueError("Unexpected source host")
    with urlopen(Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=45) as response:
        if response.status != 200 or not response.url.startswith((BASE + "/", "https://file.hanaw.com/")):
            raise ValueError("Unexpected HTTP response")
        body = response.read(40 * 1024 * 1024 + 1)
        if len(body) > 40 * 1024 * 1024:
            raise ValueError("File exceeds 40 MB")
        return body


def clean(value):
    return html.unescape(re.sub(r"<[^>]+>", " ", value)).strip()


def parse_listing(page):
    anchors = list(re.finditer(r'<a\b[^>]*\bclass="[^"]*\btitle\b[^"]*"[^>]*\bid="(\d+)_(\d+)"[^>]*>(.*?)</a>', page, re.S))
    rows = []
    for index, match in enumerate(anchors):
        block = page[match.end():anchors[index + 1].start() if index + 1 < len(anchors) else len(page)]
        date = re.search(r'(20\d{2})\.(\d{2})\.(\d{2})', block)
        if not date:
            raise ValueError("Missing publication date")
        day = "-".join(date.groups())
        clock = re.search(r'(오전|오후)\s+(\d{1,2}):(\d{2}):(\d{2})', block)
        published_at = day
        if clock:
            ampm, hour, minute, second = clock.groups()
            hour = int(hour) % 12 + (12 if ampm == "오후" else 0)
            published_at += f"T{hour:02}:{minute}:{second}+09:00"
        for link in re.finditer(r'<a\b[^>]*href="([^"]*download\.cmd\?[^"]+)"[^>]*>(.*?)</a>', block, re.S):
            url = urljoin(BASE, html.unescape(link[1]))
            if f"bbsSeq={match[2]}&" not in url:
                continue
            attach = re.search(r'attachFileSeq=(\d+)', url)
            if not attach:
                raise ValueError("Missing attachment id")
            rows.append({"source_id": f"{match[1]}_{match[2]}_{attach[1]}",
                         "title": clean(match[3]), "published_at": published_at,
                         "source_filename": clean(link[2]), "source_url": url,
                         "broker": "하나증권", "report_type": "industry"})
    if not rows:
        raise ValueError("Listing parser returned no reports; refusing silent success")
    return rows


def collect(since, output, max_pages=100, refresh=False):
    dt.date.fromisoformat(since)
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    manifest_path = output / "manifest.json"
    old = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {"reports": []}
    known = {r["source_id"]: r for r in old["reports"]}
    seen_pages = set()
    selected = {}
    for page in range(1, max_pages + 1):
        rows = parse_listing(fetch(LIST + str(page)).decode("utf-8"))
        signature = tuple(r["source_id"] for r in rows)
        if signature in seen_pages:
            raise ValueError("Repeated page: pagination failed")
        seen_pages.add(signature)
        for row in rows:
            if row["published_at"][:10] >= since:
                selected[row["source_id"]] = row
        if min(r["published_at"][:10] for r in rows) < since:
            break
        time.sleep(0.2)
    else:
        raise ValueError("Page limit reached before requested start date")
    for key, row in sorted(selected.items()):
        previous = known.get(key, {})
        path = Path(previous.get("local_path", "__missing__"))
        if not refresh and path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == previous.get("sha256"):
            continue
        content = fetch(row["source_url"])
        if not content.startswith(b"%PDF-"):
            raise ValueError(f"Not a PDF: {key}")
        sha = hashlib.sha256(content).hexdigest()
        name = f"{row['published_at'][:10].replace('-', '')}_hana_industry_{key}_{sha[:12]}.pdf"
        path = output / name
        path.write_bytes(content)
        row.update(sha256=sha, md5=hashlib.md5(content).hexdigest(), size=len(content),
                   first_seen_at=previous.get("first_seen_at", dt.datetime.now(KST).isoformat()),
                   content_observed_at=dt.datetime.now(KST).isoformat(),
                   file_name=name, local_path=str(path))
        known[key] = row
        manifest_path.write_text(json.dumps({"schema": "hana-archive-v1", "since": since,
            "reports": list(known.values())}, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"downloaded": key, "bytes": len(content)}, ensure_ascii=False), flush=True)
        time.sleep(0.2)
    print(json.dumps({"selected": len(selected), "manifest": str(manifest_path)}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", required=True, help="Inclusive publication date YYYY-MM-DD")
    parser.add_argument("--output", default="data/research_private")
    parser.add_argument("--refresh", action="store_true", help="Re-fetch in-window attachments to detect corrected PDFs")
    args = parser.parse_args()
    collect(args.since, args.output, refresh=args.refresh)
