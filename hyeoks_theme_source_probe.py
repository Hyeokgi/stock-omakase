# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 외부 테마 자료원 진단 — stockinfo7.com (읽기 전용 · 1회성 · 저장 없음)
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-04): 평소 보는 테마 순위 사이트를 연구 비교 자료로 쓸 수 있는지 진단.
# 사용자가 이용약관을 확인했고 특별한 제재 사항이 없다고 했다.
#
# 묻는 것: 접속 가능? 로그인 필요? 순위가 HTML 에 바로 있나, 자바스크립트/API 로 따로 오나?
#          기준 시각이 표시되나? robots.txt 는 무엇을 막나?
# 하지 않는 것: 저장·커밋·반복 수집. 결과는 실행 로그와 Actions 요약에만 남는다.
#               요청은 페이지당 1회, 사이 간격을 둔다.
# ==========================================================================
import re
import sys
import time

import requests

BASE = "https://stockinfo7.com"
PAGES = ["/robots.txt", "/", "/theme/rank/list", "/theme/list", "/etc/pc/theme"]
UA = "Mozilla/5.0 (research-probe; HYEOKS stock-omakase; contact via GitHub repo)"


def visible_text(html):
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", "\n", html)
    lines = [re.sub(r"\s+", " ", x).strip() for x in text.splitlines()]
    return [x for x in lines if x]


def analyze(path, r):
    html = r.text if "html" in r.headers.get("content-type", "") or path == "/" else ""
    out = {"path": path, "status": r.status_code, "final_url": r.url, "type": r.headers.get("content-type", ""),
           "bytes": len(r.content), "redirects": [h.status_code for h in r.history]}
    if not html:
        return out, []
    title = re.search(r"(?is)<title>(.*?)</title>", html)
    scripts = re.findall(r'(?i)<script[^>]+src="([^"]+)"', html)
    apis = sorted(set(re.findall(r"""["'](/[A-Za-z0-9_\-/]*(?:api|ajax|json|list|rank)[A-Za-z0-9_\-/.?=&]*)["']""", html)))[:25]
    vt = visible_text(html)
    out.update({
        "title": title.group(1).strip() if title else "",
        "tr_rows": len(re.findall(r"(?i)<tr[\s>]", html)),
        "pct_tokens": len(re.findall(r"[-+]?\d+\.\d+\s*%", html)),
        "login_form": bool(re.search(r'(?i)type="password"|/member/page/login', html)),
        "next_or_spa": bool(re.search(r"__NEXT_DATA__|id=\"__nuxt\"|id=\"app\"|id=\"root\"", html)),
        "times": sorted(set(re.findall(r"\b(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d)?\b", html)))[:10],
        "dates": sorted(set(re.findall(r"20\d\d[.\-/]\d\d[.\-/]\d\d", html)))[:10],
        "script_src": scripts[:15], "api_like": apis, "visible_lines": len(vt),
    })
    return out, vt


def main():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9"})
    print("# stockinfo7 진단 (저장 없음)\n")
    for path in PAGES:
        try:
            r = s.get(BASE + path, timeout=20, allow_redirects=True)
        except Exception as e:
            print(f"## {path}\n- 접속 실패: {type(e).__name__}: {str(e)[:150]}\n")
            time.sleep(2)
            continue
        info, vt = analyze(path, r)
        print(f"## {path}")
        for k, v in info.items():
            print(f"- {k}: {v}")
        if path == "/robots.txt":
            print("```\n" + r.text[:2000] + "\n```")
        elif vt:
            print("- 화면 글자 앞부분 (최대 40줄):")
            for x in vt[:40]:
                print(f"    {x[:140]}")
        print()
        time.sleep(2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
