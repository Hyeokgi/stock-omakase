# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 대체 테마 자료원 1차 진단 — 증권플러스 · 씽크풀 (읽기 전용 · 1회성 · 저장 없음)
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-07): stockinfo7 웹이 장중 시간본을 내지 않아(10/6 실측) 대안 후보를 진단한다.
# 후보는 사용자가 전달한 제미나이 추천 중 Claude 검토로 고른 둘. 다음 금융은 보류.
#
# 묻는 것: robots.txt 가 무엇을 막나? 이용약관에 자동 수집 제한 문구가 있나? 로그인이 필요한가?
#          테마 화면이 HTML 에 바로 있나(SPA 인가)? 화면에 기준 날짜·시각이 보이나? 공개 API 경로가 보이나?
# 하지 않는 것:
#   · robots.txt 가 막은 경로는 요청하지 않는다(robots 를 못 읽으면 그 사이트는 robots 만 보고 멈춘다).
#   · Referer 위장·헤더 조작·차단 우회를 하지 않는다. 401/403/429·로그인 화면이면 그 사이트를 멈춘다.
#   · 내부 API 를 호출하지 않는다 — 경로 문자열이 보이는지만 센다.
#   · 저장·커밋·반복 수집을 하지 않는다. 로그에는 구조 수치와 약관의 해당 문구만 남긴다.
#     테마 이름·종목 이름·시세는 찍지 않는다(공개 저장소의 로그는 공개다).
# 요청은 사이트당 최대 MAX_REQ 회, 사이 간격 GAP 초.
# ==========================================================================
import datetime
import re
import sys
import time
import urllib.parse
import urllib.robotparser

import requests

UA = "Mozilla/5.0 (research-probe; HYEOKS stock-omakase; contact via GitHub repo)"
KST = datetime.timezone(datetime.timedelta(hours=9))
GAP = 2.0
MAX_REQ = 8
STOP_HTTP = {401, 403, 429}

SITES = {
    "stockplus": {"base": "https://stockplus.com"},
    "stockplus_m": {"base": "https://m.stockplus.com"},     # 모바일 웹 — robots 를 따로 읽는다
    "thinkpool": {"base": "https://www.thinkpool.com"},
}
THEME_HINT = re.compile(r"(?i)theme|topic|thema|테마|토픽|issue|이슈")
TERMS_HINT = re.compile(r"(?i)terms|agreement|policy|약관|이용규정|service_rule|clause")
TERMS_WORDS = ["크롤", "스크래", "자동화", "자동으로", "로봇", "기계적", "수집", "복제", "재배포", "무단", "영리"]


def robots_for(sess, base, log):
    rp = urllib.robotparser.RobotFileParser()
    url = base + "/robots.txt"
    try:
        r = sess.get(url, timeout=20)
    except Exception as e:
        log(f"- robots.txt 접속 실패: {type(e).__name__} → 이 출처는 여기서 멈춘다")
        return None, None
    log(f"- robots.txt HTTP {r.status_code} · {len(r.content)}바이트")
    if r.status_code in STOP_HTTP:
        return None, r.status_code
    if r.status_code == 404:
        rp.parse([])                      # robots 없음 = 제한 없음(표준 해석). 그래도 요청 상한은 지킨다
        log("  (robots.txt 없음 — 표준상 제한 없음으로 본다)")
    elif r.status_code == 200:
        body = r.text[:3000]
        rp.parse(body.splitlines())
        log("```\n" + body + "\n```")
    else:
        log("  (robots.txt 를 해석할 수 없다 → 이 출처는 여기서 멈춘다)")
        return None, r.status_code
    return rp, r.status_code


def login_wall(r):
    """로그인 화면으로 보내졌는가 — 머리글의 로그인 버튼·입력칸만으로는 판단하지 않는다."""
    return bool(re.search(r"(?i)/(login|signin|member/login)", urllib.parse.urlparse(r.url).path)) or \
        bool(re.search(r"로그인이 필요|로그인 후 이용", r.text if "html" in r.headers.get("content-type", "") else ""))


def links(html, base):
    out = []
    for href, text in re.findall(r'(?is)<a[^>]+href="([^"#]+)"[^>]*>(.*?)</a>', html):
        url = urllib.parse.urljoin(base + "/", href.strip())
        if urllib.parse.urlparse(url).netloc == urllib.parse.urlparse(base).netloc:   # robots 는 호스트별이다
            out.append((url, re.sub(r"(?s)<[^>]+>|\s+", " ", text).strip()[:40]))
    return out


def page_shape(r):
    html = r.text if "html" in r.headers.get("content-type", "") else ""
    shape = {"http": r.status_code, "type": r.headers.get("content-type", "")[:40], "bytes": len(r.content),
             "redirects": [h.status_code for h in r.history]}
    if html:
        shape.update({
            "password_box": bool(re.search(r'(?i)type="password"', html)),
            "login_wall": login_wall(r),
            "spa": bool(re.search(r'__NEXT_DATA__|id="__nuxt"|id="app"|id="root"|window\.__INITIAL', html)),
            "tr_rows": len(re.findall(r"(?i)<tr[\s>]", html)),
            "pct_tokens": len(re.findall(r"[-+]?\d+(?:\.\d+)?\s*%", html)),
            "dates": sorted(set(re.findall(r"20\d\d[.\-/]\d\d[.\-/]\d\d", html)))[-5:],
            "times": len(set(re.findall(r"\b(?:[01]\d|2[0-3]):[0-5]\d\b", html))),
            "api_paths": len(set(re.findall(r"""["'](?:https?://[^"']+)?/(?:api|v\d)/[A-Za-z0-9_\-/]*["']""", html))),
        })
    return shape, html


def terms_lines(html):
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", "\n", text)
    hits = []
    for line in (re.sub(r"\s+", " ", x).strip() for x in text.splitlines()):
        if line and any(w in line for w in TERMS_WORDS):
            hits.append(line[:220])
    return hits[:12]


def probe_site(name, cfg, sess, log, sleep=time.sleep):
    log(f"\n## {name} — {cfg['base']}")
    used = 0
    rp, code = robots_for(sess, cfg["base"], log)
    used += 1
    if rp is None:
        return {"site": name, "stopped": f"robots {code}", "requests": used}

    def get(url):
        nonlocal used
        if used >= MAX_REQ:
            log(f"- 요청 상한 {MAX_REQ} 도달 — {url} 생략")
            return None
        if not rp.can_fetch(UA, url):
            log(f"- robots 금지 — 요청 안 함: {urllib.parse.urlparse(url).path}")
            return None
        sleep(GAP)
        used += 1
        try:
            return sess.get(url, timeout=20, allow_redirects=True)
        except Exception as e:
            log(f"- 접속 실패 {urllib.parse.urlparse(url).path}: {type(e).__name__}")
            return None

    result = {"site": name, "stopped": "", "requests": 0, "pages": []}
    home = get(cfg["base"] + "/")
    if home is None:
        result["requests"] = used
        return result
    shape, html = page_shape(home)
    log(f"- 첫 화면: {shape}")
    if home.status_code in STOP_HTTP or shape.get("login_wall"):
        result.update(stopped=f"첫 화면 HTTP {home.status_code}", requests=used)
        return result
    found = links(html, cfg["base"])
    theme = [u for u, t in found if THEME_HINT.search(u) or THEME_HINT.search(t)]
    terms = [u for u, t in found if TERMS_HINT.search(u) or TERMS_HINT.search(t)]
    log(f"- 첫 화면 링크 {len(found)}개 · 테마 후보 {len(theme)}개 · 약관 후보 {len(terms)}개")
    for u in dict.fromkeys(terms[:1]):
        r = get(u)
        if r is not None:
            hits = terms_lines(r.text)
            log(f"- 약관 {urllib.parse.urlparse(u).path} HTTP {r.status_code} · 수집 관련 문구 {len(hits)}줄")
            for h in hits:
                log(f"  > {h}")
    for u in list(dict.fromkeys(theme))[:3]:
        r = get(u)
        if r is None:
            continue
        shape, _ = page_shape(r)
        log(f"- {urllib.parse.urlparse(u).netloc}{urllib.parse.urlparse(u).path}: {shape}")
        result["pages"].append(shape)
        if r.status_code in STOP_HTTP or shape.get("login_wall"):
            log("  → 차단·로그인 신호 — 이 출처는 여기서 멈춘다(우회하지 않음)")
            result["stopped"] = f"HTTP {r.status_code}" if r.status_code in STOP_HTTP else "로그인"
            break
    result["requests"] = used
    return result


def main(argv=None, sess=None, log=print):
    sess = sess or requests.Session()
    sess.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9"})
    log(f"# 대체 테마 자료원 1차 진단 (저장 없음) · {datetime.datetime.now(KST).isoformat(timespec='seconds')}")
    log("robots 금지 경로·내부 API·우회는 요청하지 않는다. 테마·종목 이름과 시세는 찍지 않는다.")
    out = [probe_site(n, c, sess, log) for n, c in SITES.items()]
    log("\n## 요약")
    for o in out:
        log(f"- {o['site']}: 요청 {o['requests']}회 · 멈춤 {o['stopped'] or '없음'} · 본 화면 {len(o.get('pages', []))}개")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
