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
import collections
import datetime
import hashlib
import re
import sys
import time

import requests

BASE = "https://stockinfo7.com"
PAGES = ["/robots.txt", "/", "/theme/rank/list", "/theme/list", "/etc/pc/theme"]
# 장중 2차 진단에서 구조를 볼 페이지 — 상승 이유(재료) 후보
REASON_PAGES = ["/stock/top30/news/list", "/stock/top30/list", "/stock/real30/ymd/list", "/stock/event/list"]
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
            start = next((i for i, x in enumerate(vt) if x == "정부일정"), 40) + 1   # 공통 메뉴 다음부터
            n = 220 if path == "/theme/rank/list" else 30
            print(f"- 메뉴 다음 본문 글자 (최대 {n}줄):")
            for x in vt[start:start + n]:
                print(f"    {x[:160]}")
            if path == "/theme/rank/list":
                classes = re.findall(r'class="([^"]+)"', r.text)
                top = collections.Counter(c for cl in classes for c in cl.split()).most_common(25)
                print(f"- 자주 나오는 class: {top}")
                signed = re.findall(r"[+-]\d+\.\d+", r.text)
                links = sorted(set(re.findall(r'href="(/theme/[a-z]+/[^"?]*)', r.text)))[:10]
                stamps = sorted(set(re.findall(r"(?:기준|업데이트|갱신)[^<]{0,30}", r.text)))[:10]
                print(f"- 부호 붙은 소수: {len(signed)}개 · 예: {signed[:8]}")
                print(f"- 링크 패턴: {links}")
                print(f"- 기준 시각 후보: {stamps}")
        print()
        time.sleep(2)
    return 0


HEADER = re.compile(r"(20\d\d-\d\d-\d\d)\s*(\d{1,2})시\s*테마랭킹")


def rank_state(html):
    """기준 시각 문구 · 카드 수 · 앞 테마 이름. 내용은 저장하지 않고 로그에 요약만 남긴다."""
    m = HEADER.search(html)
    vt = visible_text(html)
    start = next((i for i, x in enumerate(vt) if HEADER.search(x)), None)
    names = []
    if start is not None:
        # 카드 머리: 테마명 다음 줄이 '정수%' 이고 그다음이 '&nbsp;' 인 패턴
        for i in range(start, len(vt) - 2):
            if re.fullmatch(r"-?\d+%", vt[i + 1]) and vt[i + 2] in ("&nbsp;",) and not vt[i].startswith("&nbsp;"):
                if i + 3 < len(vt) and not vt[i + 3].startswith("&nbsp;"):
                    names.append(f"{vt[i]}({vt[i + 1]})")
    forms = sorted(set(re.findall(r'<(?:select|input)[^>]+name="([^"]+)"', html)))
    return {"header": f"{m.group(1)} {m.group(2)}시" if m else "", "cards": html.count("card-header"),
            "themes": names[:8], "digest": hashlib.sha1("|".join(names).encode()).hexdigest()[:10],
            "form_fields": forms[:10]}


def parse_rank(html):
    """테마랭킹 페이지 → (기준 시각 문구, [{theme, rate, stocks:[{name, rate, cap_eok, amt_eok}]}]).

    화면 글자 순서로 읽는다: 테마 카드는 '이름 / N% / &nbsp;', 종목은 '이름 / N% / &nbsp;&nbsp;시총 / 숫자 / 억 / &nbsp;거래 N억'.
    """
    m = HEADER.search(html)
    vt = visible_text(html)
    start = next((i for i, x in enumerate(vt) if HEADER.search(x)), None)
    cards = []
    if start is None:
        return (m.group(0) if m else ""), cards
    pct = re.compile(r"-?\d+%")
    i = start + 1
    while i < len(vt) - 2:
        name, rate, nxt = vt[i], vt[i + 1], vt[i + 2]
        if pct.fullmatch(rate) and not name.startswith("&nbsp;"):
            if nxt == "&nbsp;":                                   # 테마 카드 머리
                cards.append({"theme": name, "rate": int(rate[:-1]), "stocks": []})
                i += 3
                continue
            if nxt.startswith("&nbsp;&nbsp;시총") and cards:       # 종목 행
                cap = vt[i + 3].replace(",", "") if i + 3 < len(vt) else ""
                amt = re.search(r"거래\s*([\d,]+)억", vt[i + 5]) if i + 5 < len(vt) else None
                cards[-1]["stocks"].append({"name": name, "rate": int(rate[:-1]),
                                            "cap_eok": int(cap) if cap.isdigit() else None,
                                            "amt_eok": int(amt.group(1).replace(",", "")) if amt else None})
                i += 6
                continue
        i += 1
    return (m.group(0) if m else ""), cards


def dump():
    """지금 보이는 테마랭킹을 한 번 읽어 JSON 한 줄로 로그에 남긴다 (저장소에는 저장하지 않는다)."""
    import json
    r = requests.get(BASE + "/theme/rank/list", headers={"User-Agent": UA}, timeout=20)
    header, cards = parse_rank(r.text)
    print(f"# stockinfo7 테마랭킹 덤프 — {header} · 카드 {len(cards)} · 종목행 {sum(len(c['stocks']) for c in cards)}")
    print("RANK_JSON " + json.dumps({"header": header, "cards": cards}, ensure_ascii=False))
    print()
    print("\n".join(compare(cards, header.replace(" 테마랭킹", ""))))
    return 0


def compare(cards, header, snap_dir="data/market_snapshot"):
    """stockinfo7 테마랭킹 ↔ 같은 날 우리 15:05 스냅샷 구조 비교 (수익률 없음 · 탐색 구간 날짜만)."""
    import gzip, csv, os
    import hyeoks_theme_abc as T
    from hyeoks_closing_bet import read_snapshot
    day = header[:10]
    out = [f"## 같은 날 우리 스냅샷과 구조 비교 — stockinfo7 '{header}' ↔ {day} 15:05 슬롯", ""]
    if not day:
        return out + ["- 기준 날짜를 읽지 못해 비교하지 않는다"]
    # 구조만 비교한다(테마·종목 소속·대장 후보). 익일 가격·수익률을 읽지 않으므로 잠긴 연구의 확증 구간과 겹쳐도 결과 열람이 아니다.
    p15 = os.path.join(snap_dir, f"{day}_1505.csv.gz")
    if not os.path.exists(p15):
        return out + [f"- {day} 15:05 스냅샷이 없다"]
    _, rows, _ = read_snapshot(p15)
    with gzip.open(os.path.join(snap_dir, f"{day}_1505_theme.csv.gz"), "rt", encoding="utf-8") as fh:
        trows = list(csv.DictReader(fh.read().splitlines()[1:]))
    tname = {r["code"]: r["name"] for r in trows}
    norm = lambda x: re.sub(r"\s+", "", x or "")
    by_name = {}
    for code, r in rows.items():
        by_name.setdefault(norm(r.get("itemname")), code)
    members = collections.defaultdict(set)
    for code, r in rows.items():
        for t in (r.get("themeNos") or "").split("|"):
            if t:
                members[t].add(code)
    # 1) 종목명 매칭
    names = {st["name"] for c in cards for st in c["stocks"]}
    matched = {n: by_name.get(norm(n)) for n in names}
    miss = sorted(n for n, c in matched.items() if not c)
    out.append(f"- 종목명 매칭: {len(names) - len(miss)}/{len(names)}" + (f" · 못 맞춘 이름 {miss[:10]}" if miss else ""))
    # 2) 등락률 차이 (stockinfo7 16시 정수% − 우리 15:02 등락률)
    diffs = []
    for c in cards:
        for st in c["stocks"]:
            code = matched.get(st["name"])
            if code:
                diffs.append(st["rate"] - float(rows[code].get("prevChangeRate") or 0))
    if diffs:
        ad = sorted(abs(x) for x in diffs)
        out.append(f"- 등락률 차(16시 정수% − 15:02): |차| 중앙 {ad[len(ad)//2]:.2f}%p · 1%p 넘는 행 {sum(1 for x in ad if x > 1)}/{len(ad)} (반올림 ±0.5 + 시각 차)")
    # 3) 테마 대응 + 4) 대장 비교
    A, _ = T.build_A(rows)
    Bc, _ = T.build_B_current(A)
    our_lead = {x["theme"]: x["code"] for x in Bc}
    rate_rank = [r["code"] for r in sorted(trows, key=lambda r: -float(r["changeRate"] or 0))]
    out += ["", "| stockinfo7 테마 (등락률) | 맞춘 종목 | 가장 많이 겹치는 네이버 테마 (겹침) | 네이버 등락률 순위 | stockinfo7 1위 종목 | 우리 대장 후보(그 테마) |",
            "|---|--:|---|--:|---|---|"]
    same_lead = mapped = 0
    for c in cards:
        codes = {matched.get(st["name"]) for st in c["stocks"]} - {None}
        best = max(members.items(), key=lambda kv: (len(kv[1] & codes), -len(kv[1])), default=(None, set()))
        k = len(best[1] & codes) if best[0] else 0
        nt = tname.get(best[0], "—") if k else "—"
        rk = (rate_rank.index(best[0]) + 1) if k and best[0] in rate_rank else "—"
        top = c["stocks"][0]["name"] if c["stocks"] else "—"
        ours = our_lead.get(best[0]) if k else None
        ours_name = rows[ours]["itemname"] if ours else "없음"
        if k:
            mapped += 1
            same_lead += bool(ours and matched.get(top) == ours)
        out.append(f"| {c['theme']} ({c['rate']}%) | {len(codes)}/{len(c['stocks'])} | {nt} ({k}) | {rk} | {top} | {ours_name} |")
    out += ["", f"- 대응 테마를 찾은 카드 {mapped}/{len(cards)} · 그중 stockinfo7 1위 종목 = 우리 대장 후보 {same_lead}",
            f"- 우리 대장 후보 {len(Bc)}개 중 stockinfo7 카드 어디엔가 나오는 종목 "
            f"{sum(1 for x in Bc if x['code'] in set(matched.values()))}",
            "- 겹침은 종목 소속 기준 근사다. 테마 이름이 달라도 같은 묶음일 수 있고, 같은 이름이라도 구성이 다를 수 있다"]
    return out


def watch(minutes, every):
    """장중 2차 진단 — 테마랭킹 기준 시각이 언제 바뀌는지 일정 간격으로 기록한다."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9"})
    kst = datetime.timezone(datetime.timedelta(hours=9))
    print(f"# stockinfo7 장중 진단 — {minutes}분 동안 {every}초 간격 (저장 없음)\n")
    print("| 접속 시각 KST | 상태 | 기준 시각 문구 | 카드 | 목록 지문 | 앞 테마 |")
    print("|---|---|---|--:|---|---|")
    end = time.time() + minutes * 60
    prev, changes, reasons_done = None, [], False
    while True:
        t = datetime.datetime.now(kst).strftime("%H:%M:%S")
        try:
            r = s.get(BASE + "/theme/rank/list", timeout=20)
            st = rank_state(r.text)
            flag = "" if prev is None or st["digest"] == prev["digest"] and st["header"] == prev["header"] else " 🔄"
            if flag:
                changes.append((t, prev["header"], st["header"]))
            print(f"| {t}{flag} | {r.status_code} | {st['header'] or '없음'} | {st['cards']} | {st['digest']} | "
                  f"{', '.join(st['themes'][:5])} |", flush=True)
            if prev is None:
                print(f"\n- 폼 필드(날짜 조회 단서): {st['form_fields']}\n")
            prev = st
        except Exception as e:
            print(f"| {t} | 실패 | {type(e).__name__} | | | |", flush=True)
        now_kst = datetime.datetime.now(kst)
        if not reasons_done and (now_kst.hour, now_kst.minute) >= (15, 10):
            reasons_done = True
            print("\n## 재료 후보 페이지 구조 (15:10 이후 1회)\n")
            for path in REASON_PAGES:
                try:
                    rr = s.get(BASE + path, timeout=20)
                    info, vt = analyze(path, rr)
                    print(f"### {path} — {rr.status_code} · {info.get('title', '')} · 로그인폼 {info.get('login_form')}")
                    i0 = next((i for i, x in enumerate(vt) if x == "정부일정"), 40) + 1
                    body = [x for x in vt[i0:] if x not in ("-->", "&nbsp;")]
                    k = next((i for i, x in enumerate(body) if x == "배너/푸시광고"), -1) + 1
                    for x in body[k:k + 60]:
                        print(f"    {x[:160]}")
                    hrefs = sorted(set(h for h in re.findall(r'href="([^"#]+)"', rr.text)
                                       if re.search(r"top30|rank|ymd|theme|date|day", h)))
                    print(f"    · 날짜·상세 링크 후보: {hrefs[:25]}")
                    clicks = sorted(set(re.findall(r'onclick="([^"]{0,160})"', rr.text)))
                    keep = [c for c in clicks if re.search(r"20[0-9]{2}|rank|ymd|top30|text|detail", c)]
                    print(f"    · onclick 후보: {keep[:15]}")
                    print()
                except Exception as e:
                    print(f"### {path} — 실패 {type(e).__name__}")
                time.sleep(2)
            print("| 접속 시각 KST | 상태 | 기준 시각 문구 | 카드 | 목록 지문 | 앞 테마 |")
            print("|---|---|---|--:|---|---|")
        if time.time() + every > end:
            break
        time.sleep(every)
    print("\n## 바뀐 시점\n")
    for t, a, b in changes:
        print(f"- {t} KST: '{a}' → '{b}'")
    if not changes:
        print("- 관측 동안 바뀌지 않았다")
    return 0


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--dump":
        sys.exit(dump())
    if len(sys.argv) >= 2 and sys.argv[1] == "--watch":
        sys.exit(watch(int(sys.argv[2]), int(sys.argv[3])))
    sys.exit(main())
