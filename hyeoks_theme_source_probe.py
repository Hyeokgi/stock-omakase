# -*- coding: utf-8 -*-
# ==========================================================================
# 🔎 외부 테마 자료원 진단 — stockinfo7.com (읽기 전용 · 1회성 · 저장소 저장 없음)
# --------------------------------------------------------------------------
# 사용자 지시(2026-10-04): 평소 보는 테마 순위 사이트를 연구 비교 자료로 쓸 수 있는지 진단.
# 사용자가 이용약관을 확인했고 특별한 제재 사항이 없다고 했다. Codex 교차 검토(2026-10-05)는
# 약관 제7조·제22조 관련 제한 문구를 들어 자동 누적·공개 재배포 허용을 **추정하지 말라** 고 했다 —
# 운영자 확인 전에는 수집을 시작하지 않고, 공개 로그에도 화면 내용을 남기지 않는다.
#
# 묻는 것: 접속 가능? 로그인 필요? 순위가 HTML 에 바로 있나? 기준 시각이 표시되나? 언제 갱신되나?
# 하지 않는 것: 저장·커밋·반복 수집. 결과는 실행 로그와 Actions 요약에만 남는다(공개 저장소의 로그는 공개다).
#   2026-10-05 부터 로그에는 **구조 수치만** 남긴다: 기준 시각 문구·카드 수·종목 행 수·내용 지문·매칭 집계.
#   테마 이름·종목 이름·본문 글자·카드 JSON 은 찍지 않는다.
# 요청은 페이지당 1회, 사이 간격을 둔다.
# ==========================================================================
import collections
import datetime
import hashlib
import json
import re
import sys
import time

import requests

BASE = "https://stockinfo7.com"
PAGES = ["/robots.txt", "/", "/theme/rank/list", "/theme/list", "/etc/pc/theme"]
# 장중 2차 진단에서 구조를 볼 페이지 — 상승 이유(재료) 후보
REASON_PAGES = ["/stock/top30/news/list", "/stock/top30/list", "/stock/real30/ymd/list", "/stock/event/list"]
UA = "Mozilla/5.0 (research-probe; HYEOKS stock-omakase; contact via GitHub repo)"
KST = datetime.timezone(datetime.timedelta(hours=9))


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
        elif vt and path == "/theme/rank/list":
            st = rank_state(r.text)
            print(f"- 기준 시각 문구: {st['header'] or '없음'} · 카드 {st['cards']} · 종목 행 {st['rows']} · 내용 지문 {st['digest']}")
        print()
        time.sleep(2)
    return 0


HEADER = re.compile(r"(20\d\d-\d\d-\d\d)\s*(\d{1,2})시\s*테마랭킹")


def rank_state(html):
    """기준 시각 문구 · 카드 수 · 종목 행 수 · **화면 전체 내용 지문**. 이름은 내보내지 않는다.

    🔴 2026-10-05 (Codex 교차 검토) — 예전 지문은 테마 이름과 테마 등락률만 봤다. 같은 시각·같은 테마에서
    구성 종목만 바뀌면 '변화 없음' 으로 찍혔다. 이제 카드·종목·숫자 전체를 정규화해 지문을 만들고,
    기준 시각 문구는 따로 둔다."""
    header, cards = parse_rank(html)
    m = HEADER.search(html)
    canon = json.dumps(cards, ensure_ascii=False, sort_keys=True)
    forms = sorted(set(re.findall(r'<(?:select|input)[^>]+name="([^"]+)"', html)))
    return {"header": f"{m.group(1)} {m.group(2)}시" if m else "", "cards": len(cards),
            "rows": sum(len(c["stocks"]) for c in cards),
            "digest": hashlib.sha1(canon.encode("utf-8")).hexdigest()[:12] if cards else "",
            "form_fields": forms[:10]}


def classify(status_code, state, prev):
    """한 번의 관측을 나눈다: HTTP오류 / 파싱실패 / 첫관측 / 동일 / 갱신. (접속실패는 호출부에서)"""
    if status_code != 200:
        return "HTTP오류"
    if not state["header"] or not state["cards"]:
        return "파싱실패"
    if prev is None:
        return "첫관측"
    if state["header"] == prev["header"] and state["digest"] == prev["digest"]:
        return "동일"
    return "갱신"


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


def dump(get=None):
    """지금 보이는 테마랭킹을 한 번 읽어 **구조만** 로그에 남긴다 (저장소 저장 없음, 카드 내용은 찍지 않는다).
    HTTP 오류·기준 시각 없음·카드 없음이면 종료코드 1."""
    get = get or requests.get
    t0 = datetime.datetime.now(KST)
    r = get(BASE + "/theme/rank/list", headers={"User-Agent": UA}, timeout=20)
    t1 = datetime.datetime.now(KST)
    header, cards = parse_rank(r.text) if r.status_code == 200 else ("", [])
    st = rank_state(r.text) if r.status_code == 200 else {"header": "", "cards": 0, "rows": 0, "digest": ""}
    kind = classify(r.status_code, st, None)
    print(f"# stockinfo7 테마랭킹 — 요청 {t0.isoformat(timespec='seconds')} · 수신 {t1.isoformat(timespec='seconds')} · "
          f"HTTP {r.status_code} · {kind} · {header or '기준 시각 없음'} · 카드 {len(cards)} · "
          f"종목행 {sum(len(c['stocks']) for c in cards)} · 내용 지문 {st['digest'] or '—'}")
    if kind != "첫관측":
        return 1
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
    by_name = collections.defaultdict(set)
    for code, r in rows.items():
        by_name[norm(r.get("itemname"))].add(code)
    members = collections.defaultdict(set)
    for code, r in rows.items():
        for t in (r.get("themeNos") or "").split("|"):
            if t:
                members[t].add(code)
    # 1) 종목명 매칭 — 같은 이름이 여러 코드면 모호로 빼고 따로 센다(첫 코드를 고르지 않는다)
    names = {st["name"] for c in cards for st in c["stocks"]}
    matched, ambiguous = {}, 0
    for n in names:
        hits = by_name.get(norm(n), set())
        if len(hits) > 1:
            ambiguous += 1
        matched[n] = next(iter(hits)) if len(hits) == 1 else None
    miss = sum(1 for n, c in matched.items() if not c) - ambiguous
    out.append(f"- 종목명 매칭: 유일 {len(names) - miss - ambiguous}/{len(names)} · 모호 {ambiguous} · 못 맞춤 {miss}")
    # 2) 등락률 차이 (stockinfo7 정수% − 우리 15:02 등락률). 정수 표기가 반올림인지 버림인지는 확인하지 않았다.
    diffs = []
    for c in cards:
        for st in c["stocks"]:
            code = matched.get(st["name"])
            if code:
                diffs.append(st["rate"] - float(rows[code].get("prevChangeRate") or 0))
    if diffs:
        ad = sorted(abs(x) for x in diffs)
        out.append(f"- 등락률 차(화면 정수% − 15:02): |차| 중앙 {ad[len(ad)//2]:.2f}%p · 1%p 넘는 행 {sum(1 for x in ad if x > 1)}/{len(ad)} "
                   "(표기 방식 미확인 + 시각 차)")
    # 3) 테마 대응 + 4) 화면 선두 종목 vs 우리 대장 후보 — 집계만 (이름은 공개 로그에 남기지 않는다)
    A, _ = T.build_A(rows)
    Bc, _ = T.build_B_current(A)
    our_lead = {x["theme"]: x["code"] for x in Bc}
    same_lead = mapped = 0
    for c in cards:
        codes = {matched.get(st["name"]) for st in c["stocks"]} - {None}
        best = max(members.items(), key=lambda kv: (len(kv[1] & codes), -len(kv[1])), default=(None, set()))
        k = len(best[1] & codes) if best[0] else 0
        top = c["stocks"][0]["name"] if c["stocks"] else None
        ours = our_lead.get(best[0]) if k else None
        if k:
            mapped += 1
            same_lead += bool(ours and top and matched.get(top) == ours)
    out += [f"- 대응 테마를 찾은 카드 {mapped}/{len(cards)} · 그중 화면 선두 종목 = 우리 대장 후보 {same_lead}",
            f"- 우리 대장 후보 {len(Bc)}개 중 화면 어딘가에 나오는 종목 "
            f"{sum(1 for x in Bc if x['code'] in set(v for v in matched.values() if v))}",
            "- 이 화면은 상승률 상위 종목이 속한 테마만 보여 주는 선별 화면이다 — 테마 전체 구성이 아니다",
            "- 겹침은 종목 소속 기준 근사다. 시각·후보 모집단·선택법·대응 방식이 함께 다르므로 차이를 분류 효과로 단정하지 않는다"]
    return out


def watch(minutes, every, get=None, sleep=time.sleep, now=None, deadline=None):
    """장중 2차 진단 — 테마랭킹 기준 시각·내용이 언제 바뀌는지 일정 간격으로 기록한다.
    각 관측: 요청 시작·수신 완료(KST 날짜 포함) · HTTP · 분류(접속실패/HTTP오류/파싱실패/첫관측/동일/갱신) ·
    기준 시각 문구 · 카드·종목 행 수 · 내용 지문. 유효 관측이 하나도 없으면 종료코드 1.
    간격 관측이라 갱신 시각은 '앞 관측 ~ 이 관측 사이' 로만 알 수 있다."""
    if get is None:
        sess = requests.Session()
        sess.headers.update({"User-Agent": UA, "Accept-Language": "ko-KR,ko;q=0.9"})
        get = sess.get
    now = now or (lambda: datetime.datetime.now(KST))
    end = deadline or (time.time() + minutes * 60)
    print(f"# stockinfo7 장중 진단 — {minutes}분 동안 {every}초 간격 (저장 없음 · 구조 수치만)\n")
    cols = "| 요청 시작 KST | 수신 완료 KST | HTTP | 분류 | 기준 시각 문구 | 카드 | 종목 행 | 내용 지문 |"
    print(cols)
    print("|---|---|--:|---|---|--:|--:|---|")
    prev, prev_t, changes, kinds, reasons_done = None, None, [], collections.Counter(), False
    while True:
        t0 = now()
        try:
            r = get(BASE + "/theme/rank/list", timeout=20)
            t1 = now()
            st = rank_state(r.text) if r.status_code == 200 else {"header": "", "cards": 0, "rows": 0, "digest": ""}
            kind = classify(r.status_code, st, prev)
            print(f"| {t0.isoformat(timespec='seconds')} | {t1.isoformat(timespec='seconds')} | {r.status_code} | {kind} | "
                  f"{st['header'] or '없음'} | {st['cards']} | {st['rows']} | {st['digest'] or '—'} |", flush=True)
            if kind == "갱신":
                changes.append((prev_t, t1.isoformat(timespec="seconds"), prev["header"], st["header"]))
            if kind in ("첫관측", "동일", "갱신"):
                if prev is None:
                    print(f"\n- 폼 필드(날짜 조회 단서): {st.get('form_fields', [])}\n")
                prev, prev_t = st, t1.isoformat(timespec="seconds")
        except Exception as e:
            kind = "접속실패"
            print(f"| {t0.isoformat(timespec='seconds')} | — | — | 접속실패 | {type(e).__name__} | | | |", flush=True)
        kinds[kind] += 1
        n = now()
        if not reasons_done and (n.hour, n.minute) >= (15, 10):
            reasons_done = True
            print("\n## 재료 후보 페이지 구조 (15:10 이후 1회 · 본문 글자는 찍지 않는다)\n")
            for path in REASON_PAGES:
                try:
                    rr = get(BASE + path, timeout=20)
                    info, vt = analyze(path, rr)
                    hrefs = sorted(set(h for h in re.findall(r'href="([^"#]+)"', rr.text)
                                       if re.search(r"top30|rank|ymd|theme|date|day", h)))
                    clicks = sorted(set(re.findall(r'onclick="([^"]{0,160})"', rr.text)))
                    keep = [c for c in clicks if re.search(r"20[0-9]{2}|rank|ymd|top30|text|detail", c)]
                    print(f"### {path} — HTTP {rr.status_code} · 로그인폼 {info.get('login_form')} · 본문 줄 {len(vt)}")
                    print(f"    · 날짜·상세 링크 경로 후보: {hrefs[:25]}")
                    print(f"    · onclick 함수 후보: {keep[:15]}\n")
                except Exception as e:
                    print(f"### {path} — 접속실패 {type(e).__name__}")
                sleep(2)
            print(cols)
            print("|---|---|--:|---|---|--:|--:|---|")
        if time.time() + every > end:
            break
        sleep(every)
    print("\n## 바뀐 시점 (앞 유효 관측 수신 ~ 이 관측 수신 사이에 바뀌었다)\n")
    for a, b, h0, h1 in changes:
        print(f"- {a} ~ {b}: '{h0}' → '{h1}'")
    if not changes:
        print("- 관측 동안 바뀌지 않았다")
    print(f"\n- 관측 분류: {dict(kinds)}")
    return 0 if prev is not None else 1


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--dump":
        sys.exit(dump())
    if len(sys.argv) >= 2 and sys.argv[1] == "--watch":
        sys.exit(watch(int(sys.argv[2]), int(sys.argv[3])))
    sys.exit(main())
