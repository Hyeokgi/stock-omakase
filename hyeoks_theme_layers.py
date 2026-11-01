# -*- coding: utf-8 -*-
# ==========================================================================
# 🧭 테마 3층 분류 — 테마 일지 (연구 전용, theme-layers-v1)
# --------------------------------------------------------------------------
# 근거: 허브 §3 #10 · Codex 제언(2026-10-04) §2. 사용자 결정 2026-11-01 — 규칙 기반 키워드, LLM 없음.
#   ① 기본 소속 — 15:05 스냅샷의 종목별 themeNos(다중 소속 그대로)
#   ② 당일 재료 — 15:05 까지 본 한투 뉴스 제목 · DART 공시를 종목코드로 붙이고,
#      같은 테마에서 오른 종목 둘 이상에 공통으로 나온 낱말을 재료 후보로 잡는다(확인 · 단일 · 미확인)
#   ③ 시장 반응 — 테마 등락률 · 상승 비율 · 거래대금 · 1등 쏠림 · 13:00 대비 · 전 거래일 대장 대비
# 하지 않는 것: 수익률 · 다음 날 결과 계산, 선정 조건 · 운영 Gate 연결, 시트 쓰기. 지문 대상 파일이 아니다.
# 공개 출력은 건수만. 테마 이름 · 종목 · 뉴스 제목은 비공개 파일에만 둔다.
# ==========================================================================
import argparse
import csv
import datetime
import glob
import gzip
import io
import json
import os
import re
import sys

VERSION = "theme-layers-v1"
SNAP_DIR = "data/market_snapshot"
RUNS_DIR = "data/research_runs"
CUT = "15:05:00"
MOVER = 5.0              # 테마 안에서 '오른 종목' 의 등락률 문턱(%)
MAX_MOVERS = 10          # 테마당 재료를 찾는 오른 종목 수 상한(등락률 순)
TOP_VALUE, TOP_RATE = 20, 10   # 고르는 테마 — 거래대금 상위 · 등락률 상위의 합집합
LIST_NEWS = 5            # 관련 종목이 이만큼 이상인 뉴스는 '목록 뉴스' 로 보고 재료 낱말 집계에서 뺀다
MIN_TOKEN = 2
JOSA = ("에서", "으로", "까지", "부터", "에게", "은", "는", "이", "가", "을", "를", "의", "에", "도", "로", "와", "과", "만")
STOP = set("""
특징주 특징 상승 상승세 급등 급등세 하락 하락세 급락 강세 약세 주가 신고가 상한가 하한가 거래 거래량 거래대금 매수 매도 순매수 순매도
외국인 기관 개인 종목 관련주 관련 테마 테마주 코스피 코스닥 장중 장초반 오전 오후 마감 시황 증시 전망 투자 투자자 목표가 목표주가 상향 하향 기업 주식
오늘 내일 공시 속보 단독 종합 업데이트 기대 기대감 소식 영향 효과 이유 전일 전날 대비 돌파 반등 대장 대장주 수혜 수혜주 부각 주목 연속 사흘 이틀 나흘
최고 최대 최저 대규모 추진 확대 개시 발표 공개 진행 예정 확정 가능성 우려 경고 주의 관련해 위해 통해 대한 따른 위한 있다 없다 한다 했다 된다
억원 조원 만원 천만 백만 이상 이하 미만 사상 역대 최초 올해 내년 지난해 분기 상반기 하반기 매출 영업이익 순이익 실적 흑자 적자
주요공시 기업공시 시간외 단일가 프로그램 체결 호가 상위 하위 리포트 증권 연구원 애널리스트 의견 유지 커버리지
""".split())
TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣]+")


# ── 입력 읽기 ─────────────────────────────────────────────────────────
def read_snapshot(path):
    """스냅샷 csv.gz → (메타 dict, 행 목록). 첫 줄 '#meta,...' 를 메타로 읽는다."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        first = fh.readline()
        rows = list(csv.DictReader(fh))
    meta = {}
    for part in next(csv.reader([first.strip()]))[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            meta[k] = v
    return meta, rows


def num(x, default=0.0):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return default


def news_codes(x):
    out = []
    for i in range(1, 11):
        c = str(x.get(f"iscd{i}") or "").strip()
        if c and c not in out:
            out.append(c.zfill(6) if c.isdigit() else c)
    return out


def merge_news(files_rows, day, since):
    """여러 실행의 뉴스 행 → 키별 가장 이른 받은 시각 하나. 15:05 까지 게시 · 수신된 것, `since`(전 거래일 장 마감) 이후만."""
    cut = f"{day}T{CUT}+09:00"
    best = {}
    for x in files_rows:
        p, r = x.get("published_at") or "", x.get("received_at") or ""
        if not p or not r or p > cut or r > cut or p < since:
            continue
        k = x.get("key") or x.get("cntt_usiq_srno")
        if k not in best or r < best[k]["received_at"]:
            best[k] = x
    return list(best.values())


def merge_dart(files_rows, day):
    """DART 행 → 접수번호별 가장 이른 first_seen_at, 15:05 까지 본 것만."""
    cut = f"{day}T{CUT}+09:00"
    best = {}
    for x in files_rows:
        s = x.get("first_seen_at") or ""
        if not s or s > cut:
            continue
        k = x.get("rcept_no")
        if k not in best or s < best[k]["first_seen_at"]:
            best[k] = x
    return list(best.values())


# ── 낱말 ─────────────────────────────────────────────────────────────
def tokens(title, drop=()):
    """제목 → 재료 낱말 집합. 괄호 머리말 · 숫자 · 불용어 · 종목 자신의 이름을 뺀다."""
    t = re.sub(r"[\[\(【<][^\]\)】>]{0,12}[\]\)】>]", " ", str(title or ""))
    out = set()
    for w in TOKEN_RE.findall(t):
        for j in JOSA:
            if len(w) > len(j) + 1 and w.endswith(j):
                w = w[: -len(j)]
                break
        if len(w) < MIN_TOKEN or w.isdigit() or w in STOP or w in drop:
            continue
        if re.fullmatch(r"[0-9]+[가-힣A-Za-z%]{0,2}", w):        # 3분기 · 10배 · 30% 같은 숫자 낱말
            continue
        out.add(w)
    return out


# ── 층 ①③ — 소속과 반응 ─────────────────────────────────────────────
def stock_table(rows):
    out = {}
    for r in rows:
        code = str(r.get("itemcode") or "").strip()
        if not code:
            continue
        out[code] = {"code": code, "name": r.get("itemname") or "", "rate": num(r.get("prevChangeRate")),
                     "amount": num(r.get("tradeAmount")),
                     "themes": [t for t in str(r.get("themeNos") or "").split("|") if t],
                     "topTheme": str(r.get("topThemeNo") or "")}
    return out


def pick_themes(theme_rows):
    ok = [t for t in theme_rows if (t.get("membershipStatus") or "OK") == "OK" and num(t.get("membershipCount")) >= 2]
    by_value = sorted(ok, key=lambda t: -num(t.get("totalTradingValue")))[:TOP_VALUE]
    by_rate = sorted(ok, key=lambda t: -num(t.get("changeRate")))[:TOP_RATE]
    seen, out = set(), []
    for t in by_value + by_rate:
        if t["code"] not in seen:
            seen.add(t["code"])
            out.append(t)
    return out


def members_of(stocks):
    m = {}
    for s in stocks.values():
        for t in s["themes"]:
            m.setdefault(t, []).append(s)
    return m


def reaction(theme, members, before=None, prev_leader=""):
    """층 ③. before = 13:00 같은 테마의 (등락률, 대장 코드). prev_leader = 전 거래일 거래대금 1등."""
    up = int(num(theme.get("risingCount")))
    down = int(num(theme.get("fallingCount")))
    total = sum(s["amount"] for s in members)
    lead_v = max(members, key=lambda s: s["amount"]) if members else None
    lead_r = max(members, key=lambda s: s["rate"]) if members else None
    out = {"rate": num(theme.get("changeRate")), "up": up, "down": down,
           "breadth": round(up / (up + down), 3) if up + down else None,
           "amount": total, "top1Share": round(lead_v["amount"] / total, 3) if total and lead_v else None,
           "leaderValue": lead_v["code"] if lead_v else "", "leaderRate": lead_r["code"] if lead_r else "",
           "movers": sum(1 for s in members if s["rate"] >= MOVER)}
    if before:
        out["rate1300"], out["leader1300Same"] = before[0], (before[1] == out["leaderValue"])
    if prev_leader:
        out["prevLeaderSame"] = prev_leader == out["leaderValue"]
    return out


# ── 층 ② — 재료 ─────────────────────────────────────────────────────
def evidence_index(news, dart):
    """종목코드 → [(시각, 출처, 키, 제목, 목록뉴스 여부)]."""
    idx = {}
    for x in news:
        codes = news_codes(x)
        listy = len(codes) >= LIST_NEWS
        for c in codes:
            idx.setdefault(c, []).append((x["received_at"] if x["received_at"] > x["published_at"] else x["published_at"],
                                          "news", x.get("key") or "", x.get("hts_pbnt_titl_cntt") or "", listy))
    for x in dart:
        c = str(x.get("stock_code") or "").strip()
        if c:
            idx.setdefault(c, []).append((x["first_seen_at"], "dart", x.get("rcept_no") or "", x.get("report_nm") or "",
                                          False))
    for v in idx.values():
        v.sort()
    return idx


def material(theme_name, movers, idx):
    """층 ②. movers = 오른 종목(등락률 순). 공통 낱말이 오른 종목 둘 이상에 나오면 '확인'."""
    movers = movers[:MAX_MOVERS]
    per, first = {}, {}
    with_ev = 0
    for s in movers:
        ev = idx.get(s["code"], [])
        if any(not listy for *_, listy in ev):          # 목록 뉴스만 걸린 종목은 근거 없음으로 센다
            with_ev += 1
        drop = tokens(s["name"]) | {s["name"]}
        mine = set()
        for at, src, key, title, listy in ev:
            if listy:
                continue
            for w in tokens(title, drop):
                mine.add(w)
                if w not in first or at < first[w][0]:
                    first[w] = (at, src, key)
        per[s["code"]] = mine
    support = {}
    for code, ws in per.items():
        for w in ws:
            support.setdefault(w, set()).add(code)
    ranked = sorted(((len(c), w) for w, c in support.items() if len(c) >= 2), key=lambda x: (-x[0], first[x[1]][0], x[1]))
    name_words = tokens(theme_name)
    out = {"movers": len(movers), "moversWithEvidence": with_ev, "keywords": [], "support": 0, "firstAt": "",
           "themeNameHit": sum(1 for ws in per.values() if ws & name_words)}
    if ranked:
        top = ranked[0][1]
        out.update(status="확인", keywords=[w for _, w in ranked[:3]], support=ranked[0][0],
                   firstAt=first[top][0], firstSource=first[top][1], firstKey=first[top][2],
                   supporters=sorted(support[top]))
    elif with_ev:
        out["status"] = "단일"
    else:
        out["status"] = "미확인"
    return out


# ── 하루 묶기 ───────────────────────────────────────────────────────
def build_day(day, snap, theme_rows, news, dart, snap1300=None, theme1300=None, prev_theme_rows=None):
    """하루 테마 일지 — (테마 행들, 종목 행들, 공개 요약)."""
    stocks = stock_table(snap)
    mem = members_of(stocks)
    names = {t["code"]: t.get("name") or "" for t in theme_rows}
    before = {}
    if snap1300 is not None and theme1300 is not None:
        m13 = members_of(stock_table(snap1300))
        for t in theme1300:
            ms = m13.get(t["code"], [])
            before[t["code"]] = (num(t.get("changeRate")), max(ms, key=lambda s: s["amount"])["code"] if ms else "")
    prev = {}
    for t in prev_theme_rows or []:
        top = str(t.get("topByTradingValue") or "").split("|")[0].split(":")[0]
        prev[t["code"]] = top
    idx = evidence_index(news, dart)
    themes, by_stock = [], {}
    for t in pick_themes(theme_rows):
        ms = mem.get(t["code"], [])
        movers = sorted([s for s in ms if s["rate"] >= MOVER], key=lambda s: -s["rate"])
        row = {"day": day, "themeNo": t["code"], "themeName": names[t["code"]], "members": len(ms),
               **reaction(t, ms, before.get(t["code"]), prev.get(t["code"], "")),
               **{f"m_{k}": v for k, v in material(names[t["code"]], movers, idx).items()}}
        themes.append(row)
        for s in movers:
            by_stock.setdefault(s["code"], []).append(row)
    stocks_out = []
    for code, rows in by_stock.items():
        s = stocks[code]
        confirmed = [r for r in rows if r["m_status"] == "확인" and code in r.get("m_supporters", [])]
        stocks_out.append({"day": day, "code": code, "name": s["name"], "rate": s["rate"], "themesAll": len(s["themes"]),
                           "topTheme": s["topTheme"], "materialThemes": "|".join(r["themeNo"] for r in confirmed),
                           "keywords": "|".join(sorted({w for r in confirmed for w in r["m_keywords"]})),
                           "topThemeIsMaterial": s["topTheme"] in {r["themeNo"] for r in confirmed},
                           "evidence": len(idx.get(code, []))})
    st = {"day": day, "version": VERSION, "themes": len(themes),
          "confirmed": sum(1 for r in themes if r["m_status"] == "확인"),
          "single": sum(1 for r in themes if r["m_status"] == "단일"),
          "unknown": sum(1 for r in themes if r["m_status"] == "미확인"),
          "movers": len(stocks_out), "moversWithMaterialTheme": sum(1 for r in stocks_out if r["materialThemes"]),
          "topThemeIsMaterial": sum(1 for r in stocks_out if r["topThemeIsMaterial"]),
          "news1505": len(news), "dart1505": len(dart)}
    return themes, stocks_out, st


# ── 하루 실행(PC · 깃허브 공통) ──────────────────────────────────────
def load_raw(raw_dir, kind, day):
    rows = []
    for p in sorted(glob.glob(os.path.join(raw_dir, f"research_{kind}_{day}_*.json.gz"))):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            rows.extend(json.load(fh).get("rows") or [])
    return rows


def prev_day(day, snap_dir):
    days = sorted({os.path.basename(p)[:10] for p in glob.glob(os.path.join(snap_dir, "*_1505_theme.csv.gz"))})
    earlier = [d for d in days if d < day]
    return earlier[-1] if earlier else ""


def classify_day(day, news_rows, dart_rows, snap_dir=SNAP_DIR):
    """(테마 행, 종목 행, 요약). 15:05 스냅샷이 없으면 FileNotFoundError."""
    def snapf(slot, kind=""):
        p = os.path.join(snap_dir, f"{day}_{slot}{'_' + kind if kind else ''}.csv.gz")
        return read_snapshot(p) if os.path.exists(p) else (None, None)
    meta, snap = snapf("1505")
    _, themes = snapf("1505", "theme")
    if snap is None or themes is None:
        raise FileNotFoundError(f"{day} 15:05 스냅샷 없음")
    pd_ = prev_day(day, snap_dir)
    since = f"{pd_}T15:30:00+09:00" if pd_ else f"{day}T00:00:00+09:00"
    news = merge_news(news_rows, day, since)
    dart = merge_dart(dart_rows, day)
    prev_rows = read_snapshot(os.path.join(snap_dir, f"{pd_}_1505_theme.csv.gz"))[1] if pd_ else None
    rows, stocks, st = build_day(day, snap, themes, news, dart, snapf("1300")[1], snapf("1300", "theme")[1], prev_rows)
    st["snapshotStatus"] = meta.get("themeMembershipStatus", "")
    st["newsFrom"] = min((x["received_at"][11:16] for x in news), default="")
    st["newsTo"] = max((x["received_at"][11:16] for x in news), default="")
    return rows, stocks, st


def write_csv(path, rows):
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows({k: ("|".join(v) if isinstance(v, list) else v) for k, v in r.items()} for r in rows)


# ── 깃허브 — 원자료 봉인 사본 넘기기 ────────────────────────────────
#    드라이브 창구는 쓰기 전용이라 깃허브 작업이 다시 읽을 수 없다(사용자 결정 2026-11-01).
#    수집 작업이 러너 사본을 private_store 와 같은 설정으로 봉인해 짧게 남기고, 분류 작업이 받아 푼다.
ART_PREFIX = "material"
WORKFLOWS = {"kisnews": "kis_news.yml", "dart": "dart_disclosures.yml"}
GH_API = "https://api.github.com"
RUN_COLS = ["day", "runId", "version", "startedAt", "finishedAt", "status", "reason", "newsRuns", "newsArtifacts",
            "dartRuns", "dartArtifacts", "news1505", "dart1505", "newsFrom", "newsTo", "dartCoverage", "snapshotStatus",
            "themes", "confirmed", "single", "unknown", "movers", "moversWithMaterialTheme", "topThemeIsMaterial",
            "privateFile", "privateStatus"]


def seal_dir(kind, src_dir, out_dir, key):
    """러너 사본 research_<kind>_*.json.gz → out_dir/*.enc. 키가 없으면 아무것도 만들지 않는다(평문 금지)."""
    import private_store
    if not key:
        print("ℹ️ PRIVATE_STORE_KEY 없음 — 봉인 사본을 만들지 않는다")
        return 0
    os.makedirs(out_dir, exist_ok=True)
    n = 0
    for p in sorted(glob.glob(os.path.join(src_dir, f"research_{kind}_*.json.gz"))):
        with open(p, "rb") as fh:
            blob = private_store.seal(fh.read(), key)
        with open(os.path.join(out_dir, os.path.basename(p) + ".enc"), "wb") as fh:
            fh.write(blob)
        n += 1
    print(f"🔒 봉인 사본 {n}개 ({kind})")
    return n


def gh_headers(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}


def kst_day(iso_utc):
    t = datetime.datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    return (t + datetime.timedelta(hours=9)).strftime("%Y-%m-%d")


def day_artifacts(get, repo, token, day):
    """{kind: {runId: 다운로드 주소}} — 그날(KST) 만든 수집 실행의 봉인 사본."""
    out = {k: {} for k in WORKFLOWS}
    for kind, wf in WORKFLOWS.items():
        r = get(f"{GH_API}/repos/{repo}/actions/workflows/{wf}/runs", headers=gh_headers(token), timeout=30,
                params={"per_page": 50})
        r.raise_for_status()
        for run in r.json().get("workflow_runs", []):
            if kst_day(run["created_at"]) != day:
                continue
            a = get(f"{GH_API}/repos/{repo}/actions/runs/{run['id']}/artifacts", headers=gh_headers(token), timeout=30)
            a.raise_for_status()
            for art in a.json().get("artifacts", []):
                if art.get("name") == f"{ART_PREFIX}-{kind}-{run['id']}" and not art.get("expired"):
                    out[kind][str(run["id"])] = art["archive_download_url"]
    return out


def open_artifact(get, url, token, key):
    """봉인 사본 zip → 원자료 행 목록."""
    import zipfile
    import private_store
    r = get(url, headers=gh_headers(token), timeout=120)
    r.raise_for_status()
    rows = []
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        for name in sorted(z.namelist()):
            if name.endswith(".json.gz.enc"):
                data = gzip.decompress(private_store.unseal(z.read(name), key))
                rows.extend(json.loads(data.decode("utf-8")).get("rows") or [])
    return rows


def ledger(name, day, runs_dir=RUNS_DIR):
    try:
        with open(os.path.join(runs_dir, name), encoding="utf-8", newline="") as fh:
            return [r for r in csv.DictReader(fh) if r.get("day") == day]
    except FileNotFoundError:
        return []


def expected_runs(day, runs_dir=RUNS_DIR):
    """공개 장부로 본 그날 수집 — ({kind: 원자료가 있는 runId 집합}, (뉴스 09:30 덮음, 15:00 덮음), DART 15:05 범위)."""
    news = [r for r in ledger("kis_news_runs.csv", day, runs_dir)
            if r.get("privateStatus") == "접수" and num(r.get("news")) > 0]
    dart = [r for r in ledger("dart_runs.csv", day, runs_dir)
            if r.get("privateStatus") == "접수" and num(r.get("disclosures")) > 0]
    covered = (any(r["startedAt"][11:16] <= "09:30" for r in news), any(r["finishedAt"][11:16] >= "15:00" for r in news))
    cov = "완전" if any(r.get("coverage1505") == "완전" for r in dart) else "불완전"
    return {"kisnews": {r["runId"] for r in news}, "dart": {r["runId"] for r in dart}}, covered, cov


def done_today(day, runs_dir=RUNS_DIR):
    return any(r.get("status") == "완료" for r in ledger("theme_layers_runs.csv", day, runs_dir))


def log_run(st, runs_dir=RUNS_DIR):
    os.makedirs(runs_dir, exist_ok=True)
    path = os.path.join(runs_dir, "theme_layers_runs.csv")
    new = not os.path.exists(path)
    with open(path, "a", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=RUN_COLS, extrasaction="ignore", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(st)


def run_github(day, get, repo, token, key, now, uploader, run_id="", runs_dir=RUNS_DIR, snap_dir=SNAP_DIR):
    """깃허브 실행 하루. 공개 상태 dict(건수만)를 돌려준다. 기록하지 않는 생략이면 None."""
    import hyeoks_research_daily as R
    if now.strftime("%H:%M") < "15:08":
        print(f"ℹ️ {now:%H:%M} — 15:08 전이라 아직 하지 않는다")
        return None
    if done_today(day, runs_dir):
        print(f"ℹ️ {day} 테마 일지는 이미 완료 — 생략")
        return None
    st = {"day": day, "runId": run_id, "version": VERSION, "startedAt": now.isoformat(timespec="seconds"),
          "status": "", "reason": ""}
    if not key:
        st.update(status="실패", reason="PRIVATE_STORE_KEY 없음")
        return st
    want, (morning, afternoon), cov = expected_runs(day, runs_dir)
    have = day_artifacts(get, repo, token, day)
    news_rows, dart_rows = [], []
    for kind, bucket in (("kisnews", news_rows), ("dart", dart_rows)):
        for _, url in sorted(have[kind].items()):
            bucket.extend(open_artifact(get, url, token, key))
    st.update(newsRuns=len(want["kisnews"]), newsArtifacts=len(have["kisnews"]), dartRuns=len(want["dart"]),
              dartArtifacts=len(have["dart"]), dartCoverage=cov)
    try:
        themes, stocks, sm = classify_day(day, news_rows, dart_rows, snap_dir)
    except FileNotFoundError as e:
        st.update(status="실패", reason=str(e))
        return st
    st.update({k: sm[k] for k in RUN_COLS if k in sm and k not in ("day", "version")})
    missing = [k for k in WORKFLOWS if want[k] - set(have[k])]
    gaps = ([f"원자료 사본 없음({'·'.join(missing)})"] if missing else []) + \
           ([] if morning else ["뉴스 아침 실행 없음"]) + ([] if afternoon else ["뉴스 오후 실행 없음"]) + \
           ([] if cov == "완전" else ["DART 15:05 범위 불완전"]) + \
           ([] if sm.get("snapshotStatus") == "OK" else [f"테마 구성 {sm.get('snapshotStatus') or '미상'}"])
    st.update(status="부분" if gaps else "완료", reason="; ".join(gaps))
    name = f"research_themelayers_{day}_{run_id or 'local'}.json.gz"
    data = gzip.compress(json.dumps({"version": VERSION, "day": day, "runId": run_id, "summary": sm,
                                     "themes": themes, "stocks": stocks}, ensure_ascii=False).encode("utf-8"), mtime=0)
    rc = R.upload_receipt("themelayers", run_id, name, data, uploader)
    R.append_receipts([rc], runs_dir)
    st.update(privateFile=name, privateStatus=rc["status"])
    if rc["status"] != "접수":
        st.update(status="실패", reason=(st["reason"] + "; " if st["reason"] else "") + "드라이브 보관 실패")
    return st


def main(argv=None):
    ap = argparse.ArgumentParser(description="테마 3층 분류 — 테마 일지(연구 전용)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    lo = sub.add_parser("local", help="PC 시제품 — 내려받은 원자료 폴더로")
    lo.add_argument("--day", action="append", required=True)
    lo.add_argument("--raw", required=True, help="research_kisnews_* · research_dart_* 파일 폴더")
    lo.add_argument("--snap", default=SNAP_DIR)
    lo.add_argument("--out", required=True)
    se = sub.add_parser("seal", help="수집 작업 끝 — 러너 사본 봉인")
    se.add_argument("--kind", choices=sorted(WORKFLOWS), required=True)
    se.add_argument("--out", default="sealed")
    gh = sub.add_parser("github", help="깃허브 — 그날 봉인 사본을 받아 분류 · 드라이브 보관")
    gh.add_argument("--day", default="")
    a = ap.parse_args(argv)
    if a.cmd == "local":
        os.makedirs(a.out, exist_ok=True)
        for day in a.day:
            themes, stocks, st = classify_day(day, load_raw(a.raw, "kisnews", day), load_raw(a.raw, "dart", day), a.snap)
            for name, rows in (("themes", themes), ("stocks", stocks)):
                if rows:
                    write_csv(os.path.join(a.out, f"{day}_{name}.csv"), rows)
            print("🧭 " + json.dumps(st, ensure_ascii=False))
        return 0
    import hyeoks_research_daily as R
    if a.cmd == "seal":
        seal_dir(a.kind, R.PRIVATE_DIR, a.out, os.environ.get("PRIVATE_STORE_KEY", ""))
        return 0
    import requests
    now = datetime.datetime.now(R.KST)
    day = a.day or now.strftime("%Y-%m-%d")
    ok, why = R.trading_day(day)
    if not ok:
        print(f"ℹ️ {day} 는 예정 거래일이 아니다 — 생략 {why}")
        return 0
    if day < now.strftime("%Y-%m-%d"):
        now = now.replace(hour=23, minute=59)        # 지난 날 수동 실행은 15:08 문턱을 넘긴 것으로 본다
    st = run_github(day, requests.get, os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_TOKEN"],
                    os.environ.get("PRIVATE_STORE_KEY", ""), now, R.drive_uploader(), os.environ.get("GITHUB_RUN_ID", ""))
    if st is None:
        return 0
    st["finishedAt"] = datetime.datetime.now(R.KST).isoformat(timespec="seconds")
    log_run(st)
    print("🧭 " + json.dumps({k: st.get(k) for k in RUN_COLS}, ensure_ascii=False))
    return 0 if st["status"] in ("완료", "부분") else 1


if __name__ == "__main__":
    sys.exit(main())
