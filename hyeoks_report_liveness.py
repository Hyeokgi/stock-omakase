# -*- coding: utf-8 -*-
# ==========================================================================
# 🫀 리포트 파이프라인 가동 감시 — 읽기 전용. 아무것도 쓰지 않는다
# --------------------------------------------------------------------------
# 왜 만드나 — `docs/리포트수집_설계_2026-09-15.md` D4
#   수집이 GitHub 밖(사용자 PC + Codex 앱)에 있어서, 주말에 PC 가 꺼져 있으면
#   **조용히 건너뛴다.** 분석기는 새 파일이 없으면 그냥 끝나므로
#   **"수집 실패"와 "이번 주 발간 없음"이 구분되지 않는다.** 그 구분이 이 파일의 목적이다.
#
#   9/15 백로그 실측에서 하나가 더 붙었다 — `trend.yml` 의 8건 상한은
#   **아직 한 번도 실전에서 안 돌았다**(마지막 실행 코드가 상한 커밋보다 27분 빠르다).
#   9/19 가 첫 실전이다. 그 결과를 놓치지 않으려면 지금 감시가 있어야 한다.
#
# 🔑 어떻게 구분하나 — 원천을 직접 본다
#   수집기가 만드는 파일명이 `{발간일}_hana_industry_{source_id}_{sha12}.pdf` 이고
#   `source_id` 는 하나증권 목록의 `{pid}_{bbsSeq}_{attachFileSeq}` 다.
#   그래서 **원천 목록 ∩ Drive ∩ 시트** 를 같은 키로 대조할 수 있다.
#     · 원천에 발간이 없다        → 정상(발간 없음). 수집 실패가 아니다
#     · 원천에 있는데 Drive 에 없다 → 🔴 **수집** 이 막혔다 (PC/Codex 쪽)
#     · Drive 에 있는데 시트에 없다 → 🟡 **분석** 이 밀렸다 (GitHub/Gemini 쪽, 8건 상한)
#
# ⏳ 유예 — 아직 수집할 차례가 아닌 것을 실패로 부르지 않는다
#   수집은 **토·일 21:00 KST** 에만 돈다. 화요일에 월요일 발간분이 Drive 에 없는 것은
#   정상이다. 그래서 **직전 예정 수집 시각 이전 발간분**만 '수집 대상' 으로 센다.
#   그 이후 발간분은 '대기중' 으로 따로 표시한다. 이 구분이 없으면 평일마다 거짓 경보가 난다.
#
# 무엇을 하지 않나
#   · 쓰지 않는다. 수집하지 않는다. 분석하지 않는다. 판정하지 않는다.
#   · 원천 목록을 저장하지 않는다 — 읽고 세고 버린다.
# ==========================================================================
import argparse
import datetime
import os
import sys

from hana_research import fetch, parse_listing, LIST

KST = datetime.timezone(datetime.timedelta(hours=9))
DRIVE_FOLDER_ID = "1n6FZRfEERgcGAUZKgga0CyZgDBFBJ9Og"   # hyeoks_trend.py 와 같은 값
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
TREND_SHEET = "DB_중장기"
TREND_FILE_COL = 6

COLLECT_DAYS = (5, 6)        # 토(5)·일(6). `docs/산업리포트_자동보관.md` 의 예약 요일
COLLECT_HOUR = 21            # 21:00 KST
MARKER = "_hana_industry_"
DEFAULT_WINDOW = 14          # 원천을 며칠치 훑을지. 수집 주기(7일)의 2배로 여유를 둔다

OK, LATE_COLLECT, LATE_ANALYZE = "정상", "수집지연", "분석적체"


def last_scheduled_collection(now):
    """`now` 기준 **직전** 예정 수집 시각(KST). 이보다 뒤에 나온 글은 아직 차례가 아니다."""
    d = now
    for _ in range(14):
        if d.weekday() in COLLECT_DAYS:
            slot = d.replace(hour=COLLECT_HOUR, minute=0, second=0, microsecond=0)
            if slot <= now:
                return slot
        d = (d - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59)
    return None


def source_id_of(name):
    """수집기 파일명에서 `source_id` 복원. 규약에 안 맞으면 None — 추측하지 않는다."""
    if MARKER not in str(name):
        return None
    parts = str(name).rsplit(".", 1)[0].split("_")
    #  [발간일, hana, industry, pid, bbsSeq, attachFileSeq, sha12]
    return "_".join(parts[3:6]) if len(parts) >= 7 else None


def source_ids(names):
    return {s for s in (source_id_of(n) for n in names) if s}


def scan_source(window_days, now, pages=6):
    """원천 목록에서 최근 `window_days` 일 발간분. (due, pending, 훑은페이지).

    `due`    = 직전 예정 수집 시각 **이전** 발간 → 지금쯤 Drive 에 있어야 한다
    `pending`= 그 이후 발간 → 아직 차례가 아니다
    """
    cutoff = (now - datetime.timedelta(days=window_days)).date().isoformat()
    slot = last_scheduled_collection(now)
    due, pending = {}, {}
    for page in range(1, pages + 1):
        rows = parse_listing(fetch(LIST + str(page)).decode("utf-8"))
        for r in rows:
            if r["published_at"][:10] < cutoff:
                continue
            bucket = pending if (slot and r["published_at"] > slot.isoformat()) else due
            bucket[r["source_id"]] = r
        if min(r["published_at"][:10] for r in rows) < cutoff:
            return due, pending, page
    return due, pending, pages


def manifest_ids(payload):
    """수집 manifest → (아는 source_id 전부, 중복분, 커버리지).

    🔑 왜 필요한가 — **파일명만 보면 중복 게시를 '미수집' 으로 오판한다.**
    2026-09-15 첫 실행에서 실제로 났다. 하나증권이 같은 리포트를 20:28·20:39 에
    두 번 올렸고(`2220_1288679_1` · `2220_1288680_1`), SHA256 이 같아 Drive 에는
    하나만 보관됐다. manifest 의 `duplicate_of` 가 그 연결을 갖고 있다.
    그걸 안 읽으면 감시가 **정상 동작을 실패로 부른다.**

    중복분은 대조에서 **빼는** 것이 맞다 — 원본이 이미 수집·분석됐으므로
    미수집도 미분석도 아니다.
    """
    reports = (payload or {}).get("reports") or []
    allids = {r.get("source_id") for r in reports if r.get("source_id")}
    dups = {r["source_id"] for r in reports
            if r.get("source_id") and r.get("duplicate_of")}
    cov = ((payload or {}).get("coverage_start"), (payload or {}).get("coverage_end"))
    return allids, dups, cov


def verdict(due, drive_ids, sheet_ids):
    """(상태, 미수집, 미분석). **원천에 없는 것을 문제로 만들지 않는다.**"""
    d = set(due) - drive_ids
    a = (set(due) & drive_ids) - sheet_ids
    if d:
        return LATE_COLLECT, d, a
    if a:
        return LATE_ANALYZE, d, a
    return OK, d, a


def render(now, due, pending, drive_ids, sheet_ids, pages, newest_drive, errs,
           dups=frozenset(), coverage=(None, None)):
    slot = last_scheduled_collection(now)
    #  중복 게시분은 원본이 이미 수집·분석됐다. 대조에서 뺀다 — 안 빼면 오탐이 난다.
    dup_hit = set(due) & set(dups)
    due = {k: v for k, v in due.items() if k not in dups}
    state, miss_c, miss_a = verdict(due, drive_ids, sheet_ids)
    L = [f"# 🫀 리포트 파이프라인 가동 감시 — {now.strftime('%Y-%m-%d %H:%M')} KST", ""]
    L.append(f"직전 예정 수집: **{slot.strftime('%Y-%m-%d %H:%M') if slot else '산출 불가'}** "
             f"(토·일 {COLLECT_HOUR}:00 KST)")
    L.append("")
    if errs:
        L += ["> ⚠️ 일부 조회 실패 — 아래 판정은 그만큼 불완전하다:"] + \
             [f"> - {k}: {v}" for k, v in errs.items()] + [""]

    if not due and not pending:
        L += [f"## ✅ {OK} — 발간 없음", "",
              f"최근 {DEFAULT_WINDOW}일 원천 발간이 **0건**이다. "
              "수집이 안 돈 것이 아니라 **올릴 것이 없었다.**",
              "> 이 구분이 이 감시의 존재 이유다. 둘을 섞으면 거짓 경보가 나거나 진짜 실패를 놓친다."]
    elif state == LATE_COLLECT:
        L += [f"## 🔴 {LATE_COLLECT}", "",
              f"수집 대상 **{len(due)}건** 중 Drive 에 **{len(miss_c)}건이 없다.**",
              "직전 예정 수집 시각이 지났는데 안 올라왔다 — "
              "**사용자 PC / Codex 앱 / Drive 인증** 쪽을 본다.",
              "", "누락 source_id (최대 10):",
              "```", *sorted(miss_c)[:10], "```"]
    elif state == LATE_ANALYZE:
        L += [f"## 🟡 {LATE_ANALYZE}", "",
              f"Drive 에는 있는데 시트에 **{len(miss_a)}건이 없다.** 수집은 됐고 분석이 밀렸다.",
              "`trend.yml` 의 8건 상한과 최신 우선 정렬을 본다 — "
              "**오래된 것이 뒤로 밀리는지**가 여기서 드러난다."]
    else:
        L += [f"## ✅ {OK}", "",
              f"수집 대상 **{len(due)}건**이 Drive 와 시트에 모두 있다."]

    L += ["", "| 구간 | 건수 |", "|---|--:|",
          f"| 원천 발간 — 수집 대상(직전 예정 시각 이전) | {len(due)} |",
          f"| 원천 발간 — 대기중(그 이후, 아직 차례 아님) | {len(pending)} |",
          f"| Drive 보관(규약 파일명) | {len(drive_ids)} |",
          f"| 시트 적재 `{TREND_SHEET}` | {len(sheet_ids)} |",
          f"| 중복 게시로 대조 제외 | {len(dup_hit)} |",
          f"| 🔴 미수집 | {len(miss_c)} |",
          f"| 🟡 미분석 | {len(miss_a)} |"]
    if newest_drive:
        L.append(f"| Drive 최신 보관 시각 | {newest_drive} |")
    if coverage[0]:
        L.append(f"| manifest 보증 구간 | {coverage[0]} ~ {coverage[1]} |")
    L += ["", f"> 원천 {pages}페이지를 훑었다. 최근 {DEFAULT_WINDOW}일 범위.",
          "> 중복 게시(같은 PDF 재게시)는 manifest 의 `duplicate_of` 로 가려 대조에서 뺀다. "
          "**manifest 를 못 읽으면 그만큼 오탐이 늘어난다.**",
          "> 대조 키는 `source_id`(하나증권 `pid_bbsSeq_attachFileSeq`)다. "
          "파일 내용이 같은지까지 인증한 것은 아니다.",
          "> 규약(`" + MARKER + "`)에 안 맞는 옛 파일명은 이 대조에서 빠진다 — "
          "**대조 대상이 아니지 누락이 아니다.**"]
    return "\n".join(L), state


def self_test():
    ok = True

    def chk(name, cond, got=""):
        nonlocal ok
        print(("  ✅ " if cond else "  ❌ ") + name + (f"   {got}" if got else ""))
        ok = ok and cond

    at = lambda m, d, h: datetime.datetime(2026, m, d, h, 0, tzinfo=KST)

    print("🧪 유예 — 아직 차례가 아닌 것을 실패로 부르지 않는다")
    #  2026-09: 19(토) 20(일) 21(월) … 26(토) 27(일)
    chk("월요일이면 직전 일요일 21시",
        last_scheduled_collection(at(9, 21, 10)) == at(9, 20, 21))
    chk("토요일 20시면 아직 이번 주 수집 전 → 직전은 지난 일요일",
        last_scheduled_collection(at(9, 19, 20)) == at(9, 13, 21))
    chk("토요일 22시면 방금 그 토요일 21시",
        last_scheduled_collection(at(9, 19, 22)) == at(9, 19, 21))
    chk("일요일 21시 정각은 포함(이하가 아니라 이상 아님 — <=)",
        last_scheduled_collection(at(9, 20, 21)) == at(9, 20, 21))

    print("🧪 source_id 복원 — 추측하지 않는다")
    chk("규약 파일명에서 3토막을 뽑는다",
        source_id_of("20260913_hana_industry_2220_1288753_1_6fdfea13aa33.pdf")
        == "2220_1288753_1")
    chk("구 규약은 None (대조 대상이 아니다)",
        source_id_of("20260810_industry_671677000.pdf") is None)
    chk("토막이 모자라면 None", source_id_of("x_hana_industry_1_2.pdf") is None)
    chk("빈 문자열도 None", source_id_of("") is None)
    chk("집합으로 모을 때 None 은 빠진다",
        source_ids(["20260913_hana_industry_1_2_3_abc.pdf", "옛날.pdf"]) == {"1_2_3"})

    print("🧪 판정 — 세 갈래를 구분하는가")
    due = {"a": 1, "b": 1}
    chk("전부 있으면 정상", verdict(due, {"a", "b"}, {"a", "b"})[0] == OK)
    chk("Drive 에 없으면 수집지연", verdict(due, {"a"}, {"a"})[0] == LATE_COLLECT)
    chk("Drive 엔 있는데 시트에 없으면 분석적체",
        verdict(due, {"a", "b"}, {"a"})[0] == LATE_ANALYZE)
    chk("수집지연이 분석적체보다 우선 — 앞 단계부터 고친다",
        verdict(due, {"a"}, set())[0] == LATE_COLLECT)
    chk("원천에 없는 것은 문제로 만들지 않는다 (Drive 가 더 많아도 정상)",
        verdict(due, {"a", "b", "zzz"}, {"a", "b", "zzz"})[0] == OK)
    #  due={a,b}, drive={a}, sheet={} → 미수집은 {b}, 미분석은 {a} 뿐이다.
    #  b 는 Drive 에 없으므로 '분석이 안 됐다' 가 아니라 '아직 도착도 안 했다' 다.
    chk("미분석은 'Drive 에 있는 것' 중에서만 센다 — 미도착을 미분석으로 세지 않는다",
        verdict(due, {"a"}, set())[2] == {"a"})
    chk("미수집과 미분석이 겹치지 않는다",
        not (verdict(due, {"a"}, set())[1] & verdict(due, {"a"}, set())[2]))

    print("🧪 발간 0건과 수집 실패를 섞지 않는다 — 이 감시의 존재 이유")
    txt, st = render(at(9, 21, 10), {}, {}, set(), set(), 1, None, {})
    chk("발간 0건이면 정상", st == OK)
    chk("'올릴 것이 없었다' 고 분명히 쓴다", "올릴 것이 없었다" in txt)
    chk("둘을 섞으면 안 된다고 밝힌다", "둘을 섞으면" in txt)
    txt2, st2 = render(at(9, 21, 10), {"a": 1}, {}, set(), set(), 1, None, {})
    chk("같은 0건이라도 발간이 있었으면 수집지연", st2 == LATE_COLLECT)
    chk("어느 쪽을 봐야 하는지 지목한다", "Codex" in txt2)

    print("🧪 대기중 분리 — 평일 거짓 경보 방지")
    txt3, st3 = render(at(9, 21, 10), {}, {"new": 1}, set(), set(), 1, None, {})
    chk("대기중만 있으면 실패가 아니다", st3 == OK)
    chk("대기중 건수를 따로 찍는다", "| 원천 발간 — 대기중" in txt3)

    print("🧪 조회 실패를 조용히 넘기지 않는다")
    txt4, _ = render(at(9, 21, 10), {}, {}, set(), set(), 1, None, {"Drive": "boom"})
    chk("실패 사유를 찍는다", "Drive: boom" in txt4)
    chk("판정이 불완전함을 밝힌다", "불완전하다" in txt4)

    print("🧪 구 규약 제외를 누락으로 오해하지 않게 적는가")
    chk("대조 대상이 아니라고 밝힌다", "대조 대상이 아니지 누락이 아니다" in txt)

    print("🧪 manifest — 중복 게시를 미수집으로 오판하지 않는다 (9/15 첫 실행 오탐)")
    payload = {"coverage_start": "2026-08-26", "coverage_end": "2026-09-13", "reports": [
        {"source_id": "2220_1288679_1", "duplicate_of": None},
        {"source_id": "2220_1288680_1", "duplicate_of": "2220_1288679_1"},
        {"source_id": "2220_1288690_1", "duplicate_of": None}]}
    mids, mdups, mcov = manifest_ids(payload)
    chk("manifest 가 아는 source_id 전부", len(mids) == 3)
    chk("duplicate_of 가 있는 것만 중복으로", mdups == {"2220_1288680_1"})
    chk("커버리지를 돌려준다", mcov == ("2026-08-26", "2026-09-13"))
    chk("빈 payload 도 견딘다", manifest_ids({}) == (set(), set(), (None, None)))
    chk("None 도 견딘다", manifest_ids(None)[0] == set())
    chk("source_id 없는 항목은 무시", manifest_ids({"reports": [{"x": 1}]})[0] == set())

    #  실제로 났던 오탐을 재현해 막혔는지 본다.
    due_real = {"2220_1288679_1": 1, "2220_1288680_1": 1}
    drive_real = {"2220_1288679_1"}          # Drive 엔 원본 하나뿐
    sheet_real = {"2220_1288679_1"}
    bad, _ = render(at(9, 15, 23), due_real, {}, drive_real, sheet_real, 1, None, {})
    chk("manifest 없이는 수집지연으로 오판한다 (문제 재현)", "수집지연" in bad)
    good, gst = render(at(9, 15, 23), due_real, {}, drive_real, sheet_real, 1, None, {},
                       dups=mdups, coverage=mcov)
    chk("manifest 를 주면 정상이 된다", gst == OK, gst)
    chk("제외 건수를 숨기지 않고 표에 찍는다", "| 중복 게시로 대조 제외 | 1 |" in good)
    chk("manifest 보증 구간을 찍는다", "| manifest 보증 구간 | 2026-08-26 ~ 2026-09-13 |" in good)
    chk("manifest 를 못 읽으면 오탐이 는다고 밝힌다", "오탐이 늘어난다" in good)

    print("🧪 쓰기 호출이 없는가 (읽기 전용 보장)")
    src = open(__file__, encoding="utf-8").read()
    body = src.split("def self_test")[0]
    for bad in (".update(", ".append_row(", ".batch_update(", ".clear(",
                ".files().create(", ".write_bytes(", "collect("):
        chk(f"{bad} 없음", bad not in body)

    print("\n" + ("✅ 전부 통과" if ok else "❌ 실패 있음"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true", help="네트워크·자격증명 없이 판정 로직만")
    ap.add_argument("--days", type=int, default=DEFAULT_WINDOW)
    a = ap.parse_args()
    if a.self_test:
        return self_test()

    now = datetime.datetime.now(KST)
    errs = {}
    try:
        due, pending, pages = scan_source(a.days, now)
    except Exception as e:                              # noqa: BLE001
        errs["원천"] = f"{type(e).__name__}: {e}"
        due, pending, pages = {}, {}, 0

    drive_ids, sheet_ids, newest = set(), set(), None
    dups, coverage = set(), (None, None)
    try:
        from oauth2client.service_account import ServiceAccountCredentials
        from googleapiclient.discovery import build
        import gspread
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        svc, token = build("drive", "v3", credentials=creds), None
        names, times = [], []
        while True:
            resp = svc.files().list(
                q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
                pageSize=1000, pageToken=token,
                fields="nextPageToken,files(name,createdTime)").execute()
            for f in resp.get("files", []):
                names.append(f["name"])
                times.append(f.get("createdTime", ""))
            token = resp.get("nextPageToken")
            if not token:
                break
        drive_ids = source_ids(names)
        newest = max(times) if times else None

        # manifest — 중복 게시 연결을 여기서만 알 수 있다. 없으면 오탐이 는다.
        try:
            import io as _io
            import json as _json
            from googleapiclient.http import MediaIoBaseDownload
            man = svc.files().list(
                q=(f"'{DRIVE_FOLDER_ID}' in parents and trashed=false "
                   "and name contains 'hana_archive'"),
                orderBy="name desc", pageSize=1, fields="files(id,name)").execute()
            files = man.get("files", [])
            if not files:
                errs["manifest"] = "hana_archive_*.json 을 찾지 못했다"
            else:
                buf = _io.BytesIO()
                dl = MediaIoBaseDownload(buf, svc.files().get_media(fileId=files[0]["id"]))
                done = False
                while not done:
                    _, done = dl.next_chunk()
                mids, dups, coverage = manifest_ids(_json.loads(buf.getvalue().decode("utf-8")))
                drive_ids |= mids          # manifest 가 아는 것은 수집된 것이다
        except Exception as e:                          # noqa: BLE001
            errs["manifest"] = f"{type(e).__name__}: {e}"
    except Exception as e:                              # noqa: BLE001
        errs["Drive"] = f"{type(e).__name__}: {e}"

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
        rows = gspread.authorize(creds).open_by_url(SHEET_URL).worksheet(TREND_SHEET).get_all_values()
        sheet_ids = source_ids(r[TREND_FILE_COL] for r in rows[1:]
                               if len(r) > TREND_FILE_COL)
    except Exception as e:                              # noqa: BLE001
        errs["시트"] = f"{type(e).__name__}: {e}"

    text, state = render(now, due, pending, drive_ids, sheet_ids, pages, newest, errs,
                         dups, coverage)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fp:
            fp.write(text + "\n")
    # 🔴 수집지연일 때만 붉게 띄운다. 분석적체는 8건 상한의 **의도된 동작**일 수 있어
    #    경보로 올리지 않는다 — 표에는 건수가 남으므로 추이는 보인다.
    if errs:
        return 1
    return 2 if state == LATE_COLLECT else 0


if __name__ == "__main__":
    sys.exit(main())
