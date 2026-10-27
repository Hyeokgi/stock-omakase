# KIND 상장법인목록과 구글 시트 '기업정보' 를 대조해 시트에 없는 신규 상장 종목만 덧붙이는 수집기
"""기업정보 신규 상장 동기화 `company-info-v1` (2026-10-28 사용자 지시).

왜: '기업정보' 탭은 KIND 상장법인목록을 붙여 넣은 표(10열)인데 2026-08-25 상장분 이후 갱신되지 않았다.
    스캐너 · 실적 수집기 · 아침 브리핑이 이 탭으로 종목명 → 코드를 찾으므로 신규 상장주가 빠진다.

하는 것: KIND 공식 내려받기(로그인 없음) 전체 시장 목록에서 시트에 없는 종목코드만 같은 10열로 맨 아래에 덧붙인다.
하지 않는 것: 기존 행 수정 · 삭제(상장폐지 종목도 남긴다), 상호 변경 반영(개수만 알린다).
안전장치: KIND 행 수 · 머리글 확인, 한 번에 MAX_APPEND 초과면 쓰지 않고 실패. 공개 로그에는 개수 · 상장일만(종목명 없음).
"""
import argparse
import html
import re
import sys

import krx_code

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SHEET_NAME = "기업정보"
KIND_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType="
HEADER = ["회사명", "시장구분", "종목코드", "업종", "주요제품", "상장일", "결산월", "대표자명", "홈페이지", "지역"]
MIN_KIND_ROWS = 2000      # 2026-10-28 실측 2,758 — 이보다 적으면 잘린 응답으로 본다
MAX_APPEND = 50           # 한 번에 이보다 많으면 시트 · 응답 이상을 의심하고 쓰지 않는다(첫 실행 예상 약 18)


class SyncError(RuntimeError):
    """쓰기 전에 멈춰야 하는 이상."""


def _cells(row_html):
    return [html.unescape(re.sub(r"<.*?>", "", c)).replace("\xa0", " ").strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S)]


def parse_kind(raw):
    """KIND 내려받기(EUC-KR HTML 표) → 10열 행 목록. 종목코드는 정규화한다."""
    text = raw.decode("cp949", errors="replace") if isinstance(raw, bytes) else raw
    rows = [_cells(r) for r in re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.S)]
    if not rows or rows[0][:len(HEADER)] != HEADER:
        raise SyncError("KIND 머리글이 다르다")
    out, seen = [], set()
    for r in rows[1:]:
        if len(r) < len(HEADER):
            continue
        code = krx_code.normalize(r[2])
        if not code:
            raise SyncError("KIND 종목코드 형식 이상")
        if code in seen:
            raise SyncError("KIND 종목코드 중복")
        seen.add(code)
        out.append([r[0], r[1], code] + r[3:len(HEADER)])
    if len(out) < MIN_KIND_ROWS:
        raise SyncError(f"KIND {len(out)}행 — {MIN_KIND_ROWS} 미만이라 잘린 응답으로 본다")
    return out


def plan(sheet_values, kind_rows):
    """(덧붙일 행, 상호가 다른 수). 시트 머리글이 다르면 멈춘다."""
    if not sheet_values or [c.strip() for c in sheet_values[0][:len(HEADER)]] != HEADER:
        raise SyncError("기업정보 머리글이 다르다")
    names = {}
    for r in sheet_values[1:]:
        code = krx_code.normalize(r[2]) if len(r) > 2 else ""
        if code:
            names[code] = str(r[0]).strip()
    new = sorted((r for r in kind_rows if r[2] not in names), key=lambda r: (r[5], r[2]))
    renamed = sum(1 for r in kind_rows if r[2] in names and names[r[2]] != r[0])
    return new, renamed


def run(fetch_kind, worksheet, dry_run=False):
    """0 = 정상(덧붙였거나 할 것 없음), 1 = 멈춤."""
    try:
        kind_rows = parse_kind(fetch_kind())
        new, renamed = plan(worksheet.get_all_values(), kind_rows)
        if len(new) > MAX_APPEND:
            raise SyncError(f"덧붙일 종목 {len(new)}개 — {MAX_APPEND} 초과라 쓰지 않는다")
    except SyncError as e:
        print(f"::error::기업정보 동기화 멈춤 — {e}")
        return 1
    dates = sorted({r[5] for r in new})
    print(f"🏢 KIND {len(kind_rows)}개 · 기업정보에 없는 신규 {len(new)}개"
          + (f"(상장일 {dates[0]}~{dates[-1]})" if dates else "")
          + (f" · 상호가 다른 종목 {renamed}개(반영 안 함)" if renamed else ""))
    if new and not dry_run:
        worksheet.append_rows(new, value_input_option="RAW", table_range="A:J")
        print(f"✅ 기업정보 {len(new)}행 덧붙임")
    elif new:
        print("(시험 실행 — 쓰지 않음)")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="KIND 상장법인목록 → 기업정보 신규 상장 덧붙이기")
    ap.add_argument("--dry-run", action="store_true", help="개수만 보고 시트에 쓰지 않는다")
    a = ap.parse_args(argv)
    import gspread
    import requests
    from oauth2client.service_account import ServiceAccountCredentials

    def fetch_kind():
        try:
            r = requests.get(KIND_URL, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        except requests.RequestException as e:
            raise SyncError(f"KIND 내려받기 실패 {type(e).__name__}") from e
        if r.status_code != 200:
            raise SyncError(f"KIND HTTP {r.status_code}")
        return r.content

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    ws = gspread.authorize(creds).open_by_url(SHEET_URL).worksheet(SHEET_NAME)
    return run(fetch_kind, ws, a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
