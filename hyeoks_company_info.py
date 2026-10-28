# KIND 상장법인목록과 구글 시트 '기업정보' 를 대조해 신규 상장 종목을 덧붙이고 바뀐 상호를 KIND 기준으로 고치는 수집기
"""기업정보 신규 상장 동기화 `company-info-v1` (2026-10-28 사용자 지시).

왜: '기업정보' 탭은 KIND 상장법인목록을 붙여 넣은 표(10열)인데 2026-08-25 상장분 이후 갱신되지 않았다.
    스캐너 · 실적 수집기 · 아침 브리핑이 이 탭으로 종목명 → 코드를 찾으므로 신규 상장주가 빠진다.

하는 것: KIND 공식 내려받기(로그인 없음) 전체 시장 목록에서 시트에 없는 종목코드만 같은 10열로 맨 아래에 덧붙인다.
2026-10-28 ② (사용자 지시 "KIND 기준으로 고치는 기능") — 시트에 있는 종목코드의 회사명(A열)이 KIND 와 다르면 KIND 이름으로
    고친다. 다른 열은 건드리지 않는다. 첫 대조에서 102개가 달랐다(8월 이후 상호 변경 · 표기 차이로 보인다).
하지 않는 것: 행 삭제(상장폐지 종목도 남긴다), A열 밖의 기존 값 수정.
안전장치: KIND 행 수 · 머리글 확인, 한 번에 MAX_APPEND · MAX_RENAME 초과면 아무것도 쓰지 않고 실패.
    공개 로그에는 개수 · 상장일만(종목명 · 옛 이름 없음).
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
MAX_RENAME = 150          # 상호 갱신 상한 — 첫 실행 102, 평소는 하루 몇 개. 넘으면 열 밀림 등 이상을 의심한다


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
    """(덧붙일 행, 상호 갱신 [(시트 행 번호, KIND 이름)]). 시트 머리글이 다르면 멈춘다.
    같은 코드가 시트에 두 번 있으면 이름이 다른 행을 모두 고친다."""
    if not sheet_values or [c.strip() for c in sheet_values[0][:len(HEADER)]] != HEADER:
        raise SyncError("기업정보 머리글이 다르다")
    rows_by_code = {}
    for i, r in enumerate(sheet_values[1:], start=2):            # 1행은 머리글
        code = krx_code.normalize(r[2]) if len(r) > 2 else ""
        if code:
            rows_by_code.setdefault(code, []).append((i, str(r[0]).strip()))
    new = sorted((r for r in kind_rows if r[2] not in rows_by_code), key=lambda r: (r[5], r[2]))
    renames = sorted((i, r[0]) for r in kind_rows for i, name in rows_by_code.get(r[2], ()) if name != r[0])
    return new, renames


def run(fetch_kind, worksheet, dry_run=False):
    """0 = 정상(덧붙였거나 할 것 없음), 1 = 멈춤."""
    try:
        kind_rows = parse_kind(fetch_kind())
        new, renames = plan(worksheet.get_all_values(), kind_rows)
        if len(new) > MAX_APPEND:
            raise SyncError(f"덧붙일 종목 {len(new)}개 — {MAX_APPEND} 초과라 쓰지 않는다")
        if len(renames) > MAX_RENAME:
            raise SyncError(f"상호 갱신 {len(renames)}개 — {MAX_RENAME} 초과라 쓰지 않는다")
    except SyncError as e:
        print(f"::error::기업정보 동기화 멈춤 — {e}")
        return 1
    dates = sorted({r[5] for r in new})
    print(f"🏢 KIND {len(kind_rows)}개 · 기업정보에 없는 신규 {len(new)}개"
          + (f"(상장일 {dates[0]}~{dates[-1]})" if dates else "")
          + f" · 상호가 KIND 와 다른 행 {len(renames)}개")
    if dry_run:
        if new or renames:
            print("(시험 실행 — 쓰지 않음)")
        return 0
    if renames:                                   # 덧붙이기 전에 고친다 — 행 번호가 바뀌지 않는다
        worksheet.batch_update([{"range": f"A{i}", "values": [[name]]} for i, name in renames],
                               value_input_option="RAW")
        print(f"✅ 기업정보 상호 {len(renames)}행 KIND 기준으로 고침")
    if new:
        worksheet.append_rows(new, value_input_option="RAW", table_range="A:J")
        print(f"✅ 기업정보 {len(new)}행 덧붙임")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description="KIND 상장법인목록 → 기업정보 신규 상장 덧붙이기 · 상호 갱신")
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
