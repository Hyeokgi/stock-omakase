# -*- coding: utf-8 -*-
"""
HYEOKS 증시 캘린더 자동 수집기
──────────────────────────────
지정된 구글드라이브 폴더에 매달 증권사 캘린더 PDF를 넣어두면,
아직 처리 안 한 새 파일만 찾아서 → 텍스트 추출 → Gemini로 날짜/일정/테마 구조화 →
`주요일정` 시트에 자동으로 추가한다. 이미 처리한 파일은 `캘린더_처리이력` 시트에 기록해 중복 방지.

필요 패키지 (기존 환경에 추가 설치 필요):
    pip install google-api-python-client pdfplumber --break-system-packages
"""
import os, re, io, datetime, time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pdfplumber
from google import genai

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
CALENDAR_FOLDER_ID = "1UGIswIhmShqSBAESWAcjkQKHCyKbUkUk"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
KST = datetime.timezone(datetime.timedelta(hours=9))

SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# 기존 주요일정 시트에서 실제로 쓰이던 테마구분 카테고리 목록 (새 항목도 이 스타일에 맞춰 분류하도록 Gemini에 제공)
CATEGORY_LIST = """🎤 엔터/미디어, ⚡ 에너지/친환경, 📊 거시경제/지수, 🏛️ 정치/외교/정책, 💊 제약/바이오,
🤖 AI/로봇, 🤖 AI/소프트웨어, 🚀 우주항공, 🚗 자율주행/로봇, 💾 반도체, 💻 IT/가전, 📱 IT/소프트웨어,
🛒 소비주/유통, 🆕 IPO/신규상장, 📉 시장제도, 🏗️ 건설/방산/인프라, ⏸️ 증시휴장, 📈 수급/지수편입,
🎮 게임, ⚽ 스포츠/테마, 🏛️ 무역/해운, 🛢️ 정유/에너지, 📑 실적발표, 기타"""

try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print(f"❌ Gemini 초기화 실패: {e}"); exit(1)


def safe_generate_content(contents, is_fast=False):
    # 👇 [확인 필요] hyeoks_analyst.py의 safe_generate_content와 정확히 같은 문법으로 맞춰주세요.
    model_name = 'gemini-2.5-flash' if is_fast else 'gemini-2.5-pro'
    for i in range(5):
        try:
            return client.models.generate_content(model=model_name, contents=contents)
        except Exception as e:
            if "503" in str(e) or "429" in str(e) or "quota" in str(e).lower():
                time.sleep(10 * (i + 1)); continue
            raise e
    raise Exception("❌ Gemini 재시도 최종 실패")


def get_services():
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", SCOPE)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    drive = build('drive', 'v3', credentials=creds)
    return doc, drive


def get_processed_ids(doc):
    try:
        sheet = doc.worksheet("캘린더_처리이력")
    except Exception:
        sheet = doc.add_worksheet(title="캘린더_처리이력", rows="200", cols="3")
        sheet.append_row(["파일ID", "파일명", "처리일시"])
    rows = sheet.get_all_values()[1:]
    return sheet, set(r[0].strip() for r in rows if r and r[0].strip())


def list_new_pdfs(drive, processed_ids):
    query = f"'{CALENDAR_FOLDER_ID}' in parents and trashed = false and mimeType = 'application/pdf'"
    res = drive.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    files = res.get('files', [])
    return [f for f in files if f['id'] not in processed_ids]


def download_pdf_text(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    text = ""
    with pdfplumber.open(buf) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
    return text


def extract_schedule_with_gemini(pdf_text, source_name):
    prompt = f"""아래는 증권사가 발간한 "이달의 증시 캘린더" 문서에서 추출한 원문 텍스트입니다.
여기서 날짜가 명시된 향후 일정(거시경제 지표, 실적발표, 컨퍼런스, 행사, 상장, 정책 이벤트 등)만 골라
반드시 아래 JSON 배열 형식으로만 응답하십시오. 다른 설명은 절대 포함하지 마십시오.

[
  {{"날짜": "2026-07-10", "일정내용": "짧고 명확한 일정 제목", "테마구분": "카테고리"}}
]

규칙:
- 날짜는 반드시 "yyyy-MM-dd" 형식. 연도가 명시 안 되어 있으면 문서 제목/맥락상의 연도를 사용.
- "테마구분"은 반드시 다음 목록 중 가장 가까운 것 하나만 골라 정확히 그대로 사용하십시오: {CATEGORY_LIST}
- 날짜가 불명확하거나("이달 중", "추후 확정" 등) 특정 일자를 알 수 없는 항목은 제외하십시오.
- 원문에 없는 내용을 지어내지 마십시오.

[원문 — 출처: {source_name}]
{pdf_text[:15000]}
"""
    try:
        res_text = safe_generate_content(prompt).text
        cleaned = res_text.replace('```json', '').replace('```', '').strip()
        import json
        return json.loads(cleaned)
    except Exception as e:
        print(f"⚠️ [{source_name}] Gemini 추출/파싱 실패: {e}")
        return []


def normalize_date(raw):
    """'2026. 7. 1', '2026-7-1' 등 다양한 표기를 'yyyy-MM-dd'로 통일. 실패하면 None."""
    s = str(raw).strip().replace('.', '-').replace(' ', '').strip('-')
    try:
        return datetime.datetime.strptime(s, '%Y-%m-%d').strftime('%Y-%m-%d')
    except Exception:
        return None


def append_to_schedule_sheet(doc, new_items):
    """기존 주요일정 시트 전체를 읽어 신규 항목과 합친 뒤, 날짜 정규화 + 정렬까지 마쳐서 다시 씀.
       (기존엔 append_rows로 끝에만 붙여서 정렬이 안 됐던 문제 수정)"""
    sheet = doc.worksheet("주요일정")
    header = sheet.row_values(1) or ["날짜", "일정내용", "테마구분"]
    existing = sheet.get_all_values()[1:]

    all_rows = []
    existing_keys = set()
    for r in existing:
        if len(r) < 2 or not r[0]:
            continue
        nd = normalize_date(r[0])
        if nd is None:
            all_rows.append(r)  # 날짜 인식 실패한 행은 원본 그대로 보존(유실 방지), 정렬에서만 맨 뒤로
            continue
        title = str(r[1]).strip()
        cat = r[2] if len(r) > 2 else "기타"
        all_rows.append([nd, title, cat])
        existing_keys.add(f"{nd}|{title.replace(' ', '')}")

    added = 0
    for item in new_items or []:
        nd = normalize_date(item.get("날짜", ""))
        title = str(item.get("일정내용", "")).strip()
        category = str(item.get("테마구분", "기타")).strip()
        if not nd or not title:
            continue
        key = f"{nd}|{title.replace(' ', '')}"
        if key in existing_keys:
            continue
        all_rows.append([nd, title, category])
        existing_keys.add(key)
        added += 1

    def sort_key(row):
        nd = normalize_date(row[0])
        return nd if nd else "9999-99-99"  # 날짜 인식 실패 행은 맨 뒤로

    all_rows.sort(key=sort_key)

    # 🗑️ [보관 정책] 오늘 기준 60일(2개월)보다 오래된 행은 완전히 삭제. 날짜 인식 실패 행은 유실 방지 위해 보존.
    today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
    cutoff_str = (datetime.datetime.now(KST).date() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
    before_count = len(all_rows)
    all_rows = [r for r in all_rows if (normalize_date(r[0]) is None) or (normalize_date(r[0]) >= cutoff_str)]
    removed = before_count - len(all_rows)
    if removed > 0:
        print(f"🗑️ [보관 정책] {cutoff_str} 이전 일정 {removed}건 삭제")

    # 🔎 [중복 후보 안내] 같은 날짜에 항목이 여럿이면 제목만 나란히 출력 — 자동 삭제는 안 하고 눈으로 확인하시라고 표시만
    by_date = {}
    for r in all_rows:
        nd = normalize_date(r[0])
        if nd:
            by_date.setdefault(nd, []).append(r[1])
    for d, titles in by_date.items():
        if len(titles) > 1:
            print(f"   🔎 {d} 같은 날짜 {len(titles)}건 — 중복 후보 여부 확인해보세요: {titles}")

    sheet.clear()
    sheet.update(range_name="A1", values=[header] + all_rows, value_input_option="USER_ENTERED")

    # 👁️ [오늘 이전 숨김] 정렬돼있으므로 과거 구간은 맨 앞에 연속으로 몰려있음 — 그 구간만 숨김 처리
    try:
        sheet_id = sheet.id
        requests_list = [{
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": len(all_rows) + 1},
                "properties": {"hiddenByUser": False},
                "fields": "hiddenByUser"
            }
        }]
        hide_start, hide_end = -1, -1
        for i, row in enumerate(all_rows):
            nd = normalize_date(row[0])
            if nd and nd < today_str:
                if hide_start == -1: hide_start = i + 1
                hide_end = i + 2
            else:
                break  # 정렬돼있어서 과거 구간은 항상 맨 앞에 연속으로만 존재
        if hide_start != -1:
            requests_list.append({
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": hide_start, "endIndex": hide_end},
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser"
                }
            })
        doc.batch_update({"requests": requests_list})
        if hide_start != -1:
            print(f"👁️ [숨김 처리] 오늘({today_str}) 이전 {hide_end - hide_start}건 숨김")
    except Exception as e:
        print(f"⚠️ [숨김 처리 실패, 데이터엔 영향 없음] {e}")

    return added


if __name__ == "__main__":
    print(f"📅 [HYEOKS 캘린더 수집기] 시작 (KST {datetime.datetime.now(KST).strftime('%H:%M:%S')})")
    doc, drive = get_services()
    history_sheet, processed_ids = get_processed_ids(doc)

    new_files = list_new_pdfs(drive, processed_ids)
    if not new_files:
        print("ℹ️ 새로 처리할 캘린더 PDF는 없음 — 보관정책(60일 삭제)·숨김 처리만 매일 갱신")
        append_to_schedule_sheet(doc, [])  # 새 항목 없이도 정렬/삭제/숨김은 매일 돌아가야 함
        print("🎉 캘린더 수집기 작업 완료!")
        exit(0)

    for f in new_files:
        print(f"▶ 처리 중: {f['name']}")
        try:
            pdf_text = download_pdf_text(drive, f['id'])
            if not pdf_text.strip():
                print(f"⚠️ [{f['name']}] 텍스트 추출 결과가 비어있음 (스캔 이미지 PDF일 가능성) — 스킵")
                continue

            items = extract_schedule_with_gemini(pdf_text, f['name'])
            added = append_to_schedule_sheet(doc, items)
            print(f"✅ [{f['name']}] {len(items)}건 추출 → {added}건 신규 추가")

            history_sheet.append_row([f['id'], f['name'], datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')])
        except Exception as e:
            print(f"❌ [{f['name']}] 처리 실패: {e}")
            continue

    print("🎉 캘린더 수집기 작업 완료!")
