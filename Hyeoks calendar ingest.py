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


def append_to_schedule_sheet(doc, new_items):
    """기존 주요일정 시트에 중복 없이 추가 (날짜+제목 기준 dedup)."""
    if not new_items:
        return 0
    sheet = doc.worksheet("주요일정")
    existing = sheet.get_all_values()[1:]
    existing_keys = set(f"{str(r[0]).strip()}|{str(r[1]).replace(' ', '').strip()}" for r in existing if len(r) >= 2)

    rows_to_add = []
    for item in new_items:
        date_str = str(item.get("날짜", "")).strip()
        title = str(item.get("일정내용", "")).strip()
        category = str(item.get("테마구분", "기타")).strip()
        if not date_str or not title:
            continue
        key = f"{date_str}|{title.replace(' ', '')}"
        if key in existing_keys:
            continue
        rows_to_add.append([date_str, title, category])
        existing_keys.add(key)

    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option="USER_ENTERED")
    return len(rows_to_add)


if __name__ == "__main__":
    print(f"📅 [HYEOKS 캘린더 수집기] 시작 (KST {datetime.datetime.now(KST).strftime('%H:%M:%S')})")
    doc, drive = get_services()
    history_sheet, processed_ids = get_processed_ids(doc)

    new_files = list_new_pdfs(drive, processed_ids)
    if not new_files:
        print("ℹ️ 새로 처리할 캘린더 PDF가 없습니다.")
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
