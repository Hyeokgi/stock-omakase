#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HYEOKS 백테스트_로그 — 과거 목표가·손절가 역추적 백필 스크립트 (1회성 유틸리티)

[배경]
   목표가·손절가·터치 열이 백테스트_로그에 새로 추가되기 전까지 쌓여있던 리포트TOP2_단기/중기의
   과거 행들은 이 값이 비어있음. 이미 발행된 PDF 리포트(구글드라이브, 리포트_게시 시트에 날짜별
   URL이 기록되어 있음) 안에는 각 종목마다 "[DATA] 목표가:X, 손절가:Y, 분할매수:O/X" 줄이 그대로
   남아있으므로, 이걸 다시 읽어서 채워 넣는다. 리포트를 새로 만들지 않고 이미 만들어진 PDF를
   다시 읽기만 하므로 Gemini API 비용이 들지 않음.

[사용법]
   한 번 실행하면 끝. 그 이후로는 omakase.py/hyeoks_analyst.py가 진입 시점에 자동으로
   목표가·손절가를 채워 넣으므로 이 스크립트를 다시 돌릴 필요가 없음(수동 1회 실행 전용,
   GitHub Actions 정기 스케줄에는 등록하지 않음).

[요구 사항]
   pip install pypdf google-api-python-client --break-system-packages
"""
import io
import re
import time
from collections import defaultdict

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pypdf import PdfReader

# 🔧 다른 스크립트(hyeoks_analyst.py 등)와 동일한 시트를 가리키도록 통일
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"


def extract_drive_file_id(url):
    """리포트_게시 시트의 URL(uc?id=... 또는 file/d/.../view 등 다양한 형태)에서 파일 ID만 추출."""
    m = re.search(r'[?&]id=([a-zA-Z0-9_-]+)', url) or re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    return m.group(1) if m else None


def download_pdf_bytes(drive, file_id):
    request = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf


def main():
    print("🤖 [HYEOKS 백필 유틸] 과거 리포트 목표가·손절가 역추적 시작")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    drive = build('drive', 'v3', credentials=creds)
    doc = gc.open_by_url(SHEET_URL)

    # ① 리포트_게시 시트에서 "날짜 → 드라이브 파일ID" 매핑을 만듦
    try:
        pub_rows = doc.worksheet("리포트_게시").get_all_values()
    except Exception as e:
        print(f"❌ [리포트_게시 시트 읽기 실패] {e}"); return

    date_to_fileid = {}
    for row in pub_rows:
        if len(row) < 2 or not row[0].strip() or not row[1].strip():
            continue
        fid = extract_drive_file_id(row[1].strip())
        if fid:
            date_to_fileid[row[0].strip()] = fid
    print(f"📋 리포트_게시에서 {len(date_to_fileid)}개 날짜의 드라이브 파일ID 확인")
    if not date_to_fileid:
        print("ℹ️ 발행 기록이 없습니다. 종료."); return

    # ② 백테스트_로그에서 목표가·손절가가 비어있는 리포트TOP2_단기/중기 행을 찾음
    bt_sheet = doc.worksheet("백테스트_로그")
    bt_data = bt_sheet.get_all_values()
    if not bt_data or len(bt_data[0]) < 34 or bt_data[0][32] != "목표가":
        print("❌ 백테스트_로그가 아직 신 스키마(목표가·손절가 열)가 아닙니다. omakase.py를 먼저 한 번 실행해주세요.")
        return

    targets_needed = []
    for i, row in enumerate(bt_data[1:], start=2):  # 시트 1-based 행번호(헤더가 1행이므로 데이터는 2행부터)
        if len(row) < 34:
            continue
        채널 = str(row[2]).strip()
        if 채널 not in ("리포트TOP2_단기", "리포트TOP2_중기", "리포트TOP2"):
            continue
        if str(row[32]).strip() or str(row[33]).strip():  # 이미 채워진 행은 건너뜀
            continue
        진입일, 종목명 = str(row[1]).strip(), str(row[3]).strip()
        if 진입일 not in date_to_fileid:
            continue
        targets_needed.append({'sheet_row': i, 'date': 진입일, 'name': 종목명, 'file_id': date_to_fileid[진입일]})

    print(f"🔍 목표가·손절가 역추적이 필요한 리포트 채널 행: {len(targets_needed)}개")
    if not targets_needed:
        print("✅ 채워야 할 행이 없습니다(이미 다 채워졌거나, 발행 기록이 없는 날짜뿐). 종료.")
        return

    # ③ 같은 날짜(=같은 PDF)는 한 번만 다운로드해서, 그 안에서 여러 종목을 한 번에 찾음
    by_date = defaultdict(list)
    for t in targets_needed:
        by_date[t['date']].append(t)

    updates = []  # (sheet_row, target, stop)
    for date_str, items in by_date.items():
        file_id = items[0]['file_id']
        try:
            buf = download_pdf_bytes(drive, file_id)
            reader = PdfReader(buf)
            full_text = "\n".join((p.extract_text() or "") for p in reader.pages)
        except Exception as e:
            print(f"⚠️ [{date_str}] PDF 다운로드/읽기 실패(건너뜀): {e}")
            continue

        for item in items:
            # 종목명이 등장하는 위치부터 다음 "[DATA]" 줄을 찾음 — 그 종목 리포트 구간에 속한 값을 잡기 위함
            name_idx = full_text.find(item['name'])
            if name_idx == -1:
                print(f"⚠️ [{date_str}] '{item['name']}' — PDF 본문에서 종목명을 못 찾음(건너뜀)")
                continue
            data_match = re.search(r'\[DATA\]\s*목표가[:\s]*([\d,]+),?\s*손절가[:\s]*([\d,]+)', full_text[name_idx:])
            if not data_match:
                print(f"⚠️ [{date_str}] '{item['name']}' — 이후 구간에서 [DATA] 줄을 못 찾음(건너뜀)")
                continue
            try:
                target = int(data_match.group(1).replace(',', ''))
                stop = int(data_match.group(2).replace(',', ''))
            except Exception:
                continue
            if target <= 0 and stop <= 0:  # 관망("000000" 처리 등)으로 둘 다 0인 경우는 채울 값이 없으므로 스킵
                continue
            updates.append((item['sheet_row'], target, stop))
            print(f"✅ [{date_str}] {item['name']}: 목표가 {target:,}원 / 손절가 {stop:,}원")
        time.sleep(0.5)  # 드라이브 API 과호출 방지

    # ④ 한 번에 배치 기록 (AG=목표가 32번째 열, AH=손절가 33번째 열, 0-based 인덱스 기준)
    if updates:
        batch = [{'range': f'AG{r}:AH{r}', 'values': [[t, s]]} for r, t, s in updates]
        bt_sheet.batch_update(batch, value_input_option="USER_ENTERED")
        print(f"\n🎉 {len(updates)}개 행에 목표가·손절가 역채움 완료! (익절·손절 터치 여부는 다음 omakase.py Step2 실행에서 자동 판정됩니다)")
    else:
        print("\nℹ️ 실제로 채울 수 있었던 행이 없습니다(PDF 안에서 매칭 실패한 경우들).")


if __name__ == "__main__":
    main()
