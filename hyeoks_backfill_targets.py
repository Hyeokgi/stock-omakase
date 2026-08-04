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
    if not bt_data or str(bt_data[0][0]).strip() != "trade_id":
        print("❌ 백테스트_로그 시트 자체가 없거나 형식이 다릅니다. omakase.py를 먼저 한 번 실행해주세요.")
        return

    # 🔧 [수정] 예전엔 헤더가 이미 36열(목표가·손절가·터치 포함)로 확장돼 있어야만 진행했는데,
    #    이 확장은 omakase.py의 Step1(15:00~15:30 KST)에만 일어나서, 그 시간이 아직 안 지났으면
    #    이 스크립트가 아무것도 못 하고 조용히 끝나버리는 순서 의존성 문제가 있었음.
    #    → omakase.py 실행을 기다리지 않고 이 스크립트가 직접 헤더를 확장하도록 자체 완결형으로 변경.
    BT_HEADER_TAIL = ["목표가", "손절가", "익절터치", "손절터치"]
    if len(bt_data[0]) < 32 + len(BT_HEADER_TAIL) or bt_data[0][32:32 + len(BT_HEADER_TAIL)] != BT_HEADER_TAIL:
        if len(bt_data[0]) < 32:
            print(f"❌ 백테스트_로그 헤더가 예상보다 짧습니다({len(bt_data[0])}열). omakase.py가 정상적으로 배포됐는지 확인해주세요.")
            return
        new_header = list(bt_data[0][:32]) + BT_HEADER_TAIL
        bt_sheet.update(range_name="A1", values=[new_header], value_input_option="USER_ENTERED")
        bt_data[0] = new_header
        print(f"🔧 [헤더 자체 확장] {len(bt_data[0]) - len(BT_HEADER_TAIL)}열 → {len(bt_data[0])}열로 이 스크립트가 직접 갱신했습니다 (omakase.py 실행을 기다리지 않음).")

    targets_needed = []
    for i, row in enumerate(bt_data[1:], start=2):  # 시트 1-based 행번호(헤더가 1행이므로 데이터는 2행부터)
        if len(row) < 4:  # 최소한 채널·종목명 칸까지는 있어야 함(구글시트가 끝 빈 칸을 잘라내므로 32~36 사이는 행마다 다를 수 있음)
            continue
        채널 = str(row[2]).strip()
        if 채널 not in ("리포트TOP2_단기", "리포트TOP2_중기", "리포트TOP2"):
            continue
        # 🔧 [수정] 기존 행은 구글시트가 끝 빈 칸을 잘라내서 32칸까지만 있는 경우가 대부분이라,
        #    "34칸 이상이어야 함" 같은 길이 조건으로 거르면 대상 행이 전부 걸러져버림 → 안전하게 인덱스 접근으로 교체.
        target_cell = row[32].strip() if len(row) > 32 else ""
        stop_cell = row[33].strip() if len(row) > 33 else ""
        if target_cell or stop_cell:  # 이미 채워진 행은 건너뜀
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
            # 🔑 [필수] wkhtmltopdf로 만든 PDF는 폰트 임베딩 특성상 공백이 0x20이 아니라
            #    \x01(SOH)로 추출된다. 그대로 두면 아래 정규식의 \s 가 매치되지 않아
            #    '[DATA]\x01목표가:18200,\x01손절가:13300' 을 전부 놓친다(백필이 조용히 실패하던 원인).
            #    → 개행/탭은 보존하고 나머지 제어문자만 공백으로 치환.
            full_text = "".join(
                ' ' if (ord(c) < 32 and c not in '\n\r\t') else c for c in full_text
            )
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
