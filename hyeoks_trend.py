import os, time, json, datetime, io
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai

# ==========================================
# 1. 환경 설정 및 인증
# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DRIVE_FOLDER_NAME = "증시 산업리포트"  # 구글 드라이브에 만드신 폴더명
KST = datetime.timezone(datetime.timedelta(hours=9))

print(f"📈 [HYEOKS Mid-Term] 추세추종 산업 리포트 분석기 가동 ({datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')})")

try: 
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e: 
    print(f"❌ Gemini API 초기화 실패: {e}"); exit(1)

# 구글 시트 & 드라이브 API 인증
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    
    doc = gc.open_by_url(SHEET_URL)
    db_trend_sheet = doc.worksheet("DB_중장기")
except Exception as e:
    print(f"❌ 구글 드라이브/시트 인증 실패 (secret.json 확인): {e}"); exit(1)

def parse_ai_json(text):
    """제미나이가 반환한 JSON 문자열을 딕셔너리로 안전하게 파싱합니다."""
    try:
        clean_text = text.replace('`'*3 + 'json', '').replace('`'*3, '').strip()
        return json.loads(clean_text)
    except Exception as e:
        print(f"⚠️ JSON 파싱 에러: {e}")
        return None

# ==========================================
# 2. 구글 드라이브에서 리포트(PDF) 가져오기
# ==========================================
def get_pdfs_from_drive(folder_name):
    # 1) 폴더 ID 찾기
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get('files', [])
    
    if not folders:
        print(f"❌ '{folder_name}' 폴더를 찾을 수 없습니다. (secret.json 계정에 폴더가 공유되어 있는지 확인하세요)")
        return []
    
    folder_id = folders[0]['id']
    
    # 2) 폴더 내 PDF 파일 목록 가져오기
    query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files

def download_file(file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    return file_name

# ==========================================
# 3. 메인 로직: PDF 분석 및 시트 업데이트
# ==========================================
def main():
    pdf_files = get_pdfs_from_drive(DRIVE_FOLDER_NAME)
    if not pdf_files:
        print("📭 분석할 PDF 리포트가 없습니다. 프로그램을 종료합니다.")
        return

    print(f"📥 총 {len(pdf_files)}개의 산업 리포트를 발견했습니다. 분석을 시작합니다...")
    
    # 시트의 기존 데이터를 읽어와서 이미 분석한 파일은 건너뛰기
    existing_records = db_trend_sheet.get_all_values()
    analyzed_files = [row[6] for row in existing_records[1:] if len(row) > 6] # G열(7번째)이 파일명이라고 가정

    new_results = []
    
    for file in pdf_files:
        file_id = file['id']
        file_name = file['name']
        
        if file_name in analyzed_files:
            print(f"⏭️ 이미 분석된 리포트입니다 (건너뜀): {file_name}")
            continue
            
        print(f"\n📄 리포트 다운로드 및 분석 중: {file_name}")
        local_pdf_path = download_file(file_id, file_name)
        
        try:
            # 1) Gemini에 파일 업로드
            print(" - Gemini 서버로 리포트 전송 중...")
            uploaded_file = client.files.upload(file=local_pdf_path)
            
            # 2) 윌리엄 오닐/마크 미너비니 기반 추세추종 프롬프트
            trend_prompt = """
            당신은 윌리엄 오닐과 마크 미너비니의 '추세추종(Trend Following)' 기법을 완벽하게 구사하는 여의도 최상위 퀀트 펀드 매니저입니다.
            첨부된 산업 리포트(PDF)를 딥리딩하여, 중장기 투자에 적합한 핵심 섹터와 Top Pick 종목을 발굴하십시오.
            
            [분석 지침]
            1. 리포트가 주장하는 산업의 성장성(TAM, CAGR, 구조적 변화 등)을 핵심만 파악하십시오.
            2. 해당 산업에서 가장 수혜를 볼 대장주(Top Pick)를 최대 2개만 선정하십시오.
            3. 중장기 추세추종 관점에서의 '진입 전략'을 구상하십시오.
            
            반드시 아래 JSON 형식으로만 응답하십시오.
            {
                "industry": "섹터명 (예: 전력기기, HBM 장비)",
                "core_logic": "산업의 핵심 상승 논리 요약 (80자 이내)",
                "top_pick_1": "1순위 대장주 종목명",
                "top_pick_2": "2순위 관련주 종목명 (없으면 빈칸)",
                "strategy": "중장기 추세추종 관점의 대응 전략 (예: 50일선 안착 시 분할 매수, 전고점 돌파 시 불타기 등. 100자 이내)"
            }
            """
            
            print(" - AI 딥리딩 및 전략 산출 중...")
            response = client.models.generate_content(
                model='gemini-2.5-pro',
                contents=[uploaded_file, trend_prompt]
            )
            
            # 3) 결과 파싱
            parsed_data = parse_ai_json(response.text)
            if parsed_data:
                today_str = datetime.datetime.now(KST).strftime('%Y-%m-%d')
                row_data = [
                    today_str,                              # A: 분석일자
                    parsed_data.get("industry", "N/A"),     # B: 섹터/테마명
                    parsed_data.get("core_logic", "N/A"),   # C: 핵심 모멘텀 논리
                    parsed_data.get("top_pick_1", "N/A"),   # D: Top Pick 1
                    parsed_data.get("top_pick_2", ""),      # E: Top Pick 2
                    parsed_data.get("strategy", "N/A"),     # F: 중장기 추세추종 전략
                    file_name                               # G: 리포트 원문명(중복 방지용)
                ]
                new_results.append(row_data)
                print(f" ✨ 분석 완료 -> 섹터: {row_data[1]} | Top Pick: {row_data[3]}")
                
            # 서버 메모리 관리: 업로드된 파일 삭제
            client.files.delete(name=uploaded_file.name)
            
        except Exception as e:
            print(f" ❌ AI 분석 에러 ({file_name}): {e}")
        finally:
            # 로컬 임시 파일 삭제
            if os.path.exists(local_pdf_path):
                os.remove(local_pdf_path)
                
        # API Rate Limit 방지를 위한 대기
        time.sleep(10)

    # 4) 구글 시트(DB_중장기) 업데이트
    if new_results:
        print(f"\n💾 구글 시트에 {len(new_results)}개의 중장기 전략을 저장합니다...")
        
        # 헤더가 없으면 생성
        if len(existing_records) == 0:
            headers = ["분석일자", "섹터/테마명", "핵심 상승 논리", "Top Pick 1", "Top Pick 2", "추세추종 진입 전략", "리포트 출처(파일명)"]
            db_trend_sheet.append_row(headers)
            
        for row in new_results:
            db_trend_sheet.append_row(row)
            
        print("✅ DB_중장기 시트 업데이트 완료!")
    else:
        print("✅ 새롭게 추가할 중장기 리포트가 없습니다.")

if __name__ == "__main__":
    main()
