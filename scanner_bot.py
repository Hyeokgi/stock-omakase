import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# ==========================================
# 🚨 회원님의 구글 시트 주소로 꼭 변경해 주세요!
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 
# ==========================================

print("🤖 [신정재 종가베팅 스캐너] 로봇 가동 시작...")

# 1. 구글 시트 연결 (기존과 동일)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
client = gspread.authorize(creds)
doc = client.open_by_url(SHEET_URL)

info_sheet = doc.worksheet("기업정보")
scanner_sheet = doc.worksheet("스캐너_마스터")

# 2. 기업정보 시트에서 [종목명 -> 종목코드] 매핑 사전 만들기
print("▶️ 종목코드 매핑 중...")
info_data = info_sheet.get_all_values()
name_to_code = {}
for row in info_data[1:]:
    if len(row) >= 3:
        name = row[0].strip()
        code = str(row[2]).strip().zfill(6) # 005930 처럼 6자리로 예쁘게 맞춤
        name_to_code[name] = code

# 3. 스캐너_마스터 시트의 A열(종목명) 읽어오기
scanner_data = scanner_sheet.get_all_values()
if len(scanner_data) < 2:
    print("❌ 스캐너_마스터 시트에 종목이 없습니다.")
    exit()

# 구글 시트에 한 번에 쏘기 위한 장바구니 준비
col_B_updates = []   # B열: 현재가
col_G_H_updates = [] # G열, H열: 오늘 고가, 저가
col_J_updates = []   # J열: 60일 최고가

print("▶️ 네이버 금융 비밀 API 통로 연결 (실시간 데이터 수집 중)...")
headers = {'User-Agent': 'Mozilla/5.0'}

# 2행부터 마지막 행까지 순회
for i, row in enumerate(scanner_data[1:], start=2):
    stock_name = row[0].strip() if len(row) > 0 else ""
    
    if not stock_name or stock_name not in name_to_code:
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])
        continue
        
    code = name_to_code[stock_name]
    
    try:
        # ⚡ 핵심 무기: 네이버 모바일 캔들 API (최근 60일치 캔들을 0.1초 만에 가져옴)
        url = f"https://m.stock.naver.com/api/stock/{code}/candle/day?count=60"
        res = requests.get(url, headers=headers)
        data = res.json()
        
        if data:
            today_data = data[0] # [0]번이 가장 최근(오늘) 데이터입니다.
            current_price = int(today_data['closePrice'].replace(',', ''))
            today_high = int(today_data['highPrice'].replace(',', ''))
            today_low = int(today_data['lowPrice'].replace(',', ''))
            
            # 60일 최고가 계산 (가져온 60일치 데이터의 '고가' 중 가장 높은 값 추출)
            high_60d = max([int(day['highPrice'].replace(',', '')) for day in data])
            
            # 장바구니에 담기
            col_B_updates.append([current_price])
            col_G_H_updates.append([today_high, today_low])
            col_J_updates.append([high_60d])
            
            print(f"✅ {stock_name}: 현재가 {current_price} | 고가 {today_high} | 저가 {today_low} | 60일최고 {high_60d}")
        else:
            col_B_updates.append([""])
            col_G_H_updates.append(["", ""])
            col_J_updates.append([""])
            
        time.sleep(0.1) # 서버 보호를 위한 0.1초 휴식
    except Exception as e:
        print(f"❌ {stock_name} 에러: {e}")
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])

# 4. 구글 시트에 일괄 전송 (Batch Update - 속도 극대화)
print("▶️ 구글 시트로 데이터 일괄 전송 중...")
end_row = len(scanner_data)
# value_input_option="USER_ENTERED" 옵션으로 넣어야 숫자로 깔끔하게 인식됩니다.
scanner_sheet.update(f"B2:B{end_row}", col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(f"G2:H{end_row}", col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(f"J2:J{end_row}", col_J_updates, value_input_option="USER_ENTERED")

print("🎯 모든 작업 완료! 엑셀 시트의 I, K, L열 인공지능이 1초 만에 결과를 판독합니다!")
