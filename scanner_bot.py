import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import time

# ==========================================
# 🚨 회원님의 구글 시트 주소 (변경 불필요)
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 
# ==========================================

print("🤖 [신정재 종가베팅 스캐너] 로봇 가동 시작...")

# 1. 구글 시트 연결
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
client = gspread.authorize(creds)
doc = client.open_by_url(SHEET_URL)

info_sheet = doc.worksheet("기업정보")
scanner_sheet = doc.worksheet("스캐너_마스터")

print("▶️ 종목코드 매핑 중...")
info_data = info_sheet.get_all_values()
name_to_code = {}
for row in info_data[1:]:
    if len(row) >= 3:
        name = row[0].strip()
        code = str(row[2]).strip().zfill(6)
        name_to_code[name] = code

# 3. 스캐너_마스터 시트 읽어오기 (A2셀의 필터 수식 결과물)
scanner_data = scanner_sheet.get_all_values()
if len(scanner_data) < 2:
    print("❌ 스캐너_마스터 시트에 종목이 없습니다. (필터 수식을 확인하세요)")
    exit()

col_B_updates = []   
col_G_H_updates = [] 
col_J_updates = []   

print("▶️ 네이버 금융 Fchart API 연결 (우회 수집 중)...")
headers = {'User-Agent': 'Mozilla/5.0'}

for i, row in enumerate(scanner_data[1:], start=2):
    stock_name = row[0].strip() if len(row) > 0 else ""
    
    if not stock_name or stock_name not in name_to_code:
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])
        continue
        
    code = name_to_code[stock_name]
    
    try:
        # ⚡ 핵심 무기 교체: 네이버 주식 캔들 Fchart API (절대 막히지 않는 공식 차트 데이터)
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.find_all('item')
        
        if items:
            # item['data'] 형식: "날짜|시가|고가|저가|종가|거래량"
            today_data = items[-1]['data'].split('|') 
            current_price = int(today_data[4])
            today_high = int(today_data[2])
            today_low = int(today_data[3])
            
            # 60일치 고가 추출
            high_prices = [int(item['data'].split('|')[2]) for item in items]
            high_60d = max(high_prices)
            
            col_B_updates.append([current_price])
            col_G_H_updates.append([today_high, today_low])
            col_J_updates.append([high_60d])
            
            print(f"✅ {stock_name}: 현재가 {current_price} | 고가 {today_high} | 저가 {today_low} | 60일최고 {high_60d}")
        else:
            col_B_updates.append([""])
            col_G_H_updates.append(["", ""])
            col_J_updates.append([""])
            
        time.sleep(0.2) 
    except Exception as e:
        print(f"❌ {stock_name} 에러: {e}")
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])

print("▶️ 구글 시트로 데이터 일괄 전송 중...")
end_row = len(scanner_data)

# 🚨 gspread 라이브러리 업데이트 충돌을 막기 위한 '명시적(named arguments)' 방식 적용!
scanner_sheet.update(range_name=f"B2:B{end_row}", values=col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name=f"G2:H{end_row}", values=col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name=f"J2:J{end_row}", values=col_J_updates, value_input_option="USER_ENTERED")

print("🎯 모든 작업 완료! 구글 시트를 확인해 보세요!")
