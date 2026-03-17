import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import time

# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 
# ==========================================

print("🤖 [신정재 종가베팅 스캐너] 로봇 가동 시작...")

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

scanner_data = scanner_sheet.get_all_values()

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
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        res = requests.get(url, headers=headers)
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.find_all('item')
        
        if items:
            today_data = items[-1]['data'].split('|') 
            current_price = int(today_data[4])
            today_high = int(today_data[2])
            today_low = int(today_data[3])
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

# 🧹 [핵심 업데이트] 과거 데이터 '찌꺼기' 완벽 삭제 로직
# 오늘 수집된 데이터 뒤에 150행까지 무조건 빈칸을 추가해서 덮어씁니다!
while len(col_B_updates) < 150:
    col_B_updates.append([""])
    col_G_H_updates.append(["", ""])
    col_J_updates.append([""])

print("▶️ 구글 시트로 데이터 일괄 전송 중...")

scanner_sheet.update(range_name="B2:B151", values=col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="G2:H151", values=col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="J2:J151", values=col_J_updates, value_input_option="USER_ENTERED")

print("🎯 모든 작업 완료! 찌꺼기 청소까지 완벽하게 끝났습니다!")
