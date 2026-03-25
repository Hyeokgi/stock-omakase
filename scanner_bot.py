import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import time

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 

print("🤖 [신정재 종가베팅 스캐너] 로봇 가동 시작...")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
client = gspread.authorize(creds)
doc = client.open_by_url(SHEET_URL)

info_sheet = doc.worksheet("기업정보")
scanner_sheet = doc.worksheet("스캐너_마스터")

info_data = info_sheet.get_all_values()
name_to_code = {row[0].strip(): str(row[2]).strip().zfill(6) for row in info_data[1:] if len(row) >= 3}

def search_code_from_naver(stock_name):
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).json()
        if data.get('result') and data['result'].get('stocks'): return data['result']['stocks'][0]['itemCode']
    except: pass
    return None

scanner_data = scanner_sheet.get_all_values()
target_stocks = [row[0].strip() for row in scanner_data[1:] if len(row) > 0 and row[0].strip() and row[0].strip() != "#REF!"]

print(f"▶️ 총 {len(target_stocks)}개 종목 데이터 수집 시작...")
headers = {'User-Agent': 'Mozilla/5.0'}
stock_market_data = {}

for stock_name in target_stocks:
    code = name_to_code.get(stock_name) or search_code_from_naver(stock_name)
    if not code: continue
    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        soup = BeautifulSoup(requests.get(url, headers=headers).content, 'html.parser')
        items = soup.find_all('item')
        if items:
            today_data = items[-1]['data'].split('|') 
            stock_market_data[stock_name] = {
                "price": int(today_data[4]), "high": int(today_data[2]), "low": int(today_data[3]), "high_60d": max([int(item['data'].split('|')[2]) for item in items])
            }
        time.sleep(0.2) 
    except: continue

latest_scanner_data = scanner_sheet.get_all_values()
col_B_updates, col_G_H_updates, col_J_updates = [], [], []

for row in latest_scanner_data[1:]:
    stock_name = row[0].strip() if len(row) > 0 else ""
    if stock_name in stock_market_data:
        data = stock_market_data[stock_name]
        col_B_updates.append([data["price"]])
        col_G_H_updates.append([data["high"], data["low"]])
        col_J_updates.append([data["high_60d"]])
    else:
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])

while len(col_B_updates) < 150:
    col_B_updates.append([""]); col_G_H_updates.append(["", ""]); col_J_updates.append([""])

scanner_sheet.update(range_name="B2:B151", values=col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="G2:H151", values=col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="J2:J151", values=col_J_updates, value_input_option="USER_ENTERED")
print("🎯 모든 작업 완료! 동기화 꼬임 방지 및 찌꺼기 청소가 완벽하게 끝났습니다!")
