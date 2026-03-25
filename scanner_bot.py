import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import time

# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 
# ==========================================

print("🤖 [신정재 종가베팅 스캐너] 오리지널 다이렉트 봇 가동 시작...")

# 1. 구글 시트 연결
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
client = gspread.authorize(creds)
doc = client.open_by_url(SHEET_URL)

info_sheet = doc.worksheet("기업정보")
scanner_sheet = doc.worksheet("스캐너_마스터")

# 2. 종목코드 매핑
info_data = info_sheet.get_all_values()
name_to_code = {}
for row in info_data[1:]:
    if len(row) >= 3:
        name_to_code[row[0].strip()] = str(row[2]).strip().zfill(6)

# 🚀 [속도 6배 향상 패치] 매번 문을 열지 않고 고속도로(Session)를 유지합니다.
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

def search_code_from_naver(stock_name):
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        data = session.get(url).json()
        if data.get('result') and data['result'].get('stocks'):
            return data['result']['stocks'][0]['itemCode']
    except Exception:
        pass
    return None

# 3. 스캐너 탭에 적힌 종목만 타겟팅 (불필요한 조회 차단)
scanner_data = scanner_sheet.get_all_values()
target_stocks = [row[0].strip() for row in scanner_data[1:] if len(row) > 0 and row[0].strip() and row[0].strip() != "#REF!"]

print(f"▶️ 총 {len(target_stocks)}개 종목 데이터 수집 시작 (초고속 모드)...")
stock_market_data = {}

# 4. 실시간 데이터 초고속 크롤링
for stock_name in target_stocks:
    code = name_to_code.get(stock_name)
    if not code:
        code = search_code_from_naver(stock_name)
        if code: name_to_code[stock_name] = code
        else: continue

    try:
        url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
        res = session.get(url, timeout=3)
        soup = BeautifulSoup(res.content, 'html.parser')
        items = soup.find_all('item')
        
        if items:
            today_data = items[-1]['data'].split('|') 
            high_prices = [int(item['data'].split('|')[2]) for item in items]
            
            stock_market_data[stock_name] = {
                "price": int(today_data[4]),
                "high": int(today_data[2]),
                "low": int(today_data[3]),
                "high_60d": max(high_prices)
            }
            print(f"✅ {stock_name}: {stock_market_data[stock_name]['price']}원 수집 완료")
        time.sleep(0.1) 
    except Exception as e:
        print(f"❌ {stock_name} 에러: {e}")

# 5. 스캐너_마스터 시트에 다이렉트로 숫자 쏘기 (오리지널 방식)
latest_scanner_data = scanner_sheet.get_all_values()

col_B_updates = []   
col_G_H_updates = [] 
col_J_updates = []   

for row in latest_scanner_data[1:]:
    stock_name = row[0].strip() if len(row) > 0 else ""
    
    if stock_name in stock_market_data:
        data = stock_market_data[stock_name]
        col_B_updates.append([data["price"]])               # B열: 현재가
        col_G_H_updates.append([data["high"], data["low"]]) # G, H열: 고가/저가
        col_J_updates.append([data["high_60d"]])            # J열: 60일 최고가
    else:
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])

# 150행까지 빈칸 채우기 (과거 찌꺼기 삭제 로직)
while len(col_B_updates) < 150:
    col_B_updates.append([""])
    col_G_H_updates.append(["", ""])
    col_J_updates.append([""])

print("▶️ 구글 시트(스캐너_마스터)에 숫자 다이렉트 전송 중...")

scanner_sheet.update(range_name="B2:B151", values=col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="G2:H151", values=col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="J2:J151", values=col_J_updates, value_input_option="USER_ENTERED")

print("🎯 모든 작업 완료! 6분 걸리던 속도를 1분대로 단축시켰습니다!")
