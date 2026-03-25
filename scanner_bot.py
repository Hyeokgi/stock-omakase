import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import time

# ==========================================
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit" 
# ==========================================

print("🤖 [신정재 종가베팅 스캐너] 로봇 가동 시작...")

# 1. 구글 시트 인증 및 연결
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
client = gspread.authorize(creds)
doc = client.open_by_url(SHEET_URL)

info_sheet = doc.worksheet("기업정보")
scanner_sheet = doc.worksheet("스캐너_마스터")

# 2. 종목코드 1차 매핑 (기업정보 탭)
print("▶️ 종목코드 1차 매핑 중 (기업정보)...")
info_data = info_sheet.get_all_values()
name_to_code = {}
for row in info_data[1:]:
    if len(row) >= 3:
        name = row[0].strip()
        code = str(row[2]).strip().zfill(6)
        name_to_code[name] = code

# 💡 [업그레이드] 네이버 종목 검색 API (기업정보에 없는 종목 방어용)
def search_code_from_naver(stock_name):
    try:
        url = f"https://m.stock.naver.com/api/search/all?keyword={stock_name}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        data = res.json()
        if data.get('result') and data['result'].get('stocks'):
            return data['result']['stocks'][0]['itemCode']
    except Exception:
        pass
    return None

# 3. 스캐너 탭에 있는 종목명 먼저 수집
scanner_data = scanner_sheet.get_all_values()
target_stocks = [row[0].strip() for row in scanner_data[1:] if len(row) > 0 and row[0].strip()]

print(f"▶️ 총 {len(target_stocks)}개 종목 데이터 수집 시작...")
headers = {'User-Agent': 'Mozilla/5.0'}
stock_market_data = {}

# 4. 실시간 데이터 크롤링 (메모리에 보관만 함)
for stock_name in target_stocks:
    code = name_to_code.get(stock_name)
    
    # 코드가 없다면 네이버에서 실시간 검색하여 자동 맵핑
    if not code:
        code = search_code_from_naver(stock_name)
        if code:
            print(f"🔍 [{stock_name}] 기업정보 누락 -> 네이버 자동 검색 성공 ({code})")
            name_to_code[stock_name] = code # 다음을 위해 저장
        else:
            print(f"❌ [{stock_name}] 종목코드를 찾을 수 없습니다.")
            continue

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
            
            # 💡 [핵심] 바로 시트에 쓰지 않고, 이름표를 붙여 딕셔너리에 안전하게 보관
            stock_market_data[stock_name] = {
                "price": current_price,
                "high": today_high,
                "low": today_low,
                "high_60d": high_60d
            }
            print(f"✅ {stock_name}: 현재가 {current_price} | 고가 {today_high} | 저가 {today_low} | 60일최고 {high_60d}")
        time.sleep(0.2) 
    except Exception as e:
        print(f"❌ {stock_name} 에러: {e}")

# 5. 🚨 [버그 해결] 쓰기 직전에 시트를 '다시' 읽어와서 순서 꼬임 방지
print("▶️ 구글 시트 최신 상태 다시 확인 및 업데이트 배열 생성...")
latest_scanner_data = scanner_sheet.get_all_values()

col_B_updates = []   
col_G_H_updates = [] 
col_J_updates = []   

for row in latest_scanner_data[1:]:
    stock_name = row[0].strip() if len(row) > 0 else ""
    
    # 현재 줄의 종목명과 크롤링한 데이터를 1:1로 정확히 매칭 (절대 안 섞임)
    if stock_name in stock_market_data:
        data = stock_market_data[stock_name]
        col_B_updates.append([data["price"]])
        col_G_H_updates.append([data["high"], data["low"]])
        col_J_updates.append([data["high_60d"]])
    else:
        col_B_updates.append([""])
        col_G_H_updates.append(["", ""])
        col_J_updates.append([""])

# 150행까지 빈칸 채우기 (과거 찌꺼기 삭제 로직 유지)
while len(col_B_updates) < 150:
    col_B_updates.append([""])
    col_G_H_updates.append(["", ""])
    col_J_updates.append([""])

print("▶️ 구글 시트로 데이터 일괄 전송 중...")

# 파이썬의 Int 타입이 시트에 순수 '숫자'로 꽂히도록 USER_ENTERED 옵션 유지
scanner_sheet.update(range_name="B2:B151", values=col_B_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="G2:H151", values=col_G_H_updates, value_input_option="USER_ENTERED")
scanner_sheet.update(range_name="J2:J151", values=col_J_updates, value_input_option="USER_ENTERED")

print("🎯 모든 작업 완료! 동기화 꼬임 방지 및 찌꺼기 청소가 완벽하게 끝났습니다!")
