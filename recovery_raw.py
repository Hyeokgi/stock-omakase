import requests, gspread, time
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
gc = gspread.authorize(creds)
doc = gc.open_by_url(SHEET_URL)
raw_sheet = doc.worksheet("수급_Raw")
data = raw_sheet.get_all_values()

print("🚑 [긴급 복구] 5월 14일 이후 수급_Raw 거래대금(억원) 정밀 복원 시작...")

rows = data[1:]
target_date = "2026-05-14"
session = requests.Session()
cache = {}
updated_count = 0

for row in rows:
    # row 포맷: [0]날짜, [1]순위, [2]테마명, [3]종목명, [4]종목코드, [5]등락률, [6]거래대금
    if len(row) < 7: continue
    r_date = row[0].strip()
    
    if r_date >= target_date:
        code = row[4].replace("'", "").strip().zfill(6)
        
        if code not in cache:
            try:
                url = f"https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=60&requestType=0"
                res = session.get(url, verify=False, timeout=5)
                root = ET.fromstring(res.text)
                history = {}
                for item in root.findall(".//item"):
                    d = item.get("data").split("|")
                    f_date = f"{d[0][:4]}-{d[0][4:6]}-{d[0][6:8]}"
                    close_p = int(d[4])
                    vol = int(d[5])
                    
                    # 억원 단위 거래대금 계산 (종가 * 거래량 / 1억)
                    tv_eok = (close_p * vol) // 100_000_000
                    history[f_date] = tv_eok
                cache[code] = history
                time.sleep(0.1)
            except Exception as e:
                print(f"⚠️ {row[3]} 데이터 수집 실패: {e}")
                cache[code] = {}

        correct_val = cache[code].get(r_date)
        if correct_val is not None and correct_val > 0:
            old_val = row[6]
            if str(old_val) != str(correct_val):
                row[6] = str(correct_val)
                updated_count += 1
                print(f"✅ [{r_date}] {row[3]} ({code}) : 비정상({old_val}) -> 정상({correct_val}억) 복구 완료")

if updated_count > 0:
    # 가장 안전한 구글 시트 업데이트 방식
    raw_sheet.update(values=rows, range_name="A2", value_input_option="USER_ENTERED")
    print(f"\n🎉 총 {updated_count}건의 거래대금 데이터가 완벽하게 복구되었습니다!")
else:
    print("\n✅ 수정할 데이터가 없거나 이미 모두 정상입니다.")
