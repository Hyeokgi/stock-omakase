import os, datetime, requests, json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google import genai

# 환경변수 로드
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))

client = genai.Client(api_key=GEMINI_API_KEY)

def run_sniper_bot():
    print("🎯 [HYEOKS 스나이퍼 종가베팅 봇] 가동...")
    
    # 구글 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    with open("secret.json", "w") as f: f.write(os.environ.get("GCP_CREDENTIALS"))
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    gc = gspread.authorize(creds)
    doc = gc.open_by_url(SHEET_URL)
    
    # 데이터 로드 및 필터링
    tech_data = doc.worksheet("주가데이터_보조").get_all_values()[1:]
    candidates = []
    
    for r in tech_data:
        if len(r) < 21: continue
        name, code, curr_p, chg, tajeom, vol, prog = r[0], r[1].replace("'", "").zfill(6), r[2], r[3], r[9], r[18], r[20]
        
        # 💡 [필터링] 적자 기업, 윗꼬리 저항 배제 / 눌림 및 에너지응축 관련 타점만 수집
        if "3년적자" in tajeom or "저항 출회" in r[14]: continue
        if "눌림" in tajeom or "이평수렴" in tajeom or "플랫폼" in tajeom:
            candidates.append(f"종목:{name}({code}), 현재가:{curr_p}원({chg}), 타점:{tajeom}, 거래량:{vol}, 프로그램:{prog}")

    if not candidates:
        msg = "🎯 [HYEOKS 스나이퍼 종가베팅]\n\n오늘 장은 [M-1] 눌림목 종배 조건에 부합하는 종목이 없습니다. 관망하십시오 🐆"
    else:
        # AI 프롬프트 (빠른 판단)
        prompt = f"""귀하는 HYEOKS 리서치 센터의 종가베팅(스나이퍼) 전담 AI입니다.
아래 후보 중 신정재 트레이더의 [M-1] 에너지응축 눌림목 전략(거래량 급감, 이평선 수렴, 윗꼬리 없음)에 가장 완벽한 1종목을 골라 아래 양식으로만 출력하십시오.

[출력양식]
🎯 [HYEOKS 스나이퍼 종가베팅 픽]
▪️ 종목명: 
▪️ 현재가: 
▪️ 핵심근거: (2줄 이내로 간결하게)

[후보 리스트]
{chr(10).join(candidates)}
"""
        try:
            res = client.models.generate_content(model='gemini-2.5-pro', contents=prompt)
            msg = res.text.strip()
        except Exception as e:
            msg = f"🎯 스나이퍼 봇 에러: {str(e)}"

    print(msg)

    # 1. 텔레그램 실시간 발송
    if TELEGRAM_BOT_TOKEN:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                      data={'chat_id': TELEGRAM_CHAT_ID, 'text': msg})

    # 2. 브리핑_기록 탭에 로그 저장
    now_str = datetime.datetime.now(KST).strftime('%Y. %m. %d %p %I:%M:%S')
    doc.worksheet("브리핑_기록").insert_row([now_str, msg], index=2)
    print("✅ 스나이퍼 브리핑 기록 완료")

if __name__ == "__main__":
    run_sniper_bot()
