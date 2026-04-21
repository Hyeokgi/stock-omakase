import os, time, json, requests, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 💡 환경 변수 세팅
# ==========================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))

def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': msg})

def main():
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
    print(f"🌙 [HYEOKS 심야 배치] 시간외(NXT) 데이터 수집 시작 ({now_str})")
    send_telegram(f"🌙 [HYEOKS 심야 배치 가동]\n\n총 370여 개 종목의 시간외(NXT) 데이터 수집을 시작합니다. (약 10분 소요 예정)")

    # 1. 구글 시트 연결
    gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_creds_str:
        send_telegram("🚨 GCP_CREDENTIALS 환경 변수가 없습니다.")
        return

    creds_dict = json.loads(gcp_creds_str)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_url(SHEET_URL)
    
    # 2. KIS 토큰 확보
    kis_token = ""
    try:
        setting_sheet = doc.worksheet("⚙️설정")
        for row in setting_sheet.get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN":
                kis_token = row[1]
                break
    except Exception as e:
        send_telegram(f"🚨 KIS 토큰 조회 실패: {e}")
        return

    if not (kis_token and KIS_APP_KEY and KIS_APP_SECRET):
        send_telegram("🚨 KIS API 정보가 부족하여 수집을 중단합니다.")
        return

    # 3. 데이터 시트 로드
    target_sheet = doc.worksheet("주가데이터_보조")
    all_data = target_sheet.get_all_values()
    if len(all_data) < 2:
        return

    req = requests.Session()
    headers = {
        "authorization": f"Bearer {kis_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P"
    }

    updates = []
    success_count = 0
    error_count = 0

    print(f"📊 총 {len(all_data)-1}개 종목 스캔을 시작합니다. (1.5초 간격 안전 수집)")

    # 4. 천천히 1종목씩 KIS API 호출 (심야 저속 크롤링)
    for idx, row in enumerate(all_data[1:]):
        if len(row) < 2 or not row[1].strip():
            updates.append([""])
            continue

        name = str(row[0]).strip()
        code = str(row[1]).replace("'", "").strip().zfill(6)
        
        # 기존 데이터를 기본값으로 세팅 (API 실패 시 기존 값 유지)
        existing_val = row[20] if len(row) > 20 else ""
        after_hours_text = existing_val

        try:
            res = req.get(
                "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                headers=headers, 
                params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, 
                timeout=3
            ).json()

            if res.get("rt_cd") == "0":
                output = res.get("output", {})
                
                # ovtm_untp_prdy_ctrt: 시간외 단일가 전일대비율
                # ovtm_untp_prpr: 시간외 단일가 현재가
                ah_rate_str = output.get("ovtm_untp_prdy_ctrt", "0")
                ah_price_str = output.get("ovtm_untp_prpr", "0")
                
                try:
                    ah_rate = float(ah_rate_str)
                    ah_price = int(ah_price_str)
                    
                    if ah_rate > 0:
                        after_hours_text = f"🔺 +{ah_rate:.2f}% ({ah_price:,}원)"
                    elif ah_rate < 0:
                        after_hours_text = f"🔵 {ah_rate:.2f}% ({ah_price:,}원)"
                    else:
                        after_hours_text = "➖ 0.00% (보합)"
                        
                    success_count += 1
                except:
                    pass # 숫자 변환 에러 시 기존 값 유지
            else:
                error_count += 1
        except:
            error_count += 1

        updates.append([after_hours_text])
        
        # 💡 핵심: KIS 서버 트래픽 방어를 위해 1.5초 대기 (절대 차단 안 당함)
        print(f"[{idx+1}/{len(all_data)-1}] {name}({code}) 완료 ➡️ {after_hours_text}")
        time.sleep(1.5)

    # 5. 구글 시트 'U'열 (시간외 컬럼)에 일괄 덮어쓰기 (API 호출 1번으로 최소화)
    print("💾 구글 시트에 데이터를 일괄 저장합니다...")
    try:
        # U열(21번째 열)의 2행부터 끝까지 업데이트
        target_range = f"U2:U{len(updates)+1}"
        target_sheet.update(range_name=target_range, values=updates, value_input_option="USER_ENTERED")
        
        end_str = datetime.datetime.now(KST).strftime('%H:%M')
        send_telegram(f"✅ [HYEOKS 심야 배치 완료]\n\n시각: {end_str}\n성공: {success_count}개 종목\n오류: {error_count}개 종목\n\n내일 아침 브리핑을 위한 '시간외 단일가' 갱신이 완벽하게 끝났습니다. 푹 쉬십시오 수석님! 🥂")
        print("🎉 모든 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        send_telegram(f"🚨 구글 시트 저장 중 에러 발생: {e}")

if __name__ == "__main__":
    main()
