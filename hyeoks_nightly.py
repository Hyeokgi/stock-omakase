import os, time, json, requests, datetime, re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 💡 환경 변수 세팅
# ==========================================
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
    print(f"🌙 [HYEOKS 심야 종합 배치] 시스템 가동 ({now_str})")
    send_telegram(f"🌙 [HYEOKS 심야 배치 가동]\n\n총 370여 개 종목의 [시간외 종가 / 20일선 / 60일 최고가 / 신용비율] 일괄 수집 및 계산을 시작합니다.\n(약 7~10분 소요 예정)")

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

    # 3. 데이터 시트 로드 및 헤더 확장
    target_sheet = doc.worksheet("주가데이터_보조")
    all_data = target_sheet.get_all_values()
    if len(all_data) < 2: return

    # 헤더가 짧으면 '신용잔고율'을 넣을 수 있도록 23칸(W열)까지 확장
    header = all_data[0]
    while len(header) < 23:
        header.append("")
    header[22] = "신용잔고율"
    all_data[0] = header

    req = requests.Session()
    req.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'})
    
    kis_headers = {
        "authorization": f"Bearer {kis_token}",
        "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET,
        "custtype": "P"
    }

    success_count = 0
    error_count = 0

    print(f"📊 총 {len(all_data)-1}개 종목 스캔을 시작합니다. (안전 저속 크롤링 모드)")

    today = datetime.datetime.now(KST)
    date_100_days_ago = (today - datetime.timedelta(days=100)).strftime('%Y%m%d')
    today_str_kis = today.strftime('%Y%m%d')

    for idx in range(1, len(all_data)):
        row = all_data[idx]
        while len(row) < 23: row.append("") # 행 길이 맞춤
        
        if not row[1].strip(): continue

        name = str(row[0]).strip()
        code = str(row[1]).replace("'", "").strip().zfill(6)
        
        # 기본값 세팅 (실패 시 기존값 유지)
        after_hours_text = row[20]
        ma20_text = row[5]
        high60_text = row[12]
        credit_text = row[22] if row[22] else "확인불가"

        try:
            # 💡 [작업 1] KIS API: 시간외 단일가 등락률 수집
            kis_headers["tr_id"] = "FHKST01010100"
            res_price = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3).json()
            
            if res_price.get("rt_cd") == "0":
                output = res_price.get("output", {})
                ah_rate_str = output.get("ovtm_untp_prdy_ctrt", "0")
                ah_price_str = output.get("ovtm_untp_prpr", "0")
                try:
                    ah_rate = float(ah_rate_str)
                    ah_price = int(ah_price_str)
                    if ah_rate > 0: after_hours_text = f"🔺 +{ah_rate:.2f}% ({ah_price:,}원)"
                    elif ah_rate < 0: after_hours_text = f"🔵 {ah_rate:.2f}% ({ah_price:,}원)"
                    else: after_hours_text = "➖ 0.00% (보합)"
                except: pass

            # 💡 [작업 2] KIS API: 일봉 60일치 가져와서 '20일선' 및 '60일 최고가' 계산
            kis_headers["tr_id"] = "FHKST03010100"
            params_hist = {
                "fid_cond_mrkt_div_code": "J", "fid_input_iscd": code,
                "fid_input_date_1": date_100_days_ago, "fid_input_date_2": today_str_kis,
                "fid_period_div_code": "D", "fid_org_adj_prc": "0"
            }
            res_hist = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", headers=kis_headers, params=params_hist, verify=False, timeout=3).json()
            
            if res_hist.get("rt_cd") == "0":
                hist_data = res_hist.get("output2", [])[:60] # 최근 60영업일
                if len(hist_data) >= 20:
                    closes = [int(x['stck_clpr']) for x in hist_data[:20]]
                    ma20_text = f"{int(sum(closes) / 20):,}"
                if len(hist_data) > 0:
                    highs = [int(x['stck_hgpr']) for x in hist_data]
                    high60_text = f"{max(highs):,}"

            # 💡 [작업 3] 네이버 핀셋 크롤링: 개미들의 빚투 지표 (신용잔고율)
            try:
                main_soup = BeautifulSoup(req.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
                credit_th = main_soup.find('th', string=re.compile('신용비율'))
                if credit_th:
                    credit_td = credit_th.find_next_sibling('td')
                    if credit_td:
                        cv = credit_td.text.strip()
                        credit_text = cv if "%" in cv else f"{cv}%"
            except: pass

            # 데이터 갱신
            row[5] = ma20_text
            row[12] = high60_text
            row[20] = after_hours_text
            row[21] = "" # 당일 프로그램은 밤이므로 깔끔하게 비움
            row[22] = credit_text
            all_data[idx] = row
            
            success_count += 1
            print(f"[{idx}/{len(all_data)-1}] {name} 완료 | 시간외: {after_hours_text} | 20일선: {ma20_text} | 신용: {credit_text}")
        except Exception as e:
            print(f"❌ {name} 에러: {e}")
            error_count += 1

        # 🚨 KIS 트래픽 룰 준수 및 네이버 차단 방지 (1.0초 대기)
        time.sleep(1.0)

    # 4. 구글 시트에 "A1:W마지막행" 형태로 단 1번의 통신으로 전체 덮어쓰기! (미친 속도와 안정성)
    print("💾 구글 시트에 일괄 저장합니다...")
    try:
        target_sheet.update(range_name=f"A1:W{len(all_data)}", values=all_data, value_input_option="USER_ENTERED")
        end_str = datetime.datetime.now(KST).strftime('%H:%M')
        send_telegram(f"✅ [HYEOKS 심야 배치 완료]\n\n시각: {end_str}\n성공: {success_count}종목\n오류: {error_count}종목\n\n[시간외/20일선/60일고가/신용비율] 사전 계산이 완료되었습니다. 내일 주간 스캐너 속도가 대폭 향상됩니다! 🚀")
        print("🎉 모든 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        send_telegram(f"🚨 구글 시트 저장 중 에러 발생: {e}")

if __name__ == "__main__":
    main()
