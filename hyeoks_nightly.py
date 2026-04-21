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
        requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': msg}, verify=False)

def main():
    now_str = datetime.datetime.now(KST).strftime('%Y-%m-%d %H:%M')
    print(f"🌙 [HYEOKS 심야 정밀 배치] 시스템 가동 ({now_str})")

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
        send_telegram("🚨 KIS API 정보가 부족합니다.")
        return

    # 3. 데이터 시트 로드 및 헤더 확장
    target_sheet = doc.worksheet("주가데이터_보조")
    all_data = target_sheet.get_all_values()
    if len(all_data) < 2: return

    header = all_data[0]
    while len(header) < 24: header.append("")
    header[20] = "시간외단일가"
    # header[21]은 프로그램(당일)이므로 건드리지 않음
    header[22] = "NXT야간거래"
    header[23] = "신용잔고율"
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
    today = datetime.datetime.now(KST)
    date_100_days_ago = (today - datetime.timedelta(days=100)).strftime('%Y%m%d')
    today_str_kis = today.strftime('%Y%m%d')

    print(f"📊 총 {len(all_data)-1}개 종목 독립 격리형(Isolated) 스캔 시작")

    for idx in range(1, len(all_data)):
        row = all_data[idx]
        while len(row) < 24: row.append("") # 행 길이 24칸 맞춤
        if len(row) < 2 or not row[1].strip(): continue

        name = str(row[0]).strip()
        code = str(row[1]).replace("'", "").strip().zfill(6)
        
        # 기본값 (에러가 나도 이 값들이 안전하게 보존됨)
        single_val = "기록없음"
        nxt_val = "대상아님(지원안됨)"
        ma20_text = row[5]
        high60_text = row[12]
        credit_text = row[23] if row[23] else "확인불가"

        # 💡 [방화벽 1] 시간외 단일가 수집
        try:
            kis_headers["tr_id"] = "FHKST01010100"
            res1 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", 
                           headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3)
            if res1.status_code == 200:
                out = res1.json().get("output", {})
                rate = out.get("ovtm_untp_prdy_ctrt", "0")
                prc = out.get("ovtm_untp_prpr", "0")
                if rate and float(rate) != 0:
                    r_val = float(rate)
                    single_val = f"🔺+{r_val:.2f}%" if r_val > 0 else f"🔵{r_val:.2f}%"
                    single_val += f"({int(prc):,}원)"
                else: single_val = "➖0.00%"
        except Exception as e: pass

        # 💡 [방화벽 2] NXT 야간 거래 수집 (API 미지원 에러 완벽 차단)
        try:
            kis_headers["tr_id"] = "FNPST01010100"
            res2 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-nextrade-price", 
                           headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3)
            if res2.status_code == 200:
                out_n = res2.json().get("output", {})
                n_rate = out_n.get("prdy_ctrt", "0")
                n_prc = out_n.get("stck_prpr", "0")
                if n_rate and float(n_rate) != 0:
                    nr_val = float(n_rate)
                    nxt_val = f"🔺+{nr_val:.2f}%" if nr_val > 0 else f"🔵{nr_val:.2f}%"
                    nxt_val += f"({int(n_prc):,}원)"
                else: nxt_val = "➖0.00%"
        except Exception as e: pass

        # 💡 [방화벽 3] 차트 데이터 (20일선/60일고가)
        try:
            kis_headers["tr_id"] = "FHKST03010100"
            res3 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", 
                           headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code, "fid_input_date_1": date_100_days_ago, "fid_input_date_2": today_str_kis, "fid_period_div_code": "D", "fid_org_adj_prc": "0"}, verify=False, timeout=4)
            if res3.status_code == 200:
                h_data = res3.json().get("output2", [])[:60]
                if len(h_data) >= 20: ma20_text = f"{int(sum([int(x['stck_clpr']) for x in h_data[:20]]) / 20):,}"
                if len(h_data) > 0: high60_text = f"{max([int(x['stck_hgpr']) for x in h_data]):,}"
        except Exception as e: pass

        # 💡 [방화벽 4] 네이버 신용비율
        try:
            main_soup = BeautifulSoup(req.get(f"https://finance.naver.com/item/main.naver?code={code}", verify=False, timeout=3).content, 'html.parser', from_encoding='cp949')
            credit_th = main_soup.find('th', string=re.compile('신용비율'))
            if credit_th:
                credit_td = credit_th.find_next_sibling('td')
                if credit_td: credit_text = credit_td.text.strip()
        except Exception as e: pass

        # 결과 데이터 매핑 (V열인 21번 인덱스는 절대 건드리지 않음)
        row[5] = ma20_text
        row[12] = high60_text
        row[20] = single_val
        row[22] = nxt_val
        row[23] = credit_text
        
        all_data[idx] = row
        success_count += 1
        print(f"[{idx}] {name} 완료 | 시간외:{single_val} | 신용:{credit_text}")

        time.sleep(1.2) # API 부하 방지용

    # 4. 구글 시트에 일괄 저장
    try:
        target_sheet.update(range_name=f"A1:X{len(all_data)}", values=all_data, value_input_option="USER_ENTERED")
        send_telegram(f"✅ [HYEOKS 심야 배치 완벽 복구 완료]\n\n성공: {success_count}종목 / 오류: {error_count}종목\n\n모든 종목의 독립적 수집이 완료되었습니다! 🥂")
    except Exception as e:
        send_telegram(f"🚨 저장 에러: {e}")

if __name__ == "__main__":
    main()
