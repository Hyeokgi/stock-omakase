import os, time, json, requests, datetime
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
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data={'chat_id': TELEGRAM_CHAT_ID, 'text': msg}, verify=False)

def main():
    now_obj = datetime.datetime.now(KST)
    current_hour = now_obj.hour
    now_str = now_obj.strftime('%Y-%m-%d %H:%M')
    
    # 💡 시간에 따른 페이즈(Phase) 구분
    if current_hour == 18:
        phase_name = "[Phase 1] 18:10 시간외 단일가 스냅샷"
    elif current_hour >= 20:
        phase_name = "[Phase 2] 20:30 NXT & 차트 마감 스냅샷"
    else:
        phase_name = f"[수동 실행] {current_hour}시 스냅샷"

    print(f"🌙 [HYEOKS 심야 정밀 배치] {phase_name} 가동 ({now_str})")

    gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_creds_str: return
    creds_dict = json.loads(gcp_creds_str)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_url(SHEET_URL)
    
    kis_token = ""
    try:
        for row in doc.worksheet("⚙️설정").get_all_values():
            if len(row) >= 2 and row[0] == "KIS_TOKEN":
                kis_token = row[1]; break
    except: pass
    if not kis_token: return

    target_sheet = doc.worksheet("주가데이터_보조")
    all_data = target_sheet.get_all_values()
    if len(all_data) < 2: return

    header = all_data[0]
    while len(header) < 23: header.append("")
    header[20], header[22] = "시간외단일가", "NXT야간거래"
    all_data[0] = header

    req = requests.Session()
    kis_headers = {"authorization": f"Bearer {kis_token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "custtype": "P"}

    success_count = 0
    date_100 = (now_obj - datetime.timedelta(days=100)).strftime('%Y%m%d')
    today_str = now_obj.strftime('%Y%m%d')

    for idx in range(1, len(all_data)):
        row = all_data[idx]
        while len(row) < 23: row.append("")
        if len(row) < 2 or not row[1].strip(): continue

        name, code = str(row[0]).strip(), str(row[1]).replace("'", "").strip().zfill(6)
        
        # 시트에 있는 기존 데이터를 그대로 가져옴 (서로 지우지 않기 위해)
        ma20_text, high60_text = row[5], row[12]
        single_val = row[20] if row[20] else "기록없음"
        nxt_val = row[22] if row[22] else "오픈API 미지원"

        # 💡 [Phase 1 - 18시] 시간외 단일가만 수집 (나머지는 건드리지 않음)
        if current_hour == 18 or current_hour < 20:
            try:
                kis_headers["tr_id"] = "FHKST01010100"
                res1 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price", headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3)
                if res1.status_code == 200:
                    out = res1.json().get("output", {})
                    rate, prc = out.get("ovtm_untp_prdy_ctrt", "0"), out.get("ovtm_untp_prpr", "0")
                    if prc and int(prc) > 0:
                        r_val = float(rate)
                        if r_val > 0: single_val = f"🔺+{r_val:.2f}% ({int(prc):,}원)"
                        elif r_val < 0: single_val = f"🔵{r_val:.2f}% ({int(prc):,}원)"
                        else: single_val = f"➖0.00% ({int(prc):,}원)"
                    else: single_val = "야간초기화(0원)"
            except: pass

        # 💡 [Phase 2 - 20시 이후] NXT 야간거래 및 차트 데이터만 수집
        if current_hour >= 20:
            try:
                kis_headers["tr_id"] = "FNPST01010100" # NXT 전용 API
                res2 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-nextrade-price", headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}, verify=False, timeout=3)
                if res2.status_code == 200:
                    out_n = res2.json().get("output", {})
                    n_rate, n_prc = out_n.get("prdy_ctrt", "0"), out_n.get("stck_prpr", "0")
                    if n_prc and int(n_prc) > 0:
                        nr_val = float(n_rate)
                        if nr_val > 0: nxt_val = f"🔺+{nr_val:.2f}% ({int(n_prc):,}원)"
                        elif nr_val < 0: nxt_val = f"🔵{nr_val:.2f}% ({int(n_prc):,}원)"
                        else: nxt_val = f"➖0.00% ({int(n_prc):,}원)"
            except: pass

            try:
                kis_headers["tr_id"] = "FHKST03010100"
                res3 = req.get("https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", headers=kis_headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code, "fid_input_date_1": date_100, "fid_input_date_2": today_str, "fid_period_div_code": "D", "fid_org_adj_prc": "0"}, verify=False, timeout=3)
                if res3.status_code == 200:
                    h_data = res3.json().get("output2", [])[:60]
                    if len(h_data) >= 20: ma20_text = f"{int(sum([int(x['stck_clpr']) for x in h_data[:20]]) / 20):,}"
                    if len(h_data) > 0: high60_text = f"{max([int(x['stck_hgpr']) for x in h_data]):,}"
            except: pass

        row[5], row[12], row[20], row[22] = ma20_text, high60_text, single_val, nxt_val
        all_data[idx] = row
        success_count += 1
        print(f"[{idx}] {name} | 단일가:{single_val} | NXT:{nxt_val}")
        time.sleep(1.0)

    try:
        target_sheet.update(range_name=f"A1:W{len(all_data)}", values=all_data, value_input_option="USER_ENTERED")
        send_telegram(f"✅ [HYEOKS 심야 배치 완료]\n\n실행 페이즈: {phase_name}\n성공: {success_count}종목\n\n수집이 완료되었습니다! 🥂")
    except Exception as e:
        send_telegram(f"🚨 저장 에러: {e}")

if __name__ == "__main__":
    main()
