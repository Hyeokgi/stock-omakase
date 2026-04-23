import os, time, json, requests, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 환경 변수 세팅
# ==========================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")
KIS_APP_KEY        = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET     = os.environ.get("KIS_APP_SECRET")
SHEET_URL          = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST                = datetime.timezone(datetime.timedelta(hours=9))

def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
                verify=False,
                timeout=10
            )
        except Exception as e:
            print(f"⚠️ 텔레그램 발송 실패: {e}")

# ==========================================
# KIS API: 시간외 단일가 조회 (FHKST01010100)
# ==========================================
def get_after_hours_price(code, kis_headers, req):
    """
    시간외 단일가(ovtm_untp_prpr) 조회.
    데이터가 0이면 야간초기화(0원) 반환.
    """
    try:
        h = dict(kis_headers)
        h["tr_id"] = "FHKST01010100"
        res = req.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=h,
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code},
            verify=False,
            timeout=5
        )
        if res.status_code == 200 and res.json().get("rt_cd") == "0":
            out  = res.json().get("output", {})
            prc  = out.get("ovtm_untp_prpr", "0")
            rate = out.get("ovtm_untp_prdy_ctrt", "0")
            if prc and int(prc) > 0:
                r_val = float(rate)
                if r_val > 0:
                    return f"🔺+{r_val:.2f}% ({int(prc):,}원)"
                elif r_val < 0:
                    return f"🔵{r_val:.2f}% ({int(prc):,}원)"
                else:
                    return f"➖0.00% ({int(prc):,}원)"
            else:
                return "야간초기화(0원)"
        else:
            return f"API오류({res.status_code})"
    except Exception as e:
        return f"조회실패({str(e)[:20]})"

# ==========================================
# 네이버 모바일 API: NXT 종가 + 시간외 보조
# ==========================================
def get_naver_after_price(code, req):
    """
    네이버 모바일 API에서 NXT 종가(nxtClosePrice)와
    시간외 단일가(timeExtraClosePrice)를 모두 시도.
    가장 의미있는 값을 반환.
    """
    try:
        res = req.get(
            f"https://m.stock.naver.com/api/stock/{code}/basic",
            verify=False,
            timeout=5
        )
        if res.status_code != 200:
            return None, None

        data       = res.json()
        reg_close  = float(str(data.get("closePrice") or "0").replace(",", ""))

        # NXT 종가 우선 시도
        nxt_price  = float(str(data.get("nxtClosePrice") or "0").replace(",", ""))
        nxt_rate   = float(str(data.get("nxtFluctuationsRatio") or "0").replace(",", ""))

        # 시간외 단일가 보조
        ext_price  = float(str(data.get("timeExtraClosePrice") or "0").replace(",", ""))
        ext_rate   = float(str(data.get("timeExtraFluctuationsRatio") or "0").replace(",", ""))

        best_price, best_rate, trade_type = 0.0, 0.0, ""

        if nxt_price > 0 and nxt_price != reg_close:
            best_price = nxt_price
            best_rate  = nxt_rate if nxt_rate != 0.0 else round(((nxt_price - reg_close) / reg_close) * 100, 2)
            trade_type = "NXT"
        elif ext_price > 0 and ext_price != reg_close:
            best_price = ext_price
            best_rate  = ext_rate if ext_rate != 0.0 else round(((ext_price - reg_close) / reg_close) * 100, 2)
            trade_type = "시외"

        if best_price > 0 and trade_type:
            if best_rate > 0:
                label = f"🔺+{best_rate:.2f}% ({int(best_price):,}원) [{trade_type}]"
            elif best_rate < 0:
                label = f"🔵{best_rate:.2f}% ({int(best_price):,}원) [{trade_type}]"
            else:
                label = f"➖0.00% ({int(best_price):,}원) [{trade_type}]"
            return label, trade_type
        else:
            return "➖ 0.00% (보합)", ""

    except Exception as e:
        return f"조회실패({str(e)[:20]})", ""

# ==========================================
# KIS API: NXT 전용 조회 (FNPST01010100)
# ==========================================
def get_nxt_kis_price(code, kis_headers, req):
    """
    KIS NXT 전용 TR. 개인 오픈API에서 403 날 수 있으므로
    실패 시 None 반환 → 네이버 폴백으로 이어짐.
    """
    try:
        h = dict(kis_headers)
        h["tr_id"] = "FNPST01010100"
        res = req.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-nextrade-price",
            headers=h,
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code},
            verify=False,
            timeout=5
        )
        if res.status_code == 200 and res.json().get("rt_cd") == "0":
            out    = res.json().get("output", {})
            n_prc  = out.get("stck_prpr", "0")
            n_rate = out.get("prdy_ctrt", "0")
            if n_prc and int(n_prc) > 0:
                nr = float(n_rate)
                if nr > 0:
                    return f"🔺+{nr:.2f}% ({int(n_prc):,}원) [KIS-NXT]"
                elif nr < 0:
                    return f"🔵{nr:.2f}% ({int(n_prc):,}원) [KIS-NXT]"
                else:
                    return f"➖0.00% ({int(n_prc):,}원) [KIS-NXT]"
        # 403 또는 데이터 없음 → None 반환
        return None
    except Exception:
        return None

# ==========================================
# KIS API: 차트 데이터 (MA20, 60일 최고가)
# ==========================================
def get_chart_data(code, kis_headers, req, date_100, today_str):
    ma20_text   = ""
    high60_text = ""
    try:
        h = dict(kis_headers)
        h["tr_id"] = "FHKST03010100"
        res = req.get(
            "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers=h,
            params={
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd":         code,
                "fid_input_date_1":       date_100,
                "fid_input_date_2":       today_str,
                "fid_period_div_code":    "D",
                "fid_org_adj_prc":        "0"
            },
            verify=False,
            timeout=5
        )
        if res.status_code == 200:
            h_data = res.json().get("output2", [])[:60]
            if len(h_data) >= 20:
                ma20_text = f"{int(sum(int(x['stck_clpr']) for x in h_data[:20]) / 20):,}"
            if len(h_data) > 0:
                high60_text = f"{max(int(x['stck_hgpr']) for x in h_data):,}"
    except Exception:
        pass
    return ma20_text, high60_text

# ==========================================
# 메인 실행
# ==========================================
def main():
    now_obj      = datetime.datetime.now(KST)
    current_hour = now_obj.hour
    now_str      = now_obj.strftime("%Y-%m-%d %H:%M")

    # ──────────────────────────────────────
    # ✅ 핵심 수정: Phase 분기를 명확하게
    #    17시 = Phase 1 (18:05 리셋 전 낚아채기)
    #    20시 이후 = Phase 2 (NXT + 차트)
    #    그 외 = 수동 실행 (양쪽 다 시도)
    # ──────────────────────────────────────
    if current_hour == 17:
        phase       = 1
        phase_name  = "[Phase 1] 17:50 시간외 단일가 스냅샷 (리셋 전 낚아채기)"
        run_phase1  = True
        run_phase2  = False
    elif current_hour >= 20:
        phase       = 2
        phase_name  = "[Phase 2] NXT 야간거래 + 차트 마감 스냅샷"
        run_phase1  = False
        run_phase2  = True
    else:
        phase       = 0
        phase_name  = f"[수동 실행] {current_hour}시 — Phase 1 + Phase 2 모두 시도"
        run_phase1  = True
        run_phase2  = True

    print(f"🌙 [HYEOKS 심야 정밀 배치] {phase_name} 가동 ({now_str})")

    # ── Google Sheets 연결 ──────────────────
    gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_creds_str:
        print("❌ GCP_CREDENTIALS 환경변수 없음. 종료.")
        return

    try:
        creds_dict = json.loads(gcp_creds_str)
        scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc    = client.open_by_url(SHEET_URL)
    except Exception as e:
        print(f"❌ Google Sheets 연결 실패: {e}")
        send_telegram(f"🚨 [HYEOKS 배치 오류] Sheets 연결 실패\n{e}")
        return

    # ── KIS 토큰 확보 ───────────────────────
    kis_token = ""
    try:
        setting_rows = doc.worksheet("⚙️설정").get_all_values()
        for row in setting_rows:
            if len(row) >= 2 and row[0] == "KIS_TOKEN":
                kis_token = row[1]
                break
    except Exception as e:
        print(f"⚠️ 설정 시트 읽기 실패: {e}")

    if not kis_token:
        print("❌ KIS 토큰 없음. 종료.")
        send_telegram("🚨 [HYEOKS 배치 오류] KIS 토큰을 설정 시트에서 찾을 수 없습니다.")
        return

    # ── 시트 데이터 로드 ────────────────────
    try:
        target_sheet = doc.worksheet("주가데이터_보조")
        all_data     = target_sheet.get_all_values()
    except Exception as e:
        print(f"❌ 주가데이터_보조 시트 로드 실패: {e}")
        return

    if len(all_data) < 2:
        print("⚠️ 시트 데이터가 비어있음.")
        return

    # ── 헤더 보정 ───────────────────────────
    header = all_data[0]
    while len(header) < 23:
        header.append("")
    # U열(20) = 시간외단일가, V열(21) = 소속테마(기존유지), W열(22) = NXT야간거래
    header[20] = "시간외단일가"
    header[22] = "NXT야간거래"
    all_data[0] = header

    # ── KIS 공통 헤더 ───────────────────────
    req = requests.Session()
    kis_headers = {
        "authorization": f"Bearer {kis_token}",
        "appkey":        KIS_APP_KEY,
        "appsecret":     KIS_APP_SECRET,
        "custtype":      "P",
        "Content-Type":  "application/json; charset=utf-8"
    }

    date_100  = (now_obj - datetime.timedelta(days=100)).strftime("%Y%m%d")
    today_str = now_obj.strftime("%Y%m%d")

    success_count = 0
    phase1_fail   = 0   # 0원으로 초기화된 종목 수 추적
    nxt_kis_ok    = 0   # KIS NXT 성공 카운트
    nxt_naver_ok  = 0   # 네이버 NXT 폴백 성공 카운트

    # ── 종목별 처리 루프 ────────────────────
    for idx in range(1, len(all_data)):
        row = all_data[idx]
        while len(row) < 23:
            row.append("")

        if not row[0].strip() or not row[1].strip():
            continue

        name = str(row[0]).strip()
        code = str(row[1]).replace("'", "").strip().zfill(6)

        # 기존 값 보존 (Phase가 해당 없을 때 덮어쓰지 않음)
        single_val = row[20] if row[20] else "기록없음"
        nxt_val    = row[22] if row[22] else "미수집"
        ma20_text  = row[5]
        high60_text= row[12]

        # ── Phase 1: 시간외 단일가 ──────────
        if run_phase1:
            # KIS API 1차 시도
            kis_single = get_after_hours_price(code, kis_headers, req)

            if "야간초기화(0원)" in kis_single:
                # KIS가 이미 0으로 초기화 → 네이버로 보조 시도
                naver_val, _ = get_naver_after_price(code, req)
                if naver_val and "조회실패" not in naver_val and "0.00%" not in naver_val:
                    single_val = naver_val + " (네이버보조)"
                    print(f"  [보조] {name}: KIS 0원 → 네이버 {naver_val}")
                else:
                    single_val = "야간초기화(0원)"
                    phase1_fail += 1
            else:
                single_val = kis_single

        # ── Phase 2: NXT + 차트 ─────────────
        if run_phase2:
            # NXT: KIS 1차 시도
            nxt_kis = get_nxt_kis_price(code, kis_headers, req)

            if nxt_kis:
                nxt_val   = nxt_kis
                nxt_kis_ok += 1
            else:
                # KIS NXT 실패(403 등) → 네이버 폴백
                naver_nxt, trade_type = get_naver_after_price(code, req)
                if naver_nxt and "조회실패" not in naver_nxt:
                    nxt_val      = naver_nxt
                    nxt_naver_ok += 1
                else:
                    nxt_val = "오픈API 미지원"

            # 차트 데이터 (MA20, 60일 최고가)
            new_ma20, new_high60 = get_chart_data(code, kis_headers, req, date_100, today_str)
            if new_ma20:
                ma20_text   = new_ma20
            if new_high60:
                high60_text = new_high60

        # 변경값 반영
        row[5]  = ma20_text
        row[12] = high60_text
        row[20] = single_val
        row[22] = nxt_val
        all_data[idx] = row

        success_count += 1
        print(f"[{idx:02d}] {name}({code}) | 단일가: {single_val} | NXT: {nxt_val}")

        # KIS API 호출 간격 (과부하 방지)
        time.sleep(0.8)

    # ── Google Sheets 저장 ──────────────────
    try:
        target_sheet.update(
            range_name=f"A1:W{len(all_data)}",
            values=all_data,
            value_input_option="USER_ENTERED"
        )
        print(f"\n✅ 저장 완료! ({success_count}종목)")
    except Exception as e:
        print(f"❌ 저장 실패: {e}")
        send_telegram(f"🚨 [HYEOKS 배치] 저장 실패\n{e}")
        return

    # ── 텔레그램 결과 리포트 ────────────────
    if phase == 1:
        result_detail = (
            f"✅ 정상 수집: {success_count - phase1_fail}종목\n"
            f"⚠️ 야간초기화(0원): {phase1_fail}종목\n\n"
        )
        if phase1_fail > 0:
            result_detail += (
                "💡 [진단] KIS 서버가 이미 데이터를 지운 상태입니다.\n"
                "GAS 트리거 시간을 17:40으로 더 당기는 것을 권장합니다."
            )
        else:
            result_detail += "🎉 리셋 전 데이터 낚아채기 성공!"
    elif phase == 2:
        result_detail = (
            f"✅ 처리 종목: {success_count}종목\n"
            f"📡 KIS-NXT 성공: {nxt_kis_ok}종목\n"
            f"🌐 네이버 폴백: {nxt_naver_ok}종목"
        )
    else:
        result_detail = (
            f"✅ 처리 종목: {success_count}종목\n"
            f"⚠️ 야간초기화(0원): {phase1_fail}종목\n"
            f"📡 KIS-NXT 성공: {nxt_kis_ok}종목\n"
            f"🌐 네이버 폴백: {nxt_naver_ok}종목"
        )

    msg = (
        f"🌙 [HYEOKS 심야 배치 완료]\n\n"
        f"📌 실행 페이즈: {phase_name}\n"
        f"🕐 실행 시각: {now_str}\n\n"
        f"{result_detail}"
    )
    send_telegram(msg)
    print(f"\n📲 텔레그램 발송 완료")

if __name__ == "__main__":
    main()
