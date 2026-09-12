import os, time, json, requests, datetime, sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import urllib3
from nightly_quotes import quote_label, naver_quote, after_hours_header

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 환경 변수 세팅
# ==========================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-1003778485916"
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
# 🏛️ 2026-09-14 시장 제도 개편 — 이 파일의 전제가 바뀌었다
# ==========================================
#   폐지: KRX 시간외 단일가 (16:00~18:00, 10분 주기 단일가)
#   신설: KRX 애프터마켓 "시간외접속매매" (16:00~20:00, 실시간 접속매매)
#   NXT: 프리 08:00~08:50 · 메인 09:00:30~15:20 · 애프터 15:40~20:00
#   출처: https://nextrade.co.kr/main.do (2026-09-13 확인)
#   미체결 주문의 세션 이월은 가정하지 않는다. 취소 시점은 증권사 안내를 확인한다.
#   애프터마켓 제외: ETF·ETN, 시장조치종목(이상급등·단기과열·투자경고·관리)
#   정적 VI 가 NXT 에도 신규 도입 (KRX 와 발동가가 다를 수 있음)
#
# ⚠️ **이 파일의 Phase 1 은 "18:05 리셋 전에 단일가를 낚아챈다"는 전제였다.**
#    그 시장이 없어졌으므로 전제가 사라진다. 17:50 샘플은 이제 애프터마켓
#    **중간 시점 가격**이다 — 여전히 쓸모는 있지만 '단일가'가 아니다.
#
# 🚫 KIS `ovtm_untp_prpr` · 네이버 `timeExtraClosePrice` 가 9/14 이후 무엇을
#    담을지 **추측하지 않는다.** 빈 값일 수도, 애프터마켓 실시간가일 수도 있다.
#    그래서 수집 동작은 그대로 두고 **라벨을 사실대로 바꾸고, 0/빈 값이
#    무엇을 뜻하는지 모른다는 것을 로그에 남긴다.** 실측 후 확정한다.
#
# 시간외 관측값은 모닝 브리핑 참고 자료다. 기존 정규장 수익률 정의는 유지하지만
# 제도 변경이 다음날 가격 형성/전략 성과에 영향을 주지 않는다는 뜻은 아니다.
REFORM_DATE = datetime.date(2026, 9, 14)   # 시간외 단일가 폐지 · 애프터마켓 개시


def after_hours_regime(today=None):
    """그날의 장 종료 후 제도. 라벨을 사실대로 붙이기 위한 것이다."""
    d = today or datetime.datetime.now(KST).date()
    if d < REFORM_DATE:
        return {"label": "시간외단일가", "window": "16:00~18:00",
                "kind": "10분 주기 단일가", "reset": True}
    return {"label": "애프터마켓", "window": "16:00~20:00",
            "kind": "실시간 접속매매", "reset": False}


# ==========================================
# KIS API: 장 종료 후 가격 조회 (FHKST01010100)
#   9/14 전 = 시간외 단일가 / 9/14 이후 = 애프터마켓 (필드 의미 미확정)
# ==========================================
def get_after_hours_price(code, kis_headers, req):
    """
    시간외 단일가(ovtm_untp_prpr) 조회.
    데이터가 없거나 0이면 미확인. 리셋 원인을 추정하지 않는다.
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
            return quote_label(out.get("ovtm_untp_prpr"),
                               out.get("ovtm_untp_prdy_ctrt"),
                               "KIS/ovtm_untp_prpr") or "미확인(가격·등락률 없음)"
        else:
            return f"API오류({res.status_code})"
    except Exception as e:
        return f"조회실패({str(e)[:20]})"

# ==========================================
# 네이버 모바일 API: NXT 종가 + 시간외 보조
# ==========================================
def get_naver_after_price(code, req, venue="NXT"):
    """
    네이버 모바일 API에서 NXT 종가(nxtClosePrice)와
    시간외 단일가(timeExtraClosePrice)를 모두 시도.
    요청한 시장의 필드만 반환. 결손은 보합이 아니다.
    """
    try:
        res = req.get(
            f"https://m.stock.naver.com/api/stock/{code}/basic",
            verify=False,
            timeout=5
        )
        if res.status_code != 200:
            return None, None

        return naver_quote(res.json(), venue)

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
            return quote_label(out.get("stck_prpr"), out.get("prdy_ctrt"), "KIS-NXT/stck_prpr")
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
    #    17시 = Phase 1 (실제 조회 시각은 값에 별도 기록)
    #    20시 이후 = Phase 2 (NXT + 차트)
    #    그 외 = 수동 실행 (양쪽 다 시도)
    # ──────────────────────────────────────
    _rg = after_hours_regime(now_obj.date())
    if current_hour == 17:
        phase       = 1
        phase_name  = f"[Phase 1] {_rg['label']} 관측 ({_rg['window']} {_rg['kind']})"
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
    print(f"🏛️ [장 종료 후 제도] {_rg['label']} {_rg['window']} · {_rg['kind']}"
          + ("" if _rg["reset"] else
             "  ⚠️ 9/14 개편 후 KIS ovtm_untp_prpr · 네이버 timeExtraClosePrice 가"
             " 무엇을 담는지 **미확정**이다. 0/빈 값을 '야간 초기화'로 단정하지 말 것"))

    # ── Google Sheets 연결 ──────────────────
    gcp_creds_str = os.environ.get("GCP_CREDENTIALS")
    if not gcp_creds_str:
        print("❌ GCP_CREDENTIALS 환경변수 없음. 종료.")
        return 1

    try:
        creds_dict = json.loads(gcp_creds_str)
        scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds  = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        doc    = client.open_by_url(SHEET_URL)
    except Exception as e:
        print(f"❌ Google Sheets 연결 실패: {e}")
        send_telegram(f"🚨 [HYEOKS 배치 오류] Sheets 연결 실패\n{e}")
        return 1

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
        return 1

    # ── 시트 데이터 로드 ────────────────────
    try:
        target_sheet = doc.worksheet("주가데이터_보조")
        all_data     = target_sheet.get_all_values()
    except Exception as e:
        print(f"❌ 주가데이터_보조 시트 로드 실패: {e}")
        return 1

    if len(all_data) < 2:
        print("⚠️ 시트 데이터가 비어있음.")
        return 1

    # ── 헤더 보정 ───────────────────────────
    all_data[0] = after_hours_header(all_data[0], _rg)

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
    phase1_fail   = 0   # 가격 관측값 미확인·조회 오류
    nxt_kis_ok    = 0   # KIS NXT 성공 카운트
    nxt_naver_ok  = 0   # 네이버 NXT 폴백 성공 카운트

    # ── 종목별 처리 루프 ────────────────────
    for idx in range(1, len(all_data)):
        row = all_data[idx]
        while len(row) < 28:  # 🔧 26/27번 칸까지 안전하게 쓰려면 23으로는 부족해서 28로 확장
            row.append("")

        if not row[0].strip() or not row[1].strip():
            continue

        name = str(row[0]).strip()
        code = str(row[1]).replace("'", "").strip().zfill(6)

        # 기존 값 보존 (Phase가 해당 없을 때 덮어쓰지 않음)
        # 🔧 [수정] 기존 20/22번 칸은 "프로그램"/"기관·외인 누적" 컬럼이라, 시간외/NXT 데이터를 쓰면 그 원래 데이터를
        #    덮어쓰는 사고였음. hyeoks_morning.py가 실제로 읽는 26/27번(시간외단일가/NXT야간종가)으로 정정.
        single_val = row[26] if len(row) > 26 and row[26] else "기록없음"
        nxt_val    = row[27] if len(row) > 27 and row[27] else "미수집"
        ma20_text  = row[5]
        high60_text= row[12]

        # ── Phase 1: 시간외 단일가 ──────────
        if run_phase1:
            # KIS API 1차 시도
            kis_single = get_after_hours_price(code, kis_headers, req)

            if kis_single.startswith(("미확인", "API오류", "조회실패")):
                naver_val, trade_type = get_naver_after_price(code, req, venue="시외")
                if naver_val and trade_type == "시외":
                    single_val = naver_val + " (네이버보조)"
                    print(f"  [보조] {name}: {kis_single} → 네이버 {naver_val}")
                else:
                    single_val = "미확인(가격 없음 또는 조회 오류)"
                    phase1_fail += 1
            else:
                single_val = kis_single
            if now_obj.date() >= REFORM_DATE:
                single_val += " [개편 후 필드 의미 미검증]"
            single_val += f" [조회 {datetime.datetime.now(KST).isoformat(timespec='seconds')}; 체결시각 미확인]"

        # ── Phase 2: NXT + 차트 ─────────────
        if run_phase2:
            # NXT: KIS 1차 시도
            nxt_kis = get_nxt_kis_price(code, kis_headers, req)

            if nxt_kis:
                nxt_val   = nxt_kis
                nxt_kis_ok += 1
            else:
                # KIS NXT 실패(403 등) → 네이버 폴백
                naver_nxt, trade_type = get_naver_after_price(code, req, venue="NXT")
                if naver_nxt and trade_type == "NXT":
                    nxt_val      = naver_nxt
                    nxt_naver_ok += 1
                else:
                    nxt_val = "미확인(NXT 가격 없음 또는 조회 오류)"
            nxt_val += f" [조회 {datetime.datetime.now(KST).isoformat(timespec='seconds')}; 체결시각 미확인]"

            # 차트 데이터 (MA20, 60일 최고가)
            new_ma20, new_high60 = get_chart_data(code, kis_headers, req, date_100, today_str)
            if new_ma20:
                ma20_text   = new_ma20
            if new_high60:
                high60_text = new_high60

        # 변경값 반영
        row[5]  = ma20_text
        row[12] = high60_text
        row[26] = single_val
        row[27] = nxt_val
        all_data[idx] = row

        success_count += 1
        print(f"[{idx:02d}] {name}({code}) | 단일가: {single_val} | NXT: {nxt_val}")

        # KIS API 호출 간격 (과부하 방지)
        time.sleep(0.8)

    # ── Google Sheets 저장 ──────────────────
    try:
        # Only write owned columns. Never rewrite scores/theme/program fields,
        # or send 32-wide rows into a 28-column range.
        updates = [{"range": "AA1:AB1", "values": [all_data[0][26:28]]}]
        if run_phase1:
            updates.append({"range": f"AA2:AA{len(all_data)}", "values": [[r[26]] for r in all_data[1:]]})
        if run_phase2:
            updates.extend([
                {"range": f"AB2:AB{len(all_data)}", "values": [[r[27]] for r in all_data[1:]]},
                {"range": f"F2:F{len(all_data)}", "values": [[r[5]] for r in all_data[1:]]},
                {"range": f"M2:M{len(all_data)}", "values": [[r[12]] for r in all_data[1:]]},
            ])
        target_sheet.batch_update(updates, value_input_option="RAW")
        print(f"\n✅ 저장 완료! ({success_count}종목)")
    except Exception as e:
        print(f"❌ 저장 실패: {e}")
        send_telegram(f"🚨 [HYEOKS 배치] 저장 실패\n{e}")
        return 1

    # ── 텔레그램 결과 리포트 ────────────────
    if phase == 1:
        result_detail = (
            f"✅ 수치 관측: {success_count - phase1_fail}종목 (세션 유효성 별도)\n"
            f"⚠️ 미확인·조회 오류: {phase1_fail}종목\n\n"
        )
        if phase1_fail > 0:
            result_detail += (
                "조회 실패·미지원·무거래·필드 변경 등 원인은 추가 확인이 필요합니다."
            )
        else:
            result_detail += "수치 수집 완료. 당일 해당 세션의 체결값인지는 별도 검증이 필요합니다."
    elif phase == 2:
        result_detail = (
            f"✅ 처리 종목: {success_count}종목\n"
            f"📡 KIS-NXT 성공: {nxt_kis_ok}종목\n"
            f"🌐 네이버 폴백: {nxt_naver_ok}종목"
        )
    else:
        result_detail = (
            f"✅ 처리 종목: {success_count}종목\n"
            f"⚠️ 미확인·조회 오류: {phase1_fail}종목\n"
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
    sys.exit(main() or 0)
