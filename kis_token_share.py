# 연구용 수집기가 스캐너가 시트에 저장한 오늘 한투 토큰을 읽기만 해 쓰고, 없거나 안 통하면 따로 발급하게 하는 도우미
"""한투 접근 토큰 공유 `kis-token-share-v1` (2026-10-30 사용자 지시 "알림을 공유").

왜: 한투는 토큰을 발급할 때마다 알림을 보낸다. 10/8 이후 연구 수집기(뉴스 3회 · 잠정 수급 1회 · 금요일 알림)가
    실행마다 따로 발급해 평일 알림이 하루 1회 → 5회(금요일 7회)로 늘었다.
어떻게: 스캐너(omakase.get_kis_access_token)가 하루 한 번 발급해 시트 '⚙️설정' 의 KIS_TOKEN · KIS_TOKEN_DATE 에 적는다.
    여기서는 그 값을 **읽기만** 한다 — 날짜가 오늘(KST)이고 가벼운 시세 조회 한 번이 통하면 그대로 쓴다.
    없거나 · 날짜가 지났거나 · 조회가 안 통하면 지금처럼 따로 발급한다(발급분을 시트에 쓰지 않는다 — 시트 쓰기는 스캐너만).
시트 키(secret.json)가 없는 곳(PC · 시험)에서는 바로 따로 발급한다. 토큰 값은 출력하지 않는다.
"""
import datetime
import os

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SETTINGS_SHEET = "⚙️설정"
KST = datetime.timezone(datetime.timedelta(hours=9))
CHECK_PATH = "/uapi/domestic-stock/v1/quotations/inquire-price"   # 주식현재가 시세 — 토큰이 통하는지만 본다
CHECK_TR = "FHKST01010100"
CHECK_CODE = "005930"


def read_sheet_token(keyfile="secret.json"):
    """(토큰, 날짜) 또는 None. 키 파일이 없거나 읽기에 실패하면 None — 실패를 밖으로 내지 않는다."""
    if not os.path.exists(keyfile):
        return None
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        ws = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)) \
            .open_by_url(SHEET_URL).worksheet(SETTINGS_SHEET)
        cells = {r[0]: r[1] for r in ws.get_all_values() if len(r) >= 2}
        tok, day = str(cells.get("KIS_TOKEN", "")).strip(), str(cells.get("KIS_TOKEN_DATE", "")).strip()
        return (tok, day) if tok else None
    except Exception as e:                       # noqa: BLE001
        print(f"ℹ️ 시트 토큰 읽기 실패 {type(e).__name__} — 따로 발급한다")
        return None


def token_works(get, base, key, secret, tok):
    try:
        r = get(base + CHECK_PATH, timeout=10, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": CHECK_CODE},
                headers={"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {tok}",
                         "appkey": key, "appsecret": secret, "tr_id": CHECK_TR, "custtype": "P"})
        js = r.json()
        return isinstance(js, dict) and str(js.get("rt_cd")) == "0"
    except Exception:                            # noqa: BLE001
        return False


def token_for_run(get, post, key, secret, base, issue, reader=read_sheet_token, today=None):
    """(토큰, '공유' | '발급'). issue = 기존 발급 함수(post, key, secret) — 실패하면 그 예외를 그대로 낸다."""
    today = today or datetime.datetime.now(KST).strftime("%Y-%m-%d")
    got = reader()
    if got and got[1] == today and token_works(get, base, key, secret, got[0]):
        print("🔑 한투 토큰 — 스캐너가 오늘 발급한 것을 함께 쓴다(새 발급 알림 없음)")
        return got[0], "공유"
    print("🔑 한투 토큰 — " + ("시트 토큰이 오늘 것이 아니거나 안 통해 " if got else "") + "따로 발급한다")
    return issue(post, key, secret), "발급"
