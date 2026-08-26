# -*- coding: utf-8 -*-
# ==========================================================================
# 🛡️ HYEOKS 정적데이터 수집기 (Phase 1: 관리종목/거래정지/투자경고)
# --------------------------------------------------------------------------
# 목적: DB_정적데이터 시트(A~F)의 소유권을 이 수집기가 가진다.
#       omakase.py 는 더 이상 시트를 비우지 않고(7시 clear 폐지) 순수 reader 로만 동작한다.
# 소스: Naver 금융 벌크 조치 목록 (KRX는 로그인 벽이라 미사용). 종목별 루프 없이 벌크 GET.
# 철학: fail-CLOSED — 수집 실패/비정상 시 절대 시트를 비우지 않고 전일 스냅샷을 유지한다.
#       (그래야 게이트가 '조용히 꺼지는' 사고를 막는다.)
# ==========================================================================
import os, sys, time, datetime, requests, gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
KST = datetime.timezone(datetime.timedelta(hours=9))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# 견고성 게이트 임계치: 이 수치 미만이면 '수집 실패'로 간주하고 덮어쓰기 중단
MIN_MANAGED = 20    # 관리종목 최소 기대치
MIN_TOTAL = 50      # is_junk 합집합 최소 기대치

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
})

SOURCES = [
    ("관리종목", "https://finance.naver.com/sise/management.naver"),
    ("거래정지", "https://finance.naver.com/sise/trading_halt.naver"),
    ("투자주의", "https://finance.naver.com/sise/investment_alert.naver?type=caution"),
    ("투자경고", "https://finance.naver.com/sise/investment_alert.naver?type=warning"),
    ("투자위험", "https://finance.naver.com/sise/investment_alert.naver?type=risk"),
]


def telegram_warn(msg):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        print(f"[텔레그램 미설정] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={'chat_id': TELEGRAM_CHAT_ID, 'text': msg}, timeout=10
        )
    except Exception as e:
        print(f"⚠️ 텔레그램 발송 실패: {e}")


# 🆕 [개편 대응] 신규 PC 사이트가 실제로 쓰는 JSON API. finance.naver.com HTML이 죽어도
#    이쪽만으로 위험종목 명단을 만들 수 있다(실측 커버리지 273/274 = 99.6%).
#    한 번의 호출로 전종목을 받아 세 가지 플래그를 동시에 읽는다.
#      manageStatusGb  '0' 이외 → 관리종목류
#      tradeStopYn     'Y'      → 거래정지
#      marketAlertType '01'=투자주의 / '02'=투자경고 / '03'=투자위험  ('00'=정상)
#    (코드 의미는 2026-08-26 HTML 분류와 대조해 확정: 01→투자주의 18/18, 02→투자경고 24/24, 03→투자위험 1/1)
JSON_ALL_URL = ("https://stock.naver.com/api/domestic/market/stock/default"
                "?tradeType=KRX&marketType=ALL&orderType=priceTop&startIdx=0&pageSize=3000")
MIN_JSON_SCAN = 1500   # 전종목 스캔이 이보다 적으면 응답이 잘린 것으로 보고 신뢰하지 않음


def fetch_junk_from_json():
    """신규 JSON API 전종목 스캔에서 위험종목만 골라 {code: name} 반환."""
    out = {}
    try:
        res = SESSION.get(JSON_ALL_URL, headers={'Accept': 'application/json',
                                                 'Referer': 'https://stock.naver.com/'},
                          verify=False, timeout=25)
        rows = res.json() if res.status_code == 200 else []
        if not isinstance(rows, list) or len(rows) < MIN_JSON_SCAN:
            print(f"  ⚠️ JSON 전종목 스캔이 {len(rows) if isinstance(rows, list) else '?'}건뿐 — 신뢰 임계({MIN_JSON_SCAN}) 미달로 미사용")
            return {}, 0
        for x in rows:
            code = str(x.get('itemcode', '')).strip()
            if not code.isdigit():
                continue
            if (str(x.get('manageStatusGb') or '0') != '0'
                    or x.get('tradeStopYn') == 'Y'
                    or str(x.get('marketAlertType') or '00') != '00'):
                out[code] = str(x.get('itemname', '')).strip()
        print(f"  - JSON(전종목 {len(rows)}건 스캔): 위험종목 {len(out)}건")
        return out, len(rows)
    except Exception as e:
        print(f"  ⚠️ JSON 수집 실패: {e}")
        return {}, 0


def fetch_junk_universe():
    """Naver 벌크 조치 목록을 긁어 {code: name} 합집합과 소스별 카운트를 반환."""
    junk = {}        # {code: name}
    counts = {}
    errors = []
    for label, url in SOURCES:
        try:
            res = SESSION.get(url, verify=False, timeout=8)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
            table = soup.find('table', {'class': 'type_2'})
            cnt = 0
            if table:
                for tr in table.find_all('tr'):
                    a = tr.find('a', href=lambda h: h and 'code=' in h)
                    if not a:
                        continue
                    code = a['href'].split('code=')[-1][:6]
                    if code.isdigit():
                        junk.setdefault(code, a.text.strip())
                        cnt += 1
            counts[label] = cnt
            print(f"  - {label}: {cnt}건")
        except Exception as e:
            counts[label] = 0
            errors.append(f"{label}({e})")
            print(f"  ⚠️ {label} 수집 실패: {e}")
        time.sleep(0.3)

    # 🆕 [합집합] 구 HTML과 신 JSON을 모두 긁어 합친다. 둘 다 살아있는 동안은 커버리지가 가장 넓고,
    #    한쪽이 개편으로 죽어도 나머지 한쪽이 명단을 유지한다 — 게이트가 통째로 비는 사고를 막는 게 목적.
    json_junk, json_scanned = fetch_junk_from_json()
    html_only = len(set(junk) - set(json_junk))
    json_only = len(set(json_junk) - set(junk))
    for code, name in json_junk.items():
        junk.setdefault(code, name)
    counts["JSON"] = len(json_junk)
    counts["JSON스캔"] = json_scanned
    if json_junk:
        print(f"  ⚖️ 합집합 정산 — HTML에만 {html_only}건 / JSON에만 {json_only}건 / 최종 {len(junk)}건")
    if not json_junk and json_scanned == 0:
        errors.append("JSON(미수집)")
    return junk, counts, errors


def main():
    now = datetime.datetime.now(KST)
    print(f"🛡️ [정적데이터 수집기] 가동 (KST {now.strftime('%Y-%m-%d %H:%M:%S')})")

    junk, counts, errors = fetch_junk_universe()
    total = len(junk)
    print(f"📊 is_junk 합집합: {total}종목 / 소스별: {counts}")

    # ── fail-CLOSED 견고성 게이트 ────────────────────────────────
    # 🆕 [이중 소스] 구 HTML과 신 JSON 중 하나만 살아있어도 명단은 성립한다. 따라서 '관리종목 HTML
    #    건수' 단독이 아니라 '두 소스 중 하나라도 정상인가'로 판정한다. 둘 다 실패했을 때만 중단.
    html_ok = counts.get("관리종목", 0) >= MIN_MANAGED
    json_ok = counts.get("JSON", 0) >= MIN_TOTAL
    if (not html_ok and not json_ok) or total < MIN_TOTAL:
        telegram_warn(
            f"🚨 [정적데이터 수집기] 수집 비정상 — HTML관리:{counts.get('관리종목',0)}, "
            f"JSON:{counts.get('JSON',0)}, 합집합:{total} "
            f"(임계 관리≥{MIN_MANAGED}/총≥{MIN_TOTAL}). 전일 스냅샷 유지, 덮어쓰기 중단. 오류:{errors}"
        )
        print("❌ 견고성 게이트 미달 — 시트를 비우지 않고 종료(전일 데이터 보존).")
        sys.exit(1)

    # 🆕 한쪽 소스만 죽은 경우는 계속 진행하되 반드시 알린다(조용히 열화되는 것 방지).
    if not html_ok:
        telegram_warn(f"⚠️ [정적데이터 수집기] 구 HTML 소스 이상(관리종목 {counts.get('관리종목',0)}건) — "
                      f"JSON {counts.get('JSON',0)}건으로 대체 진행. 네이버 개편 여부 확인 필요.")
    if not json_ok:
        print(f"⚠️ JSON 소스 이상(위험종목 {counts.get('JSON',0)}건) — HTML {counts.get('관리종목',0)}건으로 진행.")

    # ── 시트 쓰기 (creds 있을 때만; 로컬 검증 시엔 dry-run) ──────────
    if not os.path.exists("secret.json"):
        print("ℹ️ secret.json 없음 → DRY-RUN (fetch/parse만 검증, 시트 쓰기 생략).")
        sample = list(junk.items())[:10]
        print(f"   샘플 10: {sample}")
        return

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope))
        doc = gc.open_by_url(SHEET_URL)
        try:
            static_sheet = doc.worksheet("DB_정적데이터")
        except Exception:
            static_sheet = doc.add_worksheet(title="DB_정적데이터", rows="2000", cols="6")
            static_sheet.append_row(["종목코드", "종목명", "시가총액", "관리종목", "재무위험", "만성적자"])

        # is_junk=True 만 적재. (시총=0: 게이트되어 무관 / 재무위험·만성적자는 Phase 2)
        rows = [[f"'{code}", name, 0, "True", "False", "False"] for code, name in junk.items()]

        # 원자적 갱신: 먼저 새 데이터로 덮어쓰고(전부 빈 순간 없음), 그 아래 잔여행만 정리.
        # → 전용 그룹 분리로 omakase가 동시에 read해도 항상 '구버전 완본' 또는 '신버전 완본'을 봐서 게이트가 꺼지지 않음.
        # 🔧 [RAW 고정] USER_ENTERED 로 쓰면 시트가 "True"를 불리언으로 해석해 'TRUE'로 되돌려준다.
        #    omakase 쪽은 _sheet_bool()로 대소문자를 흡수하도록 고쳤지만, 애초에 문자열이 그대로
        #    남도록 RAW로 쓰는 게 근본 해결이다 (2026-08-26 게이트 전면 개방 사고의 원인).
        static_sheet.update(range_name="A2", values=rows, value_input_option="RAW")
        static_sheet.batch_clear([f"A{len(rows) + 2}:F"])
        print(f"✅ DB_정적데이터 갱신 완료: is_junk {len(rows)}종목 기록 (overwrite→trim 원자적 갱신).")
    except Exception as e:
        # 쓰기 단계 실패도 fail-closed: 알림만 보내고 비정상 종료(부분 기록 방지)
        telegram_warn(f"🚨 [정적데이터 수집기] 시트 쓰기 실패: {e}")
        print(f"❌ 시트 쓰기 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
