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
    return junk, counts, errors


def main():
    now = datetime.datetime.now(KST)
    print(f"🛡️ [정적데이터 수집기] 가동 (KST {now.strftime('%Y-%m-%d %H:%M:%S')})")

    junk, counts, errors = fetch_junk_universe()
    total = len(junk)
    print(f"📊 is_junk 합집합: {total}종목 / 소스별: {counts}")

    # ── fail-CLOSED 견고성 게이트 ────────────────────────────────
    if counts.get("관리종목", 0) < MIN_MANAGED or total < MIN_TOTAL:
        telegram_warn(
            f"🚨 [정적데이터 수집기] 수집 비정상 — 관리:{counts.get('관리종목',0)}, 합집합:{total} "
            f"(임계 관리≥{MIN_MANAGED}/총≥{MIN_TOTAL}). 전일 스냅샷 유지, 덮어쓰기 중단. 오류:{errors}"
        )
        print("❌ 견고성 게이트 미달 — 시트를 비우지 않고 종료(전일 데이터 보존).")
        sys.exit(1)

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

        # 성공이 확정된 시점에만 원자적 교체 (clear→write 를 수집기가 단독 소유)
        static_sheet.batch_clear(['A2:F'])
        static_sheet.update(range_name="A2", values=rows, value_input_option="USER_ENTERED")
        print(f"✅ DB_정적데이터 갱신 완료: is_junk {len(rows)}종목 기록 (clear→write 원자적 소유).")
    except Exception as e:
        # 쓰기 단계 실패도 fail-closed: 알림만 보내고 비정상 종료(부분 기록 방지)
        telegram_warn(f"🚨 [정적데이터 수집기] 시트 쓰기 실패: {e}")
        print(f"❌ 시트 쓰기 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
