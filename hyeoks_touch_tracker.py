# -*- coding: utf-8 -*-
# ==========================================================================
# 🎯 [자동화] 백테스트_로그 익절터치(AI)·손절터치(AJ) 추적기
# --------------------------------------------------------------------------
# 목적: "이 시스템대로 실제 매매했다면?" — 진입 후 장중 고가/저가가 목표가·손절가를
#       실제로 건드렸는지, 몇 거래일 만에 건드렸는지를 채널별로 누적한다.
#       (T+N 시점 수익률이 '방향성 검증'이라면, 이쪽은 '실전 체결 시뮬레이션')
#
#   익절터치 : 고가 >= 목표가 가 된 첫 거래일  → "T+3"
#   손절터치 : 저가 <= 손절가 가 된 첫 거래일  → "T+7"
#   아직 안 닿았고 추적기간(120거래일) 미경과   → "추적중"
#   추적기간 경과했는데 끝내 안 닿음            → "미터치"
#
# 스캔 구간은 진입가(T+1 시가) 기준과 맞추기 위해 T+1 거래일부터 T+120 까지.
#
# 🔒 [동시성 안전] omakase/analyst는 백테스트_로그를 통째로 재정렬(rewrite)하기 때문에
#    읽은 시점의 행 번호가 쓰기 시점엔 밀려 있을 수 있다. → 계산은 trade_id로 들고 있다가
#    쓰기 직전에 시트를 다시 읽어 trade_id→현재 행번호를 새로 매핑한 뒤 기록한다.
#
# 사용법:  python hyeoks_touch_tracker.py           (dry-run, 시트 미수정)
#          python hyeoks_touch_tracker.py --apply   (실제 기록 — 워크플로가 이걸 사용)
# ==========================================================================
import re, sys, time, datetime, requests, gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
KST = datetime.timezone(datetime.timedelta(hours=9))

COL_ID, COL_DATE, COL_CH, COL_NAME, COL_CODE = 0, 1, 2, 3, 4
COL_TARGET, COL_STOP, COL_TT, COL_ST = 32, 33, 34, 35     # AG, AH, AI, AJ
MAX_H = 120            # 추적 상한(거래일)
FETCH_LIMIT = 200      # 1회 실행 최대 종목 조회 수(폭주 방지)
APPLY = "--apply" in sys.argv

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36'})


def get_daily_bars(symbol, count=200):
    """fchart 일봉 → [{date, open, high, low, close}]"""
    for attempt in range(2):
        try:
            url = f"https://fchart.stock.naver.com/sise.nhn?symbol={symbol}&timeframe=day&count={count}&requestType=0"
            root = ET.fromstring(SESSION.get(url, verify=False, timeout=8).text)
            bars = []
            for item in root.findall(".//item"):
                raw = item.get("data")
                if not raw:
                    continue
                d = raw.split("|")
                if len(d) < 5:
                    continue
                bars.append({'date': f"{d[0][:4]}-{d[0][4:6]}-{d[0][6:8]}",
                             'open': float(d[1]), 'high': float(d[2]),
                             'low': float(d[3]), 'close': float(d[4])})
            return bars
        except Exception as e:
            if attempt == 0:
                time.sleep(1); continue
            print(f"      ⚠️ 일봉 조회 실패 {symbol}: {e}")
    return []


def parse_price(v):
    """'61,000원' / 61000 / '관망' 등 어떤 형식이 와도 정수 또는 0으로 흡수."""
    s = str(v).strip()
    if not s or any(k in s for k in ("관망", "계산", "대기", "매매금지")):
        return 0
    digits = re.sub(r'[^0-9]', '', s)
    return int(digits) if digits else 0


def is_final(v):
    """이미 확정된 칸인가('T+n' 또는 '미터치'). '추적중'/공란은 갱신 대상."""
    s = str(v).strip()
    return s.startswith("T+") or s == "미터치"


def main():
    started = datetime.datetime.now(KST)
    print(f"🎯 [익절/손절 터치 추적기] {'APPLY' if APPLY else 'DRY-RUN'} — {started.strftime('%Y-%m-%d %H:%M:%S')} KST\n")

    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", SCOPE))
    ws = gc.open_by_url(SHEET_URL).worksheet("백테스트_로그")
    rows = ws.get_all_values()
    if len(rows) < 2:
        print("데이터 없음."); return

    # ── 1) 대상 선별 (행번호가 아니라 trade_id로 들고 간다) ──
    targets, skip_final, skip_notgt = [], 0, 0
    for r in rows[1:]:
        if len(r) < 36:
            continue
        tgt, stp = parse_price(r[COL_TARGET]), parse_price(r[COL_STOP])
        if tgt <= 0 or stp <= 0:
            skip_notgt += 1
            continue
        if is_final(r[COL_TT]) and is_final(r[COL_ST]):
            skip_final += 1
            continue
        targets.append({"id": r[COL_ID].strip(), "date": r[COL_DATE].strip(),
                        "ch": r[COL_CH].strip(), "name": r[COL_NAME].strip(),
                        "code": r[COL_CODE].replace("'", "").strip().zfill(6),
                        "tgt": tgt, "stp": stp})

    print(f"📋 대상 {len(targets)}건  (확정완료 {skip_final} / 목표·손절없음 {skip_notgt})\n")
    if not targets:
        print("갱신할 행이 없습니다."); return
    if len(targets) > FETCH_LIMIT:
        print(f"⚠️ 대상이 {len(targets)}건 — 이번 회차는 {FETCH_LIMIT}건만 처리(다음 실행에서 이어서).")
        targets = targets[:FETCH_LIMIT]

    # ── 2) 계산 ──
    results, failed = {}, []
    stat = {"익절만": 0, "손절만": 0, "둘다": 0, "추적중": 0}
    for t in targets:
        bars = get_daily_bars(t["code"])
        time.sleep(0.15)
        if not bars:
            failed.append((t, "일봉 조회 실패")); continue
        ei = next((k for k, b in enumerate(bars) if b['date'] == t["date"]), None)
        if ei is None:
            failed.append((t, f"일봉에 진입일 {t['date']} 없음")); continue

        window = bars[ei + 1: ei + 1 + MAX_H]        # T+1 ~ T+120
        if not window:
            continue                                  # 진입 당일 → 아직 T+1 없음(정상)
        tt = st = ""
        for n, b in enumerate(window, start=1):
            if not tt and b['high'] >= t["tgt"]: tt = f"T+{n}"
            if not st and b['low'] <= t["stp"]:  st = f"T+{n}"
            if tt and st:
                break
        elapsed = len(window)
        if not tt: tt = "미터치" if elapsed >= MAX_H else "추적중"
        if not st: st = "미터치" if elapsed >= MAX_H else "추적중"

        if tt.startswith("T+") and st.startswith("T+"):   stat["둘다"] += 1
        elif tt.startswith("T+"):                         stat["익절만"] += 1
        elif st.startswith("T+"):                         stat["손절만"] += 1
        else:                                             stat["추적중"] += 1

        results[t["id"]] = (tt, st)
        print(f"  {t['date']} {t['ch']:<15} {t['name']:<12} 목표 {t['tgt']:>9,}→{tt:<6} 손절 {t['stp']:>9,}→{st:<6} ({elapsed}거래일)")

    print(f"\n{'='*70}")
    print(f"계산 {len(results)}건 | 익절만 {stat['익절만']} / 손절만 {stat['손절만']} / 둘다 {stat['둘다']} / 미터치·추적중 {stat['추적중']}")
    for t, why in failed:
        print(f"  ⚠️ {t['date']} {t['name']} — {why}")
    if not results:
        print("기록할 값 없음."); return

    if not APPLY:
        print("\n💡 DRY-RUN. 실제 기록: python hyeoks_touch_tracker.py --apply")
        return

    # ── 3) 쓰기 직전 재매핑 (그 사이 omakase/analyst가 정렬했을 수 있으므로) ──
    for attempt in range(3):
        try:
            fresh = ws.get_all_values()
            id_to_row = {r[COL_ID].strip(): i for i, r in enumerate(fresh[1:], start=2) if r and r[COL_ID].strip()}
            updates, lost = [], []
            for tid, (tt, st) in results.items():
                row_no = id_to_row.get(tid)
                if row_no is None:
                    lost.append(tid); continue
                updates.append({"range": f"AI{row_no}:AJ{row_no}", "values": [[tt, st]]})
            if not updates:
                print("⚠️ 재매핑 결과 대상 행을 찾지 못함."); return
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            print(f"\n✅ 기록 완료 {len(updates)}건 (trade_id 재매핑 후 AI·AJ 갱신)")
            if lost:
                print(f"   ⚠️ 사라진 trade_id {len(lost)}건(다음 실행에서 재시도): {lost[:3]}")
            return
        except Exception as e:
            print(f"⚠️ 기록 실패 (시도 {attempt + 1}/3): {e}")
            time.sleep(4)
    print("❌ 3회 재시도 후에도 기록 실패 — 다음 회차에서 자동 재시도됩니다.")
    sys.exit(1)


if __name__ == "__main__":
    main()
