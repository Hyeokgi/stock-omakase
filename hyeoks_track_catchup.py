# -*- coding: utf-8 -*-
# ==========================================================================
# ⏱️ [자동화] 백테스트_로그 추적 보완 (진입가 · 종목/지수 T+N)
# --------------------------------------------------------------------------
# 왜 필요한가:
#   omakase의 Step2 추적은 아침(07:00~08:50)에만 돈다. 그 시각엔 '오늘 일봉'이
#   아직 없어서, 오늘 도달한 호라이즌은 계산이 안 되고 항상 다음 날 아침으로 밀린다.
#   (예: 8/4 진입 → T+1은 8/5 종가인데, 8/5 아침엔 8/5 봉이 없어 8/6 아침에야 채워짐)
#   → 장 마감 후 저녁에 한 번 더 돌려서 '오늘 확정된 것'을 그날 바로 채운다.
#
# 계산 규칙(omakase Step2와 동일):
#   진입가(Q)   = 진입일 다음 거래일 시가       (실거래 기준)
#   종목T+N     = (T+N 종가 - 진입가) / 진입가
#   지수T+N     = (T+N 지수종가 - 지수 T+1 시가) / 지수 T+1 시가   ← 종목과 같은 보유창
#   각 호라이즌은 '도달한 그 거래일'에 1회 캡처하고 이후 덮어쓰지 않는다(동결).
#
# 🔒 동시성: omakase/analyst가 시트를 통째 재정렬하므로, 계산은 trade_id로 들고
#    있다가 쓰기 직전에 다시 읽어 행 번호를 재매핑한다.
#
# 사용법: python hyeoks_track_catchup.py           (dry-run)
#         python hyeoks_track_catchup.py --apply   (실제 기록)
# ==========================================================================
import re, sys, time, datetime, requests, gspread
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

try: sys.stdout.reconfigure(encoding='utf-8')
except Exception: pass

SHEET_URL = "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit"
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
KST = datetime.timezone(datetime.timedelta(hours=9))

COL_ID, COL_DATE, COL_CH, COL_NAME, COL_CODE, COL_BENCH, COL_ENTRY = 0, 1, 2, 3, 4, 13, 16
COL_AUDIT = 25                                     # 실제캡처거래일(Z)
HORIZONS = {1: (17, 21), 3: (18, 22), 5: (19, 23), 10: (20, 24),
            20: (26, 29), 60: (27, 30), 120: (28, 31)}   # h → (종목col, 지수col)
FETCH_LIMIT = 250
APPLY = "--apply" in sys.argv

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36'})


def get_daily_bars(symbol, count=200):
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
        except Exception:
            if attempt == 0:
                time.sleep(1)
    return []


def num(v):
    s = re.sub(r'[^0-9.]', '', str(v))
    try:
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def main():
    now = datetime.datetime.now(KST)
    print(f"⏱️ [추적 보완] {'APPLY' if APPLY else 'DRY-RUN'} — {now.strftime('%Y-%m-%d %H:%M')} KST\n")

    gc = gspread.authorize(ServiceAccountCredentials.from_json_keyfile_name("secret.json", SCOPE))
    ws = gc.open_by_url(SHEET_URL).worksheet("백테스트_로그")
    rows = ws.get_all_values()
    if len(rows) < 2:
        print("데이터 없음."); return

    idx_bars = {"KOSPI": get_daily_bars("KOSPI"), "KOSDAQ": get_daily_bars("KOSDAQ")}
    idx_close = {k: {b['date']: b['close'] for b in v} for k, v in idx_bars.items()}
    if not idx_bars["KOSPI"]:
        print("❌ 지수 일봉 조회 실패 — 중단"); sys.exit(1)
    print(f"📈 지수 일봉 최신: KOSPI {idx_bars['KOSPI'][-1]['date']} / KOSDAQ {idx_bars['KOSDAQ'][-1]['date']}\n")

    # ── 미완결 행만 선별 ──
    pend = []
    for r in rows[1:]:
        if len(r) < 36 or not r[COL_ID].strip():
            continue
        need = [h for h, (sc, _ic) in HORIZONS.items() if not str(r[sc]).strip()]
        if not need and str(r[COL_ENTRY]).strip() and num(r[COL_ENTRY]) > 0:
            continue
        pend.append(r)
    print(f"📋 미완결 {len(pend)}행 검사\n")

    results, zerofix, filled = {}, [], 0
    for r in pend[:FETCH_LIMIT]:
        tid, d, ch = r[COL_ID].strip(), r[COL_DATE].strip(), r[COL_CH].strip()
        bench = (r[COL_BENCH].strip() or "KOSPI").upper()
        if bench not in idx_bars:
            bench = "KOSPI"
        code = r[COL_CODE].replace("'", "").strip().zfill(6)

        bars = idx_bars[bench] if ch.startswith("지수벤치") else get_daily_bars(code)
        if not bars:
            continue
        if not ch.startswith("지수벤치"):
            time.sleep(0.12)

        ei = next((k for k, b in enumerate(bars) if b['date'] == d), None)
        if ei is None or ei + 1 >= len(bars):
            continue                                   # 진입 당일 → T+1 아직 없음(정상)

        # 진입가(Q): 비었거나 0으로 잘못 박힌 행도 재캡처
        cur_entry = num(r[COL_ENTRY])
        q_open = bars[ei + 1]['open']
        new_entry = None
        if cur_entry <= 0:
            if q_open > 0:
                new_entry = int(q_open) if not ch.startswith("지수벤치") else round(q_open, 2)
                if str(r[COL_ENTRY]).strip() not in ("", "0"):
                    pass
                if str(r[COL_ENTRY]).strip() == "0":
                    zerofix.append(f"{d} {r[COL_NAME]}")
                base = q_open
            else:
                continue                               # 시가 0 → 계산 불가(거래정지 등)
        else:
            base = cur_entry

        # 지수 base = 지수 T+1 시가 (종목과 동일 보유창)
        iser = idx_bars[bench]
        iei = next((k for k, b in enumerate(iser) if b['date'] == d), None)
        ibase = iser[iei + 1]['open'] if (iei is not None and iei + 1 < len(iser)) else 0.0

        cells, audit = {}, []
        for h, (sc, ic) in HORIZONS.items():
            if str(r[sc]).strip():
                continue                               # 이미 동결됨
            if ei + h >= len(bars):
                continue                               # 아직 도달 안 함
            s_close = bars[ei + h]['close']
            cells[sc] = f"{(s_close - base) / base * 100:.2f}%"
            hdate = bars[ei + h]['date']
            ic_close = idx_close[bench].get(hdate, 0)
            if ic_close and ibase > 0:
                cells[ic] = f"{(ic_close - ibase) / ibase * 100:.2f}%"
            audit.append(f"T+{h}:{(len(bars) - 1) - ei}")

        if new_entry is None and not cells:
            continue
        results[tid] = {"entry": new_entry, "cells": cells, "audit": audit,
                        "label": f"{d} {ch:<15} {r[COL_NAME]}"}
        filled += len(cells)
        ent = f" 진입가={new_entry:,}" if new_entry else ""
        got = " ".join(f"T+{h}" for h, (sc, _i) in HORIZONS.items() if sc in cells)
        print(f"  {d} {ch:<15} {r[COL_NAME]:<13}{ent} {got}")

    print(f"\n{'='*70}\n대상 {len(results)}행 / 채울 셀 {filled}개")
    if zerofix:
        print(f"🔧 진입가 0 이던 행 복구: {zerofix}")
    if not results:
        print("채울 것 없음(모두 최신)."); return
    if not APPLY:
        print("\n💡 DRY-RUN. 실제 기록: python hyeoks_track_catchup.py --apply")
        return

    # ── 쓰기 직전 trade_id 재매핑 ──
    for attempt in range(3):
        try:
            fresh = ws.get_all_values()
            id_row = {r[COL_ID].strip(): (i, r) for i, r in enumerate(fresh[1:], start=2) if r and r[COL_ID].strip()}
            updates = []
            for tid, info in results.items():
                if tid not in id_row:
                    continue
                rn, cur = id_row[tid]
                if info["entry"] is not None:
                    updates.append({"range": f"Q{rn}", "values": [[info["entry"]]]})
                for ci, val in info["cells"].items():
                    col = chr(65 + ci) if ci < 26 else "A" + chr(65 + ci - 26)
                    updates.append({"range": f"{col}{rn}", "values": [[val]]})
                if info["audit"]:
                    prev = str(cur[COL_AUDIT]).strip() if len(cur) > COL_AUDIT else ""
                    merged = (prev + "," if prev else "") + ",".join(info["audit"])
                    updates.append({"range": f"Z{rn}", "values": [[merged]]})
            if not updates:
                print("⚠️ 재매핑 후 대상 없음."); return
            ws.batch_update(updates, value_input_option="USER_ENTERED")
            print(f"\n✅ 기록 완료 — {len(updates)}개 셀 갱신 (trade_id 재매핑)")
            return
        except Exception as e:
            print(f"⚠️ 기록 실패 (시도 {attempt + 1}/3): {e}")
            time.sleep(4)
    print("❌ 재시도 후에도 실패 — 다음 회차에서 자동 복구"); sys.exit(1)


if __name__ == "__main__":
    main()
