# -*- coding: utf-8 -*-
# ==========================================================================
# 🔀 HYEOKS 동결 스위치 조건 확인 (§3-2) — 읽기 전용
# --------------------------------------------------------------------------
# 무엇인가
#   판정일에 "어느 스위치를 먼저 켤 것인가"를 정하는 데 필요한 숫자 하나를 잰다.
#
#     §3-2  SUPPLY_V2_BAND — 판정일 시점에 수급TOP2의 **V2 80+ 구간 평균 알파가
#           여전히 음수**이고 그 **표본이 N≥12** 면 ON.
#           충족하면 그쪽이 먼저고, ENVELOPE_BAND 는 다음 판정일로 밀린다(§3-2-1).
#
# 왜 판정기(hyeoks_verdict.py)에 안 넣고 별도 파일인가
#   판정기는 **결과를 보기 전에 굳혔다**고 공표한 코드다(로드맵 §3-1-1).
#   판정일 당일에 그 파일을 여는 것 자체가 그 약속을 흐린다. 재는 대상도 다르다 —
#   판정기는 §3-1 채널 생사를, 이 파일은 §3-2 스위치 조건을 잰다.
#   그래서 분리한다. 판정기는 오늘 한 글자도 건드리지 않는다.
#
# 무엇을 하지 않는가
#   · 시트를 읽기만 한다. 쓰지 않는다.
#   · 스위치를 켜지 않는다. 조건 충족 여부만 출력한다. 켜는 것은 사람이 한다.
#   · 문턱(음수 · N≥12)을 해석하거나 조정하지 않는다.
# ==========================================================================
import sys, argparse

from hyeoks_verdict import (SHEET_URL, COST_PCT, STOCK_COL, INDEX_COL,
                            C_CHANNEL, is_excluded, _num)

C_V2 = 10                 # BT_HEADER: 0 trade_id … 9 V1, 10 V2, 11 V2게이트
TARGET_CHANNEL = "수급TOP2"
V2_FLOOR = 80.0           # §3-2 "V2 80+ 구간"
HORIZON = 5               # 수급TOP2 의 기준 호라이즌(§3)
MIN_N = 12                # §3-2


def measure(rows):
    """수급TOP2 중 V2≥80 인 행의 순알파를 모은다. 비용은 §3-4-2 대로 뺀다."""
    vals, seen, no_v2 = [], 0, 0
    for row in rows[1:]:
        if len(row) <= C_CHANNEL or is_excluded(row):
            continue
        if str(row[C_CHANNEL]).strip() != TARGET_CHANNEL:
            continue
        seen += 1
        v2 = _num(row[C_V2]) if len(row) > C_V2 else None
        if v2 is None:
            no_v2 += 1
            continue
        if v2 < V2_FLOOR:
            continue
        si, ii = STOCK_COL[HORIZON], INDEX_COL[HORIZON]
        if len(row) <= max(si, ii):
            continue
        s, i = _num(row[si]), _num(row[ii])
        if s is None or i is None:
            continue
        vals.append(s - i - COST_PCT)
    return vals, seen, no_v2


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()

    if a.self_test:
        hdr = [""] * 34
        def mk(ch, v2, s, i):
            r = [""] * 34
            r[C_CHANNEL], r[C_V2] = ch, str(v2)
            r[STOCK_COL[5]], r[INDEX_COL[5]] = str(s), str(i)
            return r
        ok = True
        def chk(n, c, g=""):
            nonlocal ok
            print(("  ✅ " if c else "  ❌ ") + n + (f"   {g}" if g else "")); ok = ok and c
        v, seen, nv = measure([hdr, mk("수급TOP2", 85, 3.0, 1.0)])
        chk("V2 80+ 행을 잡고 비용을 뺀다", len(v) == 1 and abs(v[0] - 1.65) < 1e-9, f"{v}")
        v, _, _ = measure([hdr, mk("수급TOP2", 79.9, 3.0, 1.0)])
        chk("V2 80 미만은 제외", v == [])
        v, _, _ = measure([hdr, mk("차트TOP2", 85, 3.0, 1.0)])
        chk("다른 채널은 제외", v == [])
        v, seen, nv = measure([hdr, mk("수급TOP2", "", 3.0, 1.0)])
        chk("V2 없는 행은 세되 표본에 안 넣는다", v == [] and seen == 1 and nv == 1)
        print("\n" + ("✅ 전부 통과" if ok else "❌ 실패"))
        return 0 if ok else 1

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    rows = (gspread.authorize(creds).open_by_url(SHEET_URL)
            .worksheet("백테스트_로그").get_all_values())

    vals, seen, no_v2 = measure(rows)
    n = len(vals)
    mean = sum(vals) / n if n else None

    print("\n════════ §3-2 SUPPLY_V2_BAND 조건 확인 ════════")
    print(f"수급TOP2 전체 {seen}행 (V2 값 없음 {no_v2}행)")
    print(f"V2 ≥ {V2_FLOOR:.0f} 구간 · T+{HORIZON} · 비용 {COST_PCT}% 차감 후")
    print(f"  N = {n}   (문턱 N≥{MIN_N})")
    print(f"  평균 순알파 = {('%+.3f%%' % mean) if mean is not None else '—'}   (문턱: 음수)")

    cond_n = n >= MIN_N
    cond_neg = (mean is not None) and (mean < 0)
    print(f"\n  N≥{MIN_N}      : {'충족' if cond_n else '미충족'}")
    print(f"  평균 음수   : {'충족' if cond_neg else '미충족'}")
    if cond_n and cond_neg:
        print("\n▶ 판정: **SUPPLY_V2_BAND 조건 충족** → 오늘은 이쪽을 먼저 켠다.")
        print("        ENVELOPE_BAND 는 10/5 로 미룬다 (§3-2 한 날 하나만).")
    else:
        print("\n▶ 판정: **SUPPLY_V2_BAND 조건 미충족** → 오늘은 ENVELOPE_BAND 를 단독 ON.")
        print("        (§3-2-1 사전등록 그대로. 값은 코드 기본값, 배선은 main.yml env)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
