# -*- coding: utf-8 -*-
# ==========================================================================
# 🛡️ HYEOKS 스냅샷 결측 감시 (관측 전용)
# --------------------------------------------------------------------------
# 왜 만들었나
#   종가베팅 검증의 원재료 두 가지는 **되돌릴 수 없다.**
#
#     · 시장 스냅샷(13:00 / 15:05) — '그 시각의 값'이다. 놓치면 영원히 없다.
#       일봉으로도, 나중에 다시 받아서도 복원되지 않는다.
#     · 분봉 프로파일 — 네이버가 **최근 6거래일치만** 준다. 6일이 지나면 소멸한다.
#
#   그런데 이 둘을 쏘는 트리거가 각각 단일 실패점이다 —
#     · 스냅샷은 GAS 시간 트리거(매크로.gs)에만 의존한다. cron 폴백이 없다.
#     · 분봉은 깃허브 cron 인데, 2026-08-27 실측으로 4시간 54분 지연·드롭이 있었다.
#
#   지금까지는 **실패해도 아무도 모른다.** 워크플로가 안 돌면 실패 알림도 없다
#   ('돌지 않은 것'은 빨간 실행조차 남기지 않는다). 한 달 뒤 분석을 돌릴 때가 돼서야
#   구멍을 발견하게 되고, 그때는 이미 복구 불가다. 그걸 막는 것이 이 파일의 전부다.
#
# 무엇을 하는가
#   ① 최근 거래일 목록을 지수 일봉에서 받는다(공휴일·임시휴장 자동 반영).
#   ② 거래일마다 있어야 할 파일이 실제로 있는지 센다.
#   ③ 없는 날을 **복구 가능 / 복구 불가**로 나눠 출력한다. 분봉은 6거래일 안이면
#      아직 --date 로 메울 수 있다. 그 경우 정확한 실행 명령을 같이 찍는다.
#   ④ 있는 파일의 무결성도 본다 — capturedAt 이 의도한 창 안인가, 행수가 정상인가,
#      호가 4열이 실제로 채워지는가.
#
# 무엇을 하지 않는가
#   · 구글시트를 읽지도 쓰지도 않는다. 선정·점수·채널에 어떤 영향도 없다.
#   · 판정하지 않는다. 데이터가 있는지 없는지만 본다.
#   · 스스로 메우지 않는다. 메우는 것은 사람이 명령을 보고 결정한다
#     (자동 백필은 '언제 받은 값인지'를 흐려 표본을 오염시킨다).
#
# 종료 코드
#   0 = 구멍 없음 / 1 = 복구 가능한 구멍 있음(경고) / 2 = 복구 불가 구멍 발생(즉시 확인)
# ==========================================================================
import os, re, gzip, sys, datetime, argparse
import xml.etree.ElementTree as ET
import requests, urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

KST = datetime.timezone(datetime.timedelta(hours=9))
SNAP_DIR = "data/market_snapshot"
INTRA_DIR = "data/intraday_profile"

# 분봉이 살아 있는 창. 네이버 실측: count=2000 을 줘도 6거래일치만 온다.
# 보수적으로 5로 둔다 — '6일째'에 발견하면 그날 실패하면 끝이라 여유가 없다.
INTRADAY_RECOVERABLE_DAYS = 5

# 슬롯별 '의도한 시각' 창. 수집기의 시간 창 가드와 같은 값이어야 한다.
SLOT_WINDOW = {"1300": ("12:40", "13:40"), "1505": ("14:50", "15:25")}

# GAS 정시 발사가 끝났을 시각. 이보다 늦게 찍혔으면 백업 cron 이 대신 찍은 날로 본다.
# 창 안이라 데이터는 유효하지만, §6-12 진입가가 그날만 다른 시각의 값이므로 표시해 둔다.
SLOT_PRIMARY_BY = {"1300": "13:10", "1505": "15:05"}

# 스냅샷이 처음 쌓이기 시작한 날. 이 앞은 애초에 없는 것이므로 결측으로 세지 않는다.
SNAPSHOT_EPOCH = "2026-08-28"
# 업종·그룹사 축과 호가 4열이 들어간 날(merge 4236a7c, 19:54 KST → 실제 적용은 다음 거래일).
SECTOR_EPOCH = "2026-08-31"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"})


def trading_days(count=20):
    """지수 일봉에서 최근 거래일을 받는다. 공휴일·임시휴장이 자동으로 반영된다.

    WATCH_TEST_DAYS 가 있으면 그 목록을 쓴다 — 네트워크 없이 표·판정 로직을
    검증하기 위한 통로다(CI 자기검증용). 실제 실행에서는 설정하지 않는다."""
    stub = os.environ.get("WATCH_TEST_DAYS", "").strip()
    if stub:
        print("⚠️ WATCH_TEST_DAYS 사용 중 — 실제 거래일이 아니다(테스트 모드)")
        return sorted(d.strip() for d in stub.split(",") if d.strip())
    r = SESSION.get("https://fchart.stock.naver.com/sise.nhn"
                    f"?symbol=KOSPI&timeframe=day&count={count}&requestType=0",
                    verify=False, timeout=15)
    days = []
    for it in ET.fromstring(r.text).findall(".//item"):
        raw = (it.get("data") or "").split("|")[0]
        if len(raw) == 8:
            days.append(f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}")
    return sorted(set(days))


def read_meta(path):
    """파일 첫 줄의 #meta 를 dict 로. 파일이 깨졌으면 None."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            head = fh.readline().rstrip("\n")
            cols = fh.readline().rstrip("\n").split(",")
            rows = sum(1 for _ in fh)
    except Exception as e:
        return {"_error": str(e)}
    if not head.startswith("#meta"):
        return {"_error": "첫 줄이 #meta 가 아님"}
    meta = {"_cols": cols, "_rows": rows}
    for part in head.split(",")[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            meta[k.strip().strip('"')] = v.strip().strip('"')
    return meta


def in_window(captured_at, slot):
    """capturedAt 이 그 슬롯의 의도한 창 안인가."""
    w = SLOT_WINDOW.get(slot)
    if not w or not captured_at:
        return None
    try:
        hhmm = captured_at[11:16]
    except Exception:
        return None
    return w[0] <= hhmm <= w[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=20, help="거슬러 볼 거래일 수")
    ap.add_argument("--strict", action="store_true",
                    help="복구 가능한 구멍도 실패(2)로 처리")
    args = ap.parse_args()

    now = datetime.datetime.now(KST)
    print(f"🛡️ 스냅샷 결측 감시 — {now:%Y-%m-%d %H:%M KST}\n")

    try:
        days = trading_days(args.days)
    except Exception as e:
        print(f"❌ 거래일 목록을 못 받았다: {e}")
        print("   지수 일봉이 죽었다는 뜻이므로 이것 자체가 사고다. 헬스체크를 먼저 보라.")
        return 2
    days = [d for d in days if d >= SNAPSHOT_EPOCH]
    if not days:
        print("ℹ️ 감시 대상 거래일이 아직 없다 (스냅샷 시작일 이전).")
        return 0

    today = f"{now:%Y-%m-%d}"
    # 오늘이 거래일이고 아직 장중·직후라면 오늘은 아직 판단하지 않는다.
    if today in days and now.hour < 17:
        print(f"ℹ️ 오늘({today})은 아직 수집 중일 수 있어 감시 대상에서 뺀다.\n")
        days = [d for d in days if d != today]

    snap_have = set(os.listdir(SNAP_DIR)) if os.path.isdir(SNAP_DIR) else set()
    intra_have = set(os.listdir(INTRA_DIR)) if os.path.isdir(INTRA_DIR) else set()

    recent = days[-INTRADAY_RECOVERABLE_DAYS:]
    lost, recoverable, rows = [], [], []

    for d in days:
        cells, missing_hard, missing_soft = [], [], []

        for slot in ("1300", "1505"):
            f = f"{d}_{slot}.csv.gz"
            if f in snap_have:
                cells.append("✅")
            else:
                cells.append("❌")
                missing_hard.append(f"스냅샷 {slot}")

        # 테마·업종·그룹사 집계는 종목 스냅샷이 있을 때만 따진다.
        agg = []
        for suffix, since in (("theme", SNAPSHOT_EPOCH),
                              ("upjong", SECTOR_EPOCH), ("group", SECTOR_EPOCH)):
            if d < since:
                agg.append("–")
            elif f"{d}_1505_{suffix}.csv.gz" in snap_have:
                agg.append("✅")
            else:
                agg.append("❌")
                missing_hard.append(f"{suffix} 집계")
        cells.append("".join(agg))

        # 분봉 — 파일명 규약을 모르므로 날짜가 들어간 파일이 있는지로 본다.
        if any(d in f for f in intra_have):
            cells.append("✅")
        elif d in recent:
            cells.append("⏳")
            missing_soft.append("분봉")
        else:
            cells.append("❌")
            missing_hard.append("분봉(기한만료)")

        rows.append((d, cells))
        if missing_hard:
            lost.append((d, missing_hard))
        if missing_soft:
            recoverable.append((d, missing_soft))

    print("| 거래일 | 13:00 | 15:05 | 테마/업종/그룹 | 분봉 |")
    print("|---|:---:|:---:|:---:|:---:|")
    for d, c in rows:
        print(f"| {d} | {c[0]} | {c[1]} | {c[2]} | {c[3]} |")
    print("\n(⏳ = 아직 복구 가능 · ❌ = 없음)\n")

    # ── 무결성: 가장 최근 거래일의 파일을 실제로 열어 본다
    if rows:
        d = rows[-1][0]
        print(f"🔬 무결성 점검 — {d}")
        for slot in ("1300", "1505"):
            path = os.path.join(SNAP_DIR, f"{d}_{slot}.csv.gz")
            if not os.path.exists(path):
                print(f"   {slot}: 파일 없음")
                continue
            m = read_meta(path)
            if "_error" in m:
                print(f"   {slot}: ⚠️ 읽기 실패 — {m['_error']}")
                lost.append((d, [f"{slot} 파일 손상"]))
                continue
            cap = m.get("capturedAt", "")
            ok = in_window(cap, slot)
            mark = "✅" if ok else ("⚠️ 창 밖" if ok is False else "?")
            late = ""
            if ok and cap[11:16] > SLOT_PRIMARY_BY.get(slot, "23:59"):
                late = "  🪃 백업 발사로 보임 (GAS 정시분 없음 — 진입가 시각이 평소와 다르다)"
            print(f"   {slot}: capturedAt={cap[11:19]} {mark} · "
                  f"{m['_rows']}행 · {len(m['_cols'])}열{late}")
            if d >= SECTOR_EPOCH:
                for col in ("askBuy", "askSell", "totalBuyVolume", "totalSellVolume"):
                    if col not in m["_cols"]:
                        print(f"      ⚠️ 호가 열 없음: {col} — §6-4 모의 집행 재료가 빠진다")
                        break

    # ── 결론
    print()
    if recoverable:
        print("⏳ 아직 메울 수 있다 — 6거래일이 지나면 영구 소멸한다.")
        for d, what in recoverable:
            print(f"   · {d}: {', '.join(what)}")
        print("   메우는 법: Actions → '⏱️ HYEOKS 장중 분봉 프로파일' → Run workflow → date 에 날짜 입력")
    if lost:
        print("\n❌ 복구 불가 구멍:")
        for d, what in lost:
            print(f"   · {d}: {', '.join(what)}")
        print("   시장 스냅샷은 사후 복원이 불가능하다. 트리거(GAS/cron)가 살아 있는지 확인하라.")
    if not lost and not recoverable:
        print("✅ 구멍 없음. 전 거래일 수집 완료.")

    if lost:
        return 2
    if recoverable:
        return 2 if args.strict else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
