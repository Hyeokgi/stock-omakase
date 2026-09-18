# -*- coding: utf-8 -*-
"""
DB_실적 스키마 계약 — 생산자와 소비자가 같은 정의를 본다.

🔴 2026-09-18 GPT 교차검증 P0-1
   생산자(hyeoks_earnings_collector)의 헤더에서 V3 는 **11번째 열(index 10)** 인데
   소비자 두 곳이 `row[8]` 을 V3 로 읽고 있었다. index 8 은 `영업이익증감률(QoQ,%)` 다.

     omakase.py:2686        int(row[8]) → 실적 악화 경고 배지
     hyeoks_analyst.py:782  int(row[8]) → 장기 구조적 SEED 를 V3<20 이면 **제외**
                                        → 프롬프트의 "실적점수(V3):N점"

   단순 표시 오류가 아니다. 후보군 자체가 바뀐다.

   화석 증거: 수집기가 시트를 새로 만들 때 아직 `cols="12"` 를 쓴다. 원래 12열
   스키마였고 그때는 V3 가 index 8 이었다는 뜻이다. 이후 YoY 두 열이 5·6 에
   끼어들면서 V3 가 10 으로 밀렸는데(`summarize()` 주석에 그 이관이 적혀 있다)
   소비자는 8 에 남았다. 저장소 이력이 2026-08-04 스쿼시 임포트에서 시작하므로
   **이동 시점은 git 으로 특정할 수 없다** — 첫 커밋에 이미 어긋나 있다.

왜 조용했나: 두 소비자 모두 `try: int(row[8]) except Exception: pass` 였다.
QoQ 는 보통 "12.3" 같은 소수라 int() 가 실패하고 **V3 가 없는 것처럼** 처리된다.
그런데 QoQ 가 우연히 정수면(예: "-8") 통과해서 **그 값이 V3 로 쓰인다.**
즉 대부분은 fail-open 으로 필터가 꺼져 있었고, 일부는 틀린 값으로 걸러냈다.

그래서 이 모듈은 세 가지를 강제한다.
  ① 위치를 이름으로 찾는다 — 열이 또 움직여도 따라간다
  ② 헤더가 없거나 중복이거나 다르면 **V3 를 쓰지 않고 크게 말한다**(조용한 None 금지)
  ③ 값 범위(0~100)를 검사한다 — 이 검사만 있었어도 QoQ 오독을 잡았다
"""

HEADER = ["종목코드", "종목명", "최신분기", "매출액", "영업이익", "매출증감률(YoY,%)",
          "영업이익증감률(YoY,%)", "매출증감률(QoQ,%)", "영업이익증감률(QoQ,%)",
          "실적개선여부", "V3(실적점수)", "연속성장", "재무제표기준", "갱신일시"]

CODE_COL = HEADER.index("종목코드")
V3_COL = HEADER.index("V3(실적점수)")
STAMP_COL = HEADER.index("갱신일시")
V3_NAME = "V3(실적점수)"
STAMP_NAME = "갱신일시"

# V3 는 0~100 스케일이다(compute_v3_score 의 기본점 15, 상·하한 클램프).
# 범위 밖 값은 "다른 열을 읽고 있다" 는 신호다 — 조용히 쓰지 않는다.
V3_MIN, V3_MAX = 0, 100


def locate(sheet_header, name):
    """시트의 **실제** 헤더에서 열 위치를 찾는다. (index, 사유) 를 돌려준다.

    없거나 중복이면 index 는 None 이고 사유가 붙는다. 호출자는 사유를 반드시 말한다.
    """
    cells = [str(c).strip() for c in (sheet_header or [])]
    hits = [i for i, c in enumerate(cells) if c == name]
    if not hits:
        return None, f"'{name}' 열이 시트 헤더에 없다"
    if len(hits) > 1:
        return None, f"'{name}' 열이 {len(hits)}개다(중복) — 어느 쪽인지 알 수 없다"
    return hits[0], ""


def read_v3_map(all_values):
    """DB_실적 전체(헤더 포함)를 받아 {종목코드: V3} 와 통계를 돌려준다.

    반환: (v3_map, stats)
      stats = {"rows", "used", "blank", "unparsable", "out_of_range", "reason", "col"}

    `except: pass` 로 숨기지 않는다. 못 읽은 이유를 세어서 호출자가 말할 수 있게 한다.
    헤더를 못 믿으면 v3_map 은 비고 reason 이 채워진다 — 그 경우 V3 를 쓰지 않는다.
    """
    stats = {"rows": 0, "used": 0, "blank": 0, "unparsable": 0,
             "out_of_range": 0, "reason": "", "col": None}
    if not all_values:
        stats["reason"] = "DB_실적이 비어 있다"
        return {}, stats

    col, why = locate(all_values[0], V3_NAME)
    if col is None:
        stats["reason"] = why
        return {}, stats
    stats["col"] = col

    code_col, why_code = locate(all_values[0], HEADER[CODE_COL])
    if code_col is None:
        stats["reason"] = why_code
        return {}, stats

    out = {}
    for row in all_values[1:]:
        if len(row) <= max(col, code_col):
            continue
        code = str(row[code_col]).replace("'", "").strip()
        if not code:
            continue
        stats["rows"] += 1
        raw = str(row[col]).strip()
        if not raw:
            stats["blank"] += 1
            continue
        try:
            value = int(float(raw))
        except (TypeError, ValueError):
            stats["unparsable"] += 1
            continue
        if not (V3_MIN <= value <= V3_MAX):
            # 범위 밖 = 다른 열을 읽고 있다는 신호. 쓰지 않고 센다.
            stats["out_of_range"] += 1
            continue
        out[code.zfill(6)] = value
        stats["used"] += 1
    return out, stats


def v3_report(stats):
    """소비자가 로그에 그대로 찍을 한 줄. 조용히 넘어가지 않기 위한 것이다."""
    if stats.get("reason"):
        return f"❌ [V3 사용 중단] {stats['reason']} — 실적점수 필터를 적용하지 않는다"
    bad = stats["unparsable"] + stats["out_of_range"]
    line = (f"📊 [V3] {stats['used']}/{stats['rows']}종목 사용 "
            f"(열 index={stats['col']} · 빈칸 {stats['blank']})")
    if bad:
        line += (f" · ⚠️ 해석불가 {stats['unparsable']} · 범위밖 {stats['out_of_range']}"
                 f" — 범위밖이 있으면 **다른 열을 읽고 있다는 신호다**")
    return line


def _selftest():
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    print("🧪 DB_실적 스키마 계약")
    chk("V3 는 11번째 열이다(index 10)", V3_COL == 10, V3_COL)
    chk("갱신일시는 마지막 열", STAMP_COL == len(HEADER) - 1)
    chk("헤더는 14열", len(HEADER) == 14)

    def row(code, **kw):
        r = list(HEADER)
        r = [""] * len(HEADER)
        r[CODE_COL] = code
        for k, v in kw.items():
            r[HEADER.index(k)] = v
        return r

    full = [list(HEADER),
            row("005930", **{"V3(실적점수)": "72", "영업이익증감률(QoQ,%)": "-8"}),
            row("000660", **{"V3(실적점수)": "15", "영업이익증감률(QoQ,%)": "12.3"})]
    m, st = read_v3_map(full)
    chk("이름으로 찾은 열에서 읽는다", m == {"005930": 72, "000660": 15}, m)
    chk("QoQ 를 V3 로 읽지 않는다", -8 not in m.values() and 12 not in m.values())
    chk("통계가 쓰인 수를 센다", st["used"] == 2 and st["rows"] == 2)

    # 🔴 회귀의 핵심: 열이 움직여도 따라가는가
    moved = [h for h in HEADER]
    moved.insert(3, "새로끼운열")
    shifted = [moved]
    r = [""] * len(moved)
    r[0] = "005930"
    r[moved.index(V3_NAME)] = "72"
    shifted.append(r)
    m2, st2 = read_v3_map(shifted)
    chk("열이 끼어들어도 이름으로 따라간다", m2 == {"005930": 72} and st2["col"] == 11, st2["col"])

    no_head, st3 = read_v3_map([["종목코드", "엉뚱한열"], ["005930", "72"]])
    chk("헤더에 V3 가 없으면 **쓰지 않는다**", no_head == {} and "없다" in st3["reason"])
    chk("그리고 조용하지 않다", "V3 사용 중단" in v3_report(st3))

    dup = [list(HEADER) + [V3_NAME], row("005930", **{"V3(실적점수)": "72"}) + ["9"]]
    m4, st4 = read_v3_map(dup)
    chk("중복 열이면 거부한다", m4 == {} and "중복" in st4["reason"])

    oor = [list(HEADER), row("005930", **{"V3(실적점수)": "-8"}),
           row("000660", **{"V3(실적점수)": "250"})]
    m5, st5 = read_v3_map(oor)
    chk("범위 밖(0~100)은 쓰지 않고 센다", m5 == {} and st5["out_of_range"] == 2, st5)
    chk("범위 밖이면 경고 문구가 나온다", "다른 열을 읽고 있다" in v3_report(st5))

    bad = [list(HEADER), row("005930", **{"V3(실적점수)": "12.3"})]
    m6, st6 = read_v3_map(bad)
    chk("소수는 잘라서 받는다(조용히 버리지 않는다)", m6 == {"005930": 12}, m6)

    blank = [list(HEADER), row("005930")]
    m7, st7 = read_v3_map(blank)
    chk("빈칸은 빈칸으로 센다", m7 == {} and st7["blank"] == 1)

    chk("빈 입력도 사유를 남긴다", read_v3_map([])[1]["reason"] != "")
    chk("따옴표 코드도 맞춘다",
        read_v3_map([list(HEADER), row("'005930", **{"V3(실적점수)": "72"})])[0] == {"005930": 72})
    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _selftest() else 1)
