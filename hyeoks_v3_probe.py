# -*- coding: utf-8 -*-
"""
V3 열 오독 사고범위 계측 — 읽기 전용.

2026-09-18 GPT 교차검증 P0-1 후속. 전략 변경이 아니라 **사고범위 계측**이다.
과거 행을 고치지 않는다. 시트에 아무것도 쓰지 않는다.

옛 소비 경로는 `int(row[8])` 이었다. index 8 은 `영업이익증감률(QoQ,%)` 다.
진짜 V3 는 index 10. 두 값으로 `v3 < 20` 판정이 갈렸는지를 센다.

⚠️ 이 프로브가 말할 수 있는 것과 없는 것을 먼저 못박는다.

  말할 수 있다 — **오늘 시트 기준** 옛 값과 진짜 V3 의 판정이 갈리는 행 수
  말할 수 없다 — 과거에 실제로 몇 종목이 잘못 탈락했는가

왜냐하면 당시 `continue` 된 후보는 **어디에도 기록이 없기** 때문이다.
백테스트_로그에는 선정된 것만 남는다. 그리고 V3 는 분기마다 바뀌므로
오늘 시트의 값이 그날의 값도 아니다. 그래서 결과를 네 갈래로 나눈다.

  판정불일치   오늘 시트 기준 두 경로의 `V3<20` 판정이 다르다
  근거불일치   판정은 같지만 옛 경로가 **틀린 값**을 근거로 썼다
  동일         두 경로가 같은 값·같은 판정에 도달한다
  재구성 불가  과거 시점의 값·당시 후보군 — 기록이 없어 셀 수 없다

⚠️ 2026-09-18 정정 — 처음엔 `옛 경로가 값을 못 만들면(소수·빈칸) 영향 없음` 으로
   분류했는데 **틀렸다.** 옛 경로가 값을 못 만들면 V3 없음(fail-open)이라 **통과**이고,
   새 경로가 진짜 V3 를 읽어 20 미만이면 **탈락**이다. 판정이 뒤집힌다.
   예: QoQ="12.3"(int 실패 → 통과) / 진짜 V3=15 → 새 경로에서 탈락.
   그래서 "옛 경로가 값을 만들었는가" 가 아니라 **두 경로의 판정을 직접 비교**한다.

⚠️ 이름도 바꿨다. `확정 영향` 은 역사적 영향을 뜻하는 것처럼 읽힌다.
   이 도구가 재는 것은 **오늘 시트 기준 판정 불일치**이지 과거에 실제로 일어난 일이 아니다.
"""
import argparse
import datetime
import pathlib
import json
import os
import sys

import earnings_schema as S

THRESHOLD = 20          # hyeoks_analyst.py:814 의 구조적 SEED 문턱
OLD_COL = 8             # 옛 소비 경로가 읽던 위치
KST = datetime.timezone(datetime.timedelta(hours=9))


def old_path_value(row):
    """옛 소비 경로를 **그대로** 재현한다: try int(row[8]) except pass."""
    if len(row) <= OLD_COL:
        return None
    try:
        return int(str(row[OLD_COL]).strip())
    except (TypeError, ValueError):
        return None


def verdict(value):
    """`v3_score is not None and v3_score < 20` 을 그대로 재현한다.

    값이 없으면(None) 필터를 통과한다 — analyst 가 fail-open 이기 때문이다.
    """
    return "탈락" if (value is not None and value < THRESHOLD) else "통과"


def classify(all_values):
    """DB_실적 전체(헤더 포함) → 세 갈래 집계 + 행별 근거."""
    out = {"총행": 0, "판정불일치": 0, "근거불일치": 0, "동일": 0,
           "옛경로_값생성": 0, "옛경로_탈락": 0, "진짜V3_탈락": 0,
           "reason": "", "cases": []}
    if not all_values:
        out["reason"] = "DB_실적이 비어 있다"
        return out

    v3_col, why = S.locate(all_values[0], S.V3_NAME)
    code_col, why_code = S.locate(all_values[0], S.HEADER[S.CODE_COL])
    if v3_col is None or code_col is None:
        out["reason"] = why or why_code
        return out
    # 옛 경로는 위치를 숫자로 박았으므로, 열이 밀리지 않았다면 사고가 없다
    out["열이_밀렸는가"] = "Y" if v3_col != OLD_COL else "N"

    for row in all_values[1:]:
        if len(row) <= max(v3_col, code_col):
            continue
        code = str(row[code_col]).replace("'", "").strip()
        if not code:
            continue
        out["총행"] += 1

        old = old_path_value(row)
        raw = str(row[v3_col]).strip()
        try:
            true_v3 = int(float(raw)) if raw else None
        except (TypeError, ValueError):
            true_v3 = None

        # 🔴 옛 경로가 값을 못 만든 경우도 **판정**은 있다(통과). 건너뛰면 안 된다.
        out["옛경로_값생성"] += int(old is not None)
        old_verdict, new_verdict = verdict(old), verdict(true_v3)
        out["옛경로_탈락"] += int(old_verdict == "탈락")
        out["진짜V3_탈락"] += int(new_verdict == "탈락")

        if old_verdict != new_verdict:
            bucket = "판정불일치"
        elif old != true_v3:
            bucket = "근거불일치"      # 결론은 같아도 근거로 쓴 숫자가 다르다
        else:
            bucket = "동일"
        out[bucket] += 1
        if bucket != "동일":
            out["cases"].append({
                "code": code, "옛값(row[8])": old, "진짜V3": true_v3,
                "옛판정": old_verdict, "새판정": new_verdict, "분류": bucket,
            })
    return out


def render(res):
    lines = ["🔎 V3 열 오독 — **오늘 시트 기준** 판정 비교 (읽기 전용 · 과거 행 무수정)", ""]
    if res.get("reason"):
        lines.append(f"❌ 계측 불가: {res['reason']}")
        return "\n".join(lines)
    lines += [
        f"총 행                {res['총행']}",
        f"열이 밀렸는가        {res.get('열이_밀렸는가')}  (진짜 V3 index={S.V3_COL} · 옛 경로 index={OLD_COL})",
        "",
        f"✅ 동일               {res['동일']}   두 경로가 같은 값·같은 판정에 도달한다",
        f"⚠️ 근거 불일치        {res['근거불일치']}   판정은 같지만 옛 경로가 **틀린 값**을 근거로 썼다",
        f"🔴 판정 불일치        {res['판정불일치']}   `V3<{THRESHOLD}` 판정이 **갈린다**(후보군이 달라지는 행)",
        "",
        f"   옛 경로 탈락 판정  {res['옛경로_탈락']}",
        f"   진짜 V3 탈락 판정  {res['진짜V3_탈락']}",
        f"   (옛 경로가 값을 만든 행 {res['옛경로_값생성']} — 나머지는 int() 실패로 통과 처리됐다)",
        "",
        "❓ 재구성 불가        위 숫자는 **오늘 시트 기준**이다. 과거에 실제로 무슨 일이",
        "                     있었는지가 아니다. 당시 `continue` 된 후보는 기록이 없고",
        "                     (백테스트_로그에는 선정된 것만 남는다), V3 는 분기마다",
        "                     바뀌므로 오늘 값이 그날 값도 아니다.",
        "                     **과거 탈락 종목 수는 셀 수 없다.**",
    ]
    if res["cases"]:
        lines += ["", "불일치 행(최대 20):"]
        for c in res["cases"][:20]:
            lines.append(f"   [{c['분류']}] {c['code']}  row[8]={c['옛값(row[8])']}→{c['옛판정']}  "
                         f"V3={c['진짜V3']}→{c['새판정']}")
    return "\n".join(lines)


def fetch_sheet():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("secret.json", scope)
    doc = gspread.authorize(creds).open_by_url(
        "https://docs.google.com/spreadsheets/d/1BcZ2HtkjlArbEGcRcMo8uKG1-ZQ-kv0RvNiiLJFQzks/edit")
    return doc.worksheet("DB_실적").get_all_values()


def _selftest():
    ok = 0

    def chk(name, cond, extra=""):
        nonlocal ok
        assert cond, f"{name} {extra}"
        ok += 1
        print(f"  ✅ {name}{('   ' + str(extra)) if extra else ''}")

    def row(code, v3="", qoq=""):
        r = [""] * len(S.HEADER)
        r[S.CODE_COL] = code
        r[OLD_COL] = str(qoq)
        r[S.V3_COL] = str(v3)
        return r

    print("🧪 V3 판정 비교 (오늘 시트 기준)")
    # 옛 경로가 -8 로 **탈락**시켰는데 진짜 V3 는 72 로 통과 → 판정 불일치
    r = classify([list(S.HEADER), row("005930", v3=72, qoq=-8)])
    chk("판정이 갈리면 판정불일치", r["판정불일치"] == 1, r["cases"])
    chk("근거가 기록된다", r["cases"][0]["옛판정"] == "탈락" and r["cases"][0]["새판정"] == "통과")

    # 🔴 GPT ⑨ 가 지적한 바로 그 경우 — 처음엔 '영향 없음' 으로 분류했는데 틀렸다.
    #    QoQ="12.3" → int() 실패 → 옛 경로는 V3 없음(fail-open)으로 **통과**
    #    진짜 V3=15 → 20 미만이라 **탈락**. 판정이 뒤집힌다.
    r = classify([list(S.HEADER), row("000660", v3=15, qoq="12.3")])
    chk("옛 경로가 값을 못 만들어도 판정은 있다(통과)", r["판정불일치"] == 1, r["cases"])
    chk("그 경우를 '영향 없음' 으로 묻지 않는다", r["동일"] == 0)

    # 옛 경로가 값을 못 만들고 진짜 V3 도 20 이상이면 두 판정이 같다
    r = classify([list(S.HEADER), row("035420", v3=72, qoq="12.3")])
    chk("둘 다 통과면 근거불일치(값은 다르다)", r["근거불일치"] == 1 and r["판정불일치"] == 0)

    # 같은 값이면 동일
    r = classify([list(S.HEADER), row("035420", v3=30, qoq=30)])
    chk("값도 판정도 같으면 동일", r["동일"] == 1)

    # 둘 다 탈락이어도 값이 다르면 근거불일치
    r = classify([list(S.HEADER), row("035420", v3=5, qoq=-8)])
    chk("둘 다 탈락이어도 값이 다르면 근거불일치", r["근거불일치"] == 1)

    # 진짜 V3 가 비어 있으면 새 경로는 통과(fail-open)
    r = classify([list(S.HEADER), row("068270", v3="", qoq=-8)])
    chk("진짜 V3 가 없으면 새 경로는 통과 — 옛 경로가 걸렀으면 판정불일치",
        r["판정불일치"] == 1 and r["cases"][0]["새판정"] == "통과")

    r = classify([list(S.HEADER), row("A", 72, -8), row("B", 15, "12.3"), row("C", 30, 30)])
    chk("합이 총행과 맞는다",
        r["판정불일치"] + r["근거불일치"] + r["동일"] == r["총행"] == 3, r)

    chk("헤더가 이상하면 계측을 거부한다",
        classify([["종목코드", "엉뚱"], ["A", "1"]])["reason"] != "")
    chk("빈 시트도 사유를 남긴다", classify([])["reason"] != "")
    chk("열이 밀렸는지 보고한다", classify([list(S.HEADER)])["열이_밀렸는가"] == "Y")
    chk("문턱은 analyst 와 같은 20", THRESHOLD == 20)
    chk("리포트가 '오늘 시트 기준' 임을 밝힌다",
        "오늘 시트 기준" in render(classify([list(S.HEADER), row("A", 72, -8)])))

    # 읽기 전용 보장은 문자열이 아니라 **호출 구문**으로 검사한다.
    # (자기검증 안의 금지어 목록 자체가 문자열로 걸리면 안 된다)
    import ast
    tree = ast.parse(pathlib.Path(__file__).read_text(encoding="utf-8"))
    writes = [n.func.attr for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
              and n.func.attr in {"update", "clear", "batch_clear", "append_row",
                                  "append_rows", "add_worksheet", "delete_rows"}]
    chk("읽기 전용 보장 — 시트 쓰기 호출이 하나도 없다", writes == [], writes)

    print("\n" + f"✅ 전부 통과 ({ok}건)")
    return ok


def main():
    ap = argparse.ArgumentParser(description="V3 열 오독 사고범위 계측 (읽기 전용)")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--json", action="store_true", help="집계를 JSON 으로도 출력")
    a = ap.parse_args()
    if a.self_test:
        return 0 if _selftest() else 1
    if not os.path.exists("secret.json"):
        print("❌ secret.json 이 없다 — 이 도구는 액션에서 돌린다(v3_probe.yml).")
        return 2
    res = classify(fetch_sheet())
    print(render(res))
    if a.json:
        res.pop("cases", None)
        print("\nJSON " + json.dumps(res, ensure_ascii=False))
    print(f"\n계측 시각 {datetime.datetime.now(KST):%Y-%m-%d %H:%M:%S} KST · 시트 무수정")
    return 0


if __name__ == "__main__":
    sys.exit(main())
