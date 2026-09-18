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

  확정 영향   오늘 기준 두 경로의 판정이 다르다(구조적 SEED 라면 결과가 갈렸다)
  잠재 영향   옛 경로가 값을 만들어냈다(정수라 int() 통과) — 판정은 같더라도 근거가 틀렸다
  영향 없음   옛 경로가 값을 못 만들었다(소수·빈칸) → V3 없음으로 fail-open
  재구성 불가 과거 시점의 값·당시 후보군 — 기록이 없어 셀 수 없다
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


def classify(all_values):
    """DB_실적 전체(헤더 포함) → 네 갈래 집계 + 행별 근거."""
    out = {"총행": 0, "확정영향": 0, "잠재영향": 0, "영향없음": 0,
           "옛경로_값생성": 0, "옛경로_미달": 0, "진짜V3_미달": 0,
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

        if old is None:
            out["영향없음"] += 1
            continue

        out["옛경로_값생성"] += 1
        old_reject = old < THRESHOLD
        true_reject = (true_v3 is not None) and (true_v3 < THRESHOLD)
        out["옛경로_미달"] += int(old_reject)
        out["진짜V3_미달"] += int(true_reject)

        if true_v3 is None or old_reject != true_reject:
            out["확정영향"] += 1
            out["cases"].append({
                "code": code, "옛값(row[8])": old, "진짜V3": true_v3,
                "옛판정": "제외" if old_reject else "통과",
                "새판정": ("모름" if true_v3 is None else ("제외" if true_reject else "통과")),
            })
        else:
            out["잠재영향"] += 1
    return out


def render(res):
    lines = ["🔎 V3 열 오독 사고범위 계측 (읽기 전용 · 과거 행 무수정)", ""]
    if res.get("reason"):
        lines.append(f"❌ 계측 불가: {res['reason']}")
        return "\n".join(lines)
    lines += [
        f"총 행                {res['총행']}",
        f"열이 밀렸는가        {res.get('열이_밀렸는가')}  (진짜 V3 index={S.V3_COL} · 옛 경로 index={OLD_COL})",
        "",
        f"✅ 영향 없음          {res['영향없음']}   옛 경로가 값을 못 만들었다(소수·빈칸) → V3 없음으로 fail-open",
        f"⚠️ 잠재 영향          {res['잠재영향']}   옛 경로가 값을 만들었고 판정은 같았다 — **근거는 틀렸다**",
        f"🔴 확정 영향          {res['확정영향']}   두 경로의 `V3<{THRESHOLD}` 판정이 **갈린다**",
        "",
        f"   옛 경로가 값 생성  {res['옛경로_값생성']}   (그중 미달 판정 {res['옛경로_미달']})",
        f"   진짜 V3 미달       {res['진짜V3_미달']}",
        "",
        "❓ 재구성 불가        당시 `continue` 된 후보는 기록이 없다. 백테스트_로그에는",
        "                     선정된 것만 남는다. V3 는 분기마다 바뀌므로 오늘 값이",
        "                     그날 값도 아니다. **과거 탈락 종목 수는 셀 수 없다.**",
    ]
    if res["cases"]:
        lines += ["", "확정 영향 행(최대 20):"]
        for c in res["cases"][:20]:
            lines.append(f"   {c['code']}  row[8]={c['옛값(row[8])']}→{c['옛판정']}  "
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

    print("🧪 V3 사고범위 계측")
    # 옛 경로가 -8 로 **제외**했는데 진짜 V3 는 72 로 통과 → 확정 영향
    r = classify([list(S.HEADER), row("005930", v3=72, qoq=-8)])
    chk("판정이 갈리면 확정 영향", r["확정영향"] == 1 and r["영향없음"] == 0, r["cases"])
    chk("근거가 기록된다", r["cases"][0]["옛판정"] == "제외" and r["cases"][0]["새판정"] == "통과")

    # QoQ 가 소수면 int() 가 실패해 옛 경로가 값을 못 만든다 → 영향 없음
    r = classify([list(S.HEADER), row("000660", v3=15, qoq="12.3")])
    chk("소수 QoQ 는 영향 없음(fail-open)", r["영향없음"] == 1 and r["확정영향"] == 0)

    # 둘 다 통과 판정이면 잠재 영향 — 결과는 같아도 근거는 틀렸다
    r = classify([list(S.HEADER), row("035420", v3=72, qoq=30)])
    chk("판정이 같으면 잠재 영향", r["잠재영향"] == 1 and r["확정영향"] == 0)

    # 둘 다 제외 판정이어도 잠재 영향
    r = classify([list(S.HEADER), row("035420", v3=5, qoq=-8)])
    chk("둘 다 제외여도 잠재 영향", r["잠재영향"] == 1)

    # 진짜 V3 가 비어 있으면 확정 영향(옛 경로만 값을 만들어 냈다)
    r = classify([list(S.HEADER), row("068270", v3="", qoq=-8)])
    chk("진짜 V3 가 없는데 옛 경로가 걸렀으면 확정 영향", r["확정영향"] == 1)

    r = classify([list(S.HEADER), row("A", 72, -8), row("B", 15, "12.3"), row("C", 72, 30)])
    chk("합이 총행과 맞는다",
        r["확정영향"] + r["잠재영향"] + r["영향없음"] == r["총행"] == 3, r)

    chk("헤더가 이상하면 계측을 거부한다",
        classify([["종목코드", "엉뚱"], ["A", "1"]])["reason"] != "")
    chk("빈 시트도 사유를 남긴다", classify([])["reason"] != "")
    chk("열이 밀렸는지 보고한다", classify([list(S.HEADER)])["열이_밀렸는가"] == "Y")
    chk("문턱은 analyst 와 같은 20", THRESHOLD == 20)

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
