"""9/22 Gate 를 막은 두 오류의 회귀 시험. 자격증명·네트워크·시트 쓰기 없음.

두 오류는 성격이 같다 — **정상적인 시트 상태를 코드가 못 읽어서** 계측이 울렸다.
문턱이나 점수 공식 문제가 아니었으므로 이 시험도 그것들을 건드리지 않는다.
"""
import ast
import re
import textwrap
import unittest
from pathlib import Path

import feature_telemetry
import krx_amount
import krx_code

ROOT = Path(__file__).resolve().parents[1]
ANALYST = (ROOT / "hyeoks_analyst.py").read_text(encoding="utf-8")
OMAKASE = (ROOT / "omakase.py").read_text(encoding="utf-8")


def col_letter(n):
    out = ""
    while n:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


class 거래대금표시형식(unittest.TestCase):
    """① `수급_Raw` 의 `1,938억원` 을 읽지 못해 theme_money 4,132건이 났다."""

    # 시트가 실제로 돌려주던 형태
    REAL = ["1,938억원", "903억원", "12억원", "1,938", "0억원"]

    def test_옛_방식은_단위가_붙으면_전부_실패한다(self):
        """고치기 전 코드: `int(str(v).replace(',', '').strip())`."""
        for raw in self.REAL:
            if not raw.endswith("억원"):
                continue
            with self.assertRaises(ValueError, msg=raw):
                int(str(raw).replace(",", "").strip())

    def test_새_규칙은_전부_읽는다(self):
        got = [krx_amount.parse(v) for v in self.REAL]
        self.assertEqual(got, [(1938, "ok"), (903, "ok"), (12, "ok"),
                               (1938, "ok"), (0, "ok")])

    def test_빈_칸은_오류가_아니라_결측이고_0_이_아니다(self):
        for blank in ("", "   ", None):
            value, status = krx_amount.parse(blank)
            self.assertEqual(status, krx_amount.MISSING)
            self.assertIsNone(value, "빈 칸을 0 으로 바꾸면 안 된다")

    def test_모르는_단위는_통과시키지_않는다(self):
        """`[^0-9]` 를 전부 지우는 방식이면 1억 배 틀린 값을 조용히 더한다."""
        for bad in ("1.2조원", "193800000000원", "집계없음", "-"):
            self.assertEqual(krx_amount.parse(bad)[1], krx_amount.BAD, bad)
        self.assertEqual(re.sub(r"[^0-9]", "", "1.2조원"), "12")

    def test_음수_부호를_지우지_않는다(self):
        self.assertEqual(krx_amount.parse("-1,938억원"), (-1938, "ok"))
        self.assertEqual(re.sub(r"[^0-9]", "", "-1,938억원"), "1938")

    def test_수급_Raw_경로가_공유_규칙을_쓴다(self):
        block = ANALYST[ANALYST.index("raw_theme_daily_map = {}"):]
        block = block[:block.index("tech_data = ")]
        self.assertIn("krx_amount.parse", block)
        self.assertNotIn("int(str(row[val_idx]).replace(',', '')", block,
                         "쉼표만 지우던 옛 방식이 남아 있다")

    def test_결측을_더하지_않는다(self):
        """결측이 합계에 0 으로 들어가면 대장 판정이 조용히 흔들린다."""
        total = 0
        for raw in ("1,938억원", "", "903억원", "   "):
            value, status = krx_amount.parse(raw)
            if status == krx_amount.OK:
                total += value
        self.assertEqual(total, 2841)


class 실제블록실행(unittest.TestCase):
    """소스에 그 호출이 있는지가 아니라, **그 블록을 실제로 돌려** 결과를 본다.

    바깥 import 와 analyst 전체 실행은 검증하지 않는다. 시트도 건드리지 않는다.
    """

    HEADER = ["날짜", "순위", "테마명", "종목명", "종목코드", "등락률(%)", "거래대금(억원)"]

    def 블록(self):
        src = ANALYST
        a = src.index("    raw_theme_daily_map = {}")
        b = src.index('    except Exception as e:\n        print(f"⚠️ 역사적 주도 테마 대금', a)
        # 블록 안의 try 는 바깥(소스)에서 except 로 닫힌다. 여기서는 삼키지 않고
        # 그대로 올려서, 블록이 조용히 실패하는 것을 시험이 못 보고 지나치지 않게 한다.
        return textwrap.dedent(src[a:b]) + "except Exception:\n    raise\n"

    def 실행(self, rows):
        notes = []

        class FakeTelemetry:
            def note(self, feature, reason, klass=None, code=""):
                notes.append((feature, reason, klass, code))
                return None

        class FakeSheet:
            def get_all_values(self):
                return [실제블록실행.HEADER] + rows

        class FakeDoc:
            def worksheet(self, name):
                assert name == "수급_Raw", name
                return FakeSheet()

        ns = {"doc": FakeDoc(), "TELEMETRY": FakeTelemetry(),
              "krx_amount": krx_amount, "feature_telemetry": feature_telemetry,
              "RECEIPT_STATE": {"theme_money_missing": 0}, "print": lambda *a, **k: None}
        exec(compile(self.블록(), "raw-theme-block", "exec"), ns)
        return ns["raw_theme_daily_map"], ns["RECEIPT_STATE"], notes

    def row(self, date, theme, amount):
        return [date, "1", theme, "종목", "005930", "3.1", amount]

    def test_단위가_붙은_행을_실제로_합산한다(self):
        agg, state, notes = self.실행([
            self.row("2026-09-22", "반도체", "1,938억원"),
            self.row("2026-09-22", "반도체", "903억원"),
            self.row("2026-09-21", "조선", "12억원"),
        ])
        self.assertEqual(agg[("2026-09-22", "반도체")], 2841)
        self.assertEqual(agg[("2026-09-21", "조선")], 12)
        self.assertEqual(notes, [], "정상 표기에 계측이 울리면 안 된다")

    def test_빈_칸은_0_으로_더하지_않고_결측으로_센다(self):
        agg, state, notes = self.실행([
            self.row("2026-09-22", "반도체", "1,938억원"),
            self.row("2026-09-22", "반도체", ""),
        ])
        self.assertEqual(agg[("2026-09-22", "반도체")], 1938)
        self.assertEqual(state["theme_money_missing"], 1)
        self.assertEqual(notes, [], "빈 칸은 오류가 아니다")

    def test_읽을_수_없는_값은_여전히_CRITICAL_로_운다(self):
        agg, state, notes = self.실행([
            self.row("2026-09-22", "반도체", "1.2조원"),
        ])
        self.assertNotIn(("2026-09-22", "반도체"), agg, "못 읽은 값을 합산하면 안 된다")
        self.assertEqual(len(notes), 1)
        feature, reason, klass, sample = notes[0]
        self.assertEqual(feature, "theme_money")
        self.assertEqual(klass, feature_telemetry.CRITICAL)
        self.assertEqual(sample, "1.2조원", "무엇이 문제였는지 표본이 남아야 한다")

    def test_대장명_꼬리표를_떼는_기존_동작이_살아_있다(self):
        agg, _, _ = self.실행([self.row("2026-09-22", "반도체 (대장:삼성전자)", "100억원")])
        self.assertEqual(agg[("2026-09-22", "반도체")], 100)

    def test_9_22_재현_모든_행이_단위를_달고_있어도_이제_전부_읽는다(self):
        rows = [self.row("2026-09-22", f"테마{i % 7}", f"{i:,}억원") for i in range(1, 4133)]
        agg, state, notes = self.실행(rows)
        self.assertEqual(notes, [], "옛 방식이었다면 4,132건이 울렸다")
        self.assertEqual(sum(agg.values()), sum(range(1, 4133)))


class 잔여행(unittest.TestCase):
    """② 정리 범위가 AG 까지라 AH(RS등급)만 남은 행이 후보로 읽혔다."""

    # 9/22 시트의 729·730행: 종목 정보 없이 RS등급만 39·77
    STALE = [""] * 33 + ["39"]

    def test_옛_방식은_잔여행을_000000_종목으로_만든다(self):
        code = str(self.STALE[1]).replace("'", "").strip().zfill(6)
        self.assertEqual(code, "000000")
        with self.assertRaises(ValueError):
            int(self.STALE[29] if len(self.STALE) > 29 else "")

    def test_공유_규칙은_잔여행을_종목으로_보지_않는다(self):
        self.assertEqual(krx_code.normalize(self.STALE[1]), "")
        self.assertFalse(krx_code.is_code("000000"))

    def test_정상_종목은_그대로_통과한다(self):
        """제외 규칙이 멀쩡한 종목까지 잘라내면 안 된다."""
        for good in ("005930", "'005930", "0155E0", "00680K"):
            self.assertTrue(krx_code.is_code(good), good)

    def test_후보_루프가_공유_규칙으로_먼저_거른다(self):
        block = ANALYST[ANALYST.index("    cands_list = []"):]
        block = block[:block.index("combo_score = max(")]
        self.assertIn("krx_code.normalize(r[1])", block)
        self.assertNotIn('str(r[1]).replace("\'", "").strip().zfill(6)', block,
                         "zfill 로 000000 을 만들던 옛 방식이 남아 있다")

    def test_제외_건수와_사유를_남긴다(self):
        """조용히 버리면 다음에 같은 일이 나도 아무도 모른다."""
        block = ANALYST[ANALYST.index("    cands_list = []"):]
        self.assertIn('RECEIPT_STATE["invalid_rows"] += 1', block)
        self.assertIn('invalid_reasons', block)
        self.assertIn('"excluded"', ANALYST, "영수증에 제외 기록이 실려야 한다")

    def test_정리_범위가_실제_출력_폭과_맞는다(self):
        """이 시험이 헤더 증설 때마다 다시 어긋나는 것을 막는다."""
        tree = ast.parse(OMAKASE)
        width = None
        for node in ast.walk(tree):
            if (isinstance(node, ast.Assign)
                    and any(getattr(t, "id", "") == "extended_headers" for t in node.targets)
                    and isinstance(node.value, ast.List)):
                width = len(node.value.elts)   # AFTER_HEADER·NXT_HEADER 는 상수가 아니라 이름이다
        self.assertIsNotNone(width, "extended_headers 를 찾지 못했다")
        self.assertEqual(width, 34)
        self.assertEqual(col_letter(width), "AH")
        self.assertEqual(col_letter(33), "AG", "옛 범위는 AG 였다")

    def test_정리_범위를_고정_문자열로_쓰지_않는다(self):
        self.assertNotIn('batch_clear([f"A{len(helper_sheet_data) + 1}:AG"])', OMAKASE)
        self.assertIn("_col_letter(len(extended_headers))", OMAKASE)

    def test_열_문자_변환이_맞다(self):
        for n, want in ((1, "A"), (26, "Z"), (27, "AA"), (33, "AG"), (34, "AH"), (52, "AZ")):
            self.assertEqual(col_letter(n), want)


class 범위를_넘지_않았는지(unittest.TestCase):
    """문턱·점수·Gate 기준은 이번 수정의 대상이 아니다."""

    def test_combo_score_공식은_그대로다(self):
        self.assertIn("combo_score = max(v1_score, v2_score)", ANALYST)

    def test_후보_문턱은_그대로다(self):
        self.assertIn("c['score'] >= 30", ANALYST)

    def test_계측_등급을_낮추지_않았다(self):
        block = ANALYST[ANALYST.index("raw_theme_daily_map = {}"):]
        block = block[:block.index("tech_data = ")]
        self.assertIn("feature_telemetry.CRITICAL", block,
                      "theme_money 는 여전히 대장 판정 근거다")

    def test_Gate_기준_수를_바꾸지_않았다(self):
        import stability_gate
        self.assertEqual(len(stability_gate.KEYS), 7)


if __name__ == "__main__":
    unittest.main()
