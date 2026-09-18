# -*- coding: utf-8 -*-
"""
DB_실적 생산자 ↔ 소비자 계약 (L2) — 2026-09-18 GPT 교차검증 P0-1.

생산자 헤더에서 V3 는 **index 10** 인데 소비자 두 곳이 `row[8]` 을 읽고 있었다.
index 8 은 `영업이익증감률(QoQ,%)` 다. 그 값은 표시가 아니라

  hyeoks_analyst.py : 장기 구조적 SEED 를 `v3_score < 20` 이면 **제외**
  omakase.py        : 장기 보유 후보에 🔻실적 악화 주의 배지

에 쓰인다. 후보군 자체가 바뀌는 오류였다.

조용했던 이유는 `try: int(row[8]) except Exception: pass` 다.
QoQ 는 보통 "12.3" 같은 소수라 int() 가 실패해 **V3 가 없는 것처럼** 처리되고,
우연히 정수면(예: "-8") 그 값이 V3 로 쓰인다.

이 파일은 "함수가 맞는가"가 아니라 **"생산자와 소비자가 같은 열을 보는가"** 를 본다.
"""
import ast
import pathlib
import re
import unittest

import earnings_schema as S

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONSUMERS = ("omakase.py", "hyeoks_analyst.py")


def producer_header():
    """생산자 파일에서 헤더를 **소스로** 읽는다(수집기는 무거운 의존성을 끈다)."""
    tree = ast.parse((ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "EARNINGS_HEADER":
            return ast.literal_eval(node.value)
    raise AssertionError("생산자에 EARNINGS_HEADER 가 없다")


class SchemaAgreementTests(unittest.TestCase):
    def test_producer_and_contract_agree(self):
        """수집기 헤더와 공유 계약이 **같은 리스트**여야 한다. 어긋나면 전부 헛돈다."""
        self.assertEqual(producer_header(), S.HEADER)

    def test_v3_is_not_column_eight(self):
        """이 시험이 이번 버그의 못이다."""
        self.assertEqual(S.V3_COL, 10)
        self.assertEqual(S.HEADER[8], "영업이익증감률(QoQ,%)")
        self.assertNotEqual(S.HEADER[8], S.V3_NAME)

    def test_new_sheet_is_created_wide_enough(self):
        """GPT P0-4 — add_worksheet(cols="12") 인데 스키마는 14열이었다."""
        src = (ROOT / "hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        m = re.search(r'add_worksheet\(title="DB_실적"[^)]*cols=([^,)]+)', src, re.S)
        self.assertIsNotNone(m, "DB_실적 생성 호출을 찾지 못했다")
        self.assertNotRegex(m.group(1), r'"\d+"', "열 수를 숫자로 박아 두면 스키마와 어긋난다")
        self.assertIn("EARNINGS_HEADER", m.group(1))


class ConsumerTests(unittest.TestCase):
    """소비자가 **위치를 숫자로 박지 않고** 이름으로 찾는가."""

    def test_no_consumer_hardcodes_row_eight(self):
        bad = []
        for name in CONSUMERS:
            for i, line in enumerate((ROOT / name).read_text(encoding="utf-8").splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if re.search(r"int\(\s*row\[\d+\]\s*\)", line):
                    bad.append(f"{name}:{i}")
        self.assertEqual(bad, [], f"DB_실적 열을 숫자로 읽는 곳: {bad}")

    def test_every_consumer_uses_the_shared_contract(self):
        for name in CONSUMERS:
            with self.subTest(name):
                self.assertIn("earnings_schema", (ROOT / name).read_text(encoding="utf-8"))

    def test_consumers_do_not_swallow_the_reason(self):
        """V3 읽기 경로가 실패를 'V3 없음'처럼 가장하지 않는가.

        창을 넓게 잡으면 무관한 except 를 집으므로 `read_v3_map` 호출 직후
        열두 줄만 본다 — 그 안에 이 읽기의 try/except 가 들어 있다.
        """
        for name in CONSUMERS:
            with self.subTest(name):
                lines = (ROOT / name).read_text(encoding="utf-8").splitlines()
                at = next(i for i, l in enumerate(lines) if "read_v3_map(" in l)
                window = lines[at:at + 12]
                self.assertNotIn("except Exception: pass",
                                 "\n".join(window), f"{name}: V3 읽기가 조용히 삼킨다")
                self.assertTrue(any("print(" in l for l in window),
                                f"{name}: V3 읽기 결과를 한 줄도 말하지 않는다")


class EndToEndTests(unittest.TestCase):
    """생산자가 실제로 만드는 **14열 한 행**을 소비자 경로에 그대로 넣어 본다."""

    def produced_row(self, code="005930", v3=72, qoq=-8):
        row = [""] * len(S.HEADER)
        row[S.HEADER.index("종목코드")] = code
        row[S.HEADER.index("종목명")] = "삼성전자"
        row[S.HEADER.index("영업이익증감률(QoQ,%)")] = str(qoq)
        row[S.HEADER.index("V3(실적점수)")] = str(v3)
        row[S.HEADER.index("갱신일시")] = "2026-09-18 20:00:00"
        return row

    def test_analyst_reads_v3_not_qoq(self):
        sheet = [list(S.HEADER), self.produced_row(v3=72, qoq=-8)]
        v3_map, stats = S.read_v3_map(sheet)
        self.assertEqual(v3_map["005930"], 72)
        self.assertEqual(stats["col"], 10)

    def test_the_old_bug_would_have_excluded_this_stock(self):
        """옛 경로(row[8]=-8)는 `v3<20` 에 걸려 이 종목을 **제외**했을 것이다.
        새 경로는 72 를 읽어 통과시킨다. 후보군이 실제로 달라진다."""
        row = self.produced_row(v3=72, qoq=-8)
        old_value = int(row[8])                      # 옛 소비자가 읽던 값
        new_value = S.read_v3_map([list(S.HEADER), row])[0]["005930"]
        self.assertLess(old_value, 20)               # 제외됨
        self.assertGreaterEqual(new_value, 20)       # 통과함
        self.assertNotEqual(old_value, new_value)

    def test_decimal_qoq_used_to_vanish_silently(self):
        """QoQ 가 소수면 int() 가 실패해 V3 가 '없는 것'이 됐다 — fail-open."""
        row = self.produced_row(v3=15, qoq=12.3)
        with self.assertRaises(ValueError):
            int(row[8])
        self.assertEqual(S.read_v3_map([list(S.HEADER), row])[0]["005930"], 15)

    def test_moving_the_column_does_not_break_consumers(self):
        """GPT 요구 — EARNINGS_HEADER 에서 V3 위치를 바꿔도 소비자가 찾아야 한다."""
        moved = list(S.HEADER)
        moved.remove(S.V3_NAME)
        moved.insert(2, S.V3_NAME)
        row = [""] * len(moved)
        row[0] = "005930"
        row[2] = "72"
        v3_map, stats = S.read_v3_map([moved, row])
        self.assertEqual(v3_map, {"005930": 72})
        self.assertEqual(stats["col"], 2)

    def test_out_of_range_is_refused_not_used(self):
        """QoQ 같은 무한정 값을 V3 로 읽으면 범위 검사에 걸린다 — 두 번째 방어선."""
        row = [""] * len(S.HEADER)
        row[0] = "005930"
        row[S.V3_COL] = "-8"
        v3_map, stats = S.read_v3_map([list(S.HEADER), row])
        self.assertEqual(v3_map, {})
        self.assertEqual(stats["out_of_range"], 1)
        self.assertIn("다른 열을 읽고 있다", S.v3_report(stats))

    def test_schema_selftest(self):
        self.assertGreaterEqual(S._selftest(), 16)


if __name__ == "__main__":
    unittest.main()
