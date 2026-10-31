"""리포트 고정 문구 정리(2026-11-01, 사용자 결정 — 블록 B 시작 전) — 지시 · 과장 문구가 다시 들어오지 않는지, 보류 표시 머리말은 남았는지.

웹앱 E단계(10/31)에서 텔레그램 · 모닝 말투를 '관찰 · 조건형' 으로 바꿨고, 지문 대상인 분석기 고정 문구는 블록 평가 뒤로 미뤘다.
트랙 E 블록 A 무효 결정(11/01)으로 블록 B · C 동결(11/09 ~ 2027-01-12) 전에 넣는다. 선정 · 보류 판단 로직은 바꾸지 않는다.
"""
import pathlib
import unittest

SRC = (pathlib.Path(__file__).resolve().parent.parent / "hyeoks_analyst.py").read_text(encoding="utf-8")


class ReportWording(unittest.TestCase):
    def test_directive_phrases_are_gone(self):
        for bad in ("사격 중지", "강력 권고합니다", "현금 100%", "공격적 운영", "엄격히 권고", "Ceasefire", "수익 극대화"):
            with self.subTest(bad):
                self.assertNotIn(bad, SRC)

    def test_veto_prefix_and_zero_rule_survive(self):
        self.assertGreaterEqual(SRC.count('"⚠️ [매수 보류]'), 3)          # 보류 표시는 그대로(웹앱 · 시트가 이 머리말로 읽는다)
        self.assertIn("target_price와 stop_loss는 어떠한 계산값도 출력하지 말고 반드시 0으로 처리하십시오.", SRC)

    def test_tone_rule_is_in_the_report_prompt(self):
        self.assertIn("관찰한 사실과 조건('~이면 ~할 수 있다')으로 서술하십시오", SRC)


if __name__ == "__main__":
    unittest.main()
