"""모닝 브리핑 E단계 말투 — 지시문에 관찰 · 조건형 규칙이 있고 명령형 소제목('오늘의 대응' · '관망 권고' · '실전 액션 플랜')이 없다."""
import importlib
import os
import sys
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# CI 에는 google-genai 가 없다(2026-10-31 PR #24 병합 뒤 test_ci_wiring 실패) — 설치돼 있으면 진짜,
# 없으면 test_morning_fred.py 와 같은 스텁. 순수 지시문 조립만 본다.
try:
    importlib.import_module("google.genai")
except ImportError:
    for name, attrs in (("google", {}), ("google.genai", {"Client": object})):
        if name not in sys.modules:
            m = types.ModuleType(name)
            for k, v in attrs.items():
                setattr(m, k, v)
            sys.modules[name] = m
    sys.modules["google"].genai = sys.modules["google.genai"]

import hyeoks_morning as M  # noqa: E402


class Tone(unittest.TestCase):
    def prompt(self, report="", kor=""):
        seen = []
        with mock.patch.object(M.genai, "Client", lambda api_key=None: None), \
                mock.patch.object(M, "_generate_with_retry", lambda client, p: seen.append(p) or "본문"):
            M.generate_morning_briefing("시장", "뉴스", kor, "유동성", report)
        return seen[0]

    def test_all_three_variants_carry_the_rule_and_no_directive_titles(self):
        for p in (self.prompt(report="어제 종목"), self.prompt(kor="🚨 [전일 기준 부합 종목 부재]"), self.prompt(kor="후보")):
            self.assertIn("명령 · 단정 · 수익 보장 표현을 쓰지 마십시오", p)
            for old in ("오늘의 대응", "관망 권고", "실전 액션 플랜", "홀딩/분할익절/손절"):
                self.assertNotIn(old, p)

    def test_closed_day_prompt_too(self):
        self.assertIn("명령 · 단정 · 수익 보장 표현을 쓰지 마십시오", M._closed_day_prompt("유동성", "뉴스", ("휴장", None)))


if __name__ == "__main__":
    unittest.main()
