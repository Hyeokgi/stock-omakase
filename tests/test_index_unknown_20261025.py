# 지수 조회 실패 계측 시험(전체 검토 F) — '모름' 이 정상값으로 바뀌는 세 경로가 반환값은 그대로 두고 계측을 남기는지 확인한다
"""2026-10-25 — 판정 값은 Block A(선정 정책 고정, ~11/06) 때문에 바꾸지 않는다. 대신 그날 영수증이 degraded 가 되도록
TELEMETRY 에 OBSERVE 로 남긴다(feature_telemetry.GATE_BLOCKING)."""
import ast
import pathlib
import time
import unittest

import feature_telemetry

ROOT = pathlib.Path(__file__).resolve().parent.parent
NAMES = ("check_warning_market", "is_index_above_ma5", "get_kospi_fluctuation_rate")


def load(responses):
    """responses — URL 조각 → (json 값 또는 예외)."""
    tree = ast.parse((ROOT / "omakase.py").read_text(encoding="utf-8"))
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in NAMES]

    class Resp:
        def __init__(self, v):
            self.v = v

        def json(self):
            return self.v

    class Session:
        @staticmethod
        def get(url, verify=None, timeout=None):
            for key, v in responses.items():
                if key in url:
                    if isinstance(v, Exception):
                        raise v
                    return Resp(v)
            raise AssertionError(url)

    env = {"GLOBAL_SESSION": Session, "time": time, "TELEMETRY": feature_telemetry.Telemetry(),
           "feature_telemetry": feature_telemetry, "print": lambda *a, **k: None}
    exec(compile(ast.Module(body=funcs, type_ignores=[]), "omakase.py", "exec"), env)
    return env


def prices(n):
    return [{"closePrice": f"{1000 + i:,}"} for i in range(n)]


class IndexUnknown(unittest.TestCase):
    def test_ma5_failure_still_returns_true_but_is_counted(self):
        env = load({"KOSDAQ/price": ConnectionError("down")})
        self.assertIs(env["is_index_above_ma5"](), True)
        self.assertEqual(env["TELEMETRY"].total(), 1)

    def test_short_ma5_list_is_counted(self):
        env = load({"KOSDAQ/price": prices(3)})
        self.assertIs(env["is_index_above_ma5"](), True)
        self.assertEqual(env["TELEMETRY"].errors("index_state"), 1)

    def test_kospi_missing_field_or_failure_is_counted(self):
        env = load({"KOSPI/basic": {}})
        self.assertEqual(env["get_kospi_fluctuation_rate"](), 0.0)
        env2 = load({"KOSPI/basic": TimeoutError()})
        self.assertEqual(env2["get_kospi_fluctuation_rate"](), 0.0)
        self.assertEqual((env["TELEMETRY"].total(), env2["TELEMETRY"].total()), (1, 1))

    def test_warning_market_short_list_is_counted_and_result_unchanged(self):
        env = load({"KOSDAQ/price": prices(12), "KOSPI/basic": {"fluctuationsRatio": "0.50"}})
        self.assertFalse(env["check_warning_market"](), "판정 값은 예전과 같다(Block A)")
        self.assertEqual(env["TELEMETRY"].total(), 1)

    def test_normal_day_counts_nothing(self):
        env = load({"KOSDAQ/price": prices(20), "KOSPI/basic": {"fluctuationsRatio": "0.50"}})
        env["check_warning_market"]()
        env["is_index_above_ma5"]()
        self.assertEqual(env["TELEMETRY"].total(), 0)


if __name__ == "__main__":
    unittest.main()
