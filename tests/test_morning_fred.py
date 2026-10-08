"""모닝 브리핑 FRED 수집 — 실패가 **이유를 남기는가**, 부분 실패가 브리핑을 죽이는가.

2026-09-17 회귀: FRED 2계열(BAMLH0A0HYM2·M2SL)이 실패하자
  ① 텔레그램·로그 어디에도 원인이 없었다(`except Exception as e` 로 잡고 e 를 버림)
  ② 나머지 3계열·뉴스·한국장이 전부 정상인데 AI 브리핑 전체가 경고문으로 대체됐다
"""
import sys
import types
import unittest
import datetime

# 무거운 의존성은 스텁으로 막고 순수 로직만 본다
for name, attrs in (("requests", {"get": None}),
                    ("urllib3", {"disable_warnings": lambda *_: None,
                                   "exceptions": types.SimpleNamespace(InsecureRequestWarning=RuntimeWarning)}),
                    ("gspread", {}), ("oauth2client", {}),
                    ("oauth2client.service_account", {"ServiceAccountCredentials": object}),
                    ("google", {}), ("google.genai", {"Client": object}),
                    ("bs4", {"BeautifulSoup": object})):
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
sys.modules["oauth2client"].service_account = sys.modules["oauth2client.service_account"]
sys.modules["google"].genai = sys.modules["google.genai"]

import hyeoks_morning as M       # noqa: E402


class Resp:
    def __init__(self, code=200, payload=None, text=""):
        self.status_code, self._p, self.text = code, payload, text

    def json(self):
        if self._p is None:
            raise ValueError("no json")
        return self._p


ROWS2 = [{"date": "2026-09-16", "value": "5.4"},
         {"date": "2026-09-15", "value": "0.73"}]
OBS2 = {"observations": ROWS2}        # API 응답 전체 (fred_fetch 용)


class FredFetchTests(unittest.TestCase):
    def patch(self, fn):
        self.addCleanup(setattr, M.requests, "get", M.requests.get)
        M.requests.get = fn

    def test_success_returns_two_observations(self):
        self.patch(lambda *a, **k: Resp(200, OBS2))
        obs, why = M.fred_fetch("RRPONTSYD", key="k")
        self.assertEqual(len(obs), 2)
        self.assertEqual(why, "")

    def test_timeout_reason_is_preserved(self):
        """🔴 핵심 — 이유 없는 오류 보고는 보고가 아니다."""
        def boom(*a, **k):
            raise TimeoutError("read timed out")
        self.patch(boom)
        obs, why = M.fred_fetch("M2SL", key="k", retries=0)
        self.assertIsNone(obs)
        self.assertIn("TimeoutError", why)
        self.assertIn("read timed out", why)

    def test_http_error_carries_fred_message(self):
        self.patch(lambda *a, **k: Resp(400, {"error_message": "Bad request. series_id is not valid."}))
        obs, why = M.fred_fetch("NOPE", key="k")
        self.assertIsNone(obs)
        self.assertIn("400", why)
        self.assertIn("series_id", why)

    def test_bad_series_is_not_retried(self):
        calls = []
        def count(*a, **k):
            calls.append(1)
            return Resp(404, {"error_message": "not found"})
        self.patch(count)
        M.fred_fetch("NOPE", key="k", retries=3)
        self.assertEqual(len(calls), 1, "계열 문제는 재시도해도 같다")

    def test_transient_failure_is_retried(self):
        calls = []
        def flaky(*a, **k):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("reset")
            return Resp(200, OBS2)
        self.patch(flaky)
        obs, why = M.fred_fetch("WALCL", key="k", retries=1)
        self.assertIsNotNone(obs)
        self.assertEqual(len(calls), 2)

    def test_missing_key_says_so(self):
        obs, why = M.fred_fetch("M2SL", key="")
        self.assertIsNone(obs)
        self.assertIn("FRED_API_KEY", why)

    def test_one_observation_cannot_be_compared(self):
        self.patch(lambda *a, **k: Resp(200, {"observations": [{"date": "x", "value": "1"}]}))
        obs, why = M.fred_fetch("WALCL", key="k")
        self.assertIsNone(obs)
        self.assertIn("1개", why)


class FormatTests(unittest.TestCase):
    def test_spread_gets_percent_others_do_not(self):
        self.assertIn("%", M.format_fred("BAMLH0A0HYM2", "HY", ROWS2))
        self.assertNotIn("%", M.format_fred("WALCL", "Fed", ROWS2))

    def test_missing_value_is_not_formatted(self):
        obs = [{"date": "d", "value": "."}, {"date": "e", "value": "1"}]
        self.assertIsNone(M.format_fred("M2SL", "M2", obs))

    def test_tga_unit_label_corrected(self):
        """🔴 WTREGEN 은 **백만 달러**다. 883,335 를 십억으로 읽으면 883조 달러가 된다."""
        self.assertIn("백만 달러", M.FRED_SERIES["WTREGEN"])
        self.assertIn("십억 달러", M.FRED_SERIES["RRPONTSYD"])
        self.assertIn("백만 달러", M.FRED_SERIES["WALCL"])
        self.assertIn("십억 달러", M.FRED_SERIES["M2SL"])


class FailureDetectionTests(unittest.TestCase):
    """한국장 '종목 없음'을 실패로 오판하면 종목 없는 날마다 브리핑이 죽는다."""

    def kor_failed(self, t):
        return ("파싱 오류" in t) or ("비어있습니다" in t)

    def test_no_qualifying_stocks_is_not_a_failure(self):
        self.assertFalse(self.kor_failed(
            "🚨 [전일 기준 부합 종목 부재]\n시스템의 엄격한 필터를 통과한 주도주가 없습니다."))

    def test_real_failures_detected(self):
        self.assertTrue(self.kor_failed("🚨 파싱 오류: list index out of range"))
        self.assertTrue(self.kor_failed("구글 시트 데이터가 비어있습니다."))

    def test_normal_picks_not_flagged(self):
        self.assertFalse(self.kor_failed("▪️ [씨에스윈드] 정규종가: 52,300원"))


class MorningObservationTests(unittest.TestCase):
    def test_previous_session_skips_holiday_and_weekend(self):
        self.assertEqual(M._previous_session(datetime.date(2026, 10, 6)), "2026-10-02")
        self.assertEqual(M._previous_session(datetime.date(2026, 10, 12)), "2026-10-08")

    def test_generic_quote_is_not_mislabeled_as_krx(self):
        quote = ("전일종가 대비 -1.74% (164,100원) / 정규장종가 대비 미확인 "
                 "[시장 미확인 / 2026-10-08 19:54:12 KST]")
        line = M._morning_after_text(quote, "", "2026-10-08")
        self.assertIn("시장 미확인", line)
        self.assertIn("164,100원", line)
        self.assertIn("NXT 확인값: 미확인", line)
        self.assertNotIn("KRX:", line)

    def test_missing_and_stale_quote_are_not_no_change(self):
        old = "관측 (1,000원) [시장 미확인 / 2026-10-07 19:54:12 KST]"
        for value in ("", "미확인(당일 시간외 시세 없음)", old):
            line = M._morning_after_text(value, "", "2026-10-08")
            self.assertIn("시장 미확인: 미확인(", line)
            self.assertNotIn("특이사항 없음", line)

    def test_program_zero_is_not_claimed_as_verified_zero(self):
        self.assertIn("실제 0 여부 미검증", M._morning_program_text(
            "⚪ [수급강도 평년] 1.0배 / 프로그램:0.0억"))
        self.assertIn("+6.5억", M._morning_program_text("프로그램:+6.5억"))
        self.assertIn("원자료 없음", M._morning_program_text(""))

    def test_ai_fallback_cannot_publish_unverified_no_change_or_zero(self):
        output = "▫️ [🌙야간/시간외: 특이사항 없음] [🤖프로그램: 0.0억]"
        guarded = M._guard_morning_claims(output)
        self.assertNotIn("특이사항 없음", guarded)
        self.assertNotIn("프로그램: 0.0억", guarded)
        self.assertIn("원자료 대조 필요", guarded)
        self.assertIn("실제 0 여부 미검증", guarded)
        self.assertEqual(M._guard_morning_claims("🤖프로그램: +6.5억"), "🤖프로그램: +6.5억")

    def test_prompt_does_not_force_gap_prediction_when_quotes_missing(self):
        captured = []
        original = M.genai.Client
        class FakeClient:
            def __init__(self, **kwargs):
                self.models = self
            def generate_content(self, **kwargs):
                captured.append(kwargs["contents"])
                return types.SimpleNamespace(text="검사 완료")
        M.genai.Client = FakeClient
        try:
            M.generate_morning_briefing("시장", "뉴스", "종목", "유동성", "리포트")
            M.generate_morning_briefing("시장", "뉴스", "종목", "유동성")
        finally:
            M.genai.Client = original
        self.assertEqual(len(captured), 2)
        for prompt in captured:
            self.assertIn("'특이사항 없음'으로 바꾸지 마십시오", prompt)
            self.assertIn("시가 갭을 만들어내지 마십시오", prompt)
            self.assertNotIn("KRX·NXT 요약", prompt)


if __name__ == "__main__":
    unittest.main()
