# 컨센서스 일시 오류의 제한 재시도와 영구 오류 거부를 검증한다.
import ssl
import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError, HTTPError
import naver_sources as N


def failure(cause):
    error = N.SourceError("source failed")
    error.__cause__ = cause
    return error


class RetryTests(unittest.TestCase):
    def test_timeout_eof_and_503_retry_once(self):
        for cause in (URLError(TimeoutError()), URLError(ssl.SSLEOFError()),
                      HTTPError("u", 503, "busy", {}, None)):
            with self.subTest(cause=cause), patch.object(N.time, "sleep") as sleep:
                fetch = Mock(side_effect=[failure(cause), "ok"])
                self.assertEqual(N._consensus_retry(fetch), "ok")
                self.assertEqual(fetch.call_count, 2)
                sleep.assert_called_once_with(1)

    def test_retry_budget_is_two_attempts(self):
        fetch = Mock(side_effect=failure(URLError(TimeoutError())))
        with patch.object(N.time, "sleep"), self.assertRaises(N.SourceError):
            N._consensus_retry(fetch)
        self.assertEqual(fetch.call_count, 2)

    def test_no_retry_schema_permission_rate_limit_or_certificate(self):
        for cause in (None, HTTPError("u", 400, "bad", {}, None),
                      HTTPError("u", 403, "denied", {}, None), HTTPError("u", 429, "slow", {}, None),
                      URLError(ssl.SSLCertVerificationError())):
            with self.subTest(cause=cause), patch.object(N.time, "sleep") as sleep:
                fetch = Mock(side_effect=failure(cause))
                with self.assertRaises(N.SourceError):
                    N._consensus_retry(fetch)
                fetch.assert_called_once()
                sleep.assert_not_called()

    def test_real_consensus_wiring_retries_read_not_invalid_quarters(self):
        with patch.object(N, "read", side_effect=[failure(URLError(TimeoutError())), "sDT: '20261019'"]), \
             patch.object(N, "get_json", return_value={"JsonData": []}), patch.object(N.time, "sleep"):
            self.assertEqual(N.consensus_estimates("005930"), {})
        with patch.object(N, "read", return_value="no date") as read, patch.object(N.time, "sleep") as sleep:
            with self.assertRaisesRegex(N.SourceError, "reference date"):
                N.consensus_estimates("005930")
            read.assert_called_once()
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
