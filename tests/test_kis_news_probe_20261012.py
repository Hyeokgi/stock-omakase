# 한투 뉴스 시험 호출의 이어 받기 판정(허브 #19)을 가짜 응답으로 확인하는 시험
import unittest

import hyeoks_kis_news_probe as P


def rows(start_min, n, day="20261012"):
    """start_min 분부터 1분씩 과거로 n건. 최신이 앞."""
    out = []
    for i in range(n):
        m = start_min - i
        out.append({"data_dt": day, "data_tm": f"{m // 60:02d}{m % 60:02d}00", "cntt_usiq_srno": f"S{m}"})
    return out


class Resp:
    def __init__(self, js, status=200, headers=None):
        self._js, self.status_code, self.headers = js, status, headers or {}

    def json(self):
        return self._js


class Continuation(unittest.TestCase):
    def test_only_variant_returning_older_rows_counts_as_continuation(self):
        base = rows(15 * 60, 40)                       # 15:00 ~ 14:21

        def get(url, headers=None, params=None, timeout=None):
            if params.get("FID_INPUT_DATE_1", "").startswith("00") and not params.get("FID_INPUT_SRNO"):
                hh = params["FID_INPUT_HOUR_1"][-6:]
                start = int(hh[:2]) * 60 + int(hh[2:4]) - 1
                return Resp({"rt_cd": "0", "output": rows(start, 40)})
            return Resp({"rt_cd": "0", "output": base})  # 입력을 무시하면 같은 최신 목록

        res = dict(P.continuation(get, {}, base, sleep=lambda s: None))
        self.assertTrue(res["날짜 00YYYYMMDD·시각 0000HHMMSS"])
        self.assertFalse(res["헤더 tr_cont=N"])
        self.assertFalse(res["날짜 YYYYMMDD·시각 HHMMSS"])

    def test_compare_reports_only_counts_and_times(self):
        c = P.compare(rows(600, 3), rows(597, 3))
        self.assertEqual(set(c), {"rows", "first", "last", "overlap_with_base", "older_than_base", "not_older"})
        self.assertEqual(c["older_than_base"], 3)
        self.assertEqual(c["overlap_with_base"], 0)


if __name__ == "__main__":
    unittest.main()
