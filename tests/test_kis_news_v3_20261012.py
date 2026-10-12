# 한투 뉴스 수집 kis-news-v3 — 넘칠 때 이어 받기(허브 #19)를 가짜 응답으로 확인하는 시험
import datetime
import unittest

import hyeoks_kis_news as N


def news(i, hhmmss):
    return {"cntt_usiq_srno": str(i), "data_dt": "20261012", "data_tm": hhmmss, "hts_pbnt_titl_cntt": f"비밀제목{i}",
            "news_ofer_entp_code": "2"}


def feed_minutes(start_s, n, first_id):
    """start_s 초부터 1초씩 과거로 n건(최신이 앞)."""
    return [news(first_id + k, f"{(start_s - k) // 3600:02d}{(start_s - k) % 3600 // 60:02d}{(start_s - k) % 60:02d}")
            for k in range(n)]


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += datetime.timedelta(seconds=s)


def at(hm):
    return datetime.datetime.fromisoformat(f"2026-10-12T{hm}:00+09:00")


class Archive:
    """하루치 뉴스 전체를 들고, '최신 40건' 과 '시각 이전 40건(경계 포함)' 을 돌려주는 가짜 API."""
    def __init__(self, items):
        self.items = sorted(items, key=N.pub_at, reverse=True)
        self.calls = []

    def latest(self, upto):
        return [x for x in self.items if N.pub_at(x) <= upto][:N.PAGE]

    def before(self, t):
        self.calls.append(t)
        return [x for x in self.items if N.pub_at(x) <= t][:N.PAGE]


class ContinuationV3(unittest.TestCase):
    def test_params_use_padded_date_and_time(self):
        p = N.before_params("2026-10-12T09:05:23+09:00")
        self.assertEqual((p["FID_INPUT_DATE_1"], p["FID_INPUT_HOUR_1"]), ("0020261012", "0000090523"))
        self.assertEqual(N.before_params(""), N.PARAMS)

    def _run(self, per_poll, fetch_before=True):
        # 09:00 이전 40건 → 이후 5분마다 per_poll 건씩 게시
        base = feed_minutes(9 * 3600 - 1, 40, 0)
        later = []
        nid = 1000
        for k in range(1, 4):
            end = 9 * 3600 + k * 300 - 1
            later += feed_minutes(end, per_poll, nid)
            nid += per_poll
        arc = Archive(base + later)
        c = Clock(at("09:00"))
        polls = iter(["09:00", "09:05", "09:10", "09:15"])

        def latest():
            return arc.latest(f"2026-10-12T{next(polls)}:00+09:00")
        rows, st, over = N.run("2026-10-12", latest, at("09:16"), 300, c, c.sleep, run_id="t",
                               fetch_before=arc.before if fetch_before else None)
        return rows, st, over, arc, len(base) + len(later)

    def test_overflow_is_filled_when_continuation_reaches_previous_poll(self):
        rows, st, over, arc, total = self._run(per_poll=100)       # 5분에 100건 → 40 + 이어 받기 2회
        self.assertEqual(st["overflowPolls"], 0)
        self.assertEqual(st["recoveredPolls"], 3)
        self.assertEqual(len(rows), total, "이어 받기로 사이 뉴스를 모두 받는다")
        self.assertEqual(len({r["key"] for r in rows}), len(rows), "중복 없음")

    def test_still_counts_overflow_when_cap_is_not_enough(self):
        rows, st, over, arc, total = self._run(per_poll=300)       # 40 + 3회 × 39 < 300
        self.assertEqual(st["overflowPolls"], 3)
        self.assertEqual(st["recoveredPolls"], 0)
        self.assertEqual(st["contCalls"], 3 * N.CONT_MAX)
        self.assertTrue(all(o["contCalls"] == N.CONT_MAX for o in over))
        self.assertLess(len(rows), total)

    def test_without_fetch_before_behaves_like_v2(self):
        rows, st, over, arc, total = self._run(per_poll=100, fetch_before=False)
        self.assertEqual(st["overflowPolls"], 3)
        self.assertEqual(st["contCalls"], 0)
        self.assertEqual(arc.calls, [])
        self.assertEqual(len(rows), 40 + 3 * 40)

    def test_no_continuation_without_overflow(self):
        rows, st, over, arc, total = self._run(per_poll=20)
        self.assertEqual((st["overflowPolls"], st["contCalls"]), (0, 0))
        self.assertEqual(len(rows), total)


if __name__ == "__main__":
    unittest.main()
