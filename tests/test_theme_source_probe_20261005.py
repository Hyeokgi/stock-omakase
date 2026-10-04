"""외부 테마 자료원 진단(stockinfo7) — Codex 교차 검토(2026-10-05) 반영 회귀 시험.

① 구성 종목만 바뀌어도 내용 지문이 바뀐다.  ② HTTP 오류·파싱 실패는 종료코드 1.
③ 공개 로그에 테마·종목 이름과 카드 JSON 을 찍지 않는다.  ④ 관측마다 KST 날짜·요청/수신 시각.
네트워크 없이 합성 HTML 로만 돈다.
"""
import contextlib
import datetime
import io
import unittest
from unittest import mock

import hyeoks_theme_source_probe as P

KST = datetime.timezone(datetime.timedelta(hours=9))


def page(stock, header="2026-10-06 15시 테마랭킹", theme="합성테마"):
    return "".join(f"<div>{x}</div>" for x in [
        header, theme, "10%", "&nbsp;",
        stock, "12%", "&nbsp;&nbsp;시총", "1,000", "억", "&nbsp;거래 100억",
    ])


class Resp:
    def __init__(self, text, code=200):
        self.text, self.status_code = text, code


class ProbeTests(unittest.TestCase):
    def test_membership_change_changes_the_digest(self):
        a, b = P.rank_state(page("합성종목A")), P.rank_state(page("합성종목B"))
        self.assertEqual(a["header"], b["header"])
        self.assertNotEqual(a["digest"], b["digest"])
        self.assertEqual(P.classify(200, b, a), "갱신")
        self.assertEqual(P.classify(200, a, a), "동일")

    def test_status_classes(self):
        good = P.rank_state(page("A"))
        self.assertEqual(P.classify(503, good, None), "HTTP오류")
        self.assertEqual(P.classify(200, P.rank_state("<html>점검 중</html>"), None), "파싱실패")
        self.assertEqual(P.classify(200, good, None), "첫관측")

    def test_dump_fails_on_error_response(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(P.dump(get=lambda *a, **k: Resp("<html>Unavailable</html>", 503)), 1)
            self.assertEqual(P.dump(get=lambda *a, **k: Resp("<html>no header</html>")), 1)

    def test_dump_logs_structure_but_no_names(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = P.dump(get=lambda *a, **k: Resp(page("비밀종목명", theme="비밀테마명")))
        self.assertEqual(code, 0)
        txt = out.getvalue()
        self.assertIn("카드 1", txt)
        self.assertNotIn("비밀종목명", txt)
        self.assertNotIn("비밀테마명", txt)
        self.assertNotIn("RANK_JSON", txt)

    def test_watch_records_dates_classes_and_change_window_without_names(self):
        pages = iter([Resp(page("비밀A")), Resp("", 503), Resp(page("비밀B"))])
        clock = iter(datetime.datetime(2026, 10, 6, 13, m, s, tzinfo=KST) for m in range(0, 30) for s in (0, 2))
        out = io.StringIO()
        with mock.patch.object(P.time, "time", side_effect=[0, 0, 1000]), contextlib.redirect_stdout(out):
            code = P.watch(10, 1, get=lambda *a, **k: next(pages), sleep=lambda s: None,
                           now=lambda: next(clock), deadline=100)
        txt = out.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("2026-10-06T13:00:00+09:00", txt, "요청 시작에 KST 날짜가 있다")
        self.assertIn("| 503 | HTTP오류 |", txt)
        self.assertIn("| 갱신 |", txt)
        self.assertIn("~", txt.split("## 바뀐 시점")[1])
        self.assertNotIn("비밀A", txt)
        self.assertNotIn("비밀B", txt)

    def test_watch_without_any_valid_observation_exits_1(self):
        with mock.patch.object(P.time, "time", side_effect=[0, 1000]), contextlib.redirect_stdout(io.StringIO()):
            code = P.watch(10, 1, get=lambda *a, **k: Resp("", 503), sleep=lambda s: None,
                           now=lambda: datetime.datetime(2026, 10, 6, 13, 0, tzinfo=KST), deadline=100)
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
