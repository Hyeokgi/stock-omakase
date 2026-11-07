# 스캐너(omakase) 테마 순위를 rankings v2 로 옮긴 변경(2026-11-08) 시험 — 주소 · 순서 · 실패 시 빈 목록
"""2026-11-06 구 `/market/theme/list` 가 404 로 사라져 스캐너 테마 수집이 매 회차 'HTML·JSON 모두 실패' 였다.
omakase.py 를 통째로 가져오지 않고 함수만 꺼내 돌린다(tests/test_naver_membership_20261020.py 와 같은 방식)."""
import ast
import re
import unittest
from pathlib import Path
from unittest.mock import Mock

SRC = Path("omakase.py").read_text(encoding="utf-8")


def load(session):
    tree = ast.parse(SRC)
    func = next(x for x in tree.body if isinstance(x, ast.FunctionDef) and x.name == "fetch_theme_list_json")
    url = re.search(r'THEME_V2_URL = \(("[^"]+"\s*"[^"]+")\)', SRC).group(1)
    env = {"GLOBAL_SESSION": session, "_JSON_HEADERS": {}, "THEME_V2_URL": eval("(" + url + ")")}
    exec(compile(ast.Module(body=[func], type_ignores=[]), "omakase.py", "exec"), env)
    return env["fetch_theme_list_json"], env["THEME_V2_URL"]


def session_with(status, body):
    s = Mock()
    s.get.return_value = Mock(status_code=status, json=Mock(return_value=body))
    return s


class ThemeListV2(unittest.TestCase):
    def test_reads_v2_items_in_order_with_limit(self):
        s = session_with(200, {"items": [{"code": 435, "name": " 가 "}, {"code": "12", "name": "나"},
                                         {"name": "번호 없음"}, {"code": "7", "name": "다"}],
                               "hasNext": True, "cursor": "C"})
        fetch, url = load(s)
        self.assertEqual(fetch(2), [{"no": "435", "name": "가"}, {"no": "12", "name": "나"}])
        self.assertEqual(s.get.call_args.args[0], url)
        self.assertIn("/rankings/v2/domestic/themes?sortType=changeRate", url)
        self.assertNotIn("market/theme/list", url)

    def test_failures_return_empty_list(self):
        for status, body in ((404, {"message": "Route not found"}), (200, []), (200, {"items": []})):
            with self.subTest(status=status, body=body):
                self.assertEqual(load(session_with(status, body))[0](30), [])
        s = Mock()
        s.get.side_effect = OSError("down")
        self.assertEqual(load(s)[0](30), [])


if __name__ == "__main__":
    unittest.main()
