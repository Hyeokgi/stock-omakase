"""허브 #18 — 공개 실행 로그에 보호 종목명 목록을 찍지 않는다 (2026-10-24).

`omakase.py` 의 `📦 [포트폴리오 동기화 완료]` 줄이 `DB_중장기` 의 종목명 전체를 Actions 로그(공개)에 찍었다.
스캐너는 오프라인에서 돌릴 수 없어 소스를 읽어 본다: print 의 f-string 안에서 `portfolio_protected_names` 는
`len(...)` 으로만 쓸 수 있다.
"""
import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


class ProtectedNamesStayOutOfLogs(unittest.TestCase):
    def test_print_uses_only_the_count(self):
        tree = ast.parse((ROOT / "omakase.py").read_text(encoding="utf-8"))
        seen, bad = 0, []
        for call in ast.walk(tree):
            if not (isinstance(call, ast.Call) and getattr(call.func, "id", "") == "print"):
                continue
            for value in (v.value for arg in call.args if isinstance(arg, ast.JoinedStr)
                          for v in arg.values if isinstance(v, ast.FormattedValue)):
                names = {n.id for n in ast.walk(value) if isinstance(n, ast.Name)}
                if "portfolio_protected_names" not in names:
                    continue
                seen += 1
                ok = (isinstance(value, ast.Call) and getattr(value.func, "id", "") == "len"
                      and len(value.args) == 1 and getattr(value.args[0], "id", "") == "portfolio_protected_names")
                if not ok:
                    bad.append(call.lineno)
        self.assertGreaterEqual(seen, 1, "동기화 완료 줄을 찾지 못했다 — 시험을 갱신하라")
        self.assertEqual(bad, [], f"보호 종목명이 공개 로그로 나간다: omakase.py 줄 {bad}")


if __name__ == "__main__":
    unittest.main()
