# AI 리포트 전체 검토 수정 시험 — 공개 로그 종목 정보(C) · 텔레그램 응답 확인(D) · 가상계좌 조회 실패 보류(E)
"""2026-10-25 전체 검토(Codex Astra high · Gemini Argon medium) C · D · E. 분석기는 오프라인에서 통째로 돌릴 수 없어
C · D 는 소스를, E 는 `update_portfolio` 함수만 떼어 가짜 시트로 실제 실행해 본다."""
import ast
import datetime
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC = (ROOT / "hyeoks_analyst.py").read_text(encoding="utf-8")
TREE = ast.parse(SRC)
BANNED = {"code", "name", "stock_name", "s_code"}


def _bad_value(node):
    """f-string 칸이 종목 식별 정보를 그대로 찍는가 — 이름 · 코드 변수, ['name'] · ['code'] 첨자, AI 선정 근거 원문."""
    for n in ast.walk(node):
        if isinstance(n, ast.Name) and n.id in BANNED:
            return True
        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant) and n.slice.value in ("name", "code"):
            return True
        if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "get" and n.args \
                and isinstance(n.args[0], ast.Constant) and str(n.args[0].value).startswith("reasoning"):
            # len(...) 로 감싼 길이만은 허용
            return not any(isinstance(p, ast.Call) and getattr(p.func, "id", "") == "len" for p in ast.walk(node)
                           if p is not n and n in ast.walk(p))
    return False


class PublicLog(unittest.TestCase):
    def test_no_print_formats_stock_identity_or_ai_reasoning(self):
        bad = []
        for call in ast.walk(TREE):
            if isinstance(call, ast.Call) and getattr(call.func, "id", "") == "print":
                for arg in call.args:
                    if isinstance(arg, ast.JoinedStr) and any(isinstance(v, ast.FormattedValue) and _bad_value(v.value)
                                                              for v in arg.values):
                        bad.append(call.lineno)
        self.assertEqual(bad, [], f"공개 로그에 종목 정보가 나간다: hyeoks_analyst.py 줄 {bad}")


class TelegramResult(unittest.TestCase):
    def test_published_flag_depends_on_a_checked_send(self):
        self.assertIn('_sent = getattr(_res, "status_code", 0) == 200', SRC)
        self.assertIn("and _sent:", SRC)
        self.assertNotIn('requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument", \n', SRC)


def update_portfolio_fn(fetch):
    fn = next(n for n in ast.walk(TREE) if isinstance(n, ast.FunctionDef) and n.name == "update_portfolio")
    import price_levels

    class Sheet:
        def __init__(self, rows):
            self.rows, self.written, self.appended = rows, None, []

        def get_all_values(self):
            return self.rows

        def clear(self):
            pass

        def update(self, range_name=None, values=None, value_input_option=None):
            self.written = values

        def append_row(self, r):
            self.appended.append(r)

    class Doc:
        def __init__(self, hold, closed):
            self.s = {"가상계좌_보유": hold, "가상계좌_종료": closed}

        def worksheet(self, n):
            return self.s[n]

    class Req:
        @staticmethod
        def get(url, timeout=None):
            return fetch(url)

    headers = ["종목명", "종목코드", "매입단가", "투자금액", "현재가", "수익률(%)", "편입일", "목표가", "손절가", "수동매도"]
    hold = Sheet([headers, ["비밀종목", "'123450", "10000", "1000000", "11000", "10.00%", "2026-10-20", "13000", "9000", "매도"]])
    closed = Sheet([["종목명"]])
    env = {"doc": Doc(hold, closed), "requests": Req, "price_levels": price_levels, "datetime": datetime,
           "KST": datetime.timezone(datetime.timedelta(hours=9)), "print": lambda *a, **k: None}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), "hyeoks_analyst.py", "exec"), env)
    return env["update_portfolio"], hold, closed


class VirtualAccount(unittest.TestCase):
    def test_failed_price_keeps_the_row_and_does_not_close_it(self):
        def boom(url):
            raise ConnectionError("down")
        fn, hold, closed = update_portfolio_fn(boom)
        fn([])
        self.assertEqual(closed.appended, [], "가짜 가격으로 수동매도 종료를 만들면 안 된다")
        row = hold.written[1]
        self.assertEqual((row[4], row[5], row[9]), ("11000", "10.00%", "매도"), "현재가 · 수익률 · 수동매도 표시를 그대로 둔다")

    def test_real_price_still_closes_manual_sell(self):
        class R:
            @staticmethod
            def json():
                return {"closePrice": "10,500"}
        fn, hold, closed = update_portfolio_fn(lambda url: R)
        fn([])
        self.assertEqual(len(closed.appended), 1)
        self.assertEqual(closed.appended[0][2], 10500)


if __name__ == "__main__":
    unittest.main()
