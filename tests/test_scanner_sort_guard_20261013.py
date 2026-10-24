# 스캐너 정렬·건수 확인 예외(P1-1) 시험 — 정렬·서식이나 순위 풀 건수 되읽기가 실패해도 증거 저장·영수증이 이어지는지 확인한다
"""Codex 2026-10-13 P1 — docs/collaboration/2026-10-13_Codex_스캐너영수증누락_설계검토.md 1번."""
import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


class SortGuard(unittest.TestCase):
    """P1-1 — 정렬·서식과 순위 풀 건수 확인이 실패해도 증거 저장·영수증 발행이 이어져야 한다."""

    def setUp(self):
        self.tree = ast.parse((ROOT / "omakase.py").read_text(encoding="utf-8"))
        self.parents = {}
        for node in ast.walk(self.tree):
            for child in ast.iter_child_nodes(node):
                self.parents[child] = node

    def calls(self, name):
        out = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                f = node.func
                n = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")
                if n == name:
                    out.append(node)
        return out

    EVIDENCE_CALLS = {"record", "emit"}     # feature_store.record · rank_pool.record · production_receipt.emit

    def guarded(self, node):
        """가장 가까운 try 가 그 호출만 감싸는 **국소** try 이고, Exception 을 잡아 다시 던지지 않는가.

        바깥의 큰 try(Step1 전체)는 감싼 것으로 치지 않는다 — 그 try 의 except 로 빠지는 것이 바로
        Feature Store·순위 풀·영수증을 통째로 건너뛰게 만든 경로다. 그래서 가장 가까운 try 의 본문에
        증거 저장 호출(record·emit)이 있으면 국소 try 가 아니다.
        """
        child, cur = node, self.parents.get(node)
        while cur is not None and not isinstance(cur, ast.FunctionDef):
            if isinstance(cur, ast.Try) and child in cur.body:
                body_calls = {(c.func.attr if isinstance(c.func, ast.Attribute) else getattr(c.func, "id", ""))
                              for stmt in cur.body for c in ast.walk(stmt) if isinstance(c, ast.Call)}
                if body_calls & self.EVIDENCE_CALLS:
                    return False
                for h in cur.handlers:
                    t = h.type
                    catches = t is None or (isinstance(t, ast.Name) and t.id == "Exception")
                    reraises = any(isinstance(x, ast.Raise) for x in ast.walk(h))
                    if catches and not reraises:
                        return True
                return False
            child, cur = cur, self.parents.get(cur)
        return False

    def test_sort_call_in_the_scanner_is_guarded(self):
        calls = self.calls("sort_and_format_backtest_log")
        self.assertEqual(len(calls), 1)
        self.assertTrue(self.guarded(calls[0]), "정렬 실패가 Feature Store·순위 풀·영수증을 건너뛰게 하면 안 된다")

    def test_pool_count_readback_is_guarded(self):
        calls = self.calls("recorded_count")
        self.assertEqual(len(calls), 1)
        self.assertTrue(self.guarded(calls[0]), "건수 확인 실패가 영수증 발행을 건너뛰게 하면 안 된다")


class Behaviour(unittest.TestCase):
    """Codex 10/13 재검토 P2 — 구조만 보지 말고 **실제 코드 조각을 실행**해 예외 뒤 흐름을 확인한다.

    omakase.py 는 시트·네트워크 의존이 커서 통째로 불러오지 않는다. 대신 그 파일의 해당 문장(try·if)을
    원문 그대로 떼어 작은 함수 안에서 실행한다. 손잡이 뒤에 표지(after)를 두어, 예외 처리에 return·raise·
    break 가 끼어들면 표지에 닿지 못해 실패한다.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = (ROOT / "omakase.py").read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.src)

    def stmt_with_call(self, name, kind):
        """`name` 호출을 품은 가장 안쪽 `kind`(ast.Try·ast.If) 문장."""
        best = None
        for node in ast.walk(self.tree):
            if isinstance(node, kind):
                for c in ast.walk(node):
                    if isinstance(c, ast.Call):
                        f = c.func
                        n = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")
                        if n == name and (best is None or node.lineno > best.lineno):
                            best = node
        self.assertIsNotNone(best, name)
        return best

    def run_snippet(self, node, env, pre=""):
        """문장 원문을 들여쓰기만 맞춰 함수로 감싸 실행한다. 표지 목록과 지역 변수를 돌려준다."""
        seg = ast.get_source_segment(self.src, node, padded=True)
        lines = seg.splitlines()
        indent = min(len(l) - len(l.lstrip()) for l in lines if l.strip())
        body = "\n".join("    " + l[indent:] for l in lines)
        code = f"def _probe():\n    trail = []\n{pre}{body}\n    trail.append('after')\n    return trail, dict(locals())\n"
        g = dict(env)
        exec(compile(code, "<omakase 조각>", "exec"), g)
        return g["_probe"]()

    def test_sort_failure_falls_through_to_the_evidence_code(self):
        node = self.stmt_with_call("sort_and_format_backtest_log", ast.Try)

        def boom(*a):
            raise RuntimeError("APIError 503")
        trail, _ = self.run_snippet(node, {"sort_and_format_backtest_log": boom, "doc": None, "bt_sheet": None,
                                           "print": lambda *a, **k: None})
        self.assertEqual(trail, ["after"], "정렬이 실패해도 다음 문장(Feature Store·순위 풀·영수증)으로 가야 한다")
        ok, _ = self.run_snippet(node, {"sort_and_format_backtest_log": lambda *a: None, "doc": None,
                                        "bt_sheet": None, "print": lambda *a, **k: None})
        self.assertEqual(ok, ["after"])

    def pool_env(self, recorded):
        notes = []

        class Tel:
            @staticmethod
            def note(feature, reason, klass="CRITICAL", code=""):
                notes.append((feature, reason, klass))

        class Pool:
            recorded_count = staticmethod(recorded)

        class FT:
            OBSERVE, CRITICAL = "OBSERVE", "CRITICAL"
        env = {"rank_pool": Pool, "TELEMETRY": Tel, "feature_telemetry": FT, "today_str": "2026-10-15",
               "_ch": "차트TOP2", "_pool_runid": "1", "print": lambda *a, **k: None}
        return env, notes

    def test_count_failure_is_noted_as_observe_and_the_receipt_still_follows(self):
        node = self.stmt_with_call("recorded_count", ast.Try)

        def boom(*a):
            raise OSError("읽기 실패")
        env, notes = self.pool_env(boom)
        trail, local = self.run_snippet(node, env, pre="    _pool_rows = 0\n")
        self.assertEqual(trail, ["after"], "건수 확인이 실패해도 영수증 발행으로 가야 한다")
        self.assertEqual(local["_pool_rows"], 0, "건수를 지어내지 않는다")
        self.assertEqual([(f, k) for f, _, k in notes], [("pool_row", "OBSERVE")],
                         "계측에 남아야 영수증이 degraded 가 된다(조용히 넘기지 않는다)")

        env, notes = self.pool_env(lambda *a: 3)
        trail, local = self.run_snippet(node, env, pre="    _pool_rows = 0\n")
        self.assertEqual((trail, local["_pool_rows"], notes), (["after"], 3, []))

    def test_partial_append_still_stops_before_the_evidence_code(self):
        """P1-1 이 '부분 적재면 증거 저장을 멈춘다'(raise _append_err)를 바꾸지 않았는가."""
        sort_try = self.stmt_with_call("sort_and_format_backtest_log", ast.Try)
        guard = None
        for node in ast.walk(self.tree):
            if (isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
                    and getattr(node.test.left, "id", "") == "_append_err"):
                guard = node
        self.assertIsNotNone(guard)
        self.assertLess(guard.lineno, sort_try.lineno, "부분 적재 검사가 증거 저장보다 먼저여야 한다")
        siblings = [lst for p in ast.walk(self.tree) for f in ("body", "orelse")
                    for lst in [getattr(p, f, None)] if isinstance(lst, list) and guard in lst and sort_try in lst]
        self.assertEqual(len(siblings), 1, "부분 적재 검사와 정렬 try 는 같은 문장 목록에 나란히 있어야 한다 "
                                           "(검사를 따로 try 로 감싸면 raise 가 삼켜진다)")

        class Partial(Exception):
            pass
        env = {"_append_err": Partial("부분 적재"), "_ledger_found": ["a"], "_ledger_expected": ["a", "b"],
               "print": lambda *a, **k: None}
        with self.assertRaises(Partial):
            self.run_snippet(guard, env)
        env.update(_ledger_found=["a", "b"])
        trail, _ = self.run_snippet(guard, env)
        self.assertEqual(trail, ["after"], "원장에 모두 있으면 같은 묶음으로 이어 간다")


if __name__ == "__main__":
    unittest.main()
