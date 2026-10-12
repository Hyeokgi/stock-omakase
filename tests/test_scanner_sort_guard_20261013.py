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


if __name__ == "__main__":
    unittest.main()
