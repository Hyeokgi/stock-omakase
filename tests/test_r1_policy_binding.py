# -*- coding: utf-8 -*-
"""R1-1 정책 결속을 합성 자료로 고정한다 (2026-10-13, Codex 결정 R1 · 전략로드맵 §3-5-7 U3).

원장 행은 정책을 스스로 증명하지 못한다(원장에 policy_id·run_id 열이 없다).
그래서 판정 단계에서 영수증 · 후보 풀 행 · 실행 코드 계보를 엮어 따로 결속하고,
하나라도 빠지거나 어긋나면 **닫는다.** 날짜나 정책 문자열만으로는 열지 않는다.
실제 원장·수익률은 쓰지 않는다. 아래 값은 전부 지어낸 것이다.
"""
import copy
import unittest

import hyeoks_verdict as v

NEW = "aaaa1111"      # 경계 커밋을 포함한 실행
OLD = "bbbb2222"      # 경계 이전 실행
UNK = "cccc3333"      # 계보를 확인할 수 없는 실행
LINEAGE = {NEW: True, OLD: False, UNK: None}


def contains(sha, boundary):
    return LINEAGE.get(sha)


def header():
    h = [""] * 38
    h[0], h[1], h[2], h[3], h[4] = "trade_id", "진입일", "채널", "종목명", "종목코드"
    return h


def row(tid, day, ch, code="000001"):
    r = [""] * 38
    r[0], r[1], r[2], r[4] = tid, day, ch, code
    return r


def receipt(day, run, sha, tids, today=None):
    return {"kind": "scanner", "cycle_date": day, "run_id": run, "code_sha": sha,
            "payload": {"today_str": today or day,
                        "ledger": {"expected_trade_ids": list(tids),
                                   "found_trade_ids": list(tids)}}}


def pool(day, ch, code, run, sha, policy, picked="Y"):
    return {"date": day, "channel": ch, "code": code, "picked": picked,
            "policy_id": policy, "run_id": run, "code_sha": sha}


CHART_POLICY = "chart-top2/v1/badge-pool"
D = "2026-10-13"       # 경계(첫 적용 2026-10-12) 이후
PRIOR = "2026-10-08"   # 경계 이전


def bind_one(r, receipts, pools):
    rows = [header(), r]
    return v.bind_policy(rows, receipts, pools, contains)[1]


class BindingTests(unittest.TestCase):
    def test_fully_linked_row_binds_and_keeps_source_policy_id(self):
        rows = [header(), row("T1", D, "차트TOP2", "111111")]
        before = copy.deepcopy(rows)
        pools = [pool(D, "차트TOP2", "111111", "r1", NEW, CHART_POLICY)]
        b = v.bind_policy(rows, [receipt(D, "r1", NEW, ["T1"])], pools, contains)[1]
        self.assertTrue(b["ok"], b)
        self.assertEqual(b["policy_id"], CHART_POLICY)
        self.assertEqual(b["cohort"], (v.COHORT_BOUNDARY["차트TOP2"]["cohort"], CHART_POLICY))
        self.assertEqual(rows, before, "원장 행을 고치면 안 된다")

    def test_missing_receipt_closes(self):
        b = bind_one(row("T1", D, "차트TOP2"), [], [])
        self.assertFalse(b["ok"])
        self.assertEqual(b["reason"], "receipt_missing")

    def test_missing_trade_id_closes(self):
        b = bind_one(row("", D, "차트TOP2"), [receipt(D, "r1", NEW, [""])], [])
        self.assertEqual(b["reason"], "trade_id_missing")

    def test_policy_string_alone_does_not_certify(self):
        # 후보 풀에 기대한 정책 문자열이 그대로 있어도 영수증이 없으면 닫힌다.
        pools = [pool(D, "차트TOP2", "000001", "r1", NEW, CHART_POLICY)]
        b = bind_one(row("T1", D, "차트TOP2"), [], pools)
        self.assertFalse(b["ok"])
        self.assertEqual(b["reason"], "receipt_missing")

    def test_two_receipts_from_different_runs_are_ambiguous(self):
        recs = [receipt(D, "r1", NEW, ["T1"]), receipt(D, "r2", NEW, ["T1"])]
        b = bind_one(row("T1", D, "랜덤2"), recs, [])
        self.assertEqual(b["reason"], "receipt_ambiguous")

    def test_duplicate_copies_of_one_run_are_not_ambiguous(self):
        recs = [receipt(D, "r1", NEW, ["T1"]), receipt(D, "r1", NEW, ["T1"])]
        self.assertTrue(bind_one(row("T1", D, "랜덤2"), recs, [])["ok"])

    def test_receipt_date_mismatch_closes(self):
        b = bind_one(row("T1", D, "랜덤2"),
                     [receipt("2026-10-14", "r1", NEW, ["T1"])], [])
        self.assertEqual(b["reason"], "receipt_date_mismatch")

    def test_pre_boundary_code_closes(self):
        b = bind_one(row("T1", D, "랜덤2"), [receipt(D, "r1", OLD, ["T1"])], [])
        self.assertEqual(b["reason"], "pre_boundary_code")

    def test_unknown_lineage_closes(self):
        b = bind_one(row("T1", D, "랜덤2"), [receipt(D, "r1", UNK, ["T1"])], [])
        self.assertEqual(b["reason"], "code_ancestry_unknown")

    def test_missing_code_sha_closes(self):
        b = bind_one(row("T1", D, "랜덤2"), [receipt(D, "r1", "", ["T1"])], [])
        self.assertEqual(b["reason"], "code_sha_missing")

    def test_rank_pool_channel_needs_its_pool_row(self):
        b = bind_one(row("T1", D, "수급TOP2"), [receipt(D, "r1", NEW, ["T1"])], [])
        self.assertEqual(b["reason"], "pool_row_missing")

    def test_unpicked_pool_row_does_not_count(self):
        pools = [pool(D, "차트TOP2", "000001", "r1", NEW, CHART_POLICY, picked="N")]
        b = bind_one(row("T1", D, "차트TOP2"), [receipt(D, "r1", NEW, ["T1"])], pools)
        self.assertEqual(b["reason"], "pool_row_missing")

    def test_pool_from_another_run_conflicts(self):
        pools = [pool(D, "차트TOP2", "000001", "r9", NEW, CHART_POLICY)]
        b = bind_one(row("T1", D, "차트TOP2"), [receipt(D, "r1", NEW, ["T1"])], pools)
        self.assertEqual(b["reason"], "pool_receipt_conflict")

    def test_two_pool_rows_with_different_settings_are_ambiguous(self):
        pools = [pool(D, "수급TOP2", "000001", "r1", NEW, "supply-top2/v2/gate-pass"),
                 pool(D, "수급TOP2", "000001", "r1", NEW, "supply-top2/v2/gate-pass+band1")]
        b = bind_one(row("T1", D, "수급TOP2"), [receipt(D, "r1", NEW, ["T1"])], pools)
        self.assertEqual(b["reason"], "pool_row_ambiguous")

    def test_report_channel_has_no_registered_boundary_yet(self):
        b = bind_one(row("T1", D, "리포트TOP2_단기"), [receipt(D, "r1", NEW, ["T1"])], [])
        self.assertEqual(b["reason"], "cohort_rule_missing")

    def test_ungated_channels_are_not_bound(self):
        rows = [header(), row("T1", D, "지수벤치"), row("T2", D, "랜덤2_배지")]
        self.assertEqual(v.bind_policy(rows, [], [], contains), {})


def chart_ok(tid, day, code, run="r1", sha=NEW, policy=CHART_POLICY):
    return (row(tid, day, "차트TOP2", code), receipt(day, run, sha, [tid]),
            pool(day, "차트TOP2", code, run, sha, policy))


def ctrl_ok(tid, day, run="r1", sha=NEW):
    return row(tid, day, "랜덤2"), receipt(day, run, sha, [tid])


def build(*parts):
    rows, recs, pools = [header()], [], []
    for p in parts:
        rows.append(p[0])
        recs.append(p[1])
        if len(p) > 2:
            pools.append(p[2])
    return v.bind_policy(rows, recs, pools, contains)


class CohortGateTests(unittest.TestCase):
    def test_single_cohort_with_linked_control_opens(self):
        bs = build(chart_ok("C1", D, "111111"), ctrl_ok("R1", D))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertTrue(g["open"], g)
        self.assertEqual(g["reason"], "")

    def test_prior_cohort_rows_are_left_out_not_merged(self):
        # 경계 이전 행은 영수증이 없어도(9/19 이전 등) 판정을 닫지 않는다 — 범위 밖이다.
        bs = build(chart_ok("C1", D, "111111"), ctrl_ok("R1", D),
                   (row("C0", PRIOR, "차트TOP2", "222222"), receipt(PRIOR, "r0", OLD, [])))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertTrue(g["open"], g)
        self.assertEqual(g["channel"]["prior_cohort"], 1)
        self.assertEqual(g["channel"]["in_scope"], 1)

    def test_one_unlinked_row_in_scope_closes_the_whole_verdict(self):
        bs = build(chart_ok("C1", D, "111111"), ctrl_ok("R1", D),
                   (row("C2", D, "차트TOP2", "333333"), receipt(D, "r1", NEW, [])))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertFalse(g["open"])
        self.assertEqual(g["reason"], v.POLICY_UNVERIFIED)
        self.assertEqual(g["channel"]["failures"], {"receipt_missing": 1})

    def test_post_boundary_row_made_by_old_code_is_cohort_contradiction(self):
        bs = build(chart_ok("C1", D, "111111"), ctrl_ok("R1", D),
                   chart_ok("C2", D, "333333", run="r2", sha=OLD))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertFalse(g["open"])
        self.assertEqual(g["channel"]["failures"], {"pre_boundary_code": 1})

    def test_mixed_settings_inside_cohort_close(self):
        bs = build(chart_ok("C1", D, "111111"), ctrl_ok("R1", D),
                   chart_ok("C2", "2026-10-14", "333333", run="r2",
                            policy="chart-top2/v2/other"))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertFalse(g["open"])
        self.assertEqual(g["reason"], "cohort_mixed")

    def test_unlinked_control_closes_even_if_channel_is_clean(self):
        bs = build(chart_ok("C1", D, "111111"),
                   (row("R1", D, "랜덤2"), receipt(D, "r1", NEW, [])))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertFalse(g["open"])
        self.assertTrue(g["channel"]["open"])
        self.assertEqual(g["control"]["failures"], {"receipt_missing": 1})

    def test_no_rows_in_cohort_closes(self):
        bs = build(ctrl_ok("R1", D))
        g = v.cohort_gate(bs, "차트TOP2")
        self.assertFalse(g["open"])
        self.assertEqual(g["channel"]["reason"], "no_rows_in_cohort")

    def test_report_channel_gate_is_closed_without_boundary(self):
        bs = build((row("P1", D, "리포트TOP2_단기"), receipt(D, "r1", NEW, ["P1"])),
                   ctrl_ok("R1", D))
        g = v.cohort_gate(bs, "리포트TOP2_단기")
        self.assertFalse(g["open"])
        self.assertEqual(g["channel"]["reason"], "cohort_rule_missing")

    def test_boundary_start_is_receipt_evidenced_first_application(self):
        # R5 증거 기록(2026-10-12 run 38210556325)의 값과 같아야 한다.
        for ch in ("차트TOP2", "수급TOP2", "랜덤2"):
            rule = v.COHORT_BOUNDARY[ch]
            self.assertEqual(rule["boundary"], "c623b5c")
            self.assertEqual(rule["start"], "2026-10-12")


def _have_commits(*shas):
    """체크아웃에 그 커밋들이 있나. CI 의 기본 checkout 은 얕아서(fetch-depth 1) 과거 커밋이 없다.
    2026-10-13 — 이 확인 없이 True/False 를 기대해 main CI 가 실패했다. 없으면 git_contains 는 None(확인 불가)이어야 한다."""
    import subprocess
    return all(subprocess.run(["git", "cat-file", "-e", f"{s}^{{commit}}"], capture_output=True).returncode == 0
               for s in shas)


class GitLineageTests(unittest.TestCase):
    def test_known_boundary_is_ancestor_of_head(self):
        import subprocess
        head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                              text=True).stdout.strip()
        expect = True if _have_commits(head, "c623b5c") else None
        self.assertIs(v.git_contains(head[:8], "c623b5c"), expect)

    def test_older_commit_does_not_contain_boundary(self):
        expect = False if _have_commits("b4ededd", "c623b5c") else None
        self.assertIs(v.git_contains("b4ededd", "c623b5c"), expect)

    def test_unknown_sha_is_none_not_false(self):
        self.assertIsNone(v.git_contains("deadbeef", "c623b5c"))

    def test_non_hex_input_is_none(self):
        self.assertIsNone(v.git_contains("main; rm -rf", "c623b5c"))
        self.assertIsNone(v.git_contains("", "c623b5c"))


if __name__ == "__main__":
    unittest.main()
