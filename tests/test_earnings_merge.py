# -*- coding: utf-8 -*-
"""
DB_실적 보존 — 2026-09-18.

9/17 실적 수집기: 시간 예산 55분에 걸려 143종목 중 127개만 처리했다.
로그는 "나머지는 다음 실행에서 이어서 수집됩니다" 라고 말했지만 실제로는

  ① 쓰기 경로가 `clear()` 후 통째로 다시 써서 **나머지 16종목의 이전 행이 사라졌고**
  ② 대상 순서가 시트 순서 고정이라 다음 실행도 **같은 앞쪽 127개**를 돌았다

즉 꼬리는 이어받기는커녕 매일 지워지고 영영 안 채워졌다.
바로 옆 DB_컨센서스 경로는 이미 merge 로 올바르게 보존하고 있었다 —
같은 파일 안에서 두 시트가 다르게 동작했다.
"""
import pathlib
import sys
import types
import unittest

# 무거운 의존성은 스텁으로 막고 순수 함수만 본다
for name, attrs in (("gspread", {}), ("oauth2client", {}),
                    ("oauth2client.service_account", {"ServiceAccountCredentials": object}),
                    ("bs4", {"BeautifulSoup": object})):
    if name not in sys.modules:
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        sys.modules[name] = m
sys.modules["oauth2client"].service_account = sys.modules["oauth2client.service_account"]

import hyeoks_earnings_collector as E      # noqa: E402

H = E.EARNINGS_HEADER


def row(code, stamp="2026-09-01 20:00:00", name="종목"):
    r = [code, name] + [""] * (len(H) - 3) + [stamp]
    assert len(r) == len(H)
    return r


class MergeTests(unittest.TestCase):
    def test_unprocessed_rows_survive(self):
        """시간 예산에 잘린 종목이 시트에서 사라지면 안 된다 — 이번 사고의 핵심."""
        old = [list(H), row("000660", "2026-09-17 20:00:00"), row("005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        codes = [E._code_of(r) for r in merged[1:]]
        self.assertEqual(sorted(codes), ["000660", "005930"])

    def test_refreshed_row_wins_and_is_not_duplicated(self):
        old = [list(H), row("005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        self.assertEqual(len(merged), 2)
        self.assertEqual(merged[1][E.STAMP_COL], "2026-09-18 20:00:00")

    def test_preserved_row_keeps_its_own_timestamp(self):
        """보존된 행에 오늘 시각을 찍으면 '오늘 갱신됐다'는 거짓말이 된다."""
        old = [list(H), row("000660", "2026-09-10 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        merged = E.merge_earnings_rows(old, new)
        kept = next(r for r in merged[1:] if E._code_of(r) == "000660")
        self.assertEqual(kept[E.STAMP_COL], "2026-09-10 20:00:00")

    def test_quoted_code_matches(self):
        old = [list(H), row("'005930", "2026-09-17 20:00:00")]
        new = [list(H), row("005930", "2026-09-18 20:00:00")]
        self.assertEqual(len(E.merge_earnings_rows(old, new)), 2)

    def test_empty_sheet(self):
        new = [list(H), row("005930")]
        self.assertEqual(E.merge_earnings_rows([], new), new)

    def test_short_old_row_is_padded_not_dropped(self):
        old = [list(H), ["000660", "하이닉스"]]
        merged = E.merge_earnings_rows(old, [list(H), row("005930")])
        kept = next(r for r in merged[1:] if E._code_of(r) == "000660")
        self.assertEqual(len(kept), len(H))

    def test_header_mismatch_refuses(self):
        old = [["종목코드", "옛스키마"], row("000660")]
        with self.assertRaises(ValueError):
            E.merge_earnings_rows(old, [list(H), row("005930")])

    def test_bad_output_schema_refuses(self):
        with self.assertRaises(ValueError):
            E.merge_earnings_rows([], [["종목코드"], ["005930"]])


class StaleFirstTests(unittest.TestCase):
    def test_oldest_first(self):
        old = [list(H), row("A", "2026-09-17"), row("B", "2026-09-10"), row("C", "2026-09-18")]
        self.assertEqual(E.stale_first(["A", "B", "C"], old), ["B", "A", "C"])

    def test_never_collected_goes_first(self):
        """한 번도 못 받은 종목이 맨 앞에 와야 언젠가 받는다."""
        old = [list(H), row("A", "2026-09-17")]
        self.assertEqual(E.stale_first(["A", "Z"], old)[0], "Z")

    def test_yesterdays_tail_is_todays_head(self):
        """어제 예산에 잘린 꼬리가 오늘 맨 앞에 오는가 — ②의 회귀."""
        codes = [f"{i:06d}" for i in range(5)]
        old = [list(H)] + [row(c, "2026-09-17 20:00:00") for c in codes[:3]]
        self.assertEqual(E.stale_first(codes, old)[:2], ["000003", "000004"])

    def test_no_old_sheet_keeps_a_stable_order(self):
        self.assertEqual(E.stale_first(["B", "A"], []), ["A", "B"])

    def test_header_row_is_not_a_stock(self):
        self.assertNotIn("종목코드", E.stale_first(["A"], [list(H), row("A")]))


class SchemaTests(unittest.TestCase):
    def test_stamp_column_is_last(self):
        self.assertEqual(E.STAMP_COL, len(H) - 1)
        self.assertEqual(H[E.STAMP_COL], "갱신일시")

    def test_header_is_written_from_one_place(self):
        """지역 사본을 두면 둘이 어긋나고 병합이 헛돈다."""
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertEqual(src.count('"종목코드", "종목명", "최신분기"'), 1)


if __name__ == "__main__":
    unittest.main()


PHASE_B = "# PHASE B — 컨센서스(보조)"   # 주석 언급이 아니라 실제 구획 표시


class FailClosedTests(unittest.TestCase):
    """🔴 2026-09-18 GPT P0-2/P0-3 — 앞 커밋의 내 수정에 fail-open 이 둘 남아 있었다."""

    def source(self):
        return pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")

    def test_read_failure_blocks_the_primary_write(self):
        """못 읽었는데 쓰는 것이 보존 병합에서 가장 위험한 패턴이다.

        읽기 실패를 빈 목록으로 취급하면 새 데이터가 위쪽만 덮고 아래쪽 옛 행이
        고아로 남는다. batch_clear 가드도 `0 > N` 이라 돌지 않는다.
        """
        src = self.source()
        self.assertIn("earnings_readable", src)
        self.assertIn("기존 시트를 읽지 못해 보존이 불가능하다", src)
        # 읽기 실패 경로가 ::warning:: 이 아니라 ::error:: 여야 한다
        self.assertRegex(src, r"::error::기존 DB_실적 읽기 실패")

    def test_schema_mismatch_never_clears(self):
        """'해석할 수 없으니 전부 지운다' 는 fail-closed 가 아니다."""
        src = self.source()
        block = src[src.find("merge_earnings_rows(existing_earnings"):][:1200]
        self.assertNotIn("out_sheet.clear()", block,
                         "스키마 불일치에 clear() 가 남아 있다")

    def test_blocked_write_still_preserves_todays_collection(self):
        """멈추는 것이 틀린 값보다 낫다는 원칙이 '모은 것을 버린다' 는 뜻은 아니다."""
        src = self.source()
        self.assertIn("def stage_earnings(", src)
        self.assertIn("stage_earnings(rows_out", src)

    def test_staging_is_written_and_readable(self):
        import csv
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = E.stage_earnings([list(H), row("005930")], "시험 사유", out_dir=d)
            rows = list(csv.reader(open(path, encoding="utf-8")))
            self.assertEqual(rows[0], ["# 본표 쓰기 차단", "시험 사유"])
            self.assertEqual(rows[1], list(H))
            self.assertEqual(rows[2][0], "005930")


class PhaseSeparationTests(unittest.TestCase):
    """🔴 GPT §5 — 보조 원천이 주 산출물의 시간 예산을 먹고 있었다."""

    def source(self):
        return pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")

    def test_consensus_is_not_called_inside_the_dart_loop(self):
        """종목당 12초 타임아웃이 DART 다음 종목을 막던 구조를 끊는다."""
        src = self.source()
        dart_loop = src[src.find("for idx, code in enumerate(target_codes):"):]
        dart_loop = dart_loop[:dart_loop.find(PHASE_B)]
        self.assertNotIn("fetch_consensus_estimates(", dart_loop)

    def test_consensus_runs_after_the_primary_write(self):
        src = self.source()
        self.assertLess(src.find('out_sheet.update(range_name="A1"'),
                        src.find(PHASE_B),
                        "DB_실적 저장보다 컨센서스가 먼저면 분리한 의미가 없다")

    def test_preflight_exists_and_can_skip_the_batch(self):
        """143종목을 다시 때려 보고 원인을 추측하지 않는다(GPT §13)."""
        self.assertIn("def consensus_preflight(", self.source())
        self.assertIn("배치를 시작하지 않는다", self.source())

    def test_preflight_ok_when_any_control_succeeds(self):
        calls = []

        def fetch(code):
            calls.append(code)
            if code == E.CONSENSUS_CONTROL[0]:
                raise RuntimeError("timed out")
            return {"2026.12(E)": {}}

        ok, detail = E.consensus_preflight(fetch)
        self.assertTrue(ok)
        self.assertEqual([d[1] for d in detail], ["fail", "ok"])
        self.assertIn("timed out", detail[0][3])

    def test_preflight_fails_only_when_all_controls_fail(self):
        def dead(code):
            raise RuntimeError("Connection refused")
        ok, detail = E.consensus_preflight(dead)
        self.assertFalse(ok)
        self.assertEqual(len(detail), len(E.CONSENSUS_CONTROL))

    def test_preflight_uses_few_controls(self):
        """대조는 적어야 한다 — 진단이 곧 부하가 되면 안 된다."""
        self.assertLessEqual(len(E.CONSENSUS_CONTROL), 3)


class ConsensusHealthTests(unittest.TestCase):
    """🔴 GPT §6 — 1건 실패가 142건 성공을 **저장하기 전에** 종료시키고 있었다."""

    def test_states(self):
        self.assertEqual(E.consensus_health(143, 143), "OK")
        self.assertEqual(E.consensus_health(143, 142), "DEGRADED")
        self.assertEqual(E.consensus_health(143, 0), "FAILED")
        self.assertEqual(E.consensus_health(0, 0), "SKIPPED")

    def test_partial_success_is_written_before_the_exit_decision(self):
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        write_at = src.find("consensus_sheet.update(")
        exit_at = src.find('if state == "FAILED"')
        self.assertGreater(write_at, 0)
        self.assertGreater(exit_at, write_at,
                           "성공분을 저장하기 전에 종료하면 142건이 버려진다")

    def test_one_failure_no_longer_reddens_the_workflow(self):
        """주 산출물이 정상이면 보조 일부 실패는 초록이다."""
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertNotRegex(src, r"if consensus_failures:\s*\n\s*print[^\n]*\n\s*raise SystemExit")
        self.assertIn('if state == "DEGRADED"', src)

    def test_primary_failure_is_still_red(self):
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn("if earnings_blocked:", src)
        block = src[src.find("주 산출물과 보조 자료를 가른다"):]
        self.assertIn("raise SystemExit(1)", block)

    def test_no_arbitrary_coverage_threshold_yet(self):
        """문턱 숫자는 원인 조사 후 사전 고정한다 — 지금 90% 를 박지 않는다."""
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        body = src[src.find("def consensus_health("):][:600]
        self.assertNotRegex(body, r"0\.[89]\d*|9[05]\s*%")


class WorkflowSeparationTests(unittest.TestCase):
    """🔴 GPT §5 — 장애 도메인을 워크플로 수준에서 가른다."""

    WF = pathlib.Path(".github/workflows")

    def load(self, name):
        import yaml
        return yaml.safe_load((self.WF / name).read_text(encoding="utf-8"))

    def test_two_workflows_exist(self):
        for name in ("earnings_collector.yml", "consensus_aux.yml"):
            with self.subTest(name):
                self.assertTrue((self.WF / name).exists())

    def test_primary_runs_only_the_primary_phase(self):
        body = "\n".join(s.get("run", "")
                         for s in self.load("earnings_collector.yml")["jobs"]["build"]["steps"])
        self.assertIn("--phase primary", body)
        self.assertNotIn("--phase aux", body)

    def test_aux_runs_only_the_aux_phase(self):
        body = "\n".join(s.get("run", "")
                         for s in self.load("consensus_aux.yml")["jobs"]["build"]["steps"])
        self.assertIn("--phase aux", body)
        self.assertNotIn("--phase primary", body)

    def test_locks_are_separate(self):
        """같은 락을 쓰면 보조가 느릴 때 주가 대기열에 밀린다(2026-07 사고)."""
        a = self.load("earnings_collector.yml")["concurrency"]["group"]
        b = self.load("consensus_aux.yml")["concurrency"]["group"]
        self.assertNotEqual(a, b)

    def test_schedules_do_not_collide(self):
        def cron(name):
            return self.load(name)[True]["schedule"][0]["cron"]
        self.assertNotEqual(cron("earnings_collector.yml"), cron("consensus_aux.yml"))

    def test_phase_values_are_validated(self):
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn('if PHASE not in ("all", "primary", "aux")', src)

    def test_aux_alone_still_has_targets(self):
        """aux 단독이면 DART 루프를 안 도니 대상이 비어 버릴 수 있었다."""
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn("consensus_targets = list(target_codes)", src)

    def test_primary_skips_the_aux_phase(self):
        src = pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")
        self.assertIn("PHASE B 생략", src)
        self.assertIn("DB_실적 생략", src)
