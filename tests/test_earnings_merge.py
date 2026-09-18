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


class TargetSourceHealthTests(unittest.TestCase):
    """입력 시트가 조용히 줄어드는 경로 — 2026-09-18 재지적."""

    def source(self):
        return pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8")

    def test_get_target_stocks_returns_health(self):
        """개수(N>=50)는 source health 가 아니다. DB_중장기가 통째로 실패해도
        DB_스캐너에 100종목이 있으면 'ok' 가 됐다."""
        src = self.source()
        self.assertIn("return result, health", src)
        self.assertIn('health["DB_중장기"]', src)
        self.assertIn('health["DB_스캐너"]', src)
        self.assertIn('health["기업정보"]', src)

    def test_health_drives_the_verdict_not_the_count(self):
        src = self.source()
        self.assertIn("_bad_sheets = [k for k, v in _sheet_health.items()", src)
        self.assertIn("sanity: target 수 비정상", src)   # 개수는 보조로만 남는다

    def test_missing_sheet_key_is_also_a_failure(self):
        """예외 없이 건너뛰어 키 자체가 없는 경우도 잡아야 한다."""
        self.assertIn("_missing_sheets = [k for k in", self.source())

    def test_missing_corp_codes_are_recorded(self):
        """지금 새 임계값을 만들지 않는다 — 목록만 남겨 며칠 보고 판단한다."""
        src = self.source()
        self.assertIn('"missing_corp_codes"', src)
        self.assertIn("_missing_corp.append(code)", src)


class WorkflowStateBindingTests(unittest.TestCase):
    """🔴 workflow 결론을 **영수증의 run_id** 와 1:1 로 묶는가."""

    def source(self):
        return pathlib.Path(".github/workflow_states.py").read_text(encoding="utf-8")

    def test_binds_by_run_id_not_by_date(self):
        """main.yml 은 하루 144회 돈다. 날짜 전체를 보면 무관한 재시도 하나가
        거래일을 통째로 떨어뜨린다(false-negative 과다)."""
        src = self.source()
        self.assertIn("KIND_TO_WORKFLOW", src)
        self.assertIn("production_receipt.latest(day, kind, fingerprint=fp)", src)
        self.assertIn("actions/runs/{run_id}", src)
        self.assertNotIn("per_page=30", src)     # 날짜 전체 조회 흔적

    def test_incomplete_run_is_not_a_success(self):
        self.assertIn('if d.get("status") != "completed"', self.source())

    def test_missing_receipt_yields_no_conclusion(self):
        """영수증이 없으면 결론을 지어내지 않는다 — Builder 가 거짓으로 본다."""
        self.assertIn("영수증이 없다", self.source())

    def test_selects_receipt_by_current_fingerprint(self):
        """🔴 2026-09-18 — Builder 는 지문으로 거르는데 여기는 안 걸렀다.

        같은 날 두 지문의 실행이 섞이면 데이터 증거는 현재 지문, workflow 증거는
        늦게 끝난 이전 지문을 가리킬 수 있었다. "1:1 로 묶었다" 가 아직 아니었다.
        """
        src = self.source()
        self.assertIn("production_receipt.latest(day, kind, fingerprint=fp)", src)
        self.assertIn("stability_gate.fingerprint()", src)
        # 지문 없이 고르는 옛 **호출**이 남아 있으면 안 된다.
        # 설명 주석에는 그 문구가 일부러 남아 있으므로 실행 코드만 본다.
        import ast
        tree = ast.parse(src)
        bad = [f"line {n.lineno}" for n in ast.walk(tree)
               if isinstance(n, ast.Call)
               and isinstance(n.func, ast.Attribute) and n.func.attr == "latest"
               and not any(k.arg == "fingerprint" for k in n.keywords)]
        self.assertEqual(bad, [], f"지문 없이 영수증을 고르는 호출: {bad}")

    def test_verifies_the_runs_actual_workflow_path(self):
        """잘못된 run_id·오염된 영수증이 **다른 워크플로의 성공**을 가리켜도 막는다."""
        src = self.source()
        self.assertIn("expect_path", src)
        self.assertIn('got_path != expect_path', src)
        self.assertIn('f".github/workflows/{wf}"', src)

    def test_path_mismatch_is_reported_not_swallowed(self):
        self.assertIn("워크플로 불일치", self.source())

    def test_run_conclusion_returns_reason(self):
        """왜 인정하지 않았는지 말한다 — 조용한 빈 문자열을 남기지 않는다."""
        import ast
        tree = ast.parse(self.source())
        fn = next(n for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name == "run_conclusion")
        returns = [n for n in ast.walk(fn) if isinstance(n, ast.Return)]
        self.assertTrue(all(isinstance(r.value, ast.Tuple) for r in returns),
                        "모든 return 이 (결론, 사유) 여야 한다")


class CycleDateTests(unittest.TestCase):
    """🔴 2026-09-19 — **첫 실제 실행이 드러낸 P0.**

    실적 수집기가 23:45 KST(금 9/18) 시작 → 00:17 KST(토 9/19) 종료.
    영수증을 `datetime.now(KST)` 로 찍어 cycle_date 가 **9/19** 가 됐다.
    9/19 는 토요일이라 Gate 에서 SKIP 이고, 그 영수증은 영원히 쓰이지 않는다.
    더 나쁜 것은 scanner·analyst 는 장중에 돌아 9/18 로 기록되므로
    **required 3종이 같은 날짜에 모이지 않는다** — Gate 가 3/3 에 도달할 수 없다.

    정적 감사가 아니라 **생산 데이터가 찾은 결함**이다.
    """

    def test_midnight_crossing_stays_on_its_trading_day(self):
        import datetime
        import production_receipt as R
        kst = datetime.timezone(datetime.timedelta(hours=9))
        self.assertEqual(
            R.cycle_date_now(datetime.datetime(2026, 9, 19, 0, 17, tzinfo=kst)),
            "2026-09-18")

    def test_weekend_rolls_back(self):
        import datetime
        import production_receipt as R
        kst = datetime.timezone(datetime.timedelta(hours=9))
        self.assertEqual(
            R.cycle_date_now(datetime.datetime(2026, 9, 20, 10, 0, tzinfo=kst)),
            "2026-09-18")

    def test_trading_day_is_itself(self):
        import datetime
        import production_receipt as R
        kst = datetime.timezone(datetime.timedelta(hours=9))
        for d, expect in (((2026, 9, 18, 14, 44), "2026-09-18"),
                          ((2026, 9, 21, 9, 0), "2026-09-21")):
            with self.subTest(d):
                self.assertEqual(R.cycle_date_now(datetime.datetime(*d, tzinfo=kst)), expect)

    def test_emitters_use_the_trading_day_not_the_wall_clock(self):
        for f in ("hyeoks_earnings_collector.py", "hyeoks_analyst.py", "omakase.py"):
            with self.subTest(f):
                src = pathlib.Path(f).read_text(encoding="utf-8")
                self.assertIn("cycle_date_now", src)

    def test_run_start_is_pinned(self):
        """긴 실행이 도중에 날짜를 넘겨도 시작 시각 기준으로 고정한다."""
        self.assertIn("_RUN_STARTED_AT = datetime.datetime.now(KST)",
                      pathlib.Path("hyeoks_earnings_collector.py").read_text(encoding="utf-8"))
        self.assertIn('RECEIPT_STATE["started_at"]',
                      pathlib.Path("hyeoks_analyst.py").read_text(encoding="utf-8"))

    def test_builder_blocks_a_mismatched_cycle_date(self):
        """영수증 날짜와 feature_store 날짜가 갈리면 증거가 아니다."""
        src = pathlib.Path("evidence_builder.py").read_text(encoding="utf-8")
        self.assertIn('cycle_date_matches', src)
