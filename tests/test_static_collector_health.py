"""정적데이터 수집기의 소스 건강 판정 — 경보가 **실제 상황을 말하는가**.

2026-09-16 회귀: 9/15·9/16 실행에서 HTML 다섯 소스가 전부 0건이었는데
경보는 '관리종목 0건'만 말했다. `html_ok` 가 관리종목 하나만 봤기 때문이고,
나머지 네 소스는 0이어도 아무 말이 없는 조용히 죽는 경로였다.

네트워크·시트 없이 `health()` 순수 함수만 건다.
"""
import contextlib
import io
import sys
import types
import unittest

# gspread/bs4/oauth2client 는 액션에만 있다. 모듈 적재만 되게 최소 스텁을 끼운다.
for name in ("gspread", "bs4", "oauth2client", "oauth2client.service_account"):
    if name not in sys.modules:
        mod = types.ModuleType(name)
        if name == "bs4":
            mod.BeautifulSoup = object
        if name == "oauth2client.service_account":
            mod.ServiceAccountCredentials = object
        sys.modules[name] = mod
sys.modules["oauth2client"].service_account = sys.modules["oauth2client.service_account"]

import hyeoks_static_collector as C          # noqa: E402

HEALTHY_HTML = {"관리종목": 220, "거래정지": 120, "투자주의": 30, "투자경고": 10,
                "투자위험": 1}
JSON_OK = {"JSON": 244, "JSON스캔": 2873}


class HealthTests(unittest.TestCase):

    def test_all_healthy_gives_no_alert(self):
        dead, html_ok, json_ok, msg = C.health({**HEALTHY_HTML, **JSON_OK})
        self.assertEqual(dead, [])
        self.assertTrue(html_ok and json_ok)
        self.assertIsNone(msg)

    def test_real_2026_09_16_situation_names_all_five(self):
        """실제로 있었던 상황 — 다섯 소스 전부 0. 경보가 전부를 말해야 한다."""
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0,
                  "투자위험": 0, **JSON_OK}
        dead, html_ok, json_ok, msg = C.health(counts)
        self.assertFalse(html_ok)
        self.assertTrue(json_ok)
        # 투자위험은 평시 0~1건이라 기대치 대상이 아니다
        self.assertEqual(set(dead), {"관리종목", "거래정지", "투자주의", "투자경고"})
        for lab in dead:
            self.assertIn(lab, msg, f"{lab} 이 경보에 없다")
        self.assertIn("전부", msg)
        self.assertIn("JSON 단일 소스", msg)

    def test_old_alert_understated_this(self):
        """옛 판정(`관리종목` 하나만 보기)이 놓치던 경우를 건다."""
        counts = {"관리종목": 220, "거래정지": 0, "투자주의": 0, "투자경고": 0,
                  "투자위험": 0, **JSON_OK}
        dead, html_ok, _, msg = C.health(counts)
        self.assertFalse(html_ok, "관리종목만 멀쩡하면 통과하던 버그")
        self.assertEqual(set(dead), {"거래정지", "투자주의", "투자경고"})
        self.assertNotIn("전부", msg)          # 전부는 아니다 — 과장하지 않는다

    def test_low_risk_source_alone_is_not_an_alarm(self):
        """투자위험은 평시 0~1건이다. 0 을 이상으로 보면 매일 오탐이 된다."""
        dead, html_ok, _, msg = C.health({**HEALTHY_HTML, "투자위험": 0, **JSON_OK})
        self.assertTrue(html_ok)
        self.assertIsNone(msg)

    def test_json_failure_detected_separately(self):
        dead, html_ok, json_ok, _ = C.health({**HEALTHY_HTML, "JSON": 3})
        self.assertTrue(html_ok)
        self.assertFalse(json_ok)

    def test_alert_carries_json_scan_size(self):
        """대체 진행이 믿을 만한지 보려면 전종목 스캔 수가 같이 있어야 한다."""
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0, **JSON_OK}
        _, _, _, msg = C.health(counts)
        self.assertIn("244", msg)
        self.assertIn("2873", msg)

    def test_alert_carries_parse_shape_detail(self):
        """'표없음' 과 '요청실패' 는 다른 사건이다 — 경보에 그대로 실린다."""
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0, **JSON_OK}
        _, _, _, msg = C.health(counts, ["관리종목[표없음(table.type_2 미발견)]"])
        self.assertIn("table.type_2", msg)

    def test_missing_keys_treated_as_zero_not_ignored(self):
        dead, html_ok, _, _ = C.health(JSON_OK)
        self.assertEqual(len(dead), len(C.MIN_BY_SOURCE))
        self.assertFalse(html_ok)


class NotifyPolicyTests(unittest.TestCase):
    """2026-09-28 — 대체 진행 경보의 텔레그램 발송 여부. 생략은 딱 한 경우뿐이다."""

    ALL = ["관리종목", "거래정지", "투자주의", "투자경고"]

    def test_known_steady_state_is_silent(self):
        self.assertFalse(C.notify_degraded(self.ALL, json_ok=True))

    def test_all_html_dead_and_json_bad_is_not_silenced(self):
        self.assertTrue(C.notify_degraded(self.ALL, json_ok=False))

    def test_partial_html_death_alerts(self):
        for dead in (["거래정지"], ["거래정지", "투자주의", "투자경고"]):
            with self.subTest(dead=dead):
                self.assertTrue(C.notify_degraded(dead, json_ok=True))

    def test_policy_tracks_source_list_size(self):
        """소스가 늘면 '전부' 의 뜻도 따라간다 — 숫자 4 를 박지 않는다."""
        self.assertEqual(len(self.ALL), len(C.MIN_BY_SOURCE))


class MainSmokeTests(unittest.TestCase):
    """`main()` 을 실제로 돌린다.

    `health()` 만 검증하면 **배선이 틀린 것을 못 잡는다** — 실제로 이번에
    main 이 정의되지 않은 `degraded_msg` 를 참조해 NameError 로 죽을 상태였는데
    순수 함수 검사는 전부 통과했다. 그래서 dry-run 경로를 그대로 태운다.
    """

    def run_main(self, counts, junk_n, errors=()):
        junk = {f"{i:06d}": f"종목{i}" for i in range(junk_n)}
        sent = []
        orig_fetch, orig_warn = C.fetch_junk_universe, C.telegram_warn
        C.fetch_junk_universe = lambda: (junk, counts, list(errors))
        C.telegram_warn = lambda m: sent.append(m)
        cwd = __import__('os').getcwd()
        try:
            # secret.json 이 없으면 DRY-RUN 으로 빠진다(시트를 건드리지 않는다)
            self.assertFalse(__import__('os').path.exists('secret.json'),
                             'secret.json 이 있으면 이 검사가 시트를 건드린다')
            C.main()
            return sent, None
        except SystemExit as e:
            return sent, e.code
        finally:
            C.fetch_junk_universe, C.telegram_warn = orig_fetch, orig_warn

    def test_degraded_path_runs_and_logs_without_telegram(self):
        """9/16 실제 상황(HTML 전부 사망 + JSON 정상) — 죽지 않고 돈다.

        🔴 2026-09-28 사용자 결정: 이 상태가 9/16 부터 매일 반복되는 평상 상태가 됐다.
           텔레그램은 보내지 않고 **로그에는 같은 문구를 그대로** 남긴다.
        """
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0,
                  "투자위험": 0, "JSON": 244, "JSON스캔": 2873}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            sent, code = self.run_main(counts, 244)
        self.assertIsNone(code, "정상 종료해야 한다")
        self.assertEqual(sent, [], "평상 상태는 텔레그램을 보내지 않는다")
        log = out.getvalue()
        self.assertIn("4/4 소스 미달", log)        # 경보 문구는 로그에 그대로
        self.assertIn("전부", log)
        self.assertIn("텔레그램 생략", log)

    def test_partial_html_death_still_alerts(self):
        """HTML 일부만 죽거나 일부 부활 = 상태 변화 — 지금처럼 텔레그램을 보낸다."""
        counts = {"관리종목": 220, "거래정지": 0, "투자주의": 0, "투자경고": 0,
                  "투자위험": 0, **JSON_OK}
        sent, code = self.run_main(counts, 244)
        self.assertIsNone(code)
        self.assertEqual(len(sent), 1)
        self.assertIn("3/4 소스 미달", sent[0])

    def test_html_dead_and_json_below_floor_still_aborts_with_alarm(self):
        """JSON 까지 기준 미달이면 생략 대상이 아니다 — 🚨 로 중단한다."""
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0,
                  "투자위험": 0, "JSON": 30, "JSON스캔": 2873}
        sent, code = self.run_main(counts, 30)
        self.assertEqual(code, 1)
        self.assertEqual(len(sent), 1)
        self.assertIn("수집 비정상", sent[0])

    def test_healthy_path_sends_nothing(self):
        sent, code = self.run_main({**HEALTHY_HTML, **JSON_OK}, 244)
        self.assertIsNone(code)
        self.assertEqual(sent, [])

    def test_both_sources_dead_aborts_without_touching_sheet(self):
        """둘 다 죽으면 **중단**하고 전일 데이터를 남긴다(fail-closed)."""
        counts = {"관리종목": 0, "거래정지": 0, "투자주의": 0, "투자경고": 0, "JSON": 0}
        sent, code = self.run_main(counts, 3)
        self.assertEqual(code, 1)
        self.assertIn("수집 비정상", sent[0])
        self.assertIn("전일 스냅샷 유지", sent[0])


if __name__ == "__main__":
    unittest.main()
