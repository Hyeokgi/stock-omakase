# -*- coding: utf-8 -*-
"""
목적지 회귀 시험 — 2026-09-18.

내가 어제 8개 모듈의 목적지를 secret 으로 통일했는데 **그 secret 은 옛 채널**이었다.
다음 모닝 브리핑부터 리포트·브리핑이 안 쓰는 채널로 갔을 것이다.
그 실수는 테스트가 아니라 사용자가 잡았다. 그래서 여기에 못을 박는다.

소스 수준으로 본다 — 이 모듈들은 gspread/bs4 를 import 하므로 테스트에서
실제 import 하면 환경에 따라 실패한다. 우리가 지키려는 불변식은
"어느 모듈도 목적지를 secret 에서 읽지 않는다" 이고, 그건 소스로 확인 가능하다.
"""
import pathlib
import re
import unittest

import telegram_target as T

ROOT = pathlib.Path(__file__).resolve().parent.parent

# 텔레그램을 실제로 발송하는 모듈 전부
SENDERS = [
    "omakase.py", "hyeoks_analyst.py", "hyeoks_morning.py", "hyeoks_nightly.py",
    "scanner_bot.py", "hyeoks_static_collector.py", "notify_naver_health.py",
]

# 주석/문서 줄은 제외하고 **실행되는 코드**만 본다
SECRET_READ = re.compile(r'os\.environ\.get\(\s*["\']TELEGRAM_CHAT_ID["\']')
HARDCODED = re.compile(r'TELEGRAM_CHAT_ID\s*=\s*["\']-?\d')


def code_lines(name):
    for i, line in enumerate((ROOT / name).read_text(encoding="utf-8").splitlines(), 1):
        if not line.lstrip().startswith("#"):
            yield i, line


class DestinationTests(unittest.TestCase):
    def test_no_sender_reads_the_stale_secret(self):
        """secret TELEGRAM_CHAT_ID 는 옛 채널 값이다 — 아무도 읽으면 안 된다."""
        bad = [f"{n}:{i}" for n in SENDERS for i, l in code_lines(n) if SECRET_READ.search(l)]
        self.assertEqual(bad, [], f"secret 을 목적지로 읽는 곳: {bad}")

    def test_no_sender_hardcodes_its_own_copy(self):
        """사본 5개가 흩어져 있던 것이 애초의 문제다 — 상수는 telegram_target 한 곳."""
        bad = [f"{n}:{i}" for n in SENDERS for i, l in code_lines(n) if HARDCODED.search(l)]
        self.assertEqual(bad, [], f"채널 ID 를 직접 박아 둔 곳: {bad}")

    def test_every_sender_uses_the_shared_target(self):
        for name in SENDERS:
            with self.subTest(name):
                src = (ROOT / name).read_text(encoding="utf-8")
                self.assertIn("telegram_target", src)

    def test_workflows_do_not_rewire_the_secret(self):
        """워크플로가 넘기면 언젠가 누가 다시 읽는다. 배선 자체를 두지 않는다."""
        bad = []
        for wf in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
            for i, line in enumerate(wf.read_text(encoding="utf-8").splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if re.search(r'TELEGRAM_CHAT_ID\s*:', line):
                    bad.append(f"{wf.name}:{i}")
        self.assertEqual(bad, [], f"secret 배선이 남아 있다: {bad}")

    def test_bot_token_still_wired_everywhere(self):
        """되돌리면서 어제의 **옳은** 수정(main.yml 토큰 미배선)까지 지우면 안 된다."""
        need = ["main.yml", "ai_report.yml", "morning_briefing.yml", "nightly_batch.yml",
                "scanner_run.yml", "static_collector.yml", "naver_healthcheck.yml"]
        for wf in need:
            with self.subTest(wf):
                src = (ROOT / ".github" / "workflows" / wf).read_text(encoding="utf-8")
                self.assertIn("TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}", src)

    def test_module_selftest(self):
        self.assertEqual(T._selftest(), 6)


if __name__ == "__main__":
    unittest.main()
