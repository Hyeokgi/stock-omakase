"""한투 토큰 공유 — 시트의 오늘 토큰이 통하면 함께 쓰고, 아니면 따로 발급(시트에는 쓰지 않음)."""
import contextlib
import io
import unittest

import kis_token_share as K


class Resp:
    def __init__(self, js):
        self.js = js

    def json(self):
        return self.js


class TokenShareTests(unittest.TestCase):
    def setUp(self):
        self.issued = 0
        self.checked = []

    def issue(self, post, key, secret):
        self.issued += 1
        return "새토큰"

    def get(self, ok):
        def _get(url, timeout=None, params=None, headers=None):
            self.checked.append((url, headers["authorization"], headers["tr_id"]))
            return Resp({"rt_cd": "0" if ok else "1", "msg_cd": "EGW00123"})
        return _get

    def run_(self, reader, ok=True):
        with contextlib.redirect_stdout(io.StringIO()) as out:
            got = K.token_for_run(self.get(ok), None, "k", "s", "https://kis", self.issue, reader=reader,
                                  today="2026-10-30")
        return got, out.getvalue()

    def test_todays_working_sheet_token_is_shared_without_issuing(self):
        (tok, src), log = self.run_(lambda: ("시트토큰", "2026-10-30"))
        self.assertEqual((tok, src, self.issued), ("시트토큰", "공유", 0))
        self.assertEqual(self.checked, [("https://kis" + K.CHECK_PATH, "Bearer 시트토큰", K.CHECK_TR)])
        self.assertNotIn("시트토큰", log, "토큰 값을 출력하지 않는다")

    def test_old_missing_or_broken_token_falls_back_to_issuing(self):
        for name, reader, ok in (("어제 것", lambda: ("시트토큰", "2026-10-29"), True),
                                 ("없음", lambda: None, True),
                                 ("안 통함", lambda: ("시트토큰", "2026-10-30"), False)):
            with self.subTest(name):
                self.issued = 0
                (tok, src), _ = self.run_(reader, ok)
                self.assertEqual((tok, src, self.issued), ("새토큰", "발급", 1))

    def test_no_keyfile_means_no_sheet_access(self):
        self.assertIsNone(K.read_sheet_token(keyfile="없는파일.json"))

    def test_collectors_use_the_shared_path_and_nobody_writes_the_sheet(self):
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        for f in ("hyeoks_kis_news.py", "hyeoks_flow_estimate.py", "hyeoks_friday_dip_alert.py"):
            self.assertIn("token_for_run(get, post, key, secret, BASE, token)", (root / f).read_text(encoding="utf-8"), f)
        src = (root / "kis_token_share.py").read_text(encoding="utf-8")
        for w in ("update_cell", "append_row", "batch_update", ".update("):
            self.assertNotIn(w, src, "시트 쓰기는 스캐너만 한다")
        for wf in ("kis_news.yml", "flow_estimate.yml", "friday_dip_alert.yml"):
            text = (root / ".github/workflows" / wf).read_text(encoding="utf-8")
            self.assertIn("gspread oauth2client", text, wf)
            self.assertIn('Path("secret.json").unlink(missing_ok=True)', text, wf)


if __name__ == "__main__":
    unittest.main()
