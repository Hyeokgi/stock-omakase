"""대체 테마 자료원 진단 — robots 존중·차단 시 멈춤·요청 상한·우회 없음."""
import unittest
from urllib.parse import urlparse

import hyeoks_theme_alt_probe as P


class Resp:
    def __init__(self, url, status=200, text="", ctype="text/html"):
        self.url, self.status_code, self.text = url, status, text
        self.content = text.encode()
        self.headers = {"content-type": ctype}
        self.history = []


class Sess:
    def __init__(self, pages):
        self.pages, self.seen, self.headers = pages, [], {}

    def get(self, url, **kw):
        self.seen.append((url, kw.get("headers")))
        return self.pages.get(url, Resp(url, 404, "", "text/plain"))


CFG = {"base": "https://x.example.com"}
HOME = '<a href="/theme/rank">테마</a><a href="/secret/theme">테마2</a><a href="/terms">이용약관</a><a href="https://evil.com/theme">밖</a><a href="https://m.x.example.com/theme">모바일</a>'


def run(pages):
    s, lines = Sess(pages), []
    r = P.probe_site("x", CFG, s, lines.append, sleep=lambda _: None)
    return r, s, "\n".join(lines)


class Probe(unittest.TestCase):
    def test_robots_disallow_is_respected_and_offsite_links_ignored(self):
        r, s, log = run({
            "https://x.example.com/robots.txt": Resp("", 200, "User-agent: *\nDisallow: /secret/\n", "text/plain"),
            "https://x.example.com/": Resp("https://x.example.com/", 200, HOME),
            "https://x.example.com/terms": Resp("https://x.example.com/terms", 200, "<p>무단으로 자동 수집을 금지합니다</p>"),
        })
        urls = [u for u, _ in s.seen]
        self.assertNotIn("https://x.example.com/secret/theme", urls)
        self.assertNotIn("https://evil.com/theme", urls)
        self.assertTrue(all(urlparse(u).netloc == "x.example.com" for u in urls), "다른 호스트는 그 호스트의 robots 없이 요청하지 않는다")
        self.assertIn("https://x.example.com/theme/rank", urls)
        self.assertIn("robots 금지", log)
        self.assertIn("무단으로 자동 수집", log)

    def test_stops_on_block_and_never_sends_spoofed_headers(self):
        r, s, _ = run({
            "https://x.example.com/robots.txt": Resp("", 404, "", "text/plain"),
            "https://x.example.com/": Resp("https://x.example.com/", 403, "blocked"),
        })
        self.assertTrue(r["stopped"])
        self.assertEqual(len(s.seen), 2)
        self.assertTrue(all(h is None for _, h in s.seen), "요청별 Referer 등 헤더 조작 없음")

    def test_unreadable_robots_stops_the_site(self):
        r, s, _ = run({"https://x.example.com/robots.txt": Resp("", 500, "", "text/plain")})
        self.assertEqual(len(s.seen), 1)
        self.assertIn("robots", r["stopped"])

    def test_request_cap(self):
        many = "".join(f'<a href="/theme/{i}">테마</a>' for i in range(30))
        _, s, _ = run({"https://x.example.com/robots.txt": Resp("", 404, "", "text/plain"),
                       "https://x.example.com/": Resp("https://x.example.com/", 200, many)})
        self.assertLessEqual(len(s.seen), P.MAX_REQ)

    def test_login_redirect_is_a_wall_but_a_header_password_box_is_not(self):
        self.assertTrue(P.login_wall(Resp("https://x.example.com/member/login?next=/theme")))
        self.assertFalse(P.login_wall(Resp("https://x.example.com/theme", 200, '<input type="password">')))


if __name__ == "__main__":
    unittest.main()
