import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import hana_research as h


def listing(seq=123, date="2026.09.13", filename="future_20260914.pdf"):
    return f'''<a class="item title" id="2220_{seq}">업황 &amp; 위험</a>
    <span>{date}</span><span>오후 4:47:33</span>
    <a href="/main/research/research/download.cmd?bbsSeq={seq}&amp;attachFileSeq=1&amp;bbsCd=2220">{filename}</a>'''


class HanaResearchTests(unittest.TestCase):
    def test_publication_not_filename(self):
        row = h.parse_listing(listing())[0]
        self.assertEqual(row["published_at"], "2026-09-13T16:47:33+09:00")
        self.assertEqual(row["title"], "업황 & 위험")
        self.assertNotIn("&amp;", row["source_url"])

    def test_empty_layout_fails(self):
        with self.assertRaises(ValueError):
            h.parse_listing("<html>maintenance</html>")

    def test_missing_date_fails(self):
        with self.assertRaises(ValueError):
            h.parse_listing(listing(date="unknown"))

    def test_unauthorized_host_fails(self):
        with self.assertRaises(ValueError):
            h.fetch("https://example.com/file.pdf")

    def test_repeated_page_fails(self):
        with tempfile.TemporaryDirectory() as output, patch.object(h, "fetch", return_value=listing().encode()):
            with self.assertRaisesRegex(ValueError, "Repeated page"):
                h.collect("2026-09-01", output)

    def test_non_pdf_rejected(self):
        page = listing() + listing(seq=122, date="2026.08.20")
        with tempfile.TemporaryDirectory() as output, patch.object(h, "fetch", side_effect=[page.encode(), b"<html>error</html>"]):
            with self.assertRaisesRegex(ValueError, "Not a PDF"):
                h.collect("2026-09-01", output)

    def test_download_resume(self):
        page = listing() + listing(seq=122, date="2026.08.20")
        with tempfile.TemporaryDirectory() as output:
            with patch.object(h, "fetch", side_effect=[page.encode(), b"%PDF-test"]):
                h.collect("2026-09-01", output)
            with patch.object(h, "fetch", return_value=page.encode()) as fetch:
                h.collect("2026-09-01", output)
                self.assertEqual(fetch.call_count, 1)
            self.assertEqual(len(list(Path(output).glob("*.pdf"))), 1)


if __name__ == "__main__":
    unittest.main()
