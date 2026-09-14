import unittest
from unittest.mock import patch
import naver_sources as n


class SourceTests(unittest.TestCase):
    def row(self, **kw):
        return dict(YYMM="2026.09(E)", MAIN="IFRS연결", SALES="1,234.5", OP="-20", NP="0", **kw)

    def test_exact_search(self):
        p={"items":[{"name":"삼성전자우","code":"005935"},{"name":"삼성전자","code":"005930"}]}
        self.assertEqual(n.exact_code(p,"삼성전자"),"005930")
        self.assertIsNone(n.exact_code(p,"삼성"))

    def test_ambiguous_search(self):
        self.assertIsNone(n.exact_code({"items":[{"name":"X","code":"000001"},{"name":"X","code":"000002"}]},"X"))

    def test_news(self):
        self.assertEqual(n.news_titles({"articles":[{"title":"A &amp; B"},{"title":"A &amp; B"}]}),["A & B"])

    def test_empty_news_failure(self):
        with self.assertRaises(n.SourceError): n.news_titles({"articles":[]})

    def test_quarter_units_and_negative(self):
        self.assertEqual(n.parse_consensus({"JsonData":[self.row()]})["2026.09(E)"],
                         {"매출액":1234.5,"영업이익":-20.0,"당기순이익":0.0})

    def test_actual_excluded(self):
        r=self.row();r["YYMM"]="2026.06(A)"
        self.assertEqual(n.parse_consensus({"JsonData":[r]}),{})

    def test_wrong_basis(self):
        r=self.row();r["MAIN"]="IFRS별도"
        with self.assertRaises(n.SourceError): n.parse_consensus({"JsonData":[r]})

    def test_nonfinite(self):
        r=self.row();r["OP"]="NaN"
        with self.assertRaises(n.SourceError): n.parse_consensus({"JsonData":[r]})

    def test_duplicate_period(self):
        with self.assertRaises(n.SourceError): n.parse_consensus({"JsonData":[self.row(),self.row()]})

    def test_schema_failure(self):
        with self.assertRaises(n.SourceError): n.parse_consensus([])

    def test_preserve_unprocessed(self):
        header=list("1234567")
        old=[header,["A","old"],["B","keep"]]
        new=[header,["A","fresh"]]
        self.assertEqual(n.merge_consensus_rows(old,new),[header,["A","fresh"],["B","keep"]])

    def test_header_failure(self):
        with self.assertRaises(n.SourceError): n.merge_consensus_rows([["bad"]],[list("1234567")])

    def test_query_quarter_and_basis(self):
        with patch.object(n,"read",return_value="sDT: '20260911'"), patch.object(n,"get_json",return_value={"JsonData":[self.row()]}) as fetch:
            n.consensus_estimates("005930")
            self.assertIn("frq=1",fetch.call_args.args[0])
            self.assertIn("finGubun=IFRSL",fetch.call_args.args[0])
            self.assertIn("sDT=20260911",fetch.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
