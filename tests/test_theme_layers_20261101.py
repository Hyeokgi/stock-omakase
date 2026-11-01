"""테마 3층 분류(theme-layers-v1, 2026-11-01) — 15:05 자르기 · 종목코드 연결 · 공통 낱말 · 확인/단일/미확인 · 반응 지표를 손으로 만든 자료로 확인."""
import csv
import gzip
import io
import json
import os
import shutil
import tempfile
import unittest

import hyeoks_theme_layers as L

D = "2026-11-02"


def news(key, title, codes, pub, recv=None):
    x = {"key": key, "hts_pbnt_titl_cntt": title, "published_at": f"{D}T{pub}+09:00",
         "received_at": f"{D}T{recv or pub}+09:00"}
    for i, c in enumerate(codes, 1):
        x[f"iscd{i}"] = c
    return x


def stock(code, name, rate, amount, themes, top=""):
    return {"itemcode": code, "itemname": name, "prevChangeRate": str(rate), "tradeAmount": str(amount),
            "themeNos": "|".join(themes), "topThemeNo": top or (themes[0] if themes else "")}


def theme(code, name, rate, up, down, value, top_value="", n=3):
    return {"code": code, "name": name, "changeRate": str(rate), "risingCount": str(up), "fallingCount": str(down),
            "totalTradingValue": str(value), "topByTradingValue": top_value, "membershipStatus": "OK",
            "membershipCount": str(n)}


class Merge(unittest.TestCase):
    def test_news_cut_and_earliest(self):
        since = "2026-10-30T15:30:00+09:00"
        rows = [news("a", "t", ["000010"], "10:00:00", "10:05:00"), news("a", "t", ["000010"], "10:00:00", "10:02:00"),
                news("b", "t", ["000010"], "15:04:00", "15:06:00"),                     # 15:05 뒤에 받음 → 뺀다
                news("c", "t", ["000010"], "15:06:00"),                                  # 15:05 뒤 게시 → 뺀다
                dict(news("d", "t", ["000010"], "09:00:00"), published_at="2026-10-30T15:00:00+09:00")]  # 전날 장중
        got = L.merge_news(rows, D, since)
        self.assertEqual([x["key"] for x in got], ["a"])
        self.assertEqual(got[0]["received_at"], f"{D}T10:02:00+09:00")

    def test_dart_cut(self):
        rows = [{"rcept_no": "1", "first_seen_at": f"{D}T11:00:00+09:00"},
                {"rcept_no": "1", "first_seen_at": f"{D}T09:10:00+09:00"},
                {"rcept_no": "2", "first_seen_at": f"{D}T15:08:00+09:00"}]
        got = L.merge_dart(rows, D)
        self.assertEqual([(x["rcept_no"], x["first_seen_at"][11:16]) for x in got], [("1", "09:10")])


class Tokens(unittest.TestCase):
    def test_strip(self):
        got = L.tokens("[특징주] 가나전자, 원전 수주에 급등…체코원전 기대감 3분기 30%", drop={"가나전자"})
        self.assertEqual(got, {"원전", "수주", "체코원전"})


class Material(unittest.TestCase):
    def setUp(self):
        self.m = [{"code": "000010", "name": "가나", "rate": 20}, {"code": "000020", "name": "다라", "rate": 12},
                  {"code": "000030", "name": "마바", "rate": 8}]

    def test_confirmed(self):
        idx = L.evidence_index([news("n1", "가나 원전 수주", ["000010"], "10:00:00"),
                                news("n2", "다라도 원전 부품 공급", ["000020"], "09:30:00"),
                                news("n3", "코스닥 상승률 상위 원전 바이오 로봇", ["000010", "000020", "000030", "000040", "000050"],
                                     "09:00:00")], [])
        got = L.material("원자력발전", self.m, idx)
        self.assertEqual((got["status"], got["keywords"], got["support"]), ("확인", ["원전"], 2))
        self.assertEqual(got["firstAt"][11:19], "09:30:00")           # 목록 뉴스(09:00)는 집계에서 뺀다
        self.assertEqual(got["supporters"], ["000010", "000020"])
        self.assertEqual(got["moversWithEvidence"], 2)

    def test_dart_counts_and_single(self):
        idx = L.evidence_index([news("n1", "가나 원전 수주", ["000010"], "10:00:00")],
                               [{"rcept_no": "r1", "stock_code": "000030", "report_nm": "단일판매ㆍ공급계약체결",
                                 "first_seen_at": f"{D}T09:20:00+09:00"}])
        got = L.material("원자력발전", self.m, idx)
        self.assertEqual(got["status"], "단일")                       # 근거는 있으나 공통 낱말 없음
        self.assertEqual(got["moversWithEvidence"], 2)

    def test_unknown(self):
        self.assertEqual(L.material("x", self.m, {})["status"], "미확인")


class Day(unittest.TestCase):
    def test_build_day(self):
        snap = [stock("000010", "가나", 20, 900, ["1", "2"], top="2"), stock("000020", "다라", 12, 300, ["1"]),
                stock("000030", "마바", -1, 100, ["1"]), stock("000040", "사아", 6, 50, ["2"])]
        themes = [theme("1", "원자력발전", 9, 2, 1, 1300), theme("2", "반도체", 3, 2, 0, 950, n=2)]
        snap13 = [stock("000010", "가나", 10, 100, ["1"]), stock("000020", "다라", 5, 200, ["1"])]
        themes13 = [theme("1", "원자력발전", 4, 2, 0, 300)]
        prev = [theme("1", "원자력발전", 1, 1, 1, 100, top_value="000020:다라:100")]
        nws = [news("n1", "가나 원전 수주", ["000010"], "10:00:00"), news("n2", "다라 원전 공급", ["000020"], "11:00:00")]
        rows, stocks, st = L.build_day(D, snap, themes, nws, [], snap13, themes13, prev)
        t1 = next(r for r in rows if r["themeNo"] == "1")
        self.assertEqual((t1["members"], t1["movers"], t1["breadth"], t1["top1Share"]), (3, 2, 0.667, 0.692))
        self.assertEqual((t1["leaderValue"], t1["leader1300Same"], t1["prevLeaderSame"]), ("000010", False, False))
        self.assertEqual((t1["m_status"], t1["m_keywords"]), ("확인", ["원전"]))
        s1 = next(s for s in stocks if s["code"] == "000010")
        self.assertEqual((s1["materialThemes"], s1["topTheme"], s1["topThemeIsMaterial"]), ("1", "2", False))
        self.assertEqual((st["themes"], st["confirmed"], st["movers"], st["topThemeIsMaterial"]), (2, 1, 3, 1))   # 다라는 대표 테마 = 재료 테마


class Snapshot(unittest.TestCase):
    def test_read_meta(self):
        d = tempfile.mkdtemp()
        p = os.path.join(d, "s.csv.gz")
        with gzip.open(p, "wt", encoding="utf-8") as fh:
            fh.write('#meta,capturedAt=2026-11-02T15:02:55+09:00,"KOSPI=5,659.34(-4.69)",membershipStatus=OK\n'
                     "code,name\n1,가\n")
        meta, rows = L.read_snapshot(p)
        self.assertEqual((meta["membershipStatus"], meta["KOSPI"], rows[0]["name"]), ("OK", "5,659.34(-4.69)", "가"))


HAS_OPENSSL = shutil.which("openssl") is not None
KEY = "시험용-키-아님"


class Resp:
    def __init__(self, js=None, content=b""):
        self._js, self.content = js, content

    def json(self):
        return self._js

    def raise_for_status(self):
        pass


def write_snap(d):
    def put(name, header, rows, meta="#meta,themeMembershipStatus=OK,membershipStatus=OK"):
        with gzip.open(os.path.join(d, name), "wt", encoding="utf-8", newline="") as fh:
            fh.write(meta + "\n")
            w = csv.DictWriter(fh, fieldnames=header)
            w.writeheader()
            w.writerows(rows)
    sh = ["itemcode", "itemname", "prevChangeRate", "tradeAmount", "themeNos", "topThemeNo"]
    th = ["code", "name", "changeRate", "risingCount", "fallingCount", "totalTradingValue", "topByTradingValue",
          "membershipStatus", "membershipCount"]
    put(f"{D}_1505.csv.gz", sh, [stock("000010", "가나", 20, 900, ["1"]), stock("000020", "다라", 12, 300, ["1"])])
    put(f"{D}_1505_theme.csv.gz", th, [theme("1", "원자력발전", 9, 2, 0, 1200, n=2)])


def write_ledgers(d, news_runs=("11", "12"), dart_cov="완전"):
    with open(os.path.join(d, "kis_news_runs.csv"), "w", encoding="utf-8", newline="") as fh:
        fh.write("day,runId,startedAt,finishedAt,news,privateStatus\n")
        times = {"11": ("09:03", "12:13"), "12": ("12:19", "15:04")}
        for r in news_runs:
            fh.write(f"{D},{r},{D}T{times[r][0]}:00+09:00,{D}T{times[r][1]}:00+09:00,10,접수\n")
    with open(os.path.join(d, "dart_runs.csv"), "w", encoding="utf-8", newline="") as fh:
        fh.write(f"day,runId,disclosures,coverage1505,privateStatus\n{D},21,5,{dart_cov},접수\n")


class Github(unittest.TestCase):
    def setUp(self):
        import datetime
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d, True)
        write_snap(self.d)
        self.now = datetime.datetime(2026, 11, 2, 15, 20, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
        self.rows = {"u11": [news("n1", "가나 원전 수주", ["000010"], "10:00:00")],
                     "u12": [news("n2", "다라 원전 공급", ["000020"], "13:00:00")], "u21": []}
        self.uploaded = []
        self.orig = L.open_artifact
        L.open_artifact = lambda get, url, token, key: self.rows[url]
        self.addCleanup(setattr, L, "open_artifact", self.orig)

    def get(self, arts):
        def g(url, **kw):
            if url.endswith("/kis_news.yml/runs"):
                return Resp({"workflow_runs": [{"id": int(r), "created_at": "2026-11-02T00:03:00Z"} for r in ("11", "12")]
                             + [{"id": 9, "created_at": "2026-11-01T14:59:00Z"}]})     # 11/01 23:59 KST — 전날
            if url.endswith("/dart_disclosures.yml/runs"):
                return Resp({"workflow_runs": [{"id": 21, "created_at": "2026-11-02T03:19:00Z"}]})
            rid = url.split("/runs/")[1].split("/")[0]
            kind = "dart" if rid == "21" else "kisnews"
            return Resp({"artifacts": [{"name": f"material-{kind}-{rid}", "archive_download_url": "u" + rid,
                                        "expired": False}] if rid in arts else []})
        return g

    def run_(self, arts=("11", "12", "21"), now=None, key=KEY):
        up = lambda name, data: self.uploaded.append((name, data)) or "drive-id"   # noqa: E731
        return L.run_github(D, self.get(arts), "o/r", "tok", key, now or self.now, up, "77", self.d, self.d)

    def test_complete(self):
        write_ledgers(self.d)
        st = self.run_()
        self.assertEqual((st["status"], st["reason"], st["confirmed"], st["news1505"]), ("완료", "", 1, 2))
        self.assertEqual((st["newsArtifacts"], st["dartArtifacts"], st["privateStatus"]), (2, 1, "접수"))
        body = json.loads(gzip.decompress(self.uploaded[0][1]))
        self.assertEqual(body["themes"][0]["m_keywords"], ["원전"])
        L.log_run(st, self.d)
        self.assertIsNone(self.run_())                                    # 그날 완료가 있으면 다시 하지 않는다

    def test_partial_when_artifact_or_coverage_missing(self):
        write_ledgers(self.d, dart_cov="불완전")
        st = self.run_(arts=("11", "21"))
        self.assertEqual(st["status"], "부분")
        self.assertIn("원자료 사본 없음(kisnews)", st["reason"])
        self.assertIn("DART 15:05 범위 불완전", st["reason"])
        L.log_run(st, self.d)
        write_ledgers(self.d)                                             # 늦게 끝난 DART 오후 실행이 장부를 채웠다
        self.assertEqual(self.run_()["status"], "완료")                   # '부분' 은 다음 실행이 다시 한다

    def test_guards(self):
        write_ledgers(self.d)
        self.assertIsNone(self.run_(now=self.now.replace(hour=12, minute=13)))   # 아침 수집 끝 — 15:08 전
        self.assertEqual(self.run_(key="")["status"], "실패")
        os.remove(os.path.join(self.d, f"{D}_1505.csv.gz"))
        st = self.run_()
        self.assertEqual((st["status"], st["reason"]), ("실패", f"{D} 15:05 스냅샷 없음"))
        self.assertEqual(self.uploaded, [])
        self.assertEqual(L.kst_day("2026-11-01T15:00:00Z"), "2026-11-02")

    @unittest.skipUnless(HAS_OPENSSL, "openssl 없음")
    def test_seal_roundtrip(self):
        import zipfile
        src, out = os.path.join(self.d, "priv"), os.path.join(self.d, "sealed")
        os.makedirs(src)
        raw = gzip.compress(json.dumps({"rows": [{"key": "n1"}]}).encode("utf-8"))
        with open(os.path.join(src, f"research_kisnews_{D}_11.json.gz"), "wb") as fh:
            fh.write(raw)
        self.assertEqual(L.seal_dir("kisnews", src, os.path.join(self.d, "none"), ""), 0)   # 키 없으면 만들지 않는다
        self.assertFalse(os.path.exists(os.path.join(self.d, "none")))
        self.assertEqual(L.seal_dir("kisnews", src, out, KEY), 1)
        with open(os.path.join(out, f"research_kisnews_{D}_11.json.gz.enc"), "rb") as fh:
            sealed = fh.read()
        self.assertNotIn(b"n1", sealed)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr(f"research_kisnews_{D}_11.json.gz.enc", sealed)
        rows = self.orig(lambda url, **kw: Resp(content=buf.getvalue()), "u", "tok", KEY)
        self.assertEqual(rows, [{"key": "n1"}])


class Wiring(unittest.TestCase):
    """워크플로 텍스트 — 분류 작업이 기다리는 이름과 수집 작업이 남기는 사본 이름이 맞는지."""
    WF = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".github", "workflows")

    def read(self, name):
        with open(os.path.join(self.WF, name), encoding="utf-8") as fh:
            return fh.read()

    def test_names_match(self):
        import re
        tl = self.read("theme_layers.yml")
        for wf, kind in (("kis_news.yml", "kisnews"), ("dart_disclosures.yml", "dart")):
            with self.subTest(wf):
                src = self.read(wf)
                name = re.search(r"^name: (.+)$", src, re.M).group(1).strip()
                self.assertIn(f'"{name}"', tl)                            # workflow_run 이 정확한 이름을 기다린다
                self.assertEqual(L.WORKFLOWS[kind], wf)
                self.assertIn(f"seal --kind {kind} --out sealed", src)
                self.assertIn(f"name: {L.ART_PREFIX}-{kind}-${{{{ github.run_id }}}}", src)
                self.assertIn("PRIVATE_STORE_KEY: ${{ secrets.PRIVATE_STORE_KEY }}", src)
        self.assertIn("actions: read", tl)
        self.assertIn("python hyeoks_theme_layers.py github", tl)


if __name__ == "__main__":
    unittest.main()
