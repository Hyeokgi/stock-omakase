r"""KRX 단축코드 형식 — 계좌 쪽 두 모듈이 **같은 규칙**을 써야 한다.

2026-09-15 회귀: 두 모듈 모두 `\d{6}` / `isdigit()` 을 써서 영문 섞인 실제
단축코드(`0155E0` 등)를 거부했고, 계좌 재구성 첫 실행이 스냅샷을 읽다 죽었다.
같은 결함은 2026-08-28 에 수집기(`hyeoks_market_snapshot._is_code`)에서 이미
고쳐졌다 — 계좌 모듈만 옛 가정을 받았다.

두 정의가 어긋나면(한쪽만 받으면) 입력과 계산이 어긋나므로 여기서 묶어 둔다.
"""
import gzip, csv, glob, pathlib, re, unittest
import account_source_adapter as A
import hyeoks_account as H

# 2026-09-15 15:05 스냅샷에서 실제로 관측된 영숫자 코드 (전부 tradeStopYn=N)
REAL = ['0155E0', '0220W0', '0126Z0', '0039P0', '0015N0', '0011A0',
        '0197V0', '0007C0', '0015S0', '0220WL', '0001A0', '0120G0']
PLAIN = ['005930', '000660', '035720']
BAD = ['', '000000', '1234567', '가나다라마바', '00 930', '0155e0!',
       'ABCDEFG', '0155E', None, '  ']


class KrxCodeTests(unittest.TestCase):
    def test_real_alphanumeric_codes_accepted(self):
        for c in REAL:
            self.assertEqual(A.normalize_code(c), c, c)
            self.assertEqual(H.normalize_code(c), c, c)

    def test_plain_numeric_codes_accepted(self):
        for c in PLAIN:
            self.assertEqual(A.normalize_code(c), c)
            self.assertEqual(H.normalize_code(c), c)

    def test_sheet_quirks_normalized(self):
        # 시트는 앞자리 0 을 지우거나 아포스트로피를 붙인다
        self.assertEqual(A.normalize_code("'005930"), '005930')
        self.assertEqual(A.normalize_code(5930), '005930')
        self.assertEqual(A.normalize_code('12345'), '012345')   # 숫자만 0 을 채운다
        self.assertEqual(A.normalize_code('0155E'), '')         # 영숫자는 채우지 않는다
        self.assertEqual(A.normalize_code('0155e0'), '0155E0')   # 소문자 표기 허용
        for raw in ("'005930", 5930, '0155e0'):
            self.assertEqual(A.normalize_code(raw), H.normalize_code(raw), raw)

    def test_bad_rejected_by_both(self):
        for c in BAD:
            self.assertEqual(A.normalize_code(c), '', repr(c))
            self.assertEqual(H.normalize_code(c), '', repr(c))

    def test_hangul_not_accepted(self):
        # 파이썬 isalnum() 은 한글을 참으로 본다. 수집기의 규칙보다 좁혀 둔 이유다.
        self.assertTrue('가나다라마바'.isalnum())
        self.assertEqual(A.normalize_code('가나다라마바'), '')

    def test_every_real_snapshot_code_passes(self):
        """실제 스냅샷 전량. 하나라도 거부되면 계좌 재구성이 또 죽는다."""
        files = sorted(glob.glob('data/market_snapshot/*_1505.csv.gz'))
        if not files:
            self.skipTest('스냅샷 없음')
        checked = 0
        for path in files:
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                f.readline()
                for row in csv.DictReader(f):
                    raw = row.get('itemcode')
                    self.assertNotEqual(A.normalize_code(raw), '',
                                        f'{path}: {raw!r} ({row.get("itemname")})')
                    checked += 1
        self.assertGreater(checked, 1000)


if __name__ == '__main__':
    unittest.main()


# ══════════════════════════════════════════════════════════════════════
# 2026-09-20 추가 — 같은 결함이 **세 번째** 장소에서 나왔다.
#
#   생산 로그: `컨센서스 원천 실패 0015N0: Invalid stock code`
#   원인: naver_sources 가 `\d{6}` 이었다. 이 시험이 계좌 두 모듈만 묶어 뒀으므로
#         그 파일은 아무도 보지 않았다. 규칙이 **복사**돼 있던 것이 근본 원인이다.
#   조치: 정본을 `krx_code.py` 하나로 두고, 아래에서 모든 사본을 묶는다.
# ══════════════════════════════════════════════════════════════════════
import krx_code
import naver_sources


CASES_ACCEPT = ["005930", "0155E0", "0220W0", "00680K", "0015N0"]
CASES_REJECT = ["가나다라마바", "\u0660\u0661\u0662\u0663\u0664\u0665", "0059300", "005930A",
                "155E0", "00-930", "005_30", "00593.", "00593$", "", "000000"]


class SharedRuleTests(unittest.TestCase):
    """정본과 모든 사본이 **같은 답**을 내는가."""

    def copies(self):
        return {
            "krx_code": krx_code.normalize,
            "account_source_adapter": A.normalize_code,
            "hyeoks_account": H.normalize_code,
        }

    def test_every_copy_agrees_with_the_canonical_rule(self):
        for raw in CASES_ACCEPT + CASES_REJECT:
            answers = {name: fn(raw) for name, fn in self.copies().items()}
            self.assertEqual(len(set(answers.values())), 1,
                             f"{raw!r} 에서 규칙이 어긋난다: {answers}")

    def test_real_alphanumeric_codes_are_accepted(self):
        for raw in CASES_ACCEPT:
            self.assertTrue(krx_code.is_code(raw), raw)

    def test_arbitrary_alphanumerics_are_not_blanket_accepted(self):
        """넓히는 것과 아무거나 받는 것은 다르다. `str.isalnum()` 은 한글도 참이다."""
        self.assertTrue("가나다라마바".isalnum(), "전제 확인")
        self.assertFalse(krx_code.is_code("가나다라마바"))
        self.assertFalse(krx_code.is_code("\u0660\u0661\u0662\u0663\u0664\u0665"))

    def test_wrong_length_is_rejected(self):
        for raw in ("0059300", "005930A", "0155E00", "155E0", "E0"):
            self.assertFalse(krx_code.is_code(raw), raw)

    def test_short_numeric_is_zero_filled_not_rejected(self):
        """계좌 모듈의 기존 동작. 앞자리 0 이 잘려 들어온 숫자코드다."""
        self.assertEqual(krx_code.normalize("00593"), "000593")
        self.assertEqual(krx_code.normalize("593"), "000593")

    def test_special_characters_are_rejected(self):
        for raw in ("00-930", "005_30", "00593.", "0059 0", "00593$", "0059/0"):
            self.assertFalse(krx_code.is_code(raw), raw)


class NaverSourcesUsesTheSharedRuleTests(unittest.TestCase):
    """세 번째 사본을 없앴는지 — 소스와 동작 양쪽으로 본다."""

    def src(self):
        return pathlib.Path("naver_sources.py").read_text(encoding="utf-8")

    def test_the_numeric_only_pattern_is_gone(self):
        live = "\n".join(l for l in self.src().splitlines()
                         if not l.lstrip().startswith("#"))
        self.assertNotIn(r'\d{6}', live,
                         "숫자 6자리 제한이 남아 있으면 영문 섞인 코드를 또 버린다")

    def test_it_delegates_instead_of_copying(self):
        self.assertIn("krx_code", self.src())

    def test_the_production_failure_no_longer_reproduces(self):
        """`0015N0` 이 형식 단계에서 거부되지 않아야 한다(네트워크는 타지 않는다)."""
        try:
            naver_sources.consensus_estimates("0015N0")
        except naver_sources.SourceError as e:
            self.assertNotIn("Invalid stock code", str(e),
                             "생산이 찾은 그 거부가 그대로 남아 있다")
        except Exception:
            pass          # 네트워크 계열은 이 시험의 관심사가 아니다

    def test_a_non_code_is_still_refused_at_the_format_stage(self):
        with self.assertRaises(naver_sources.SourceError) as cm:
            naver_sources.consensus_estimates("가나다라마바")
        self.assertIn("Invalid stock code", str(cm.exception))

