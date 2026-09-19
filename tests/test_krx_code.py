r"""KRX 단축코드 형식 — 계좌 쪽 두 모듈이 **같은 규칙**을 써야 한다.

2026-09-15 회귀: 두 모듈 모두 `\d{6}` / `isdigit()` 을 써서 영문 섞인 실제
단축코드(`0155E0` 등)를 거부했고, 계좌 재구성 첫 실행이 스냅샷을 읽다 죽었다.
같은 결함은 2026-08-28 에 수집기(`hyeoks_market_snapshot._is_code`)에서 이미
고쳐졌다 — 계좌 모듈만 옛 가정을 받았다.

두 정의가 어긋나면(한쪽만 받으면) 입력과 계산이 어긋나므로 여기서 묶어 둔다.
"""
import gzip, csv, glob, re, unittest
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
