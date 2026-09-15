import gzip
from pathlib import Path
import tempfile
import unittest
import account_source_adapter as a

D=['2026-09-01','2026-09-02','2026-09-03']
def rows():
    r=['']*38;r[1]=D[0];r[2]='차트TOP2';r[4]='000001'
    return [['']*38,r]

class SourceTests(unittest.TestCase):
    def test_next_open_excludes_signal(self):
        self.assertEqual(a.required_cells(rows(),D,lambda _:1)[0],{('000001',D[1])})
    def test_closing_1505_preserves_signal_day(self):
        self.assertEqual(a.required_cells(rows(),D,lambda _:20,entry_model='closing_1505')[0],{('000001',D[0]),('000001',D[1])})
    def test_excluded_row(self):
        r=rows();r[1][25]='제외';self.assertEqual(a.required_cells(r,D,lambda _:1)[0],set())
    def test_index_row(self):
        r=rows();r[1][2]='지수벤치';self.assertEqual(a.required_cells(r,D,lambda _:1)[0],set())
    def test_asof_caps_range(self):
        self.assertEqual(a.required_cells(rows(),D,lambda _:20,as_of=D[1])[0],{('000001',D[1])})
    def test_bad_row_is_reported_not_raised(self):
        # 2026-09-15 회귀: 잘못된 행 하나에 raise 로 멈춰 어느 행인지 알 수 없었다
        r=rows();bad=['']*38;bad[1]=D[0];bad[2]='차트TOP2';bad[4]='1234567';r.append(bad)
        need,_u,inv=a.required_cells(r,D,lambda _:1)
        self.assertEqual(need,{('000001',D[1])})
        self.assertEqual([b['행'] for b in inv],[3])
    def test_bad_row_code_hidden_in_committed_report(self):
        g=dict(required_cells=0,invalid_rows=1,tradable_known=0,tradable_missing=0,
               price_missing=None,snapshot_dates_in_window=0,first_snapshot=None,
               entry_not_in_calendar=0,sample_tradable_missing=[],
               sample_invalid=[{'행':3,'사유':'종목코드가 6자리 영숫자(KRX 단축코드) 형식이 아님','코드':'1234567','채널':'차트TOP2'}])
        self.assertNotIn('1234567',a.gap_report(g,D[-1]))
        self.assertIn('1234567',a.gap_report(g,D[-1],include_samples=True))
    def test_invalid_date_and_infinity(self):
        for data in ('20261340|1|2|1|2','20260901|inf|inf|inf|inf'):
            self.assertEqual(a.parse_fchart(f'<r><item data="{data}"/></r>'),([],1))
    def test_duplicate_bars_rejected(self):
        _,bad=a.parse_fchart('<r><item data="20260901|1|2|1|2"/><item data="20260901|1|2|1|2"/></r>')
        self.assertEqual(bad,1)
    def snap(self,tmp,stamp='2026-09-02T15:02:00+09:00',slot='1505',body='000001,N\n'):
        with gzip.open(Path(tmp)/(D[1]+'_1505.csv.gz'),'wt',encoding='utf-8') as f:
            f.write(f'#meta,capturedAt={stamp},slot={slot}\nitemcode,tradeStopYn\n'+body)
    def test_snapshot_keeps_actual_capture_not_exact_1505(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.snap(tmp);m=a.tradable_map(D[1],tmp)['000001']
            self.assertTrue(m['status_at_snapshot']);self.assertIn('15:02',m['captured_at'])
            self.assertEqual(m['captured_at_semantics'],'collector_start_not_quote_time')
    def test_snapshot_bad_metadata(self):
        for stamp,slot in [('2026-09-01T15:05:00+09:00','1505'),('2026-09-02T13:00:00+09:00','1505'),('2026-09-02T15:05:00','1505'),('2026-09-02T15:05:00+09:00','1300')]:
            with tempfile.TemporaryDirectory() as tmp:
                self.snap(tmp,stamp,slot)
                with self.assertRaises(ValueError):a.tradable_map(D[1],tmp)
    def test_duplicate_snapshot_code(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.snap(tmp,body='000001,N\n000001,Y\n')
            with self.assertRaises(ValueError):a.tradable_map(D[1],tmp)
    def test_unknown_snapshot_flag_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.snap(tmp,body='000001,\n');self.assertEqual(a.tradable_map(D[1],tmp),{})
    def test_prices_retained_without_daily_tradability(self):
        with tempfile.TemporaryDirectory() as tmp:
            p={'000001':{D[1]:dict(open=1,high=2,low=1,close=2)}}
            s,g=a.build_source(rows(),D,p,D[-1],lambda _:1,root=tmp)
            self.assertEqual(s['prices']['000001'][D[1]]['close'],2)
            self.assertNotIn('tradable',s['prices']['000001'][D[1]])
            self.assertEqual(g['tradable_missing'],1)
            g['price_missing']=None
            self.assertIn('미검사',a.gap_report(g,D[-1]))

if __name__=='__main__':unittest.main()
