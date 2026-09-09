import copy
import unittest
import json
from pathlib import Path
import tempfile
import priority_trials as t

def fixture(key='closing_1505'):
    days=['2026-09-10','2026-09-11','2026-09-14','2026-09-15','2026-09-16','2026-09-17',
          '2026-09-18','2026-09-21','2026-09-22','2026-09-23','2026-09-24','2026-09-25']
    h=t.PROFILES[key]['horizon'];exit_day=days[h]
    sel=days[0]+('T15:04:00+09:00' if key=='closing_1505' else 'T18:00:00+09:00')
    entry=days[0]+'T15:05:00+09:00' if key=='closing_1505' else days[1]+'T09:00:00+09:00'
    b=dict(schema='priority-trials-v1',calendar_evidence='SYNTHETIC',as_of=exit_day+'T15:30:00+09:00',
        sessions={d:{'open':d+'T09:00:00+09:00','close':d+'T15:30:00+09:00'} for d in days},
        policies={key:dict(version='fixture-v1',rule_source='SYNTHETIC',frozen_at=days[0]+'T08:00:00+09:00',
                          start_date=days[0],buy_fee=0,sell_fee=0,buy_slippage=0,sell_slippage=0)},
        signals=[dict(id='one',strategy=key,code='000001',signal_date=days[0],selected_at=sel,source='SYNTHETIC',
                      policy_version='fixture-v1',entry={'at':entry,'price':100,'tradable':True,'source':'SYNTHETIC'},
                      exit={'at':exit_day+'T15:30:00+09:00','price':110,'tradable':True,'source':'SYNTHETIC'})],days=[])
    for i,d in enumerate(days):
        at=sel if i==0 else d+('T15:04:00+09:00' if key=='closing_1505' else 'T08:30:00+09:00')
        if t.timestamp(at)<=t.timestamp(b['as_of']) and i<len(days)-1:
            b['days'].append(dict(strategy=key,date=d,recorded_at=at,source='SYNTHETIC',policy_version='fixture-v1',signal_ids=['one'] if i==0 else []))
    return b

class PriorityTests(unittest.TestCase):
    def test_cli_preserves_input_result_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'source.json';p.write_text(json.dumps(fixture()),encoding='utf-8')
            self.assertEqual(t.main(['--input',str(p),'--output-dir',str(Path(tmp)/'out')]),0)
            self.assertEqual(len(list((Path(tmp)/'out').glob('trial-*/COMPLETE.json'))),1)
    def test_1505_outside_special_session_blocked(self):
        b=fixture();b['sessions']['2026-09-10']['close']='2026-09-10T15:00:00+09:00'
        self.assertEqual(self.run_one(b)['state'],'data_blocked')
    def run_one(self,b,key='closing_1505'):return t.evaluate(b)['strategies'][key]
    def test_closing_uses_1505_not_next_open(self):
        r=self.run_one(fixture());self.assertAlmostEqual(r['mean_net_pct'],10);self.assertFalse(r['live_approved'])
    def test_report_short_horizon_and_evening_report(self):
        r=self.run_one(fixture('report_short'),'report_short');self.assertEqual(r['closed_count'],1)
    def test_report_mid_horizon(self):
        r=self.run_one(fixture('report_mid'),'report_mid');self.assertEqual(r['profile']['horizon'],10);self.assertEqual(r['closed_count'],1)
    def test_one_strategy_missing_does_not_block_another(self):
        r=t.evaluate(fixture())['strategies'];self.assertEqual(r['report_mid']['state'],'policy_missing');self.assertEqual(r['closing_1505']['state'],'paper_only')
    def test_policy_change_blocked(self):
        b=fixture();b['signals'][0]['policy_version']='changed';self.assertEqual(self.run_one(b)['blocked_count'],1)
    def test_future_selection_blocked(self):
        b=fixture();b['signals'][0]['selected_at']='2026-09-10T15:06:00+09:00';self.assertEqual(self.run_one(b)['blocked_count'],1)
    def test_wrong_entry_time_blocked(self):
        b=fixture();b['signals'][0]['entry']['at']='2026-09-10T15:02:00+09:00';self.assertEqual(self.run_one(b)['blocked_count'],1)
    def test_nextday_uses_nextday_state(self):
        b=fixture();b['signals'][0]['exit']['tradable']=False;self.assertEqual(self.run_one(b)['closed_count'],0)
    def test_missing_exit_not_zero_profit(self):
        b=fixture();del b['signals'][0]['exit'];r=self.run_one(b);self.assertIsNone(r['mean_net_pct']);self.assertEqual(r['blocked_count'],1)
    def test_missing_day_not_no_signal(self):
        b=fixture();b['days'].pop();r=self.run_one(b);self.assertEqual(r['state'],'data_blocked');self.assertTrue(r['missing_days'])
    def test_unmatured_remains_pending(self):
        b=fixture();b['as_of']='2026-09-10T15:30:00+09:00';b['days']=b['days'][:1];r=self.run_one(b);self.assertEqual(r['pending_count'],1);self.assertEqual(r['closed_count'],0)
    def test_costs_reduce_return(self):
        b=fixture();b['policies']['closing_1505']['sell_fee']=.01;self.assertAlmostEqual(self.run_one(b)['mean_net_pct'],8.9)
    def test_duplicate_stock_rejected(self):
        b=fixture();r=copy.deepcopy(b['signals'][0]);r['id']='two';b['signals'].append(r);self.assertEqual(self.run_one(b)['state'],'data_blocked')
    def test_nonfinite_price_blocked(self):
        b=fixture();b['signals'][0]['entry']['price']=float('inf')
        with self.assertRaises(ValueError):t.evaluate(b)
    def test_no_signal_day_is_counted(self):
        r=self.run_one(fixture());self.assertEqual(r['no_signal_days'],1)
    def test_future_policy_not_backdated(self):
        b=fixture();b['policies']['closing_1505']['frozen_at']='2026-09-11T00:00:00+09:00';self.assertEqual(self.run_one(b)['state'],'data_blocked')

if __name__=='__main__':unittest.main()
