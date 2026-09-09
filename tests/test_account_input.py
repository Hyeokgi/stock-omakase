import copy
import json
from pathlib import Path
import tempfile
import unittest

import hyeoks_account as a
import account_input_builder as builder
from test_account_nav import fixture


def source(policy='none'):
    b=fixture();b['schema']='account-input-v3';b['theme_policy']=policy
    for key in ('calendar_verification','price_verification'):
        b[key]={'status':'verified','evidence':'SYNTHETIC ONLY: fixture, not real market data'}
    b['calendar_verification']['sessions']=list(b['sessions'])
    b['theme_quality']={b['sessions'][0]:{'000001':{'status':'representative','source':'synthetic ledger'}}}
    return b


class InputTests(unittest.TestCase):
    def test_missing_middle_calendar_session_rejected(self):
        b=source();del b['sessions'][2]
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_policy_changes_orders_not_just_label(self):
        b=source('representative');b['config']['max_theme_weight']=.2
        self.assertEqual(a.calculate(b)['accounts']['strategy']['rejected'][0]['reason'],'theme_cap')
        b['theme_policy']='none'
        self.assertEqual(len(a.calculate(b)['accounts']['strategy']['closed_trades']),1)

    def test_representative_requires_source(self):
        b=source('representative');b['theme_quality'][b['sessions'][0]]['000001']['source']=''
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_none_does_not_require_or_fabricate_themes(self):
        b=source();del b['theme_map'];r=a.calculate(b)
        self.assertIsNone(r['accounts']['strategy']['max_theme_weight'])
        self.assertEqual(r['accounts']['strategy']['closed_trades'][0]['themes'],[])

    def test_representative_is_explicitly_reported(self):
        r=a.calculate(source('representative'))
        self.assertIn('representative',a.report(r))

    def test_complete_rejects_representative(self):
        with self.assertRaises(a.InputError):a.calculate(source('complete'))

    def test_complete_accepts_evidenced_complete(self):
        b=source('complete');b['theme_quality'][b['sessions'][0]]['000001']['status']='complete'
        self.assertEqual(a.calculate(b)['theme_policy'],'complete')

    def test_unknown_not_treated_as_no_theme(self):
        b=source('complete');b['theme_quality'][b['sessions'][0]]['000001']['status']='unknown'
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_partial_rejected_for_complete(self):
        b=source('complete');b['theme_quality'][b['sessions'][0]]['000001']['status']='partial'
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_verified_none_requires_distinct_stock_id(self):
        b=source('complete');b['theme_quality'][b['sessions'][0]]['000001']['status']='verified_none'
        with self.assertRaises(a.InputError):a.calculate(b)
        b['theme_map'][b['sessions'][0]]['000001']=['NO_THEME:000001']
        a.calculate(b)

    def test_v3_requires_explicit_policy(self):
        b=source();del b['theme_policy']
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_v3_unverified_price_rejected(self):
        b=source();b['price_verification']['status']='unverified'
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_v3_missing_calendar_evidence_rejected(self):
        b=source();del b['calendar_verification']['evidence']
        with self.assertRaises(a.InputError):a.calculate(b)

    def test_builder_preserves_source_and_reconciles(self):
        b=source();old=copy.deepcopy(b);built,q=builder.prepare(b)
        self.assertEqual(b,old);self.assertEqual(q['status'],'ready')
        self.assertEqual(q['physical_data_rows'],q['eligible_orders'])
        self.assertEqual(q['input_sha256'],a.digest(built))

    def test_builder_does_not_invent_tradability(self):
        b=source();del b['prices']['000001'][b['sessions'][1]]['tradable']
        built,q=builder.prepare(b)
        self.assertIsNone(built);self.assertEqual(q['status'],'blocked')

    def test_builder_rejects_missing_held_price(self):
        b=source();del b['prices']['000001'][b['sessions'][2]]
        self.assertIsNone(builder.prepare(b)[0])

    def test_blocked_cli_emits_only_quality(self):
        with tempfile.TemporaryDirectory() as tmp:
            b=source();b['price_verification']['status']='unverified'
            p=Path(tmp)/'source.json';p.write_text(json.dumps(b))
            self.assertEqual(builder.main(['--source',str(p),'--output-dir',tmp]),2)
            self.assertEqual(len(list(Path(tmp).glob('input-*/quality.json'))),1)
            self.assertEqual(len(list(Path(tmp).glob('input-*/input.json'))),0)

    def test_ready_cli_emits_frozen_input(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'source.json';p.write_text(json.dumps(source()))
            self.assertEqual(builder.main(['--source',str(p),'--output-dir',tmp]),0)
            self.assertEqual(len(list(Path(tmp).glob('input-*/input.json'))),1)


if __name__=='__main__':unittest.main()
