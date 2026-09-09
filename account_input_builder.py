"""Offline v3 input gate: prepared source JSON -> private quality report + frozen input.

No live Sheets access, no implicit Naver adjustment/tradability assumptions.
The provider must supply the full calendar, prices and verification evidence.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile

import hyeoks_account as account


def prepare(source):
    bundle = copy.deepcopy(source)
    bundle['schema'] = 'account-input-v3'
    quality = dict(status='blocked', source_sha256=account.digest(source),
                   physical_data_rows=max(0,len(bundle.get('rows',[]))-1),
                   input_provider_obligations=[
                       'Calendar completeness must be verified against an independent reference.',
                       'Adjusted OHLC basis and corporate-action handling must be evidenced.',
                       'tradable must come from verified status, not positive price or volume.',
                       'Theme quality is point-in-time; unknown is not verified-none.'])
    try:
        result = account.calculate(bundle)
    except (account.InputError, KeyError, TypeError, ValueError, AttributeError, IndexError) as exc:
        quality['error_type'] = type(exc).__name__
        quality['error_detail_private'] = str(exc)
        return None,quality
    categories = {}
    for item in result['diagnostics']:
        categories[item['status']] = categories.get(item['status'],0)+1
    accounts = result['accounts']
    eligible = sum(len(r['closed_trades'])+len(r['open_positions'])+len(r['rejected']) for r in accounts.values())
    blank = sum(not any(str(c).strip() for c in row) for row in bundle['rows'][1:])
    if eligible+sum(categories.values())+blank != quality['physical_data_rows']:
        quality['error_type']='RowReconciliationError'
        return None,quality
    quality.update(status='ready',input_sha256=account.digest(bundle),theme_policy=result['theme_policy'],
                   eligible_orders=eligible,blank_rows=blank,diagnostic_counts=categories,
                   rejected_orders=sum(len(r['rejected']) for r in accounts.values()),
                   limitations=result['limitations'])
    return bundle,quality


def write_checked(path, value):
    data = account.canonical(value)
    with path.open('xb') as stream:
        stream.write(data);stream.flush();os.fsync(stream.fileno())
    if hashlib.sha256(path.read_bytes()).digest()!=hashlib.sha256(data).digest():
        raise OSError('readback mismatch')


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',required=True,help='private prepared source JSON, not a live URL')
    parser.add_argument('--output-dir',default='data/account_private')
    args=parser.parse_args(argv)
    try:
        source=json.loads(Path(args.source).read_text(encoding='utf-8-sig'))
        bundle,quality=prepare(source)
        root=Path(args.output_dir);root.mkdir(parents=True,exist_ok=True)
        run=Path(tempfile.mkdtemp(prefix='input-',dir=root))
        write_checked(run/'quality.json',quality)
        if bundle is None:
            print('INPUT_BLOCKED: inspect private quality.json; no runnable input emitted')
            return 2
        write_checked(run/'input.json',bundle)
        print('INPUT_READY: '+run.name+'; input_sha256='+quality['input_sha256'])
        return 0
    except (OSError,ValueError,TypeError,AttributeError):
        print('INPUT_FAILED: inspect private source and storage locally')
        return 2


if __name__=='__main__':raise SystemExit(main())
