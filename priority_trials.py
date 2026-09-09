"""Independent forward PAPER trade evaluation for report short/mid and 15:05 entry.

No network, order placement, account NAV, significance test, or automatic adoption.
Frozen decisions + point-in-time paper fills are supplied by a private recorder.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import statistics
import tempfile

PROFILES = {
    'report_short': {'channel':'리포트TOP2_단기','horizon':5,'entry':'next_open'},
    'report_mid': {'channel':'리포트TOP2_중기','horizon':10,'entry':'next_open'},
    'closing_1505': {'channel':'테마대장_종베','horizon':1,'entry':'signal_1505'},
}
KST=dt.timezone(dt.timedelta(hours=9))


def timestamp(value):
    t=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if t.tzinfo is None: raise ValueError('timezone required')
    return t.astimezone(KST)


def sha(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def required_text(value):
    if not isinstance(value,str) or not value.strip(): raise ValueError('evidence reference required')
    return value


def rate(value):
    if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value) or not 0<=value<1:
        raise ValueError('explicit numeric fee/slippage in [0,1) required')
    return value


def quote(q, target):
    # Exact target timestamp; any permitted delay requires a new explicit policy.
    if not isinstance(q,dict): raise ValueError('missing point-in-time fill observation')
    required_text(q.get('source'))
    if timestamp(q['at']) != target: raise ValueError('fill timestamp does not match policy')
    if q.get('tradable') is not True: raise ValueError('fill availability unknown or unavailable')
    p=q.get('price')
    if isinstance(p,bool) or not isinstance(p,(int,float)) or not math.isfinite(p) or p<=0:
        raise ValueError('positive finite fill price required')
    return p


def evaluate_strategy(bundle, key, calendar, asof):
    spec=PROFILES[key]
    policy=bundle.get('policies',{}).get(key)
    if not policy: return dict(state='policy_missing',live_approved=False)
    version=required_text(policy.get('version'));required_text(policy.get('rule_source'))
    frozen=timestamp(policy['frozen_at'])
    fees={k:rate(policy[k]) for k in ('buy_fee','sell_fee','buy_slippage','sell_slippage')}
    dates=list(calendar)
    start=policy['start_date']
    if start not in calendar: raise ValueError('policy start outside calendar')
    records=[r for r in bundle.get('signals',[]) if r.get('strategy')==key]
    ids=[required_text(r.get('id')) for r in records]
    if len(ids)!=len(set(ids)): raise ValueError('duplicate signal ID within strategy')
    keys=[(r['signal_date'],r['code']) for r in records]
    if len(keys)!=len(set(keys)): raise ValueError('duplicate stock within strategy/day')
    logs={}
    for log in bundle.get('days',[]):
        if log.get('strategy')!=key: continue
        day=log['date']
        if day in logs or day not in calendar or day<start: raise ValueError('invalid/duplicate day log')
        required_text(log.get('source'))
        if log.get('policy_version')!=version: raise ValueError('mixed day policy')
        ids_for_day=log['signal_ids']
        if len(ids_for_day)!=len(set(ids_for_day)): raise ValueError('duplicate day signal IDs')
        if sorted(ids_for_day)!=sorted(r['id'] for r in records if r['signal_date']==day):
            raise ValueError('day log and signal records disagree')
        logged=timestamp(log['recorded_at'])
        i=dates.index(day)
        if key=='closing_1505':
            decision_deadline=timestamp(day+'T15:05:00+09:00')
            if not timestamp(calendar[day]['open'])<=decision_deadline<=timestamp(calendar[day]['close']):
                raise ValueError('15:05 outside supplied session')
        else:
            if i+1>=len(dates): raise ValueError('next session calendar required for decision deadline')
            decision_deadline=timestamp(calendar[dates[i+1]]['open'])
        if not frozen<=logged<=min(decision_deadline,asof): raise ValueError('day record not prospective')
        logs[day]=log
    trades=[]
    for r in records:
        day=r['signal_date'];out={'id':r['id'],'signal_date':day}
        try:
            required_text(r.get('source'))
            if day not in logs or day<start: raise ValueError('missing prospective day record')
            if r.get('policy_version')!=version: raise ValueError('mixed signal policy version')
            code=required_text(r.get('code'))
            if len(code)!=6 or not code.isdigit() or code=='000000': raise ValueError('invalid stock code')
            decision=timestamp(r['selected_at'])
            if decision.date().isoformat()!=day: raise ValueError('signal day differs from decision day')
            if not frozen<=decision<=timestamp(logs[day]['recorded_at']): raise ValueError('decision timing invalid')
            i=dates.index(day)
            if spec['entry']=='next_open':
                if i+1>=len(dates):
                    out['state']='pending_entry_calendar';trades.append(out);continue
                entry_at=timestamp(calendar[dates[i+1]]['open'])
            else: entry_at=timestamp(day+'T15:05:00+09:00')
            if decision>entry_at: raise ValueError('selection after intended entry')
            if entry_at>asof:
                out['state']='pending_entry';trades.append(out);continue
            buy=quote(r.get('entry'),entry_at)
            j=i+spec['horizon']
            if j>=len(dates):
                out['state']='open_exit_calendar_pending';trades.append(out);continue
            exit_at=timestamp(calendar[dates[j]]['close'])
            if exit_at>asof:
                out['state']='open_not_matured';trades.append(out);continue
            sell=quote(r.get('exit'),exit_at)
            cost=buy*(1+fees['buy_slippage'])*(1+fees['buy_fee'])
            proceeds=sell*(1-fees['sell_slippage'])*(1-fees['sell_fee'])
            out.update(state='closed',net_return_pct=100*(proceeds/cost-1))
        except (ValueError,KeyError,TypeError,AttributeError) as exc:
            out.update(state='blocked',reason=str(exc))
        trades.append(out)
    expected=[d for i,d in enumerate(dates) if d>=start and (
        (key=='closing_1505' and timestamp(d+'T15:05:00+09:00')<=asof) or
        (key!='closing_1505' and i+1<len(dates) and timestamp(calendar[dates[i+1]]['open'])<=asof))]
    missing=[d for d in expected if d not in logs]
    closed=[r['net_return_pct'] for r in trades if r['state']=='closed']
    blocked=sum(t['state']=='blocked' for t in trades)
    return dict(state='data_blocked' if blocked or missing else 'paper_only',live_approved=False,
                policy_version=version,policy_sha256=sha(policy),profile=spec,days_recorded=len(logs),
                missing_days=missing,no_signal_days=sum(not l['signal_ids'] for l in logs.values()),
                total_signals=len(records),closed_count=len(closed),blocked_count=blocked,
                pending_count=sum(t['state'] not in ('closed','blocked') for t in trades),
                mean_net_pct=statistics.mean(closed) if closed else None,
                median_net_pct=statistics.median(closed) if closed else None,
                win_rate_pct=100*sum(x>0 for x in closed)/len(closed) if closed else None,
                metrics_scope='available_closed_trades_only; not account return or selection effect',trades=trades)


def evaluate(bundle):
    if bundle.get('schema')!='priority-trials-v1': raise ValueError('wrong schema')
    required_text(bundle.get('calendar_evidence'))
    asof=timestamp(bundle['as_of'])
    calendar=bundle['sessions']
    if not calendar or list(calendar)!=sorted(calendar): raise ValueError('sorted calendar required')
    for day,item in calendar.items():
        if dt.date.fromisoformat(day).isoformat()!=day: raise ValueError('invalid session day')
        opened,closed=timestamp(item['open']),timestamp(item['close'])
        if opened.date().isoformat()!=day or closed.date().isoformat()!=day or opened>=closed:
            raise ValueError('session boundaries invalid')
    unknown=set(bundle.get('policies',{}))-set(PROFILES)
    unknown|={r.get('strategy') for r in bundle.get('signals',[])+bundle.get('days',[])}-set(PROFILES)
    if unknown: raise ValueError('unknown strategy')
    results={}
    for key in PROFILES:
        try: results[key]=evaluate_strategy(bundle,key,calendar,asof)
        except (ValueError,KeyError,TypeError,AttributeError) as exc:
            results[key]=dict(state='data_blocked',live_approved=False,reason=str(exc))
    return dict(schema='priority-trials-result-v1',input_sha256=sha(bundle),as_of=bundle['as_of'],strategies=results,
                warning='Paper trade diagnostics only. No NAV/MDD, significance, matched control, or live approval.')


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',required=True)
    p.add_argument('--output-dir',default='data/priority_private')
    args=p.parse_args(argv)
    try:
        b=json.loads(Path(args.input).read_text(encoding='utf-8-sig'));result=evaluate(b)
        root=Path(args.output_dir);root.mkdir(parents=True,exist_ok=True)
        run=Path(tempfile.mkdtemp(prefix='trial-',dir=root))
        from account_input_builder import write_checked
        write_checked(run/'input.json',b);write_checked(run/'result.json',result)
        # Freeze this calculator as well; hashes/readback for source use binary bytes.
        content=Path(__file__).read_bytes()
        with (run/'priority_trials.py').open('xb') as f:f.write(content)
        if (run/'priority_trials.py').read_bytes()!=content:raise OSError('source readback failed')
        write_checked(run/'COMPLETE.json',{'input_sha256':sha(b),'result_sha256':sha(result),
                     'source_sha256':hashlib.sha256(content).hexdigest()})
    except (OSError,ValueError,KeyError,TypeError,AttributeError):
        print('TRIAL_FAILED: inspect private inputs locally');return 2
    states={k:r['state'] for k,r in result['strategies'].items()}
    print(json.dumps(states,ensure_ascii=False))
    return 2 if any(x=='data_blocked' for x in states.values()) else 0


if __name__=='__main__':raise SystemExit(main())
