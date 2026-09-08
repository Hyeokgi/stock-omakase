"""Cash NAV reconstruction from frozen, synchronous prices (no network).

Ledger 진입일 is SIGNAL day; buy next session open, sell signal+H close.
Sparse ledger returns are NOT account prices. Defaults are research assumptions.
"""
from __future__ import annotations
import argparse
import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import uuid
from hyeoks_verdict import ABORT, HORIZON, DEFAULT_HORIZON, validate_ledger, is_excluded

VERSION = 'account-nav-v2'


class InputError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def number(value, label, positive=False):
    try:
        n = float(str(value).replace(',', '').replace('%', '').strip())
    except (TypeError, ValueError):
        raise InputError(f'{label}: invalid number') from None
    if not math.isfinite(n) or (positive and n <= 0):
        raise InputError(f'{label}: invalid finite/positive value')
    return n


def date(value):
    try:
        if not isinstance(value, str) or dt.date.fromisoformat(value).isoformat() != value:
            raise ValueError
    except (TypeError, ValueError):
        raise InputError(f'invalid ISO date: {value!r}') from None
    return value


@dataclasses.dataclass(frozen=True)
class Config:
    initial_cash: float = 1_000_000
    ticket_cash: float = 50_000
    max_stock_weight: float = .10
    max_theme_weight: float = .20
    buy_fee: float = .00175
    sell_fee: float = .00175
    slippage: float = 0.0

    def validate(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if isinstance(value, bool) or not isinstance(value, (float, int)):
                raise InputError(f'{field.name}: require numeric value')
            number(value, field.name)
        if not 0 < self.ticket_cash <= self.initial_cash:
            raise InputError('require 0 < ticket_cash <= initial_cash')
        if not 0 < self.max_stock_weight <= 1 or not 0 < self.max_theme_weight <= 1:
            raise InputError('weight caps must be in (0, 1]')
        if any(not 0 <= x < 1 for x in (self.buy_fee, self.sell_fee, self.slippage)):
            raise InputError('fees/slippage must be in [0, 1)')


def group(channel):
    if channel.startswith('지수벤치'):
        return None
    if channel == '랜덤2':
        return 'random'
    if channel == '랜덤2_배지':
        return 'random_badge'
    if channel.startswith('랜덤'):
        raise InputError(f'unknown control channel: {channel}')
    return 'strategy'


def validate_bundle(bundle):
    if bundle.get('schema') != 'account-input-v2':
        raise InputError('require account-input-v2 frozen input, not sparse marks')
    for key in ('calendar_source', 'price_source', 'captured_at', 'price_basis'):
        if not isinstance(bundle.get(key), str) or not bundle[key].strip():
            raise InputError(f'missing provenance: {key}')
    if bundle['price_basis'] != 'consistent_adjusted_ohlc':
        raise InputError('require consistent_adjusted_ohlc throughout frozen history')
    as_of, sessions = date(bundle['as_of']), bundle['sessions']
    if not sessions or sessions != sorted(set(sessions)):
        raise InputError('calendar sessions must be sorted, unique and nonempty')
    for d in sessions:
        date(d)
    if as_of not in sessions:
        raise InputError('as_of must be a completed supplied trading session')
    try:
        captured = dt.datetime.fromisoformat(bundle['captured_at'].replace('Z', '+00:00'))
        if captured.tzinfo is None or captured.astimezone(dt.timezone(dt.timedelta(hours=9))).date() < dt.date.fromisoformat(as_of):
            raise ValueError
    except ValueError:
        raise InputError('captured_at must have timezone and not precede as_of') from None
    if not bundle.get('prices') or not bundle.get('theme_map'):
        raise InputError('prices and point-in-time theme_map required')
    rows = bundle['rows']
    issues = validate_ledger(rows, today=dt.date.fromisoformat(as_of))
    errors = [f'{name}: {detail}' for severity, name, detail in issues if severity == ABORT]
    for col, name in {3:'종목명', 4:'종목코드', 5:'주도테마', 16:'진입가(T+1시가)'}.items():
        if not rows or len(rows[0]) <= col or rows[0][col] != name:
            errors.append(f'account column {col} must be {name}')
    if errors:
        raise InputError('; '.join(errors))
    cfg = Config(**bundle.get('config', {}))
    cfg.validate()
    return sessions, as_of, cfg, issues


def orders_from_ledger(bundle, sessions, as_of):
    orders, diagnostics = [], []
    indices = {d:i for i,d in enumerate(sessions)}
    for row_no, row in enumerate(bundle['rows'][1:], 2):
        if not any(str(c).strip() for c in row):
            continue
        if len(row) < 17:
            raise InputError(f'row {row_no}: truncated record')
        if is_excluded(row):
            diagnostics.append(dict(row=row_no, status='explicit_exclusion'))
            continue
        signal, channel = date(str(row[1]).strip()), str(row[2]).strip()
        if not channel or signal not in indices or signal > as_of:
            raise InputError(f'row {row_no}: invalid signal date/channel')
        g = group(channel)
        if g is None:
            diagnostics.append(dict(row=row_no, status='index_ledger_not_pooled'))
            continue
        idx = indices[signal]
        if idx+1 >= len(sessions) or sessions[idx+1] > as_of:
            diagnostics.append(dict(row=row_no, status='pending_entry'))
            continue
        code = str(row[4]).strip().lstrip("'").zfill(6)
        if len(code) != 6 or not code.isdigit() or code == '000000':
            raise InputError(f'row {row_no}: invalid code')
        themes = bundle['theme_map'].get(signal, {}).get(code)
        if not isinstance(themes, list) or not themes or any(not isinstance(t,str) or not t for t in themes):
            raise InputError(f'row {row_no}: missing point-in-time theme IDs')
        horizon = HORIZON.get(channel, DEFAULT_HORIZON)
        exit_date = sessions[idx+horizon] if idx+horizon < len(sessions) else None
        orders.append(dict(id=str(row[0]), channel=channel, group=g, code=code,
                           signal=signal, entry=sessions[idx+1], exit=exit_date,
                           horizon=horizon, themes=sorted(set(themes))))
    return sorted(orders, key=lambda x:(x['entry'], x['channel'], x['id'])), diagnostics


def bar(prices, code, day):
    raw = prices.get(code, {}).get(day)
    if raw is None:
        raise InputError(f'missing synchronized price: {code} {day}')
    out = {k:number(raw.get(k), f'{code}/{day}/{k}', True) for k in ('open','high','low','close')}
    if not out['low'] <= min(out['open'],out['close']) <= max(out['open'],out['close']) <= out['high']:
        raise InputError(f'inconsistent OHLC: {code} {day}')
    if not isinstance(raw.get('tradable'),bool):
        raise InputError(f'explicit tradable boolean required: {code} {day}')
    out['tradable'] = raw['tradable']
    return out


def nav_metrics(series, initial_cash):
    peak, mdd, trough = initial_cash, 0.0, None
    for point in series:
        peak = max(peak,point['nav'])
        dd = 1-point['nav']/peak
        if dd > mdd:
            mdd,trough = dd,point['date']
    final = series[-1]['nav'] if series else initial_cash
    return dict(return_pct=100*(final/initial_cash-1), mdd_pct=100*mdd,
                mdd_date=trough, final_nav=final)


def simulate(orders, sessions, prices, cfg):
    """Open buys BEFORE close sells, whole shares, no leverage, no stale marks.

    Halted planned exits remain open. Invalid valuation aborts, never zero-fills.
    Caps restrict new buys; subsequent market drift does not force liquidation.
    """
    cfg.validate()
    cash,realized,fees = cfg.initial_cash,0.0,0.0
    active,trades,rejected,series = [],[],[],[]
    peak_positions = peak_codes = 0
    peak_stock_weight = peak_theme_weight = 0.0
    for day in sessions:
        opening = sorted([o for o in orders if o['entry']==day],key=lambda o:(o['channel'],o['id']))
        bars = {c:bar(prices,c,day) for c in {p['code'] for p in active}|{o['code'] for o in opening}}
        def exposures(field):
            codes,themes = {},{}
            for p in active:
                value = p['qty']*bars[p['code']][field]
                codes[p['code']] = codes.get(p['code'],0)+value
                for t in p['themes']:
                    themes[t] = themes.get(t,0)+value
            return codes,themes
        for order in opening:
            b = bars[order['code']]
            if not b['tradable']:
                rejected.append(dict(id=order['id'],date=day,reason='not_tradable'))
                continue
            codes,themes = exposures('open')
            fill = b['open']*(1+cfg.slippage)
            qty = math.floor(cfg.ticket_cash/(fill*(1+cfg.buy_fee)))
            debit = qty*fill*(1+cfg.buy_fee)
            value = qty*b['open']
            nav_after = cash+sum(codes.values())-debit+value
            reason = None
            if qty<=0: reason='below_one_share'
            elif debit>cash+1e-8: reason='insufficient_cash'
            elif codes.get(order['code'],0)+value>cfg.max_stock_weight*nav_after+1e-8: reason='stock_cap'
            elif any(themes.get(t,0)+value>cfg.max_theme_weight*nav_after+1e-8 for t in order['themes']): reason='theme_cap'
            if reason:
                rejected.append(dict(id=order['id'],date=day,reason=reason))
                continue
            cash -= debit
            fees += qty*fill*cfg.buy_fee
            active.append(dict(order,qty=qty,entry_fill=fill,cost_basis=debit))
        peak_positions = max(peak_positions,len(active))
        peak_codes = max(peak_codes,len({p['code'] for p in active}))
        for field in ('open','close'):
            codes,themes = exposures(field)
            nav = cash+sum(codes.values())
            if nav<=0: raise InputError('nonpositive NAV')
            peak_stock_weight = max(peak_stock_weight,max(codes.values(),default=0)/nav)
            peak_theme_weight = max(peak_theme_weight,max(themes.values(),default=0)/nav)
        remaining = []
        for p in active:
            b = bars[p['code']]
            if p['exit'] is not None and day>=p['exit'] and b['tradable']:
                fill = b['close']*(1-cfg.slippage)
                proceeds = p['qty']*fill*(1-cfg.sell_fee)
                profit = proceeds-p['cost_basis']
                cash += proceeds
                fees += p['qty']*fill*cfg.sell_fee
                realized += profit
                trades.append(dict(p,exit_actual=day,exit_fill=fill,pnl=profit,return_pct=100*profit/p['cost_basis']))
            else:
                remaining.append(p)
        active = remaining
        value = sum(p['qty']*bars[p['code']]['close'] for p in active)
        unrealized = sum(p['qty']*bars[p['code']]['close']-p['cost_basis'] for p in active)
        nav = cash+value
        if cash < -1e-7 or not math.isclose(nav-cfg.initial_cash,realized+unrealized,abs_tol=1e-6):
            raise InputError('cash/NAV/PnL reconciliation failed')
        series.append(dict(date=day,cash=cash,market_value=value,nav=nav,
                           realized=realized,unrealized=unrealized,open_positions=len(active)))
    result = nav_metrics(series,cfg.initial_cash)
    result.update(series=series,closed_trades=trades,open_positions=active,rejected=rejected,
                  fees=fees,max_concurrent=peak_positions,max_unique_codes=peak_codes,
                  max_stock_weight=peak_stock_weight,max_theme_weight=peak_theme_weight,
                  closed_win_rate_pct=(100*sum(t['pnl']>0 for t in trades)/len(trades) if trades else None))
    return result


def calculate(bundle):
    sessions,as_of,cfg,issues = validate_bundle(bundle)
    orders,diagnostics = orders_from_ledger(bundle,sessions,as_of)
    dates = [d for d in sessions if d<=as_of]
    accounts = {g:simulate([o for o in orders if o['group']==g],dates,bundle['prices'],cfg)
                for g in ('strategy','random','random_badge')}
    benchmarks = {}
    for name,prices in sorted(bundle.get('indices',{}).items()):
        order = dict(id='benchmark',channel=name,code=name,entry=dates[0],exit=None,
                     signal=dates[0],themes=[name],horizon=None)
        benchmarks[name] = simulate([order],dates,{name:prices},Config(cfg.initial_cash,cfg.initial_cash,1,1,0,0,0))
    return dict(version=VERSION,as_of=as_of,config=dataclasses.asdict(cfg),input_sha256=digest(bundle),
                diagnostics=diagnostics,ledger_warnings=issues,accounts=accounts,benchmarks=benchmarks)


def report(result):
    lines = [f"# 계좌 NAV 재구성 — {result['as_of']}",'',
             '고정 입력에 대한 연구용 가상 집행입니다. 실거래 계좌 실적이 아닙니다.',
             '종목 가격·수량과 현금으로 평가하며 지수 수익률을 차감하지 않습니다.',
             '신호 다음 거래일 시가 매수, 신호+H 거래일 종가 청산. 당일 시가 매수는 종가 매도보다 먼저입니다.',
             '동기화한 일별 종가 MDD이며 장중 MDD·실체결은 미검증입니다. 비동기 마크 보간은 없습니다.',
             '동일 수정주가 기준 가상 수량 모형입니다. 실제 배당/권리처리 현금흐름 계좌와 다를 수 있습니다.',
             '비용·슬리피지는 고정 가정이며 실측값이 아닙니다. 상한은 신규 매수에 적용하며 이후 비중 변동으로 강제매도하지 않습니다.','',
             f"입력 SHA256: `{result['input_sha256']}`",'',
             '| 계좌 | 수익률 | 종가 MDD | 확정 거래 승률 | 종료/열린 포지션 | 거절 |',
             '|---|---:|---:|---:|---:|---:|']
    for name,r in {**result['accounts'],**{'index:'+k:v for k,v in result['benchmarks'].items()}}.items():
        win = '미정' if r['closed_win_rate_pct'] is None else f"{r['closed_win_rate_pct']:.2f}%"
        lines.append(f"| {name} | {r['return_pct']:+.2f}% | {r['mdd_pct']:.2f}% | {win} | {len(r['closed_trades'])}/{len(r['open_positions'])} | {len(r['rejected'])} |")
    lines += ['','지수는 무비용 가상 whole-unit buy-and-hold이며 랜덤 계좌와 합치지 않습니다.',
              '동일 자금 설정이어도 채널별 신호 날짜·보유기간 차이가 있어 이 표만으로 선정 능력의 인과효과를 확정하지 않습니다.',
              '## 고정 설정','','```json',json.dumps(result['config'],ensure_ascii=False,indent=2),'```','',
              f"원장 경고 {len(result['ledger_warnings'])}건, 제외/대기 기록 {len(result['diagnostics'])}건.",
              '세부 NAV·거절·열린 포지션·입력·코드는 같은 실행 묶음에 보존합니다. 통계적 우위·채택 판정은 하지 않습니다.','']
    return '\n'.join(lines)


def save_bundle(bundle,result,output):
    """Exclusive run directory, readback hashes, atomic visible completion.

    Local_verified does not imply durable/private cloud storage.
    """
    output = Path(output); output.mkdir(parents=True,exist_ok=True)
    run_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+uuid.uuid4().hex
    staging = Path(tempfile.mkdtemp(prefix='.incomplete-',dir=output))
    payloads = {'input.json':canonical(bundle),'result.json':canonical(result),'report.md':report(result).encode('utf-8')}
    for filename in ('hyeoks_account.py','hyeoks_verdict.py'):
        payloads[filename] = Path(__file__).with_name(filename).read_bytes()
    hashes = {}
    for name,content in payloads.items():
        with (staging/name).open('xb') as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        actual = hashlib.sha256((staging/name).read_bytes()).hexdigest()
        if actual != hashlib.sha256(content).hexdigest():
            raise InputError('preservation readback mismatch')
        hashes[name] = actual
    manifest = dict(version=VERSION,run_id=run_id,files=hashes,status='local_verified',
                    github_run_id=os.getenv('GITHUB_RUN_ID'),github_run_attempt=os.getenv('GITHUB_RUN_ATTEMPT'))
    for name,content in [('manifest.json',canonical(manifest)),('COMPLETE',b'local_verified\n')]:
        with (staging/name).open('xb') as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
    final = output/run_id
    if final.exists(): raise FileExistsError(final)
    staging.rename(final)
    return final


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input',help='private frozen account-input-v2 JSON; no live fetch')
    parser.add_argument('--output-dir',default='data/account_private')
    parser.add_argument('--self-test',action='store_true')
    args = parser.parse_args(argv)
    if args.self_test:
        import unittest
        suite = unittest.defaultTestLoader.discover(str(Path(__file__).parent/'tests'),pattern='test_account*.py')
        if suite.countTestCases() == 0:
            return 1
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.input:
        parser.error('--input required: sparse ledger marks cannot reconstruct NAV')
    try:
        bundle = json.loads(Path(args.input).read_text(encoding='utf-8-sig'))
        result = calculate(bundle)
        path = save_bundle(bundle,result,args.output_dir)
    except (InputError,KeyError,TypeError,ValueError,OSError) as exc:
        print(f'ACCOUNT_FAILED: {type(exc).__name__}; inspect private input locally')
        return 2
    print(f'ACCOUNT_LOCAL_VERIFIED: {path.name}; input_sha256={result["input_sha256"]}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
