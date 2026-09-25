"""Pre-selection evidence. Scanner hooks are offline; CLI archives via existing private Drive route."""
import datetime as dt
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import threading

# Scanner comparisons can return numpy.bool_, not Python bool. Keep this
# archive helper usable without NumPy, but never coerce strings/numbers/NA.
BOOLEAN_TYPES = (bool,)
try:
    from numpy import bool_ as _numpy_bool
except ImportError:
    pass
else:
    BOOLEAN_TYPES += (_numpy_bool,)

import feature_store
import krx_code
from hyeoks_trading_calendar import scheduled_session, load_nontrading, next_trading_day

VERSION = 'badge-observations-v1'
CONDITION_SCHEMA = 'badge-conditions-v2'
ROOT = Path('data/research_private/badge_observations')
KST = dt.timezone(dt.timedelta(hours=9))
# These are independent computed conditions, NOT the priority-suppressed display badges.
FLAGS = ('is_foreigner_active_buy', 'is_long_term_pick', 'is_super_leader',
         'is_minervini_template', 'is_true_theme_leader', 'is_theme_daejang',
         'is_jongbe_cand', 'is_accumulation_cand', 'is_platform_breakout',
         'is_envelope_over_under', 'is_v2_gate_passed', 'is_upper_limit',
         'is_junk', 'is_financial_risk')
METRICS = ('trading_value', 'vol_ratio_yest', 'vol_ratio_10d', 'raw_rs_score',
           'current_price', 'ma5', 'ma20', 'acc_i_buy_eok', 'acc_f_buy_eok')
_lock = threading.Lock()
_active = False
_features = {}
_start = None
_folder = None


def timestamp(value):
    if value.tzinfo is None:
        raise ValueError('timezone required')
    return value.astimezone(KST)


def in_window(now):
    now = timestamp(now)
    return (dt.time(14, 20) <= now.time() <= dt.time(15, 10)
            and scheduled_session(now.date().isoformat()))


def clean(value):
    if isinstance(value, BOOLEAN_TYPES):
        return bool(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    return str(value)


def save_once(path, value):
    """Publish complete compressed evidence without replacing a previous capture."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(clean(value), ensure_ascii=False, sort_keys=True,
                      allow_nan=False).encode('utf-8')
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix='.tmp')
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(gzip.compress(body, mtime=0))
        os.link(tmp, path)  # exclusive: existing evidence is never overwritten
    finally:
        os.unlink(tmp)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def begin(targets, unresolved=(), now=None, root=ROOT):
    """Start receipt survives a later scanner exception via always-upload step."""
    global _active, _features, _start, _folder
    try:
        now = timestamp(now or dt.datetime.now(KST))
        with _lock:
            _active, _features, _start, _folder = False, {}, now, None
        if not in_window(now):
            return
        key = now.strftime('%Y%m%dT%H%M%S%f')
        folder = Path(root) / key
        save_once(folder / 'started.json.gz', {
            'version': VERSION, 'scan_started_at': now.isoformat(),
            'targets': targets, 'unresolved_names': sorted(unresolved),
            'run_id': os.getenv('GITHUB_RUN_ID', ''),
            'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', ''),
            'code_sha': os.getenv('GITHUB_SHA', ''), 'state': 'STARTED',
        })
        with _lock:
            _folder, _active = folder, True
    except Exception as exc:
        print(f'::warning::badge observation start failed: {type(exc).__name__}: {exc}')


def capture(code, context):
    """Whitelist only. Never retain locals, tokens or arbitrary scanner context."""
    if not _active:
        return
    try:
        flags, missing_reasons = {}, {}
        for key in FLAGS:
            raw = context.get(key)
            if isinstance(raw, BOOLEAN_TYPES):
                flags[key] = bool(raw)
            else:
                flags[key] = None
                missing_reasons[key] = ('missing_input' if key not in context else
                                        'null_input' if raw is None else
                                        'invalid_boolean_type')
        diagnostic = context.get('minervini_diag') or {}
        has_data = diagnostic.get('has_data')
        if not isinstance(has_data, BOOLEAN_TYPES) or not bool(has_data):
            flags['is_minervini_template'] = None
            missing_reasons['is_minervini_template'] = 'history_not_verified'
        metrics = {k: context.get(k) for k in METRICS}
        value = clean({'condition_schema': CONDITION_SCHEMA,
                       'computed_at': dt.datetime.now(KST).isoformat(),
                       'conditions': flags, 'metrics': metrics,
                       'condition_missing_reasons': missing_reasons,
                       'minervini_diagnostic': {k: diagnostic.get(k)
                                               for k in ('has_data', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6')}})
        with _lock:
            if _active:
                _features[krx_code.normalize(code)] = value
    except Exception as exc:
        print(f'::warning::badge condition capture failed: {type(exc).__name__}')


def build(targets, results, features, started, available, unresolved=(), market=None):
    started, available = timestamp(started), timestamp(available)
    if available < started or available.date() != started.date():
        raise ValueError('invalid scan interval')
    day = available.date().isoformat()
    cutoff = available.replace(hour=15, minute=5, second=0, microsecond=0)
    # Select evidence using availability, never by its later return.
    timing = started >= cutoff - dt.timedelta(minutes=20) and available <= cutoff
    indexed = {}
    for r in results:
        if len(r) > 1:
            code = krx_code.normalize(r[1])
            if code in indexed:
                raise ValueError('duplicate result code')
            indexed[code] = r
    rows = []
    for name, raw_code in sorted(targets.items()):
        code = krx_code.normalize(raw_code)
        r, evidence = indexed.get(code), features.get(code)
        healthy = bool(code and r and len(r) == len(feature_store.RESULT_FIELDS)
                       and isinstance(r[2], (int, float)) and math.isfinite(r[2])
                       and r[2] > 0 and evidence)
        fields = dict(zip(feature_store.RESULT_FIELDS, r or []))
        if not healthy:
            # Fallback zeros are NOT observed negative badge conditions.
            evidence = None
        rows.append({'code': code, 'name': name,
                     'status': 'COMPUTED_UNVERIFIED' if healthy else 'MISSING_OR_FAILED',
                     'raw_result': fields, 'independent_conditions': evidence,
                     'display_tajeom': fields.get('tajeom') if healthy else None,
                     'timely_computed': bool(healthy and timing),
                     'execution_verified': False})
    # A complete row does NOT imply complete conditions. Keep the denominator
    # and expose field-level missingness without treating unknown as False.
    condition_quality = {}
    for key in FLAGS:
        counts = {'true': 0, 'false': 0, 'missing': 0, 'invalid_type': 0}
        for row in rows:
            evidence = row['independent_conditions'] or {}
            value = evidence.get('conditions', {}).get(key)
            if isinstance(value, BOOLEAN_TYPES):
                counts['true' if bool(value) else 'false'] += 1
            else:
                counts['missing'] += 1
            if evidence.get('condition_missing_reasons', {}).get(key) == 'invalid_boolean_type':
                counts['invalid_type'] += 1
        condition_quality[key] = counts
    return clean({
        'version': VERSION, 'date': day, 'scan_started_at': started.isoformat(),
        'condition_schema': CONDITION_SCHEMA, 'condition_quality': condition_quality,
        'available_at': available.isoformat(), 'decision_at': cutoff.isoformat(),
        'timing_ok': timing, 'scope': 'scanner_targets_NOT_entire_market',
        'expected': len(targets), 'returned': len(indexed),
        'missing_or_failed': sum(r['status'] == 'MISSING_OR_FAILED' for r in rows),
        'unresolved_names': sorted(unresolved), 'market_context': market or {},
        'run_id': os.getenv('GITHUB_RUN_ID', ''),
        'run_attempt': os.getenv('GITHUB_RUN_ATTEMPT', ''),
        'code_sha': os.getenv('GITHUB_SHA', ''),
        'switches': {k: os.getenv(k, default) for k, default in (
            ('ENVELOPE_BAND', 'off'), ('SUPPLY_V2_BAND', 'off'),
            ('SUPPLY_V2_BAND_RANGE', '45-79'))},
        'label_contract': {'entry': 'separate_verified_1505_price',
                           'exit': 'next_KRX_session_open',
                           'scheduled_exit_date': next_trading_day(day, load_nontrading()),
                           'status': 'NOT_JOINED', 'costs': 'NOT_APPLIED'},
        'source_quote_timestamp': 'UNKNOWN', 'rows': rows,
    })


def finish(targets, results, unresolved=(), market=None, now=None):
    global _active
    if not _active:
        return
    try:
        with _lock:
            _active = False  # Late/deadline worker results cannot alter this capture.
            features, started, folder = dict(_features), _start, _folder
        payload = build(targets, results, features, started, now or dt.datetime.now(KST),
                        unresolved, market)
        digest = save_once(folder / 'observations.json.gz', payload)
        save_once(folder / 'completed.json.gz', {
            'state': 'COMPLETE', 'sha256': digest, 'expected': payload['expected'],
            'missing_or_failed': payload['missing_or_failed'],
            'condition_schema': CONDITION_SCHEMA,
            'condition_quality': payload['condition_quality'],
            'timing_ok': payload['timing_ok'], 'version': VERSION})
        print(f"Badge observations: {payload['expected']} targets, "
              f"{payload['missing_or_failed']} missing/failed; timing_ok={payload['timing_ok']}")
        missing = {k: v['missing'] for k, v in payload['condition_quality'].items()
                   if v['missing']}
        invalid = sum(v['invalid_type'] for v in payload['condition_quality'].values())
        print(f'Badge condition quality ({CONDITION_SCHEMA}): '
              f'missing_by_flag={json.dumps(missing, sort_keys=True)}; invalid_type={invalid}')
        if invalid:
            print('::warning::badge conditions contain invalid boolean types; '
                  'unknown values were preserved, not converted to false')
    except Exception as exc:
        print(f'::warning::badge observation finish failed: {type(exc).__name__}: {exc}')


def _note_failure(folder, state, reason):
    """실패를 **배지 자신의 상태**에 남긴다.

    주 시스템(main.yml)은 이걸로 빨개지지 않는다. 대신 조용하지도 않다 —
    상태 파일과 로그 경고 양쪽에 남는다. 수집 실패를 "수집 없음" 과 구분한다.
    """
    try:
        save_once(Path(folder) / 'upload_status.json.gz',
                  {'version': VERSION, 'state': state, 'reason': reason,
                   'noted_at': dt.datetime.now(KST).isoformat()})
    except Exception as exc:                       # 상태 기록 실패도 조용하지 않다
        print(f'::warning::badge status note failed: {type(exc).__name__}')


def archive(root=ROOT, uploader=None):
    """One attempt per bundle. An ambiguous upload is never automatically retried.

    Source data is NOT a public-repository artifact. Use the already deployed
    private Drive freeze route. An ID acknowledges storage, not price validation.
    """
    import hyeoks_run_freeze as freeze
    uploader = uploader or (lambda name, data: freeze.upload(freeze.DEFAULT_GAS_URL, name, data))
    failures = 0
    for folder in sorted(Path(root).glob('*')):
        if not folder.is_dir() or not (folder / 'started.json.gz').exists():
            continue
        receipt = folder / 'upload_receipt.json.gz'
        intent = folder / 'upload_intent.json.gz'
        if receipt.exists():
            continue
        if intent.exists():
            print('::warning::badge archive outcome uncertain; inspect Drive before retry')
            _note_failure(folder, 'UNCERTAIN', 'intent without receipt; no automatic retry')
            failures += 1
            continue
        try:
            parts = {p.name: json.loads(gzip.decompress(p.read_bytes()))
                     for p in folder.glob('*.json.gz')}
            data = freeze.bundle_bytes({'version': VERSION, 'parts': parts})
            digest = hashlib.sha256(data).hexdigest()
            name = f'freeze_{folder.name}_badges_{digest[:16]}.json.gz'
            save_once(intent, {'filename': name, 'sha256': digest, 'bytes': len(data)})
            fid = uploader(name, data)
            if not fid:
                raise ValueError('missing Drive id')
            save_once(receipt, {'state': 'UPLOAD_ACKNOWLEDGED', 'drive_id': fid,
                                'filename': name, 'sha256': digest, 'bytes': len(data),
                                'readback_verified': False})
            print(f'Badge archive acknowledged: sha256={digest}; readback not verified')
            if 'completed.json.gz' not in parts:
                print('::warning::badge scan incomplete; start evidence archived, not a valid sample')
                _note_failure(folder, 'INCOMPLETE', 'no completed.json.gz; not a valid sample')
                failures += 1
        except Exception as exc:
            print(f'::warning::badge archive failed or uncertain: {type(exc).__name__}; no automatic retry')
            _note_failure(folder, 'FAILED', f'{type(exc).__name__}; no automatic retry')
            failures += 1
    # 🔴 2026-09-20 — 예전에는 `1 if failures else 0` 이었다. 종료코드로 쓰던 잔재다.
    #    그 값을 요약 메시지에 "N건" 으로 실었더니 **5건이 실패해도 1건으로 보고**됐다.
    #    종료코드로 더는 쓰지 않으므로 실제 건수를 돌려준다. `if archive():` 는 그대로다.
    return failures


def select_predecision(bundles, day):
    """Latest available batch before 15:05, never cherry-pick by coverage/return.

    Return the whole batch including missing rows. Timing alone does not certify
    prices or market access. No timely batch means no sample, not a later fallback.
    """
    candidates = []
    for b in bundles:
        if b.get('version') != VERSION or b.get('date') != day:
            continue
        start = timestamp(dt.datetime.fromisoformat(b['scan_started_at']))
        end = timestamp(dt.datetime.fromisoformat(b['available_at']))
        cutoff = dt.datetime.fromisoformat(day).replace(hour=15, minute=5, tzinfo=KST)
        if cutoff - dt.timedelta(minutes=20) <= start <= end <= cutoff:
            candidates.append(b)
    return max(candidates, key=lambda b: (b['available_at'], b.get('run_id', ''),
                                          b.get('run_attempt', '')), default=None)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--upload', action='store_true')
    args = parser.parse_args()
    if not args.upload:
        parser.error('--upload required')
    # 🔴 2026-09-20 — 이 스텝은 **주 시스템을 막지 않는다.**
    #    배지 수집은 연구 자료다. 그런데 main.yml 안에 있으므로 여기서 exit 1 을 내면
    #    run 결론이 failure 가 되고, evidence_builder 의 no_unexplained_failure 가
    #    그 결론을 보고 **그 거래일 전체를 FAIL 로 만든다**(실측 확인).
    #    consensus_aux 를 분리한 이유와 같은 범주다 — 보조 자료의 실패가 주 산출물의
    #    판정을 흐리지 않게 한다. 실패는 자체 상태(upload_status.json.gz)와 경고에 남는다.
    # 🔴 ① 처리한 실패 경로뿐 아니라 **예기치 못한 예외**도 주 시스템을 막지 않는다.
    #    (이전 판은 archive() 내부의 미포착 예외가 그대로 올라가 CLI 가 죽었다.
    #     모듈 로드 자체의 실패는 이 코드가 돌기 전이라 여기서 막을 수 없다 —
    #     그 마지막 한 겹은 main.yml 의 셸 가드가 맡는다.)
    try:
        failed = archive()
    except Exception as exc:                       # noqa: BLE001 — 연구 수집은 생산을 막지 않는다
        print(f'::warning::[배지 수집 실패] 처리되지 않은 예외 {type(exc).__name__}: {exc} — '
              '배지 실패만으로 주 시스템을 실패 처리하지 않는다')
        raise SystemExit(0)
    if failed:
        # 🔴 ③ 이 코드는 주 산출물의 정상 여부를 **확인하지 않는다.** 확인하지 않은 것을
        #    주장하지 않는다. 말할 수 있는 것은 "배지 실패만으로 떨어뜨리지 않는다" 뿐이다.
        #    주 산출물 자체의 판정은 evidence_builder 의 7기준이 따로 본다.
        print(f'::warning::[배지 수집 저하] {failed}건 미전송/불확실 — 자체 상태에 기록했다. '
              '배지 실패만으로 주 시스템을 실패 처리하지 않는다'
              '(주 산출물의 정상 여부는 Gate 기준이 따로 판정한다)')
    raise SystemExit(0)
