"""Frozen pilot/main cohorts. No prices, network, or strategy approvals."""
import hashlib
import json

VERSION = 'closing-window-v2'


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False).encode('utf-8')).hexdigest()


def lock_cohort(state, name, rows):
    value = {'dates': [r['date'] for r in rows], 'sha256': fingerprint(rows)}
    if name in state and state[name] != value:
        raise ValueError(f'{name}: frozen input changed; review a new study version, not an overwrite')
    state[name] = value


def split_windows(series, state, pilot_days, calibrate, parameters):
    """Only the first pilot sets N; later rows cannot move the endpoint.

    Lock all selected day statistics, including exclusions. A late backfill or
    correction affecting an already frozen cohort is an error, not a new test.
    """
    policy = {'version': VERSION, **parameters}
    if 'policy' in state and state['policy'] != policy:
        raise ValueError('frozen study policy changed')
    state['policy'] = policy
    if len(series) < pilot_days:
        if 'pilot' in state:
            raise ValueError('frozen pilot is no longer complete')
        return None, [], []
    pilot = series[:pilot_days]
    lock_cohort(state, 'pilot', pilot)
    if 'calibration' not in state:
        state['calibration'] = calibrate([r['rel'] for r in pilot])
    calibration = state['calibration']
    win = calibration['window']
    main = series[pilot_days:pilot_days + win] if win else []
    previous = state.get('main_progress')
    if previous:
        prefix = main[:len(previous['dates'])]
        if {'dates': [r['date'] for r in prefix], 'sha256': fingerprint(prefix)} != previous:
            raise ValueError('main progress changed; late backfill/correction requires review')
    if main:
        state['main_progress'] = {'dates': [r['date'] for r in main], 'sha256': fingerprint(main)}
    if 'main' in state or (win and len(main) == win):
        lock_cohort(state, 'main', main)
    return calibration, pilot, main
