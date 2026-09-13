"""Read-only AI performance context. No SDK imports, writes or horizon fallback."""
import datetime
import math

from hyeoks_tajeom import POLICY_SINCE
from hyeoks_verdict import HORIZON, KST, maturity_note

MEMORY_VERSION = 'performance-memory-v2-20260913'
CHANNELS = (('리포트TOP2_단기', '단기'), ('리포트TOP2_중기', '중기'))


def performance_summary(rows, today=None):
    """Latest 20 eligible signals per channel, using only its fixed horizon.

    Weekday maturity checks reject definitely early data, not certify exchange
    sessions. Input must be the formatted-value ledger, percentages in %-points.
    Invalid headers/duplicate IDs fail closed; the caller may omit this context.
    """
    if not rows:
        return ''
    today = today or datetime.datetime.now(KST).date()
    names = [str(x).strip() for x in rows[0]]
    required = ['trade_id', '진입일', '채널', '실제캡처거래일'] + [
        f'종목T+{HORIZON[ch]}' for ch, _ in CHANNELS]
    if any(names.count(name) != 1 for name in required):
        raise ValueError('AI Memory: required header missing or duplicated')
    cols = {name: names.index(name) for name in required}
    cell = lambda row, name: str(row[cols[name]]).strip() if len(row) > cols[name] else ''
    grouped = {ch: [] for ch, _ in CHANNELS}
    seen = set()
    for row in rows[1:]:
        ch = cell(row, '채널')
        if ch not in grouped or '제외' in cell(row, '실제캡처거래일'):
            continue
        trade_id = cell(row, 'trade_id')
        if not trade_id:
            continue
        if trade_id in seen:
            raise ValueError('AI Memory: duplicate trade_id')
        seen.add(trade_id)
        day = cell(row, '진입일')[:10]
        try:
            signal_date = datetime.date.fromisoformat(day)
        except ValueError:
            continue
        if signal_date > today or day < POLICY_SINCE:
            continue
        grouped[ch].append((day, trade_id, row))

    lines = []
    for ch, label in CHANNELS:
        horizon = HORIZON[ch]
        recent = sorted(grouped[ch], key=lambda item: item[:2])[-20:]
        values = []
        for day, _, row in recent:
            if maturity_note(day, horizon, today) in ('INVALID', 'NOT_MATURED'):
                continue
            value = cell(row, f'종목T+{horizon}').replace(',', '')
            if value.endswith('%'):
                value = value[:-1].strip()
            try:
                value = float(value)
            except ValueError:
                continue
            if math.isfinite(value):
                values.append(value)
        if not values:
            continue  # Never substitute T+1/T+3 or entry price.
        mean = sum(values) / len(values)
        wins = sum(x > 0 for x in values) / len(values) * 100
        note = ' (표본 적어 참고만)' if len(values) < 15 else ''
        lines.append(f'- {label} T+{horizon}: 최근 {len(recent)}개 신호 중 유효 기록 {len(values)}건; '
                     f'평균 종목수익률 {mean:+.1f}%, 양수 수익 비율 {wins:.0f}%{note}')
    if not lines:
        return ''
    return (f'[{MEMORY_VERSION}; 기준일 {today}; 정책 신호일 {POLICY_SINCE} 이후]\n'
            '기간별 고정 수익률만 사용. 비용·지수 미차감이며 투자 승인 지표가 아니다.\n'
            '확실한 조기값은 제외했으나 거래일 달력·기록 시점은 독립 검증되지 않았다.\n'
            + '\n'.join(lines))
