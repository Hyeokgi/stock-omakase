"""Reviewed scheduled closures, not a certificate of actual market opening."""
import datetime
import json
from pathlib import Path

DEFAULT_DIR = Path(__file__).resolve().parent / 'data' / 'market_snapshot'


class ClosureCalendar(set):
    start = None
    end = None

    def check(self, day):
        if self.start and not self.start <= day <= self.end:
            raise ValueError(f'거래일 달력 검토 범위 밖: {day}; 공식 자료로 범위를 갱신하세요')


def load_nontrading(snap_dir=DEFAULT_DIR):
    root = Path(snap_dir)
    path, scope = root / 'nontrading.txt', root / 'calendar_scope.json'
    out = ClosureCalendar()
    if root.resolve() == DEFAULT_DIR.resolve() and not scope.exists():
        raise ValueError('production calendar metadata missing')
    if scope.exists():
        meta = json.loads(scope.read_text(encoding='utf-8'))
        out.start, out.end = meta['start'], meta['end']
        datetime.date.fromisoformat(out.start)
        datetime.date.fromisoformat(out.end)
        if out.start > out.end or not meta.get('sources') or not path.exists():
            raise ValueError('invalid calendar scope or missing closure list')
    if path.exists():
        for line in path.read_text(encoding='utf-8').splitlines():
            day = line.split('#')[0].strip()
            if day:
                if datetime.date.fromisoformat(day).isoformat() != day:
                    raise ValueError(f'invalid closure date: {day}')
                out.check(day)
                out.add(day)
    return out


def next_trading_day(day, nontrading):
    d = datetime.date.fromisoformat(day)
    if isinstance(nontrading, ClosureCalendar):
        nontrading.check(day)
    for _ in range(12):
        d += datetime.timedelta(days=1)
        if isinstance(nontrading, ClosureCalendar):
            nontrading.check(d.isoformat())
        if d.weekday() < 5 and d.isoformat() not in nontrading:
            return d.isoformat()
    return None


def scheduled_session(day):
    calendar = load_nontrading()
    calendar.check(day)
    return datetime.date.fromisoformat(day).weekday() < 5 and day not in calendar
