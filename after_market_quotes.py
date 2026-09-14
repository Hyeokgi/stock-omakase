"""After-market observations: no inferred venue or regular-session close.

The generic Naver AFTER_MARKET payload does not identify an exchange.
Its previous-close return is not a return from today's regular close.
"""
import datetime as dt
import math

KST = dt.timezone(dt.timedelta(hours=9))
AFTER_HEADER = '시간외 관측(시장·기준·시각 표시)'
NXT_HEADER = 'NXT 관측(확인된 시장만)'


def number(value):
    if isinstance(value, bool):
        return None
    try:
        n = float(str(value).replace(',', ''))
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def scanner_after_quote(data, now):
    """Return AA, AB, market label; never populate NXT from generic data.

    Today's regular close has no verified source in this adapter. We preserve
    the explicitly labelled provider previous-close return, not a synthetic 0%.
    Old observations retain their timestamp but are not presented as live.
    """
    missing = ('미확인(당일 시간외 시세 없음)', '', '시간외 미확인')
    if now.tzinfo is None:
        raise ValueError('timezone-aware now required')
    now = now.astimezone(KST)
    info = data.get('overMarketPriceInfo') if isinstance(data, dict) else None
    if not isinstance(info, dict) or info.get('tradingSessionType') != 'AFTER_MARKET':
        return missing
    price = number(info.get('overPrice'))
    try:
        observed = dt.datetime.fromisoformat(info['localTradedAt'])
        if observed.tzinfo is None:
            return missing
        observed = observed.astimezone(KST)
    except (ValueError, TypeError, KeyError):
        return missing
    age = (now - observed).total_seconds()
    if price is None or price <= 0 or observed.date() != now.date() or age < -60:
        return missing
    rate = number(info.get('fluctuationsRatio'))
    # Require the provider's explicit previous-close comparison family.
    previous = number(info.get('compareToPreviousClosePrice'))
    rate_text = '전일대비 미확인'
    if rate is not None and previous is not None and price - previous > 0:
        expected = previous / (price - previous) * 100
        if abs(expected - rate) <= .03:
            rate_text = f'전일종가 대비 {rate:+.2f}%'
    freshness = ' / 지연 관측' if age > 600 else ''
    label = (f'{rate_text} ({price:,.0f}원) / 정규장종가 대비 미확인 '
             f'[시장 미확인 / {observed:%Y-%m-%d %H:%M:%S} KST{freshness}]')
    return label, '', '시간외(시장 미확인)'
