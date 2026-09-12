"""Pure quote formatting. Missing/invalid data is not a flat return.

These helpers validate numeric observations, not exchange-session freshness.
No timestamp or trade availability is inferred from a positive price.
"""
import math


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(str(value).replace(',', '').strip())
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def quote_label(price, rate, source):
    price, rate = number(price), number(rate)
    if price is None or price <= 0 or rate is None:
        return None
    icon = '🔺' if rate > 0 else '🔵' if rate < 0 else '➖'
    rate_text = '0.00' if rate == 0 else f'{rate:+.2f}'
    return f'{icon}{rate_text}% ({price:,.0f}원) [{source}]'


def naver_quote(data, venue):
    """Choose only the requested field family; never cross-fill KRX and NXT."""
    if venue not in ('NXT', '시외'):
        raise ValueError('explicit venue required')
    if not isinstance(data, dict):
        return None, None
    prefix = 'nxt' if venue == 'NXT' else 'timeExtra'
    price = number(data.get(prefix + 'ClosePrice'))
    rate = number(data.get(prefix + 'FluctuationsRatio'))
    regular = number(data.get('closePrice'))
    if rate is None and price is not None and regular is not None and regular > 0:
        rate = (price / regular - 1) * 100
    label = quote_label(price, rate, '네이버/' + prefix + 'ClosePrice')
    return (label, venue) if label else (None, None)


def after_hours_header(header, regime):
    """AA/AB are the consumer contract; preserve all unrelated headers."""
    result = list(header)
    result.extend([''] * max(0, 28 - len(result)))
    result[26] = f"{regime['label']}({regime['window']}) 관측값"
    result[27] = 'NXT야간거래 관측값'
    return result
