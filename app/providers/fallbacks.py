"""Quote fallbacks for the QuantPilot Yahoo provider boundary.

Yahoo remains the source provider. These adapters are only consulted when a
Yahoo batch fails or omits a symbol. The endpoint shapes and field positions
come from the cloned global-stock-data source audit; no values are seeded or
invented when an upstream is unavailable.
"""
from __future__ import annotations

import json
import re
import time

from .. import net
from .base import Quote

EASTMONEY_URL = "https://push2.eastmoney.com/api/qt/stock/get"
SINA_URL = "https://hq.sinajs.cn/list=gb_{symbol}"


def _number(value):
    if value in (None, "", "-"):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result else None


def _eastmoney(symbol: str) -> Quote | None:
    for prefix in (105, 106):  # Nasdaq, then NYSE.
        url = f"{EASTMONEY_URL}?secid={prefix}.{symbol}&fields=" \
              "f43,f44,f45,f46,f47,f48,f55,f57,f58,f59,f60,f170"
        try:
            raw = net.fetch(url, ttl=2, retries=1, timeout=10)
            data = (json.loads(raw.decode("utf-8", "replace")).get("data")
                    or {})
        except (net.FetchError, json.JSONDecodeError, UnicodeError, AttributeError):
            continue
        if not data:
            continue
        decimals = int(_number(data.get("f59")) or 3)
        divisor = 10 ** decimals

        def price(key):
            value = _number(data.get(key))
            return round(value / divisor, decimals) if value is not None else None

        current = price("f43")
        previous = price("f60")
        if current is None:
            continue
        change_pct = _number(data.get("f170"))
        change_pct = change_pct / 100 if change_pct is not None else None
        return Quote(
            symbol=symbol,
            price=current,
            change=(current - previous) if current is not None and previous is not None else None,
            change_pct=change_pct,
            volume=_number(data.get("f47")),
            prev_close=previous,
            day_high=price("f44"),
            day_low=price("f45"),
            source="eastmoney",
            extra={"name": data.get("f58"), "amount": data.get("f48"),
                   "turnover_rate": data.get("f55"), "provider": "eastmoney"},
        )
    return None


def _sina(symbol: str) -> Quote | None:
    url = SINA_URL.format(symbol=symbol.lower())
    try:
        raw = net.fetch(url, ttl=2, retries=1, timeout=10)
        text = raw.decode("gbk", "replace")
        match = re.search(r'"([^\"]+)"', text)
    except (net.FetchError, UnicodeError):
        return None
    if not match:
        return None
    fields = match.group(1).split(",")
    if len(fields) < 30:
        return None

    def field(index):
        return _number(fields[index]) if index < len(fields) else None

    current, previous = field(1), field(26)
    if current is None:
        return None
    change_pct = field(2)
    return Quote(
        symbol=symbol,
        price=current,
        change=(current - previous) if previous is not None else None,
        change_pct=change_pct,
        volume=field(10),
        prev_close=previous,
        day_high=field(6),
        day_low=field(7),
        source="sina",
        extra={"name": fields[0], "open": field(5), "high_52w": field(8),
               "low_52w": field(9), "market_cap": field(12),
               "provider": "sina"},
    )


def quotes(symbols: list[str], excluded: set[str] | None = None) -> list[Quote]:
    """Return only values obtained from live fallback endpoints."""
    excluded = excluded or set()
    out: list[Quote] = []
    for raw_symbol in symbols:
        symbol = (raw_symbol or "").strip().upper()
        if not symbol or symbol in excluded:
            continue
        quote = _eastmoney(symbol) or _sina(symbol)
        if quote:
            out.append(quote)
    return out
