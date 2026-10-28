"""Bounded Trading Economics market-table adapter for the migrated terminal."""
from __future__ import annotations

import concurrent.futures
import http.client
import re
import sqlite3
import time
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path

DB_PATH = Path(__file__).with_name("commodity_quotes.db")
TABLES = {
    "commodities": {"url": "https://tradingeconomics.com/commodities", "groups": ("Energy", "Metals", "Agricultural", "Industrial", "Livestock", "Index", "Electricity"), "label": "COMMODITIES"},
    "fx": {"url": "https://tradingeconomics.com/currencies", "groups": ("Major", "Europe", "America", "Asia", "Australia", "Africa"), "label": "FX"},
    "index": {"url": "https://tradingeconomics.com/stocks", "groups": ("Major", "Europe", "America", "Asia", "Australia", "Africa"), "label": "INDEX"},
    "bond": {"url": "https://tradingeconomics.com/bonds", "groups": ("Major", "Europe", "America", "Asia", "Australia", "Africa", "Oceania"), "label": "BOND"},
}
_CACHE: dict[str, tuple[float, dict]] = {}
_CACHE_TTL = 60


def _empty(kind: str, message: str = "Trading Economics table unavailable.") -> dict:
    spec = TABLES[kind]
    return {"available": False, "source": spec["url"], "as_of": None,
            "freshness": "unavailable", "fetch_ms": None, "group": kind,
            "groups": list(spec["groups"]), "rows": [], "message": message}


def _number(value: str):
    value = value.strip().replace(",", "")
    try:
        return float(value.replace("%", "")) if value not in ("", "-") else None
    except ValueError:
        return None


def _clean_cells(fragment):
    cells = re.findall(r"<td\b[^>]*>(.*?)</td>", fragment, re.I | re.S)
    return [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", unescape(c))).strip() for c in cells], cells


def _parse(kind: str, raw: str, fetched_at: float, fetch_ms: int) -> dict:
    spec = TABLES[kind]; rows = []
    for group in spec["groups"]:
        marker = re.search(rf'<input[^>]+value="{re.escape(group)}"[^>]*>', raw, re.I)
        if not marker: continue
        next_marker = re.search(r'<input[^>]+id="[^\"]*group"[^>]+value="[^\"]+"', raw[marker.end():], re.I)
        section = raw[marker.end(): marker.end() + next_marker.start() if next_marker else len(raw)]
        for match in re.finditer(r'<tr[^>]*data-symbol="([^"]+)"[^>]*>(.*?)</tr>', section, re.I | re.S):
            clean, cells = _clean_cells(match.group(2))
            if len(clean) < 8: continue
            offset = 1 if clean[0] == "" else 0
            if len(clean) <= offset + 7: continue
            name = clean[offset]
            href = re.search(r'href="/[^"]+/(?:currency|stock-market|government-bond-yield|commodity)/?([^"/?]+)', match.group(2), re.I)
            slug = (href.group(1) if href else match.group(1)).lower()
            unit_match = re.search(r"<div[^>]*>(.*?)</div>", cells[offset], re.I | re.S)
            units = re.sub(r"<[^>]+>", " ", unescape(unit_match.group(1))).strip() if unit_match else ""
            date_index = offset + (7 if kind == "bond" else 8)
            row = {"slug": slug, "name": name, "grp": group, "units": units,
                   "price": _number(clean[offset + 1]), "chg": _number(clean[offset + 2]),
                   "pct_chg": _number(clean[offset + 3]), "weekly": _number(clean[offset + 4]),
                   "monthly": _number(clean[offset + 5]), "ytd": _number(clean[offset + 6]),
                   "yoy": None if kind == "bond" else _number(clean[offset + 7]),
                   "ts": clean[date_index] if date_index < len(clean) else "", "updated_at": None}
            rows.append(row)
    if not rows: raise ValueError(f"Trading Economics {kind} table returned no rows")
    return {"available": True, "source": spec["url"], "as_of": fetched_at,
            "freshness": "live-table", "age_seconds": 0, "fetch_ms": fetch_ms,
            "group": kind, "groups": list(spec["groups"]), "rows": rows}


def _fetch(kind: str) -> dict:
    now = time.time(); cached = _CACHE.get(kind)
    if cached and now - cached[0] < _CACHE_TTL: return cached[1]
    started = time.perf_counter()
    req = urllib.request.Request(TABLES[kind]["url"], headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "ignore")
    result = _parse(kind, raw, time.time(), round((time.perf_counter() - started) * 1000))
    _CACHE[kind] = (time.time(), result)
    return result


def refresh_all() -> dict[str, dict]:
    """Refresh each source once with bounded four-way concurrency."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="te") as pool:
        futures = {pool.submit(_fetch, kind): kind for kind in TABLES}
        out = {}
        for future in concurrent.futures.as_completed(futures):
            kind = futures[future]
            try: out[kind] = future.result()
            except (OSError, ValueError, TimeoutError, http.client.IncompleteRead, urllib.error.URLError) as exc:
                out[kind] = _empty(kind, str(exc))
    return out


def market(kind: str = "commodities") -> dict:
    kind = kind.lower()
    if kind not in TABLES: return _empty("commodities", "Unknown market table.")
    try: return _fetch(kind)
    except (OSError, ValueError, TimeoutError, http.client.IncompleteRead, urllib.error.URLError) as exc:
        return _empty(kind, str(exc))


def commodities(group: str | None = None, limit: int = 200) -> dict:
    result = market("commodities")
    if group and group.upper() not in ("FICC", "COMMODITIES"):
        result = {**result, "group": group,
                  "rows": [r for r in result["rows"] if r["grp"].upper() == group.upper()]}
    result["rows"] = result["rows"][:max(1, min(int(limit), 500))]
    return result
