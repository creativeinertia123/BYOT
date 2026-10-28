"""HOPE FX economic board adapted from freox_all_v5.patch.

The patch's Streamlit/data_feed boundary is not present in BYOT, so this module
keeps the same HOPE country taxonomy and return ladder behind the existing
QuantPilot YahooProvider and a small SQLite read model. Yahoo is attempted first
for every cross. The keyless daily endpoint is only a narrow fallback when
Yahoo has no usable response; failed fast data is never presented as fresh.
"""
from __future__ import annotations

import concurrent.futures
import html as html_lib
import json
import math
import re
import sqlite3
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..providers import yahoo

DB_PATH = Path(__file__).with_name("fx_econ.db")
CACHE_TTL = 60
CALENDAR_TTL = 1800
TE_CALENDAR_URL = "https://tradingeconomics.com/calendar"
LOCAL_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")

HOPE_COUNTRIES = [
    "argentina", "australia", "brazil", "canada", "china", "euro-area",
    "france", "germany", "india", "indonesia", "italy", "japan", "mexico",
    "russia", "saudi-arabia", "south-africa", "south-korea", "turkey",
    "united-kingdom", "united-states", "egypt", "ethiopia", "iran",
    "united-arab-emirates",
]
HOPE_CCY = {
    "argentina": "ARS", "australia": "AUD", "brazil": "BRL", "canada": "CAD",
    "china": "CNY", "euro-area": "EUR", "france": "EUR", "germany": "EUR",
    "india": "INR", "indonesia": "IDR", "italy": "EUR", "japan": "JPY",
    "mexico": "MXN", "russia": "RUB", "saudi-arabia": "SAR", "south-africa": "ZAR",
    "south-korea": "KRW", "turkey": "TRY", "united-kingdom": "GBP",
    "united-states": "USD", "egypt": "EGP", "ethiopia": "ETB", "iran": "IRR",
    "united-arab-emirates": "AED",
}
HOPE_CATEGORIES = {
    "firing": ("initialjoblessclaims", "joblessclaims", "continuingjoblessclaims", "claims"),
    "unemployment": ("unemploymentrate", "unemployment"),
    "hiring": ("nonfarmpayroll", "employmentchange", "adpemployment", "payroll", "employment"),
    "housing": ("housing", "buildingpermit", "buildingapprov", "dwellingapprov", "homenationallendingindex"),
    "pmi": ("manufacturingpmi", "servicespmi", "compositepmi", "ismmanufacturing", "ismnonmanufacturing", "iservices", "pmi"),
    "ppi": ("producerprice", "ppi"),
    "cpi": ("inflationrate", "inflation", "consumerprice", "cpi", "harmonisedcpi", "harmonizedcpi", "pceprice"),
}
# Yahoo symbols are chosen in the same direction as the patch's USD-cross
# badges. For unsupported exotic crosses, the daily fallback remains explicit.
YAHOO_SYMBOLS = {
    "ARS": "ARS=X", "AUD": "AUDUSD=X", "BRL": "BRL=X", "CAD": "CAD=X", "CNY": "CNY=X",
    "EUR": "EURUSD=X", "INR": "INR=X", "IDR": "IDR=X", "JPY": "JPY=X", "MXN": "MXN=X",
    "RUB": "RUB=X", "SAR": "SAR=X", "ZAR": "ZAR=X", "KRW": "KRW=X", "TRY": "TRY=X",
    "GBP": "GBPUSD=X", "USD": "DX-Y.NYB", "EGP": "EGP=X", "ETB": "ETB=X", "IRR": "IRR=X",
    "AED": "AED=X",
}
_CACHE: tuple[float, dict] | None = None
_CALENDAR_CACHE: tuple[float, dict] | None = None
_LOCK = __import__("threading").Lock()


def _schema(db: sqlite3.Connection) -> None:
    db.executescript("""
      CREATE TABLE IF NOT EXISTS fx_ticks (
        ccy TEXT NOT NULL, ts TEXT NOT NULL, rate REAL NOT NULL, src TEXT NOT NULL,
        PRIMARY KEY (ccy, ts)
      );
      CREATE TABLE IF NOT EXISTS fx_rates (
        ccy TEXT NOT NULL, date TEXT NOT NULL, rate REAL NOT NULL, src TEXT NOT NULL,
        updated_at TEXT NOT NULL, PRIMARY KEY (ccy, date)
      );
      CREATE TABLE IF NOT EXISTS calendar_events (
        event_id TEXT PRIMARY KEY, event_at_utc TEXT NOT NULL, event_at_local TEXT NOT NULL,
        country TEXT NOT NULL, ccy TEXT, category TEXT NOT NULL, title TEXT NOT NULL,
        actual TEXT, previous TEXT, consensus TEXT, forecast TEXT, url TEXT,
        source TEXT NOT NULL, fetched_at TEXT NOT NULL
      );
    """)


def _db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    _schema(db)
    return db


def _norm(text: str) -> str:
    return "".join(ch for ch in (text or "").lower() if ch.isalnum())


def hope_category(title: str) -> str:
    value = _norm(title)
    for category, needles in HOPE_CATEGORIES.items():
        if any(needle in value for needle in needles):
            return category
    return ""


def _finite(value) -> bool:
    try:
        return value is not None and math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _yahoo_one(ccy: str) -> tuple[str, dict]:
    symbol = YAHOO_SYMBOLS[ccy]
    out = {"ccy": ccy, "symbol": symbol, "fast": [], "daily": [], "source": None, "error": None}
    try:
        fast = yahoo.provider.bars(symbol, rng="1d", interval="1m")
        out["fast"] = [(int(ts), float(close)) for ts, close in zip(fast.timestamps, fast.close) if _finite(close)]
    except Exception as exc:  # provider boundary: try daily before failing closed
        out["error"] = str(exc)[:240]
    try:
        daily = yahoo.provider.bars(symbol, rng="2y", interval="1d")
        out["daily"] = [(int(ts), float(close)) for ts, close in zip(daily.timestamps, daily.close) if _finite(close)]
    except Exception as exc:
        out["error"] = (out["error"] + "; " if out["error"] else "") + str(exc)[:240]
    if out["fast"] or out["daily"]:
        out["source"] = "yahoo"
    return ccy, out


def _daily_fallback() -> tuple[dict[str, float], str | None]:
    try:
        req = urllib.request.Request("https://open.er-api.com/v6/latest/USD", headers={"User-Agent": "BYOT/1.0"})
        with urllib.request.urlopen(req, timeout=12) as response:
            payload = json.loads(response.read().decode("utf-8"))
        rates = payload.get("rates") or {}
        return {ccy: float(rates[ccy]) for ccy in set(HOPE_CCY.values()) if _finite(rates.get(ccy))}, "open.er-api.com"
    except Exception:
        return {}, None


def _cell(row: str, ident: str) -> str | None:
    match = re.search(rf"(?:id|data-testid)=[\"']{re.escape(ident)}[\"'][^>]*>(.*?)</", row, re.I | re.S)
    if not match:
        return None
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html_lib.unescape(match.group(1)))).strip() or None


def _calendar_rows(raw: str, fetched_at: datetime) -> list[dict]:
    rows = []
    hope_countries = set(HOPE_COUNTRIES)
    pattern = re.compile(r"<tr\s+data-url=[\"']([^\"']+)[\"'][^>]*data-id=[\"']([^\"']+)[\"'][^>]*data-country=[\"']([^\"']+)[\"'][^>]*data-category=[\"']([^\"']*)[\"'][^>]*data-event=[\"']([^\"']*)[\"'][^>]*>(.*?)(?=<tr\s+data-url=)", re.I | re.S)
    ccy_by_country = {_norm(country): ccy for country, ccy in HOPE_CCY.items()}
    for match in pattern.finditer(raw):
        url, event_id, country, provider_category, provider_event, body = match.groups()
        country_key = _norm(country)
        if country_key not in {_norm(x) for x in hope_countries}:
            continue
        date_match = re.search(r"class=[\"'][^\"']*\b(20\d{2}-\d{2}-\d{2})\b", body, re.I)
        time_match = re.search(r"calendar-date-[^\"']*[\"'][^>]*>\s*([^<]+)", body, re.I)
        if not date_match or not time_match:
            continue
        local_text = f"{date_match.group(1)} {time_match.group(1).strip()}"
        try:
            event_local = datetime.strptime(local_text, "%Y-%m-%d %I:%M %p").replace(tzinfo=LOCAL_TZ)
        except ValueError:
            continue
        title = _cell(body, "event") or provider_event.replace("-", " ").title()
        category = hope_category(title)
        if not category:
            continue
        event_utc = event_local.astimezone(timezone.utc)
        iso = event_utc.isoformat().replace("+00:00", "Z")
        rows.append({"event_id": event_id, "event_at": iso, "event_local": event_local.isoformat(),
                     "country": country, "ccy": ccy_by_country.get(country_key), "category": category,
                     "title": title, "actual": _cell(body, "actual"), "previous": _cell(body, "previous"),
                     "consensus": _cell(body, "consensus"), "forecast": _cell(body, "forecast"),
                     "url": "https://tradingeconomics.com" + url if url.startswith("/") else url,
                     "provider_category": provider_category, "source": TE_CALENDAR_URL,
                     "fetched_at": fetched_at.isoformat().replace("+00:00", "Z")})
    return rows


def _calendar_refresh(force: bool = False) -> dict:
    global _CALENDAR_CACHE
    if not force and _CALENDAR_CACHE and time.time() - _CALENDAR_CACHE[0] < CALENDAR_TTL:
        return _CALENDAR_CACHE[1]
    fetched_at = datetime.now(timezone.utc)
    try:
        request = urllib.request.Request(TE_CALENDAR_URL, headers={
            "User-Agent": "Mozilla/5.0 (BYOT economic calendar)",
            "Cookie": "cal-timezone-offset=480",
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8", "ignore")
        rows = _calendar_rows(raw, fetched_at)
        db = _db()
        try:
            db.execute("DELETE FROM calendar_events")
            db.executemany("INSERT INTO calendar_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
                (r["event_id"], r["event_at"], r["event_local"], r["country"], r["ccy"], r["category"], r["title"],
                 r["actual"], r["previous"], r["consensus"], r["forecast"], r["url"], r["source"], r["fetched_at"])
                for r in rows
            ])
            db.commit()
        finally:
            db.close()
        rows.sort(key=lambda row: (row["event_at"], row["country"], row["title"]))
        payload = {"available": bool(rows), "source": TE_CALENDAR_URL,
                   "as_of": fetched_at.isoformat().replace("+00:00", "Z"),
                   "freshness": "calendar-window", "age_seconds": 0,
                   "rows": rows, "categories": list(HOPE_CATEGORIES),
                   "message": "" if rows else "No HOPE events in the calendar window."}
    except Exception as exc:
        payload = {"available": False, "source": TE_CALENDAR_URL, "as_of": None,
                   "freshness": "unavailable", "age_seconds": None, "rows": [],
                   "categories": list(HOPE_CATEGORIES), "message": str(exc)[:240]}
    _CALENDAR_CACHE = (time.time(), payload)
    return payload


def _persist(results: dict[str, dict], fallback: dict[str, float], now: datetime) -> None:
    db = _db()
    try:
        for ccy, result in results.items():
            for ts, rate in result["fast"]:
                stamp = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                db.execute("INSERT OR REPLACE INTO fx_ticks VALUES (?,?,?,?)", (ccy, stamp, rate, "yahoo"))
            for ts, rate in result["daily"]:
                day = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
                db.execute("INSERT OR REPLACE INTO fx_rates VALUES (?,?,?,?,?)", (ccy, day, rate, "yahoo", now.isoformat()))
        day = now.date().isoformat()
        for ccy, rate in fallback.items():
            db.execute("INSERT OR REPLACE INTO fx_rates VALUES (?,?,?,?,?)", (ccy, day, rate, "open.er-api.com", now.isoformat()))
        db.commit()
    finally:
        db.close()


def _pct(current: float | None, prior: float | None) -> float | None:
    if not _finite(current) or not _finite(prior) or float(prior) == 0:
        return None
    return (float(current) / float(prior) - 1.0) * 100.0


def _ladder(ccy: str, fast: list[tuple[int, float]], daily: list[tuple[int, float]], fallback: float | None) -> dict:
    fast = sorted(fast)
    daily = sorted(daily)
    current = (fast[-1][1] if fast else daily[-1][1] if daily else fallback)
    def lagged(rows, count):
        return rows[-count-1][1] if len(rows) > count else None
    row = {"1m": _pct(current, lagged(fast, 1)), "5m": _pct(current, lagged(fast, 5)),
           "15m": _pct(current, lagged(fast, 15)), "1h": _pct(current, lagged(fast, 60)),
           "5d": _pct(current, lagged(daily, 5)), "14d": _pct(current, lagged(daily, 14)),
           "1w": _pct(current, lagged(daily, 7)), "63d": _pct(current, lagged(daily, 63)),
           "253d": _pct(current, lagged(daily, 253))}
    return {"ccy": ccy, "pair": f"USD/{ccy}", "price": current, "returns": row,
            "source": "yahoo" if fast or daily else ("open.er-api.com" if fallback is not None else None),
            "fast_available": bool(fast)}


def refresh(force: bool = False) -> dict:
    global _CACHE
    with _LOCK:
        if not force and _CACHE and time.time() - _CACHE[0] < CACHE_TTL:
            return _CACHE[1]
        started = time.perf_counter(); now = datetime.now(timezone.utc)
        currencies = sorted(set(HOPE_CCY.values()))
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="fx-econ") as pool:
            for future in concurrent.futures.as_completed([pool.submit(_yahoo_one, ccy) for ccy in currencies]):
                ccy, result = future.result(); results[ccy] = result
        fallback, fallback_source = _daily_fallback()
        _persist(results, fallback, now)
        rows = [_ladder(ccy, results.get(ccy, {}).get("fast", []), results.get(ccy, {}).get("daily", []), fallback.get(ccy)) for ccy in currencies]
        rows = [r for r in rows if r["price"] is not None]
        fast_count = sum(1 for r in rows if r["fast_available"])
        payload = {"available": bool(rows), "source": "Yahoo Finance chart adapter" if fast_count else (fallback_source or "unavailable"),
                   "provider": "yahoo" if fast_count else fallback_source, "as_of": now.isoformat().replace("+00:00", "Z"),
                   "freshness": "live-minute" if fast_count else "daily-fallback" if rows else "unavailable",
                   "age_seconds": 0, "fetch_ms": round((time.perf_counter()-started)*1000),
                   "fast_count": fast_count, "currency_count": len(rows), "rows": rows,
                   "hope_categories": list(HOPE_CATEGORIES), "hope_countries": HOPE_COUNTRIES,
                   "message": "" if rows else "FX feed unavailable"}
        _CACHE = (time.time(), payload)
        return payload


def snapshot() -> dict:
    return refresh(False)


def home_snapshot(category: str = "all") -> dict:
    """Return the point-in-time HOPE calendar window for HOME."""
    category = (category or "all").lower()
    with _LOCK:
        payload = _calendar_refresh(False)
    if category in HOPE_CATEGORIES:
        payload = {**payload, "category": category,
                   "rows": [r for r in payload["rows"] if r["category"] == category]}
    else:
        payload = {**payload, "category": "all"}
    return payload
