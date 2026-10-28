"""Yahoo-backed raw factor universe for the ROTARAW macro subtab.

The module is intentionally separate from the committed FF_Rotation SQLite
read model. It persists each Yahoo symbol atomically, records terminal status,
and exposes only derived views built from those persisted daily closes.
"""
from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from ..providers import yahoo
from ..providers.base import ProviderError

DB_PATH = Path(os.environ.get("ROTA_RAW_DB", Path(__file__).with_name("rota_raw.db")))
PERIODS = (7, 14, 30, 60, 90, 252)

CN = [
    "KSTR", "CNXT", "KGRN", "KTEC", "TCHI", "KWEB", "CHIQ", "KURE", "FXI",
    "ASHR", "MCHI", "CXSE", "ASHS", "510720.SS", "513120.SS", "513320.SS",
    "513550.SS", "515220.SS", "516310.SS", "516620.SS", "159901.SZ", "3167.HK",
    "2841.HK", "3189.HK", "2820.HK", "2826.HK", "2809.HK", "3134.HK", "2845.HK", "2815.HK",
]
EU = [
    "EXV3.DE", "EXV2.DE", "EXH6.DE", "EXV5.DE", "EXV9.DE", "EXH8.DE", "EXH7.DE",
    "EXH3.DE", "EXH1.DE", "EXV1.DE", "EXH5.DE", "EXH2.DE", "EXV4.DE", "EXH4.DE",
    "EXV8.DE", "EXV7.DE", "EXV6.DE", "EXI5.DE", "EXH9.DE",
]
GLOBAL = ["SPY", "QQQ", "GLD", "SLV", "COPX", "NIKL", "KWEB", "SOXX"]
GROUPS = {"CN": CN, "EU": EU, "GLOBAL": GLOBAL}
SECTORS = {
    "EXV3.DE": "Technology", "EXV2.DE": "Telecommunications", "EXH6.DE": "Media",
    "EXV5.DE": "Automobiles & Parts", "EXV9.DE": "Travel & Leisure", "EXH8.DE": "Retail",
    "EXH7.DE": "Personal & Household Goods", "EXH3.DE": "Food & Beverage", "EXH1.DE": "Oil & Gas",
    "EXV1.DE": "Banks", "EXH5.DE": "Insurance", "EXH2.DE": "Financial Services",
    "EXV4.DE": "Healthcare", "EXH4.DE": "Industrial Goods & Services", "EXV8.DE": "Construction & Materials",
    "EXV7.DE": "Chemicals", "EXV6.DE": "Basic Resources", "EXI5.DE": "Property", "EXH9.DE": "Utilities",
    "KSTR": "STAR Market 50 hard technology", "KTEC": "STAR Market 50 hard technology",
    "CNXT": "ChiNext growth & innovation", "159901.SZ": "ChiNext growth & innovation",
    "KGRN": "ESG leaders broad", "TCHI": "Growth leaders (technology-heavy)", "KWEB": "China internet",
    "CHIQ": "Consumer discretionary", "KURE": "Healthcare", "FXI": "Large-cap SOE value",
    "ASHR": "A-shares CSI 300 broad", "MCHI": "MSCI China all-cap broad", "CXSE": "Ex-SOE private sector broad",
    "ASHS": "A-shares CSI 500 mid & small", "510720.SS": "SOE dividend value", "513120.SS": "Hong Kong innovative medicines",
    "513320.SS": "Hang Seng New Economy broad", "513550.SS": "Hong Kong Connect 50 broad", "515220.SS": "Coal",
    "516310.SS": "Banks", "516620.SS": "Film & television media", "3167.HK": "New China sectors",
    "2841.HK": "Gaming & esports", "3189.HK": "Baijiu (white liquor)", "2820.HK": "Biotech",
    "2826.HK": "Cloud computing", "2809.HK": "Clean energy", "3134.HK": "Solar",
    "2845.HK": "Electric vehicles & battery", "2815.HK": "Little Giant (ChiNext/STAR innovative SMEs)",
    "SPY": "US large-cap broad", "QQQ": "US Nasdaq 100 growth", "GLD": "Gold bullion",
    "SLV": "Silver bullion", "COPX": "Copper miners", "NIKL": "Nickel miners", "SOXX": "Semiconductors",
}
ALL = list(dict.fromkeys(CN + EU + GLOBAL))
_LOCK = threading.Lock()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS price_bars (
        ticker TEXT NOT NULL, date TEXT NOT NULL, close REAL NOT NULL,
        source TEXT NOT NULL, fetched_at REAL NOT NULL,
        PRIMARY KEY (ticker, date)
      );
      CREATE TABLE IF NOT EXISTS ticker_status (
        ticker TEXT PRIMARY KEY, status TEXT NOT NULL, rows INTEGER NOT NULL DEFAULT 0,
        as_of TEXT, error TEXT, updated_at REAL NOT NULL
      );
    """)
    return db


def _download(ticker: str) -> tuple[str, list[tuple[str, float]], str | None]:
    try:
        bars = yahoo.provider.bars(ticker, rng="2y", interval="1d")
        rows = []
        for ts, close in zip(bars.timestamps, bars.close):
            if close is None or not math.isfinite(float(close)):
                continue
            date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
            rows.append((date, float(close)))
        if not rows:
            return ticker, [], "empty Yahoo daily history"
        return ticker, rows, None
    except (ProviderError, OSError, ValueError) as exc:
        return ticker, [], str(exc)[:500]


def refresh(force: bool = False, workers: int = 4) -> dict:
    """Fetch the fixed universe with bounded concurrency and serial writes."""
    with _LOCK:
        db = _connect()
        try:
            known = {r[0]: r[1] for r in db.execute("SELECT ticker,status FROM ticker_status")}
            todo = ALL if force else [t for t in ALL if known.get(t) not in ("complete", "unavailable")]
            if not todo:
                return status(db)
            with ThreadPoolExecutor(max_workers=max(1, min(int(workers), 6)), thread_name_prefix="rota-raw") as pool:
                futures = [pool.submit(_download, ticker) for ticker in todo]
                for future in as_completed(futures):
                    ticker, rows, error = future.result()
                    now = time.time()
                    if rows:
                        db.execute("DELETE FROM price_bars WHERE ticker=?", (ticker,))
                        db.executemany("INSERT INTO price_bars VALUES (?,?,?,?,?)",
                                       [(ticker, d, close, "yahoo", now) for d, close in rows])
                        db.execute("INSERT OR REPLACE INTO ticker_status VALUES (?,?,?,?,?,?)",
                                   (ticker, "complete", len(rows), rows[-1][0], None, now))
                    else:
                        db.execute("INSERT OR REPLACE INTO ticker_status VALUES (?,?,?,?,?,?)",
                                   (ticker, "unavailable", 0, None, error, now))
                    db.commit()
            return status(db)
        finally:
            db.close()


def status(db: sqlite3.Connection | None = None) -> dict:
    own = db is None
    db = db or _connect()
    try:
        rows = list(db.execute("SELECT status,COUNT(*),COALESCE(SUM(rows),0) FROM ticker_status GROUP BY status"))
        counts = {r[0]: {"tickers": r[1], "rows": r[2]} for r in rows}
        return {"universe": len(ALL), "counts": counts, "last_refresh": max((r[0] for r in db.execute("SELECT updated_at FROM ticker_status")), default=None)}
    finally:
        if own:
            db.close()


def _pearson(a: list[float], b: list[float]) -> float | None:
    if len(a) < 3 or len(a) != len(b):
        return None
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    da = [x - ma for x in a]; db = [x - mb for x in b]
    den = math.sqrt(sum(x*x for x in da) * sum(x*x for x in db))
    return round(sum(x*y for x, y in zip(da, db)) / den, 4) if den else None


def snapshot(period: int = 30, group: str = "ALL") -> dict:
    period = int(period) if int(period) in PERIODS else 30
    group = group.upper() if group.upper() in {"CN", "EU", "GLOBAL", "ALL"} else "ALL"
    db = _connect()
    try:
        selected = ALL if group == "ALL" else GROUPS[group]
        data = {}
        for ticker in selected:
            data[ticker] = [(d, float(c)) for d, c in db.execute(
                "SELECT date,close FROM price_bars WHERE ticker=? ORDER BY date DESC LIMIT 252", (ticker,))][::-1]
        series, returns = [], {}
        for ticker in selected:
            rows = data[ticker][-period:]
            if not rows: continue
            base = rows[0][1]
            series.append({"ticker": ticker, "sector": SECTORS.get(ticker, "Other"),
                           "points": [[d, round(c / base, 6)] for d, c in rows if base]})
            returns[ticker] = {d: (c / rows[i-1][1] - 1.0) for i, (d, c) in enumerate(rows) if i and rows[i-1][1]}
        matrix = []
        for left in selected:
            row = []
            for right in selected:
                common = sorted(set(returns.get(left, {})) & set(returns.get(right, {})))
                row.append(_pearson([returns[left][d] for d in common], [returns[right][d] for d in common]))
            matrix.append(row)
        return {"available": bool(series), "source": "Yahoo Finance chart daily closes", "provider": "yahoo",
                "version": DB_PATH.stat().st_mtime_ns,
                "period": period, "group": group, "updated_at": max((p[0] for rows in data.values() for p in rows), default=None),
                "status": status(db), "tickers": selected, "series": series, "correlation": {"tickers": selected, "rows": matrix}}
    finally:
        db.close()


if __name__ == "__main__":
    print(json.dumps(refresh(force=True), indent=2))
