"""Read-only bridge for SQLite output produced by the supplied FF_Rotation.cpp."""
from __future__ import annotations

import sqlite3
import os
import math
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.environ.get("FF_ROTATION_DB", Path(__file__).with_name("ff_rotation.db")))
REGIONS = ("cn", "us", "eu")
TABLES = [
    "factor_momentum_global",
    *(f"xsec_mean_beta_{r}" for r in REGIONS),
    *(f"spearman_rho_{r}" for r in REGIONS),
    *(f"pearson_rho_{r}" for r in REGIONS),
]
COLUMNS = {
    "factor_momentum": ("date", "horizon", "factor", "pct"),
    "xsec_mean_beta": ("date", "horizon", "factor", "beta"),
    "spearman_rho": ("date", "region", "ticker", "lookback", "factor", "rho"),
    "pearson_rho": ("date", "ticker", "lookback", "rho"),
}
_CACHE_VERSION = None
_CACHE_PAYLOAD = None
CHART_LOOKBACKS = (28, 62, 128, 256)
SECTOR_BY_TICKER = {
    "XLK":"Technology", "XLC":"Communication Services", "XLY":"Consumer Discretionary",
    "XLP":"Consumer Staples", "XLE":"Oil & Gas", "XLF":"Financials", "XLV":"Healthcare",
    "XLI":"Industrials", "XLB":"Materials", "XLRE":"Property", "XLU":"Utilities",
    "EXV3.DE":"Technology", "EXV2.DE":"Telecommunications", "EXH6.DE":"Media",
    "EXV5.DE":"Automobiles & Parts", "EXV9.DE":"Travel & Leisure", "EXH8.DE":"Retail",
    "EXH7.DE":"Personal & Household Goods", "EXH3.DE":"Food & Beverage", "EXH1.DE":"Oil & Gas",
    "EXV1.DE":"Banks", "EXH5.DE":"Insurance", "EXH2.DE":"Financial Services",
    "EXV4.DE":"Healthcare", "EXH4.DE":"Industrial Goods & Services", "EXV8.DE":"Construction & Materials",
    "EXV7.DE":"Chemicals", "EXV6.DE":"Basic Resources", "EXI5.DE":"Property", "EXH9.DE":"Utilities",
    "KSTR":"STAR Market 50 hard technology", "KTEC":"STAR Market 50 hard technology",
    "CNXT":"ChiNext growth & innovation", "159901.SZ":"ChiNext growth & innovation",
    "KGRN":"ESG leaders broad", "TCHI":"Growth leaders (technology-heavy)", "KWEB":"China internet",
    "CHIQ":"Consumer discretionary", "KURE":"Healthcare", "FXI":"Large-cap SOE value",
    "ASHR":"A-shares CSI 300 broad", "MCHI":"MSCI China all-cap broad", "CXSE":"Ex-SOE private sector broad",
    "ASHS":"A-shares CSI 500 mid & small", "510720.SS":"SOE dividend value", "513120.SS":"Hong Kong innovative medicines",
    "513320.SS":"Hang Seng New Economy broad", "513550.SS":"Hong Kong Connect 50 broad", "515220.SS":"Coal",
    "516310.SS":"Banks", "516620.SS":"Film & television media", "3167.HK":"New China sectors",
    "2841.HK":"Gaming & esports", "3189.HK":"Baijiu", "2820.HK":"Biotech", "2826.HK":"Cloud computing",
    "2809.HK":"Clean energy", "3134.HK":"Solar", "2845.HK":"Electric vehicles & battery",
    "2815.HK":"Little Giant", "SPY":"US large-cap broad", "QQQ":"US Nasdaq 100 growth",
    "GLD":"Gold bullion", "SLV":"Silver bullion", "COPX":"Copper miners", "NIKL":"Nickel miners",
}


def _bubble_charts(db: sqlite3.Connection, existing: set[str]) -> list[dict]:
    """Build compact chart payloads from the committed C++ output only."""
    if "spearman_rho" not in existing:
        return []
    charts = []
    for lookback in CHART_LOOKBACKS:
        latest = db.execute(
            "SELECT MAX(date) FROM spearman_rho WHERE lookback = ?", (lookback,)
        ).fetchone()[0]
        if latest is None:
            charts.append({"lookback": lookback, "points": [], "factors": []})
            continue
        factor_rows = db.execute(
            "SELECT factor, AVG(pct) FROM ("
            "SELECT factor,pct FROM factor_momentum_cn WHERE date=? AND horizon=? UNION ALL "
            "SELECT factor,pct FROM factor_momentum_us WHERE date=? AND horizon=? UNION ALL "
            "SELECT factor,pct FROM factor_momentum_eu WHERE date=? AND horizon=?) "
            "GROUP BY factor ORDER BY factor",
            (latest, lookback, latest, lookback, latest, lookback),
        ).fetchall()
        factor_returns = {str(factor): pct for factor, pct in factor_rows if pct is not None}
        raw = db.execute(
            "SELECT region,ticker,factor,rho FROM spearman_rho "
            "WHERE date=? AND lookback=? AND rho IS NOT NULL ORDER BY factor,ticker,region",
            (latest, lookback),
        ).fetchall()
        points = []
        factor_rhos: dict[str, list[float]] = {}
        ticker_rhos: dict[str, list[float]] = {}
        ticker_factors: dict[str, list[str]] = {}
        for region, ticker, factor, rho in raw:
            factor = str(factor)
            if factor not in factor_returns:
                continue
            rho = float(rho)
            factor_rhos.setdefault(factor, []).append(rho)
            ticker_rhos.setdefault(str(ticker), []).append(rho)
            ticker_factors.setdefault(str(ticker), []).append(factor)
            points.append({"region": region, "ticker": ticker, "sector": SECTOR_BY_TICKER.get(str(ticker), "Other"), "factor": factor,
                           "rho": rho, "return": float(factor_returns[factor]),
                           "size": max(18.0, min(180.0, abs(rho) * 135.0))})
        ticker_pearson = {}
        for ticker, values in ticker_rhos.items():
            returns = [float(factor_returns[factor]) for factor in ticker_factors[ticker]]
            ticker_pearson[ticker] = _pearson(values, returns)
        for point in points:
            point["pearson"] = ticker_pearson.get(str(point["ticker"]))
        factor_pearsons: dict[str, list[float]] = {}
        for point in points:
            if point["pearson"] is not None:
                factor_pearsons.setdefault(str(point["factor"]), []).append(float(point["pearson"]))
        factors = [{"factor": factor, "rho": sum(rhos) / len(rhos),
                    "return": float(factor_returns[factor]),
                    "pearson": (sum(factor_pearsons[factor]) / len(factor_pearsons[factor])
                                if factor_pearsons.get(factor) else None),
                    "size": max(70.0, min(260.0, abs(float(factor_returns[factor])) * 28.0))}
                   for factor, rhos in factor_rhos.items() if rhos]
        charts.append({"lookback": lookback, "date": latest, "points": points,
                       "factors": factors})
    return charts


def _descriptor(name: str) -> dict:
    prefix = "spearman_rho" if name.startswith("spearman_rho_") else name.rsplit("_", 1)[0]
    return {"name": name, "columns": list(COLUMNS[prefix]), "rows": [],
            "region": name.rsplit("_", 1)[-1]}


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) < 3 or len(left) != len(right):
        return None
    lm, rm = sum(left) / len(left), sum(right) / len(right)
    ld = [v - lm for v in left]
    rd = [v - rm for v in right]
    den = math.sqrt(sum(v * v for v in ld) * sum(v * v for v in rd))
    return round(sum(a * b for a, b in zip(ld, rd)) / den, 6) if den else None


def _pearson_rows(db: sqlite3.Connection, region: str, latest: str) -> list[list]:
    """Correlate six per-ticker Spearman values with six regional returns."""
    momentum = {}
    for factor, pct in db.execute(
        f"SELECT factor,pct FROM factor_momentum_{region} WHERE date=?", (latest,)
    ):
        for horizon in CHART_LOOKBACKS:
            row = db.execute(
                f"SELECT pct FROM factor_momentum_{region} WHERE date=? AND horizon=? AND factor=?",
                (latest, horizon, factor),
            ).fetchone()
            if row and row[0] is not None:
                momentum.setdefault(horizon, {})[factor] = float(row[0])
    out = []
    for horizon in CHART_LOOKBACKS:
        factor_returns = momentum.get(horizon, {})
        tickers = db.execute(
            "SELECT ticker FROM spearman_rho WHERE date=? AND region=? AND lookback=? GROUP BY ticker",
            (latest, region, horizon),
        ).fetchall()
        for (ticker,) in tickers:
            pairs = db.execute(
                "SELECT factor,rho FROM spearman_rho WHERE date=? AND region=? AND lookback=? AND ticker=? AND rho IS NOT NULL",
                (latest, region, horizon, ticker),
            ).fetchall()
            factors = [str(f) for f, rho in pairs if str(f) in factor_returns and rho is not None]
            rho_values = [float(rho) for f, rho in pairs if str(f) in factor_returns and rho is not None]
            return_values = [factor_returns[f] for f in factors]
            rho = _pearson(rho_values, return_values)
            if rho is not None:
                out.append([latest, ticker, horizon, rho])
    return out


def snapshot() -> dict:
    global _CACHE_VERSION, _CACHE_PAYLOAD
    descriptors = [_descriptor(name) for name in TABLES]
    if not DB_PATH.exists():
        _CACHE_VERSION = None
        _CACHE_PAYLOAD = None
        return {"available": False, "source": "FF_Rotation.cpp SQLite output",
                "engine": "FF_Rotation.cpp", "version": None,
                "freshness": "unavailable", "tables": descriptors,
                "message": "No FF_Rotation SQLite output is available yet."}
    version = DB_PATH.stat().st_mtime_ns
    if _CACHE_VERSION == version and _CACHE_PAYLOAD is not None:
        return _CACHE_PAYLOAD
    try:
        with sqlite3.connect(DB_PATH) as db:
            existing = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table in descriptors:
                if table["name"] == "factor_momentum_global":
                    table["rows"] = [list(row) for row in db.execute(
                        "SELECT date,horizon,factor,AVG(pct) FROM ("
                        "SELECT date,horizon,factor,pct FROM factor_momentum_cn UNION ALL "
                        "SELECT date,horizon,factor,pct FROM factor_momentum_us UNION ALL "
                        "SELECT date,horizon,factor,pct FROM factor_momentum_eu) "
                        "GROUP BY date,horizon,factor ORDER BY date DESC,horizon LIMIT 500"
                    )]
                    continue
                source_name = "spearman_rho" if table["name"].startswith("spearman_rho_") else table["name"]
                if table["name"].startswith("pearson_rho_"):
                    latest = db.execute(
                        "SELECT MAX(date) FROM spearman_rho WHERE region=?", (table["region"],)
                    ).fetchone()[0]
                    table["rows"] = _pearson_rows(db, table["region"], latest) if latest else []
                    continue
                if source_name not in existing: continue
                quoted = '"' + source_name.replace('"', '""') + '"'
                if source_name == "spearman_rho":
                    table["rows"] = [list(row) for row in db.execute(
                        f"SELECT * FROM {quoted} WHERE region = ? ORDER BY date DESC LIMIT 500",
                        (table["region"],))]
                else:
                    table["rows"] = [list(row) for row in db.execute(
                        f"SELECT * FROM {quoted} ORDER BY 1 DESC LIMIT 500")]
            charts = _bubble_charts(db, existing)
        _CACHE_VERSION = version
        latest_dates = [db.execute(f"SELECT MAX(date) FROM {table}").fetchone()[0]
                        for table in ("spearman_rho", "factor_momentum_cn", "factor_momentum_us", "factor_momentum_eu")]
        _CACHE_PAYLOAD = {"available": True, "source": "FF_Rotation.cpp SQLite output",
                          "engine": "FF_Rotation.cpp", "version": version,
                          "freshness": "cpp-output", "as_of": max((d for d in latest_dates if d), default=None),
                          "today": datetime.now(timezone.utc).date().isoformat(), "tables": descriptors,
                          "bubble_charts": charts}
        return _CACHE_PAYLOAD
    except sqlite3.Error as exc:
        return {"available": False, "source": "FF_Rotation.cpp SQLite output",
                "engine": "FF_Rotation.cpp", "version": version,
                "freshness": "error", "tables": descriptors, "message": str(exc)}
