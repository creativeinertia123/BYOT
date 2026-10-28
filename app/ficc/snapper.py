#!/usr/bin/env python3
"""
snapper.py — parallel Trading Economics commodity quote snapshotter.

Reproduces the live-quotes table from https://tradingeconomics.com/commodities
(Energy / Metals / Agricultural / Industrial / Livestock / Electricity) for
every commodity listed there, using tedata (highcharts_api method).

Performance model
-----------------
Serial tedata scrape:        15-40 s per page  ->  95 pages = 24-63 min
This snapper:
  * W parallel worker PROCESSES (default 8), each owning a shard of the list
  * ONE browser per worker, reused across its pages (tedata supports reuse;
    kills per-page driver-init overhead — the biggest serial cost)
  * => full snapshot in ~2-5 min depending on TE response times.

Then diffs are pushed to the C++ ticker_server (see ticker_server.cpp) which
fans out quote updates to board clients in microseconds with full catch-up
snapshot for late joiners. That distribution layer is what makes the board
feel "tick-live"; the source data itself is only as fresh as Trading
Economics updates it (many commodity rows refresh daily).

Setup
-----
    pip install tedata selenium pandas
    python snapper.py --resolve        # one-time: fill missing slugs via tedata search
    g++ -O3 -std=c++17 -pthread ticker_server.cpp -o ticker_server

Run
---
    ./ticker_server 7379 &             # C++ distribution layer
    python snapper.py --loop --interval 120 --ticker-port 7379
    python snapper.py                  # single snapshot pass (no loop)
    python snapper.py --workers 12     # more parallel browsers
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import queue
import socket
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
COMMODITIES_JSON = BASE / "commodities.json"
DB_PATH = BASE / "commodity_quotes.db"
LOG_FILE = BASE / "snapper.log"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"),
                              logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("snapper")

DRIVER_DEAD = ("stale", "connection", "marionette", "session", "decode")


# ------------------------------------------------------------------ fetching

def asof_pct(s, target_date) -> float | None:
    """Percent change from the last observation at/before target_date to last."""
    if s.empty:
        return None
    ref = s[s.index <= target_date]
    if ref.empty:
        return None
    base = ref.iloc[-1]
    return round((s.iloc[-1] / base - 1) * 100, 2) if base else None


def build_row(slug: str, name: str, group: str, series, meta: dict) -> dict:
    series = series.dropna().sort_index()
    last, prev = series.iloc[-1], (series.iloc[-2] if len(series) > 1 else None)
    last_date = series.index[-1]
    chg = round(float(last - prev), 6) if prev is not None else None
    pct = round(float((last / prev - 1) * 100), 2) if prev else None
    return {
        "slug": slug, "name": name, "group": group,
        "units": meta.get("units", ""),
        "price": round(float(last), 6), "chg": chg, "pct_chg": pct,
        "weekly": asof_pct(series, last_date - timedelta(days=5)),
        "monthly": asof_pct(series, last_date - timedelta(days=21)),
        "ytd": asof_pct(series, datetime(last_date.year, 1, 1)),
        "yoy": asof_pct(series, last_date - timedelta(days=365)),
        "ts": last_date.isoformat(),
    }


def fetch_shard(shard: list, out: mp.Queue, worker_id: int) -> None:
    """Worker: one reused tedata browser scrapes its shard of commodities."""
    import tedata as ted

    scr = None
    for slug, name, group in shard:
        row, err = None, None
        for attempt in range(1, 4):
            try:
                if scr is None:
                    scr = ted.TE_Scraper()  # fresh driver only when needed
                url = f"https://tradingeconomics.com/commodity/{slug}"
                try:
                    scraped = ted.scrape_chart(scraper=scr, URL=url)
                except TypeError:
                    scraped = ted.scrape_chart(URL=url)  # older tedata API
                series = scraped.series
                if series is None or series.dropna().empty:
                    raise ValueError("empty series")
                row = build_row(slug, name, group, series,
                                getattr(scraped, "metadata", {}) or {})
                break
            except Exception as exc:
                err = str(exc)
                if any(k in err.lower() for k in DRIVER_DEAD):
                    try:
                        scr.driver.quit()
                    except Exception:
                        pass
                    scr = None  # stale driver -> rebuild next attempt
                time.sleep(5 * attempt)
        out.put((slug, row, err))
        time.sleep(2)  # be polite between pages even within a worker


# ------------------------------------------------------------------ storage

def store_rows(rows: list[dict]) -> None:
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=10000")
        con.execute("CREATE TABLE IF NOT EXISTS commodity_quotes ("
                    "  slug TEXT PRIMARY KEY, name TEXT, grp TEXT, units TEXT,"
                    "  price REAL, chg REAL, pct_chg REAL,"
                    "  weekly REAL, monthly REAL, ytd REAL, yoy REAL,"
                    "  ts TEXT, updated_at TEXT NOT NULL)")
        now = datetime.now(timezone.utc).isoformat()
        con.executemany(
            "INSERT OR REPLACE INTO commodity_quotes VALUES"
            " (:slug,:name,:group,:units,:price,:chg,:pct_chg,"
            "  :weekly,:monthly,:ytd,:yoy,:ts,:updated_at)",
            [dict(r, updated_at=now) for r in rows])


def publish(rows: list[dict], host: str, port: int) -> None:
    """Push changed quotes to the C++ ticker server (best effort)."""
    try:
        with socket.create_connection((host, port), timeout=2) as s:
            for r in rows:
                units = (r["units"] or "-").replace(" ", "_")
                def f(v): return "-" if v is None else v
                s.sendall(f'PUB {r["slug"]} {r["group"]} {units} {f(r["price"])} '
                          f'{f(r["chg"])} {f(r["pct_chg"])} {r["ts"]}\n'.encode())
    except OSError:
        logger.warning("ticker server unreachable at %s:%d — board not updated "
                       "(quotes are still in the DB).", host, port)


# ------------------------------------------------------------------ resolver

def resolve_slugs() -> None:
    """Fill missing slugs in commodities.json using tedata's search_TE().

    One search per unresolved commodity name (one-time, ~30-60 min for the
    initial run); prefers results whose metric name matches exactly and
    whose URL is a /commodity/ page. Progress is saved per item, so reruns
    only process what is still missing.
    """
    import tedata as ted

    manifest = json.loads(COMMODITIES_JSON.read_text())
    todo = [c for c in manifest if not c.get("slug")]
    logger.info("Resolving %d missing slugs via tedata search...", len(todo))
    for i, c in enumerate(todo, 1):
        slug = None
        try:
            s = ted.search_TE()
            s.search_trading_economics(c["name"])
            tbl = getattr(s, "result_table", None)
            try:
                s.driver.quit()
            except Exception:
                pass
            if tbl is not None and len(tbl):
                cols = list(tbl.columns)
                c_metric = "metric" if "metric" in cols else cols[2]
                c_url = "url" if "url" in cols else cols[-1]
                want = norm(c["name"])
                for _, r in tbl.iterrows():
                    u = str(r[c_url])
                    if "/commodity/" in u and (norm(r[c_metric]) == want
                                               or want in norm(r[c_metric])):
                        slug = u.split("/commodity/")[-1].strip("/")
                        break
        except Exception as exc:
            logger.warning("[%d/%d] %s: search failed: %s", i, len(todo), c["name"], exc)
        c["slug"] = slug
        logger.info("[%d/%d] %-18s -> %s", i, len(todo), c["name"], slug or "UNRESOLVED")
        if i % 5 == 0:  # checkpoint the manifest periodically
            COMMODITIES_JSON.write_text(json.dumps(manifest, indent=2))
    COMMODITIES_JSON.write_text(json.dumps(manifest, indent=2))
    missing = [c["name"] for c in manifest if not c.get("slug")]
    logger.info("Resolution done. Still unresolved: %s", ", ".join(missing) or "none")


def norm(text: str) -> str:
    import re
    return re.sub(r"[^a-z]", "", (text or "").lower())


# ---------------------------------------------------------------------- main

def snapshot(args) -> dict:
    manifest = json.loads(COMMODITIES_JSON.read_text())
    targets = [c for c in manifest if c.get("slug")]
    skipped = [c["name"] for c in manifest if not c.get("slug")]
    if skipped:
        logger.warning("Skipping %d commodities with no slug (run --resolve): %s",
                       len(skipped), ", ".join(skipped))

    workers = max(1, min(args.workers, len(targets)))
    shards = [targets[i::workers] for i in range(workers)]
    logger.info("Snapshot: %d commodities across %d workers.", len(targets), workers)

    out: mp.Queue = mp.Queue()
    procs = [mp.Process(target=fetch_shard,
                        args=([(c["slug"], c["name"], c["group"]) for c in shard],
                              out, i),
                        daemon=True)
             for i, shard in enumerate(shards) if shard]
    t0 = time.time()
    for p in procs:
        p.start()

    rows, failed = [], []
    for _ in range(len(targets)):
        try:
            slug, row, err = out.get(timeout=300)
        except queue.Empty:
            logger.error("Timed out waiting for workers; partial snapshot.")
            break
        if row:
            rows.append(row)
        else:
            failed.append((slug, err))
            logger.error("%s: FAILED - %s", slug, err)

    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.terminate()

    if rows:
        store_rows(rows)
        publish(rows, args.ticker_host, args.ticker_port)
    logger.info("Snapshot done in %ds: %d ok, %d failed.",
                time.time() - t0, len(rows), len(failed))
    return {"ok": len(rows), "failed": len(failed),
            "failures": [f"{s}: {e}" for s, e in failed]}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workers", type=int, default=8,
                    help="parallel browser workers (default 8)")
    ap.add_argument("--interval", type=int, default=120,
                    help="loop interval seconds (default 120)")
    ap.add_argument("--loop", action="store_true", help="snapshot forever")
    ap.add_argument("--resolve", action="store_true",
                    help="fill missing commodity slugs via tedata search, then exit")
    ap.add_argument("--ticker-host", default="127.0.0.1")
    ap.add_argument("--ticker-port", type=int, default=7379)
    args = ap.parse_args()

    if args.resolve:
        resolve_slugs()
        return 0

    if not args.loop:
        snap = snapshot(args)
        return 0 if snap["ok"] else 1

    logger.info("Loop mode: snapshot every %ds. Ctrl+C to stop.", args.interval)
    try:
        while True:
            snap = snapshot(args)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
