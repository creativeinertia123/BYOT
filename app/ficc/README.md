# commodities_board

Reproduces the **live quotes table** from https://tradingeconomics.com/commodities —
all 95 commodities across Energy, Metals, Agricultural, Industrial, Livestock
and Electricity — using [tedata](https://github.com/HelloThereMatey/tedata),
with a C++ distribution layer for tick-live fan-out.

## Architecture

```
 tradingeconomics.com                workers (Python, multiprocessing)
 /commodity/<slug> pages   <-----    8 x (one reused tedata browser each)
        |                                   |
   15-40 s/page, parallel           shard i::8 of the 95 list
        |                                   |
        +------------------->  commodity_quotes.db (SQLite, WAL)
                                          |  diffs (TCP, line protocol)
                                          v
                              ticker_server (C++, -O3, zero-dep)
                                          |  QUOTE broadcast, µs fan-out
                                   dashboard clients
                                   (late joiners get full SNAPSHOT)
```

## Why C++ can't replace the scraper (and what it does instead)

tedata's 15-40 s/page is dominated by **remote page load + chart-data fetch +
TE anti-scraping** — no language makes a remote server respond faster, and
Selenium cannot be driven faster from C++. The two real levers:

1. **Parallelism + driver reuse** (Python, `snapper.py`): 8 workers, one
   browser per worker reused across its shard. 95 pages x 15-40 s serial
   (24-63 min) becomes ~**2-5 min per full pass**.
2. **Distribution** (C++, `ticker_server.cpp`): once a pass lands, quote diffs
   fan out to any number of board clients in microseconds, with instant
   catch-up snapshot for late joiners. Zero dependencies, `g++ -O3` only.

The board is then refreshed every `--interval` seconds, and *feels* live.
Honest ceiling: Trading Economics itself updates many commodity rows only
daily (see the `Date` column on the site), so this is as tick-live as the
source permits. True exchange-grade tick latency would require a paid market
data feed, not scraping.

## Setup

```bash
pip install tedata selenium pandas

# 1. C++ distribution layer
g++ -O3 -std=c++17 -pthread ticker_server.cpp -o ticker_server
./ticker_server 7379 &

# 2. Fill the 55 commodity slugs the manifest can't guess (one-time,
#    uses tedata's search_TE; ~30-60 min, checkpoints every 5 items)
python snapper.py --resolve

# 3. Run the board
python snapper.py --loop --interval 120 --ticker-port 7379
```

A prebuilt Linux x86-64 `ticker_server` binary is included, but rebuild from
source on your own machine (`g++ -O3 -std=c++17 -pthread ticker_server.cpp -o ticker_server`).

## Board client (example)

```python
import socket
s = socket.create_connection(("127.0.0.1", 7379))
buf = b""
while True:
    buf += s.recv(4096)
    while b"
" in buf:
        line, buf = buf.split(b"
", 1)
        parts = line.decode().split()
        if parts[0] in ("SNAPSHOT", "QUOTE"):
            _, slug, price, chg, pct, ts = parts
            print(f"{slug:20s} {price:>12s} {chg:>10s} {pct:>7s}%  {ts}")
        elif parts[0] == "ENDSNAPSHOT":
            print("--- caught up, now live ---")
```

Wire that into any UI (terminal curses, Streamlit, web) — updates arrive as
`QUOTE` lines the instant a snapshot pass publishes them.

## Files

| File | Role |
|---|---|
| `commodities.json` | Manifest: 95 commodities (name, group, slug). 40 slugs pre-filled; run `--resolve` for the rest. |
| `snapper.py` | Parallel snapshotter: shards the list across reused tedata browsers, computes Price/Chg/%Chg/Weekly/Monthly/YTD/YoY exactly like the TE table, upserts SQLite, pushes diffs to the ticker. |
| `ticker_server.cpp` | Zero-dependency C++17 TCP pub/sub server: `PUB` from publishers, `QUOTE` broadcast to clients, `SNAPSHOT`+`ENDSNAPSHOT` catch-up, clean SIGINT/SIGTERM shutdown. |

## Operational notes

- **Workers**: `--workers 12` helps on fast machines/networks; beyond ~12 you
  mostly add TE-side load and ban risk. `--interval 120` is a sane default.
- **Failures**: per-commodity 3-attempt retries with driver rebuild on stale
  webdriver errors; failures are logged with reasons and reported in the
  summary (`ok`/`failed` counts).
- **Persistence**: `commodity_quotes.db` always holds the latest full snapshot,
  so the board renders even if the ticker server was down during a pass.
- Do not run the ticker server on a public interface without firewalling the
  PUB command (publishers are trusted).
