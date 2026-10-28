# BOB / QuantPilot source-faithful migration plan

## Decision

Migrate the cloned `Ben05-sys/QuantPilot` application itself, one source file at a time, rather than wrapping its demo or replacing its HTTP server. The source commit is `cc6afb5bad45715a7395fcccb09d786d8a1c160d`. The source `app/` package, `pilot.py`, SQLite store, provider modules, SSE implementation, and original `app/web/index.html` are the runtime.

The managed website name remains BOB at project level, while the source terminal remains visually and behaviourally QuantPilot. No source tab, route, provider contract, or terminal scaffold is redesigned.

## Data boundary

Yahoo remains the primary live quote and chart provider, using the source's crumb, websocket, caching, lag, and extended-hours logic. The cloned global-stock-data source was used only to implement a narrow fallback adapter behind the existing `Quote` provider boundary: Eastmoney push2 is tried first for a missing Yahoo quote, then Sina `hq.sinajs` is tried. Fallback responses carry their provider name; failures remain unavailable. No fixtures, seeded prices, replay shims, or browser-side provider calls are used by the runtime.

The source's existing Nasdaq, SEC, CBOE, Yahoo news, calendars, options, SQLite store, and SSE paths remain intact. F3 now has a bounded 30-second `/api/quotes` repaint for its displayed canvas cells because root-cause tracing showed the original map was snapshot-only. F11 `OPTNS` uses the existing `app/options.py` CBOE delayed chain one ticker at a time. No MongoDB is required; the source already persists snapshots and HTTP cache in SQLite. A future historical read model would use SQL, not both SQL and MongoDB.

## Managed runtime adapter

The only deployment-specific changes are: honour the configured host in `app/server.py`, permit the managed proxy when the app binds `0.0.0.0`, expose `/health` and `/manus-routes.json`, and run `pilot.py` in a Python 3.11 container on port 3000. The source's loopback defaults remain unchanged for local use.

## Structure

- `app/`: cloned QuantPilot application and original modules.
- `app/web/index.html`: original terminal UI.
- `app/providers/fallbacks.py`: small Yahoo-missing Eastmoney/Sina adapter.
- `app/options.py`: existing delayed CBOE chain shaping, exposed through the source server.
- `pilot.py`: cloned launcher.
- `Dockerfile`: Python container entrypoint only.
- `public/manus-routes.json`: managed page declaration.
- `DATA_SOURCE_AUDIT.md`: source and provider evidence.

## Verification

Run source syntax checks, the source server tests where their documented local fixtures are required, `git diff --check`, inspect the original UI route map, start on port 3000, verify `/`, `/health`, `/manus-routes.json`, `/api/status`, `/api/filters`, `/api/screen`, `/api/events`, and confirm Yahoo-first fallback attribution without synthetic values.
