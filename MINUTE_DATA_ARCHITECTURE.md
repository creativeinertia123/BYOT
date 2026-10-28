# Five-year minute-bar ROTA feasibility

## Finding

The originally requested five-year, 60-second dataset cannot be obtained from the current Yahoo-backed QuantPilot path as specified. A direct test of the Yahoo chart endpoint with `SPY`, `range=5y`, and `interval=1m` was rate-limited with HTTP 429. More importantly, Yahoo's published historical-download documentation says availability depends on the instrument and licensing; it does not establish five years of one-minute history for 5,259 symbols. The current QuantPilot F6 store contains seven point-in-time universe snapshots and an empty `bars` table. It is not a historical minute feed.

The approved bounded change is therefore five-year daily history from the existing Yahoo chart path, with a per-ticker three-year fallback when a five-year response is empty or materially short. The isolated latest-F6 test for ticker `A` returned 1,255 daily observations from 2021-10-25 through 2026-10-23. Full ingestion is running separately and is not treated as complete until its manifest and row counts are verified.

A five-year daily estimate is roughly 1,260 rows per symbol. For 5,259 F6-US symbols plus roughly 90 ETF Y assets, that is about 6.75 million rows before corrections and metadata. This is materially smaller than the rejected minute design, but still needs checkpointed ingestion and a durable data location rather than an ephemeral WebDev `/tmp` artefact.

Daily ETF data must not be silently forward-filled into minute bars. That would create synthetic intraday observations and false regression frequency. If an ETF has no licensed one-minute history, it should be stored with `frequency=1d` and `is_resampled=1` only for clearly labelled display or separate low-frequency analysis. It should be excluded from a genuine minute-frequency regression unless the user explicitly approves the synthetic transformation.

## Proposed storage boundary

Keep the existing SQLite database for QuantPilot application metadata, universe snapshots, job state, manifests, and data-quality records. Do not put billions of minute bars into the current SQLite `bars` table.

Use partitioned Parquet as the data plane, partitioned by `asset_class`, `symbol_bucket`, `year`, and `month`, with columns `symbol`, `ts_utc`, `open`, `high`, `low`, `close`, `volume`, `source`, `frequency`, `as_of`, and `is_resampled`. Use DuckDB as the SQL query layer over Parquet. DuckDB can read and write Parquet through SQL and push filters and projections into the files. The application can retain SQLite as the control plane while DuckDB queries the Parquet data plane.

A manifest table should track each partition and source response: symbol, interval, earliest timestamp, latest timestamp, row count, source, retrieval time, provider status, checksum, and completeness. A failed or rate-limited partition must remain unavailable rather than being filled with copied values. Ingestion should be idempotent on `(symbol, interval, ts_utc, source)` and write temporary files followed by atomic rename.

## Required decision before implementation

A licensed historical intraday provider or an existing five-year archive must be selected before the ingestion job is written. The current Yahoo endpoint is not evidence of five-year one-minute availability, and a bulk request for 5,259 symbols would be both operationally excessive and potentially outside provider terms. The implementation can then add a bounded backfill worker, checkpointed batches, rate-limit handling, Parquet compaction, and a F6-to-ROTA manifest bridge without changing the QuantPilot UI scaffold.

The first safe implementation milestone is a provider-agnostic storage layer plus a one-symbol/one-ETF acceptance fixture sourced from the approved provider. It should prove minute timestamp normalisation, Parquet round-trip queries, SQLite manifest updates, gap detection, restartability, and a real-data freshness report before scaling to 2.63 billion rows.

## Sources

[1]: https://help.yahoo.com/kb/SLN2311.html "Yahoo Finance historical data help"
[2]: https://parquet.apache.org/ "Apache Parquet project"
[3]: https://duckdb.org/docs/lts/data/parquet/overview.html "DuckDB Parquet documentation"
