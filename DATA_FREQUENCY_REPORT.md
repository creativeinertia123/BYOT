# BOB data-frequency assessment

## Current QuantPilot timings

BOB uses several different freshness classes rather than one universal “live” interval.

- **Visible quote fallback:** the browser polls quiet visible rows every 3 seconds while the websocket is healthy; the server-side visible-quote cache is 2 seconds. When the stream is unavailable, this becomes the main quote path.
- **Websocket ticks:** Yahoo’s websocket is the primary path for subscribed viewport and watchlist symbols. The source records venue-timestamp-to-arrival latency. Its measured source delay was about 2.9 seconds median and 3.9 seconds at p90 in the source’s regular-hours observation. The stream is viewport-scoped, capped at 500 symbols, and drops ticks older than 15 minutes.
- **Bulk screener quotes:** the screen cache is 10 seconds. Screens up to 2,000 candidates can be re-priced from live quotes; wider screens explicitly use the snapshot path rather than pretending to be tick-fresh.
- **Universe and fundamentals:** the market clock wakes every 30 seconds, rolls 1,000 symbols every 90 seconds, and rebuilds the universe at session transitions plus one midday refresh. The boot staleness threshold is 12 hours. These are not tick data and should not be refreshed like quotes.
- **Charts:** intraday Yahoo chart requests bypass the HTTP cache; daily bars use a one-hour cache. This gives frequency according to the selected interval instead of treating daily history as real-time.
- **Strips and slower panels:** indices poll in the browser every 20 seconds, futures every 60 seconds, with server TTLs of 8 seconds during an active session and 60 seconds while closed. News uses roughly 120 seconds, live-news uses 240 seconds, and CBOE options responses are delayed and cached for several minutes.

The published header values that were removed were presentation noise, not a fix to the underlying data path. Status remains authoritative in the API and in the source freshness fields.

## OpenBB assessment

OpenBB is a provider-connection and normalisation layer, not a market-data host. Its current provider documentation says each package connects one source and that coverage, rate limits, and terms come from that source. The current provider list includes CBOE delayed data, Nasdaq, SEC, and other public or third-party providers. [OpenBB providers](https://docs.openbb.co/odp/python/extensions/providers) [OpenBB repository](https://github.com/openbq-org/OpenBB)

OpenBB would improve BOB’s **provider abstraction**, not automatically reduce Yahoo’s source delay. The bounded integration path would be a server-side adapter that maps OpenBB outputs into the existing `Quote`, `Bars`, and provenance fields. It should be used for historical bars, fundamentals, macro series, and a licensed provider selected for quotes. It should not replace the working Yahoo websocket until a provider with a defined websocket/SLA and acceptable terms is configured.

## teData assessment

`HelloThereMatey/tedata` scrapes Trading Economics chart pages with Selenium and BeautifulSoup. Its README says a reasonable series download takes about 15–30 seconds, its historical download is limited to 10 years after Trading Economics anti-scraping changes, and its recent work supports higher-frequency chart series such as commodities. [teData repository](https://github.com/HelloThereMatey/tedata)

teData is not suitable for BOB’s equity tick path. It is a useful optional source for macroeconomic indicators and commodity context, where minute-, hourly-, daily-, or monthly-frequency data is appropriate. It should run as an asynchronous, rate-limited enrichment job with source timestamps and a long TTL. It must never sit in the request path for visible quotes, websocket ticks, F3 heatmap percentages, or F11 options.

## Recommended frequency improvements

1. **Keep the current split by data class.** Do not shorten every timer. Preserve websocket ticks for subscribed symbols, three-second fallback polling for quiet rows, 90-second rolling re-quotes, and slower TTLs for news, calendars, fundamentals, and macro data.
2. **Add a licensed low-latency quote adapter before changing the UI.** Use OpenBB’s provider abstraction only if the selected provider supplies the required real-time or delayed contract. Preserve the measured `source`, `as_of`, `lag_seconds`, and `stale` fields so the UI can distinguish venue delay from application delay.
3. **Use an event-driven quote fan-out.** A single server-side feed subscription should publish to the viewport and watchlist through the existing SSE path. Do not create one upstream request per browser tab or per heatmap cell.
4. **Keep F3 on the existing bounded refresh.** The map now refreshes up to 400 displayed cells every 30 seconds. If a higher-frequency provider is added, reduce this only after measuring provider cost and rate limits; a 5–15 second map refresh is a reasonable next experiment, not a default promise.
5. **Add OpenBB for slower enrichment first.** Historical bars, fundamentals, SEC/Nasdaq data, macro series, and provider comparison are lower-risk integration points. Use a feature flag and shadow comparison before making it primary.
6. **Add teData only as an asynchronous macro/commodity connector.** Store its series frequency and source timestamp, refresh on the series’ natural cadence, and expose the result as contextual data rather than a quote.

The highest-value improvement is therefore a licensed websocket or streaming provider with an explicit latency contract, not scraping more aggressively. OpenBB can make that provider swappable; teData can broaden macro context; neither one by itself turns the current Yahoo feed into exchange-direct data.
