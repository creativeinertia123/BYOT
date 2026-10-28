## ROTA API and beta repair
- [x] Confirm the active ROTATION timer runs every 60 seconds only while MCRO is visible.
- [x] Confirm no initial or unchanged yellow flash; flash occurs only after the authoritative C++ SQLite version/signature changes.
- [x] Replace regional factor-momentum cards with one `factor_momentum_global` table averaging CN, US, and EU rows.
- [x] Diagnose `xsec_mean_beta_us` NULL values as invalid/singular OLS systems; reject non-finite/singular solves and average only finite valid fits.
- [x] Verify local `/api/rotation`: seven tables, 500 global factor rows, and 500 non-null US beta rows.

## ROTARAW Yahoo raw universe
- [x] Add ROTARAW immediately to the right of ROTATION under F12 MCRO.
- [x] Migrate the supplied dumb_china_factor universe into persisted Yahoo daily-close storage, including the requested SOXX addition and the supplied EU tickers.
- [x] Provide period selectors for 7D, 14D, 30D, 60D, 90D, and 252D with normalised performance and return-correlation views.
- [x] Preserve bounded Yahoo ingestion: four workers, serial SQLite commits, per-ticker terminal status, and no fabricated fallback values.
- [x] Preserve the existing 60-second MCRO refresh; ROTARAW flashes only when its persisted raw-cache version changes.
- [x] Configure the 16:00 Asia/Shanghai daily Yahoo refresh session.

## FXECON Freox patch migration
- [x] Add an ECON label and FXECON subtab immediately to the right of ROTACHRTS under F12 MCRO.
- [x] Migrate the patch's HOPE country/currency taxonomy and full return ladder into the existing BYOT terminal rather than replacing the Streamlit scaffold.
- [x] Prioritise the QuantPilot Yahoo chart adapter; preserve a narrow open.er-api daily fallback and explicit unavailable/freshness states.
- [x] Persist minute ticks and daily closes in `app/ficc/fx_econ.db`; retain the existing 60-second MCRO refresh and flash only when returned values change.

## HOME HOPE calendar
- [x] Move HOPE out of FXECON into HOME immediately to the right of FXECON.
- [x] Add firing, unemployment, hiring, housing, pmi, ppi, and cpi as HOME sub-subtabs.
- [x] Remove the `24 monitored countries` text from the FX board.
- [x] Parse and persist point-in-time Trading Economics calendar rows with Asia/Shanghai local timestamps and UTC equivalents; retain the existing 60-second tab refresh and 30-minute source TTL.

## MCRO date and Pearson correction
- [x] Preserve the C++ SQLite source date, add a dynamic TODAY display, and expose three derived Pearson rho tables for CN, US, and EU.
- [x] Move ROTARAW ticker legends below the chart, remove TRADING DATE and completion/provider text, and add latest Y-M-D.
- [x] Plot Pearson rho on the ROTACHRTS Y axis and add latest Y-M-D beside COUNTRY.
- [x] Remove the requested FXECON status/source text and SOURCE column; add latest Y-M-D.
- [x] Remove the HOME Trading Economics source text and rename visible HOPE labels to HOME.
