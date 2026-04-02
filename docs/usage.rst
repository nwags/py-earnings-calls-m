Usage
=====

CLI help:

.. code-block:: bash

   py-earnings-calls --help

Common bootstrap commands:

.. code-block:: bash

   py-earnings-calls refdata refresh
   py-earnings-calls transcripts import-bulk --dataset ./data/motley_fool_kaggle.csv
   py-earnings-calls lookup refresh

Issuer refdata refresh examples:

.. code-block:: bash

   # Fetch official SEC issuer reference files into refdata/sec_sources/.
   py-earnings-calls refdata fetch-sec-sources

   # Build issuers.parquet from sec_sources plus any same-name overrides in refdata/inputs/.
   py-earnings-calls refdata refresh

   # Treat this CSV/parquet as the sole issuer input for this run.
   py-earnings-calls refdata refresh --universe ./data/issuer_universe.csv

Bulk adapter examples:

.. code-block:: bash

   py-earnings-calls transcripts import-bulk --adapter local_tabular --dataset ./data/transcripts.jsonl
   py-earnings-calls transcripts import-bulk --adapter motley_fool_pickle --dataset ./data/motley-fool-data.pkl

Transcript backfill examples:

.. code-block:: bash

   py-earnings-calls transcripts backfill --manifest ./data/motley_fool_backfill.csv
   py-earnings-calls transcripts backfill --manifest ./data/motley_fool_backfill.jsonl
   py-earnings-calls transcripts backfill --url https://www.fool.com/earnings/call-transcripts/example/

Transcript datetime audit + selective re-backfill:

.. code-block:: bash

   # Read-only audit for fetched Motley Fool transcript rows.
   py-earnings-calls transcripts audit-datetime --provider motley_fool --limit 100

   # Export suspect rows as backfill-safe CSV manifest.
   py-earnings-calls transcripts audit-datetime --provider motley_fool --limit 100 --write-manifest ./data/motley_fool_datetime_suspects.csv

   # Selectively re-backfill only suspect rows.
   py-earnings-calls transcripts backfill --manifest ./data/motley_fool_datetime_suspects.csv

   # Refresh lookup artifacts after correction runs.
   py-earnings-calls lookup refresh

Motley Fool fetched transcript datetime precedence:

- transcript-visible call datetime
- transcript-structured metadata datetime
- article publish datetime fallback
- otherwise no datetime inferred

Transcript API query examples:

.. code-block:: bash

   curl "http://localhost:8000/transcripts?ticker=AAPL&start=2024-01-01&end=2024-03-31&limit=50&offset=0"
   curl "http://localhost:8000/transcripts?cik=0000320193"
   curl "http://localhost:8000/transcripts?ticker=AAPL&cik=0000320193"

Narrow provider-aware read resolution examples:

.. code-block:: bash

   # Local-first default (no remote side effects unless explicitly requested)
   curl "http://localhost:8000/transcripts/c1?resolution_mode=local_only"

   # Explicit resolve-if-missing on call-id scoped transcript read
   curl "http://localhost:8000/transcripts/c1?resolution_mode=resolve_if_missing"
   curl "http://localhost:8000/transcripts/c1/content?resolution_mode=resolve_if_missing"

   # Deterministic forecast snapshot path (provider + symbol + as_of_date)
   curl "http://localhost:8000/forecasts/snapshots/finnhub/AAPL/2026-03-26?resolution_mode=resolve_if_missing"

   # Derived issuer-centric forecast read (local-first, no remote side effects)
   curl "http://localhost:8000/forecasts/by-cik/0000320193"
   curl "http://localhost:8000/forecasts/by-cik/0000320193?as_of_date=2026-03-26&limit=100&offset=0"

Date semantics:
- ``start`` is inclusive from 00:00:00 of the provided day
- ``end`` is inclusive through 23:59:59.999999 of the provided day

Run local API:

.. code-block:: bash

   make run-api

Forecast refresh examples:

.. code-block:: bash

   py-earnings-calls forecasts refresh-daily --provider-mode single --provider finnhub --date 2026-03-26 --symbol AAPL
   py-earnings-calls forecasts refresh-daily --provider-mode fallback --provider-priority finnhub --provider-priority fmp --date 2026-03-26 --symbol AAPL --symbol MSFT

Operator resolution examples:

.. code-block:: bash

   py-earnings-calls resolve transcript --call-id c1 --resolution-mode resolve_if_missing
   py-earnings-calls resolve forecast-snapshot --provider finnhub --symbol AAPL --date 2026-03-26 --resolution-mode resolve_if_missing

Storage layout migration and verification:

.. code-block:: bash

   # Plan migration (no writes)
   py-earnings-calls storage migrate-layout --dry-run

   # Apply copy-first migration to archive layout
   py-earnings-calls storage migrate-layout

   # Verify archive layout coverage and manifest state
   py-earnings-calls storage verify-layout

   # Plan safety-checked legacy cleanup
   py-earnings-calls storage cleanup-legacy --dry-run

   # Apply safety-checked legacy cleanup
   py-earnings-calls storage cleanup-legacy

Monitor examples:

.. code-block:: bash

   py-earnings-calls monitor poll --date 2026-03-27
   py-earnings-calls monitor poll --date 2026-03-27 --warm
   py-earnings-calls monitor loop --date 2026-03-27 --interval-seconds 30 --max-iterations 5 --warm

Reconciliation examples:

.. code-block:: bash

   py-earnings-calls reconcile run --date 2026-03-27
   py-earnings-calls reconcile run --date 2026-03-27 --catch-up-warm

Bootstrap helper script:

.. code-block:: bash

   ./scripts/bootstrap.sh
