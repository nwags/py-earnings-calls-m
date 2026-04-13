Usage
=====

Wave 1 migration note:

- ``docs/WAVE1_MIGRATION_NOTE.md``
- ``docs/WAVE2_MIGRATION_NOTE.md``
- ``docs/WAVE3_MIGRATION_NOTE.md``

CLI help:

.. code-block:: bash

   py-earnings-calls --help

Output mode notes:

- ``--quiet``: minimal human-readable summary
- ``--verbose``: bounded additive detail
- ``--summary-json`` (where available): machine output mode that takes precedence over ``--quiet`` / ``--verbose``

Runtime visibility notes (runtime-work commands):

- ``--log-level {debug,info,warning,error}``
- ``--log-file <path>``
- ``--progress-json``: compact NDJSON progress events on ``stderr``
- ``--progress-heartbeat-seconds FLOAT``: idle heartbeat interval; ``0`` disables

Stdout/stderr contract:

- ``--summary-json`` remains machine-clean on ``stdout``
- progress/runtime activity goes to ``stderr`` (and optional ``--log-file``)
- progress event schema is stable:
  - ``event``
  - ``phase``
  - ``elapsed_seconds``
  - ``counters``
  - ``detail``

Common bootstrap commands:

.. code-block:: bash

   py-earnings-calls refdata refresh
   py-earnings-calls transcripts import-bulk --dataset ./data/motley_fool_kaggle.csv
   py-earnings-calls lookup refresh

Output mode examples:

.. code-block:: bash

   py-earnings-calls lookup refresh --quiet
   py-earnings-calls lookup refresh --verbose
   py-earnings-calls storage verify-layout --summary-json --quiet

Service runtime wrapper:

.. code-block:: bash

   python -m py_earnings_calls.service_runtime api --host 127.0.0.1 --port 8000
   python -m py_earnings_calls.service_runtime api --summary-json
   python -m py_earnings_calls.service_runtime monitor-once --date 2026-03-27 --summary-json --progress-json
   python -m py_earnings_calls.service_runtime monitor-loop --date 2026-03-27 --interval-seconds 30 --max-iterations 5 --progress-json

Optional container wrapper (host-native remains first-class):

.. code-block:: bash

   docker build -t py-earnings-calls-m:local .

   docker run --rm -p 8000:8000 \
     -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
     -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
     -v "$(pwd)/refdata:/workspace/refdata" \
     -v "$(pwd)/data:/workspace/data" \
     py-earnings-calls-m:local \
     python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000

   docker compose up api

   # Optional monitor workflows through the same runtime surface
   docker run --rm \
     -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
     -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
     -v "$(pwd)/refdata:/workspace/refdata" \
     -v "$(pwd)/data:/workspace/data" \
     py-earnings-calls-m:local \
     python -m py_earnings_calls.service_runtime monitor-once --date 2026-03-27 --summary-json --progress-json

   docker run --rm \
     -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
     -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
     -v "$(pwd)/refdata:/workspace/refdata" \
     -v "$(pwd)/data:/workspace/data" \
     py-earnings-calls-m:local \
     python -m py_earnings_calls.service_runtime monitor-loop --date 2026-03-27 --interval-seconds 30 --max-iterations 5 --progress-json

Container env note:

- ``docker-compose.yml`` uses shell environment passthrough, so a local ``.env`` file is optional.
- Keep secrets external (for example, export ``FINNHUB_API_KEY`` / ``FMP_API_KEY`` in your shell).
- Default mounts are runtime-data-only: ``.earnings_cache/``, ``refdata/``, and ``data/``.
- Migration note: this is an additive optional wrapper; existing host-native runtime contracts are unchanged.

Dev-only editable source mount (optional, not default):

.. code-block:: bash

   docker run --rm -p 8000:8000 \
     -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
     -v "$(pwd):/app" \
     -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
     -v "$(pwd)/refdata:/workspace/refdata" \
     -v "$(pwd)/data:/workspace/data" \
     py-earnings-calls-m:local \
     python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000

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
   m-cache earnings providers list --summary-json
   m-cache earnings providers show --provider finnhub --summary-json
   m-cache earnings resolve transcript --call-id c1 --resolution-mode resolve_if_missing --summary-json
   m-cache earnings resolve forecast-snapshot --provider finnhub --symbol AAPL --date 2026-03-26 --resolution-mode resolve_if_missing --summary-json
   m-cache earnings aug list-types --summary-json
   m-cache earnings aug inspect-target --resource-family transcripts --call-id c1 --summary-json
   m-cache earnings aug inspect-runs --resource-family transcripts --call-id c1 --summary-json
   m-cache earnings aug inspect-artifacts --resource-family transcripts --call-id c1 --summary-json
   m-cache earnings aug target-descriptor --call-id c1 --summary-json
   m-cache earnings aug submit-run --input-json ./producer_run_submission.json --summary-json
   m-cache earnings aug submit-artifact --input-json ./producer_artifact_submission.json --summary-json

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
   py-earnings-calls monitor poll --date 2026-03-27 --summary-json --progress-json

Reconciliation examples:

.. code-block:: bash

   py-earnings-calls reconcile run --date 2026-03-27
   py-earnings-calls reconcile run --date 2026-03-27 --catch-up-warm

Bootstrap helper script:

.. code-block:: bash

   ./scripts/bootstrap.sh
