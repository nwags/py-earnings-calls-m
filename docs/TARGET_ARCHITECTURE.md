
## Goal

Turn `py-earnings-calls-m` into a resumable ingestion engine for transcript history and forecast snapshots.

## Target package layout

```text
py_earnings_calls/
  cli.py
  config.py
  http.py
  rate_limit.py
  models.py
  filters.py
  lookup.py
  monitoring.py
  reconciliation.py
  runtime_output.py
  service_runtime.py
  refdata/
    __init__.py
    normalize.py
    schema.py
    sources.py
    builder.py
  adapters/
    __init__.py
    base.py
    transcripts_kaggle.py
    transcripts_motley_fool.py
    forecasts_finnhub.py
    forecasts_fmp.py
  api/
    __init__.py
    app.py
    models.py
    service.py
  pipelines/
    refdata_refresh.py
    transcript_import.py
    transcript_backfill.py
    forecast_refresh.py
    lookup_refresh.py
  storage/
    __init__.py
    paths.py
    writes.py
```

## Execution model

### Stage 1: Reference data refresh
Load or refresh issuer / symbol universe inputs into canonical parquet.

### Stage 2: Transcript history import
Import local bulk transcript datasets into normalized transcript call tables.

### Stage 3: Transcript freshness backfill
Use explicit adapters to fetch transcript pages and persist raw + parsed artifacts.
Persist transcript artifacts under archive domain root with `data/` + `full-index/` split.

### Stage 4: Forecast refresh
Use explicit provider adapters to fetch daily or targeted estimate snapshots.
Persist forecast raw snapshots under archive domain root with `data/` + `full-index/` split.

### Stage 5: Lookup refresh
Build deterministic local lookup artifacts from normalized transcript and forecast tables, including issuer-grouped derived forecast lookup by CIK.

### Stage 6: API local-first serving
Serve transcript metadata/content, transcript list queries (ticker/CIK/date range), latest forecasts, and derived forecast-by-CIK reads from local artifacts.
Allow explicit provider-aware `resolve_if_missing` only on narrow deterministic single-record read paths.

### Stage 7: Monitoring
Track incremental work through one-shot poll and bounded loop (`interval` + `max-iterations`), with durable seen-state and provider/update events.

### Stage 8: Reconciliation
Run one-shot reconciliation comparing expected freshness targets against local presence and snapshot recency, persisting discrepancies/events.

## Concurrency rules

- Never allow unbounded parallelism.
- Use one shared rate limiter per process.
- Make worker counts configurable independently from request budgets.
- Keep transcript parsing serial at first.
- Parallelize network I/O before parsing.

## HTTP rules

- Use one declared contactable user agent.
- Reuse sessions.
- Retry only transient failures.
- Capture explicit failure reasons.
- Never persist obvious error pages as transcripts.

## Compatibility policy

Backward compatibility is nice but optional.
Correctness, observability, and clear source boundaries matter more.
