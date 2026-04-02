
You are refactoring a Python repo named `py-earnings-calls-m`.

Your job is to align the repo with the canonical files in `docs/` and the instructions in `AGENTS.md`.

Do not preserve placeholder behavior if it conflicts with:

- correctness,
- bounded concurrency,
- explicit provider separation,
- canonical storage contracts,
- transcript-history-via-bulk-import strategy,
- forecast-history-via-snapshot strategy.

Repository facts:

- Transcript history should start with bulk imports.
- Transcript freshness should come from explicit adapters.
- Forecast data is more naturally adapter/API-driven.
- Local-first lookup/API behavior is desired.
- Storage layout should be centered on earnings calls and forecast snapshots, not SEC archive paths.

Target outcomes:

1. Add reliable bulk transcript import from local datasets.
2. Add a Motley Fool transcript adapter with explicit URL-manifest driven backfill.
3. Add forecast adapters for Finnhub and FMP with snapshot persistence.
4. Build lookup artifacts for transcript calls and forecast points.
5. Expose a small FastAPI surface over local artifacts.
6. Keep monitor/reconciliation additive and offline-testable.

Constraints:

- Use Python.
- Keep changes readable and well-factored.
- Add or update tests for each substantive behavior change.
- Update docs in the same patch set.
- Prefer parquet for normalized outputs.
- Keep transcript parsing serial unless profiling shows a reason to parallelize it.
- Treat provider-specific quota and failure handling explicitly.

Deliver work in phases and explain tradeoffs briefly in commit-style summaries.
