
## Primary objective

Turn the scaffold into a usable earnings transcript and forecast ingestion tool.

## Phase 1
- Confirm canonical storage layout.
- Confirm normalized parquet schemas.
- Confirm package naming / CLI naming.
- Add tests for config and path generation.

## Phase 2
- Finish transcript bulk import adapter(s):
  - Kaggle Motley Fool CSV/parquet loader
  - generic local CSV/JSONL loader
- Persist `transcript_calls.parquet` and `transcript_artifacts.parquet`.
- Add dedupe keys and idempotent writes.

## Phase 3
- Finish Motley Fool freshness adapter:
  - URL manifest input
  - HTML fetch
  - transcript extraction
  - raw/parsed persistence
- Add polite HTTP behavior, retries, and explicit failure reasons.

## Phase 4
- Finish forecast adapters:
  - Finnhub calendar + estimates workflow
  - FMP symbol estimate workflow
- Persist snapshot-level and point-level parquet artifacts.
- Add provider-priority and fallback controls.

## Phase 5
- Build lookup refresh/query artifacts:
  - `local_lookup_transcripts.parquet`
  - `local_lookup_forecasts.parquet`
- Add CLI query surfaces.

## Phase 6
- Add API foundation:
  - `GET /health`
  - `GET /transcripts/{call_id}`
  - `GET /transcripts/{call_id}/content`
  - `GET /forecasts/{symbol}/latest`
- Keep API local-first and additive.

## Phase 7
- Add monitor + reconciliation:
  - seen-state
  - event artifacts
  - bounded poll loop
  - discrepancy artifacts
- Keep tests offline via injectable adapter boundaries.

## Phase 8
- Harden packaging, docs, examples, and operator UX.
- Add runtime summaries, quiet/verbose output modes, and container wrapper if desired.

## Acceptance criteria

- A user can bulk-import transcript history without editing code.
- A user can refresh forecast snapshots for a daily target universe.
- Local lookup tables are deterministic and rebuildable.
- The repo can serve locally cached transcripts and latest forecasts through a small API.
- Provider-specific logic is isolated to adapters.
