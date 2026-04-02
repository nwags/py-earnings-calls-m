
## Mission

Refactor `py-earnings-calls-m` into a local-first ingestion system for:

1. earnings call transcripts, and
2. earnings forecast snapshots.

The new system must prioritize:

1. correctness and repeatability,
2. bounded and polite HTTP behavior,
3. explicit source-adapter boundaries,
4. canonical local storage contracts,
5. parquet-first normalized outputs,
6. reproducible local development.

## Non-negotiables

- Do **not** hard-code business logic to one transcript provider.
- Do **not** assume transcript freshness and transcript history come from the same source.
- Do **not** assume forecast history exists as a clean free bulk dataset.
- Do **not** allow unbounded parallelism.
- Do **not** mix raw source payloads with normalized parquet outputs.
- Prefer typed, testable modules over hidden global state.
- Prefer additive adapters over source-specific hacks in the CLI layer.

## Source of truth hierarchy

If there is conflict, use this order:

1. `docs/TARGET_ARCHITECTURE.md`
2. `docs/STORAGE_LAYOUT.md`
3. `docs/REFDATA_SCHEMA.md`
4. `docs/DATA_SOURCES.md`
5. tests
6. legacy scaffold placeholders

## Required architecture direction

Implement a staged pipeline:

1. refresh or load issuer/universe reference data,
2. bulk-import transcript history,
3. backfill or refresh transcript freshness through adapters,
4. refresh forecast snapshots through adapters,
5. build lookup artifacts,
6. expose a small local-first API surface,
7. add monitoring / reconciliation after ingestion foundations are reliable.

## Expected new repo capabilities

- import transcript history from local bulk datasets,
- fetch and persist transcript pages from explicit adapters,
- refresh per-symbol forecast snapshots from explicit adapters,
- maintain deterministic lookup artifacts for local retrieval,
- support future event extraction and research workflows downstream.

## Delivery standard

Every meaningful change should include:

- code,
- tests,
- doc updates,
- a short migration note when behavior changes.
