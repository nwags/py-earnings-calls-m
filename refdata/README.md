# refdata

Canonical local data root for this repository.

## Layout

- `refdata/sec_sources/`: fetched official SEC issuer reference files.
- `refdata/normalized/`: parquet-first normalized outputs produced by pipelines.
- `refdata/inputs/`: optional local input files used for deterministic imports.

Do not mix raw provider payloads into `refdata/normalized/`.
Raw payloads belong under `.earnings_cache/` per storage contracts.
