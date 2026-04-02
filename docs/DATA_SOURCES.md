
## Transcript sources

### Historical bootstrap
The scaffold assumes transcript history comes from local bulk datasets first.

Examples:
- Kaggle export of Motley Fool earnings call transcripts
- local CSV / JSONL / parquet archives
- one-time manual collections imported through adapters

Bulk transcript adapter modes:
- `kaggle_motley_fool`: provider-aware CSV/parquet normalization for common Motley Fool/Kaggle variants
- `local_tabular`: neutral local tabular import (CSV and JSONL required; parquet supported)
- `motley_fool_pickle`: explicit adapter for known local DataFrame pickle files (`.pkl` / `.pickle`)

Safety/clarity note:
Pickle ingestion is intentionally explicit and adapter-scoped. Generic local tabular import does not perform broad arbitrary pickle loading.

### Freshness adapters
Initial target:
- Motley Fool transcript page fetch + parse

Design note:
Discovery and fetching should be separated. URL-manifest driven backfill is a good first implementation because it avoids binding core logic to fragile site discovery too early.

Backfill manifest contract:
- CSV manifest is the required format (`url` column required, `symbol` optional)
- JSONL manifest is optionally supported with the same fields
- direct URL CLI input remains available for compatibility, but flows through the same manifest-row pipeline path

Failure handling contract:
- failures use stable reason codes
- latest known failure state is persisted by `provider + url` (bounded state, not an unbounded event log)
- obvious error pages / non-transcript pages are rejected and never persisted as successful transcript artifacts

Current failure reason codes:
- `HTTP_ERROR`
- `RETRY_EXHAUSTED`
- `MISSING_TRANSCRIPT_BODY`
- `EMPTY_TRANSCRIPT_TEXT`
- `NON_TRANSCRIPT_PAGE`
- `PARSE_ERROR`

Datetime audit contract (Motley Fool fetched rows):
- use `py-earnings-calls transcripts audit-datetime` to classify existing fetched rows by datetime evidence quality
- audit is read-only except optional `--write-manifest`
- corrective updates still occur only through explicit re-backfill (`transcripts backfill --manifest ...`)

## Forecast sources

### Initial target providers
- Finnhub
- FMP

Refresh provider modes:
- `single`: explicit single provider (`--provider`)
- `fallback`: ordered provider fallback (`--provider-priority` repeated in order)

Fallback semantics:
- per symbol, providers are attempted in the configured order
- first provider with usable normalized points wins for that symbol
- lower-priority providers are skipped for that symbol once data is selected

### Operating assumption
Forecast data should be persisted as **snapshots** over time rather than treated as one mutable current-state row.

That means the same symbol may accumulate many snapshots across dates and providers.

## Practical source strategy

- transcript history: bulk import first
- transcript freshness: explicit adapters
- forecast history: accumulated adapter snapshots
- forecast freshness: daily refresh of active reporting universe
- monitor/reconciliation: local-target checks + bounded reattempts, no broad discovery/crawling

## Provider-aware resolution policy direction

- public read paths stay local-first by default (`local_only`)
- remote resolution is explicit (`resolve_if_missing`) and provider-policy governed
- transcript provider-aware read resolution is call-id scoped and requires local canonical metadata (`provider`, `source_url`)
- forecast provider-aware read resolution is deterministic snapshot scoped (`provider + symbol + as_of_date`)
- `refresh_if_stale` is operator/admin-gated in this phase

## Archive/index storage strategy

- transcript and forecast caches are domain archive roots
- each domain uses `data/` for payload artifacts and `full-index/` for minimal archive manifests
- transcript archive grouping prefers canonical CIK with deterministic `UNKNOWN` fallback
- archive accession IDs are deterministic and derived only from canonical identity:
  - transcripts: `tr-<hash16>` from `call_id` identity
  - forecasts: `fs-<hash16>` from snapshot canonical identity (`provider + symbol + as_of_date`)
- normalized parquet remains runtime authority; full-index manifests are non-authoritative tracking artifacts

## Issuer refdata sources (ticker/CIK mapping)

Fetch official SEC issuer references into:

- `refdata/sec_sources/`

via:

- `py-earnings-calls refdata fetch-sec-sources`

`py-earnings-calls refdata refresh` uses this issuer-input contract:

- `--universe` provided: treat that CSV/parquet file as the sole issuer input for the run
- no `--universe`: resolve SEC-style files from both:
  - `refdata/sec_sources/` (fetched official references)
  - `refdata/inputs/` (manual/operator overrides)
- if both roots contain the same filename, `refdata/inputs/` wins for that file
- resolved files:
  - `company_tickers_exchange.json`
  - `company_tickers.json`
  - `company_tickers_mf.json`
  - `ticker.txt`
  - `cik-lookup-data.txt` (used for company-name enrichment by CIK)

Deterministic precedence without `--universe`:

1. `company_tickers_exchange.json`
2. `company_tickers.json`
3. `company_tickers_mf.json`
4. `ticker.txt`

Normalization rules:

- ticker: uppercase + trim
- CIK: digits-only canonical form, zero-padded to 10 digits
- CIK values longer than 10 digits use the rightmost 10 digits

Operational authority:

- runtime ticker/CIK resolution uses normalized parquet (`refdata/normalized/issuers.parquet`)
- raw SEC-style files are build inputs only, not runtime authority
