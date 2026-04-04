# py-earnings-calls-m

`py-earnings-calls-m` is a local-first ingestion and workflow tool for two related datasets:

1. earnings call transcripts
2. earnings forecasts / estimate snapshots

The design mirrors operator-friendly patterns from `py-sec-edgar-m` while adapting storage and ingestion contracts to earnings calls data.

## Quick Start

```bash
pip install -r requirements.txt
pip install -e .
pytest
py-earnings-calls --help
```

## Operator Output Modes

Main operator commands support human output modes:

- `--quiet`: minimal human summary (key status/counts only)
- `--verbose`: bounded additive detail (truncated for large nested fields)

Machine output mode:

- `--summary-json` (where supported) always takes precedence over human modes
- when `--summary-json` is present, `--quiet` / `--verbose` are ignored
- JSON output remains script-safe and free of human commentary

Runtime visibility flags (runtime-work commands):

- `--log-level {debug,info,warning,error}`
- `--log-file <path>`
- `--progress-json` (compact NDJSON on stderr)
- `--progress-heartbeat-seconds <float>` (idle heartbeat interval; `0` disables)

Stream contract:

- `--summary-json` output stays on stdout only
- progress/runtime activity goes to stderr (and optional `--log-file`)
- progress schema is stable: `event`, `phase`, `elapsed_seconds`, `counters`, `detail`

Additional requirement groups:

- `requirements/test.txt`
- `requirements/docs.txt`
- `requirements/dev.txt`

## First-Run Workflow

```bash
py-earnings-calls refdata refresh
py-earnings-calls transcripts import-bulk --dataset ./data/motley_fool_kaggle.csv
py-earnings-calls lookup refresh
```

Progress examples:

```bash
py-earnings-calls lookup refresh --progress-json
py-earnings-calls transcripts backfill --manifest ./data/motley_fool_backfill.csv --progress-json --log-file ./logs/backfill.log
py-earnings-calls monitor loop --date 2026-03-27 --interval-seconds 30 --max-iterations 5 --progress-json --progress-heartbeat-seconds 5
```

Service runtime wrappers:

```bash
python -m py_earnings_calls.service_runtime api --summary-json
python -m py_earnings_calls.service_runtime monitor-once --date 2026-03-27 --summary-json --progress-json
python -m py_earnings_calls.service_runtime monitor-loop --date 2026-03-27 --interval-seconds 30 --max-iterations 5 --progress-json
```

## Optional Container Runtime Wrapper

Container usage is optional. Host-native workflows remain first-class.

Build image:

```bash
docker build -t py-earnings-calls-m:local .
```

Run API container (default runtime-data mounts only):

```bash
docker run --rm -p 8000:8000 \
  -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
  -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
  -v "$(pwd)/refdata:/workspace/refdata" \
  -v "$(pwd)/data:/workspace/data" \
  py-earnings-calls-m:local \
  python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000
```

Optional monitor runs use the same runtime surface:

```bash
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
```

Optional compose wrapper:

```bash
docker compose up api
```

Compose reads environment variables from the shell. A `.env` file is optional and can be used for convenience, but it is not required.

Dev-only editable source mount (optional, not default):

```bash
docker run --rm -p 8000:8000 \
  -e PY_EARNINGS_CALLS_PROJECT_ROOT=/workspace \
  -v "$(pwd):/app" \
  -v "$(pwd)/.earnings_cache:/workspace/.earnings_cache" \
  -v "$(pwd)/refdata:/workspace/refdata" \
  -v "$(pwd)/data:/workspace/data" \
  py-earnings-calls-m:local \
  python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000
```

Migration note: this container wrapper pass is additive only. No storage/schema/runtime-output contract changes are required for existing host-native workflows.

## Issuer Refdata (Ticker/CIK)

Bootstrap official SEC issuer references:

```bash
py-earnings-calls refdata fetch-sec-sources
```

`refdata refresh` builds canonical issuer mapping parquet at:

- `refdata/normalized/issuers.parquet`

Input behavior:

- if `--universe` is provided, that file is the sole issuer input for the run
- otherwise, refresh resolves SEC-style issuer files from:
  - `refdata/sec_sources/` (official fetched SEC references)
  - `refdata/inputs/` (operator overrides/manual local files)
- when the same filename exists in both roots, `refdata/inputs/` overrides `refdata/sec_sources/`
- resolved files are:
  - `company_tickers_exchange.json`
  - `company_tickers.json`
  - `company_tickers_mf.json`
  - `ticker.txt`
  - `cik-lookup-data.txt` (company-name enrichment by CIK)

Normalization behavior:

- ticker: trim + uppercase
- CIK: digits-only canonical form, zero-padded to 10 digits
- if CIK contains more than 10 digits, the rightmost 10 digits are kept

Deterministic precedence (when `--universe` is not used):

1. `company_tickers_exchange.json`
2. `company_tickers.json`
3. `company_tickers_mf.json`
4. `ticker.txt`
5. `cik-lookup-data.txt` enrichment

If no issuer inputs are available, refresh still writes `issuers.parquet` with canonical empty schema.

## Transcript Bulk Import Formats

- `kaggle_motley_fool`: CSV or parquet Motley Fool/Kaggle-style transcript exports
- `local_tabular`: neutral local tabular datasets (CSV and JSONL; parquet also accepted)
- `motley_fool_pickle`: explicit opt-in for known local DataFrame-backed Motley Fool pickle datasets (`.pkl`/`.pickle`)

Pickle ingestion is intentionally explicit and adapter-scoped, not part of the generic loader path.

## Transcript Identity and call_id Rules

- primary identity: `provider` + `provider_call_id`
- fallback identity (when provider id is missing): normalized `symbol` + normalized `call_datetime` + normalized `title`
- `call_id` is derived from the final normalized identity

## Transcript Freshness Backfill (Motley Fool)

Backfill is manifest-driven:

- required format: CSV manifest with `url` column
- optional format: JSONL manifest with `url` field
- optional `symbol` field can be provided in either format

CLI examples:

```bash
py-earnings-calls transcripts backfill --manifest ./data/motley_fool_backfill.csv
py-earnings-calls transcripts backfill --url https://www.fool.com/earnings/call-transcripts/example/
```

Failure handling is explicit and bounded:

- failures are tracked as latest known state keyed by `provider + url`
- successful fetches clear prior failure state for the same `provider + url`
- obvious non-transcript/error pages are rejected and not persisted as successful transcripts

Stable failure reason codes:
- `HTTP_ERROR`
- `RETRY_EXHAUSTED`
- `MISSING_TRANSCRIPT_BODY`
- `EMPTY_TRANSCRIPT_TEXT`
- `NON_TRANSCRIPT_PAGE`
- `PARSE_ERROR`

Datetime extraction precedence for fetched Motley Fool transcripts:

1. transcript-specific visible call datetime in page transcript content
2. transcript-specific structured metadata in transcript content blocks
3. provider/article publish metadata as fallback only
4. otherwise `call_datetime` remains `None`

Re-fetch correction behavior:

- backfill may correct an existing row datetime only when the newly extracted signal is stronger
- backfill will not downgrade an existing transcript-specific datetime to a weaker article-level date

Datetime audit + selective re-backfill workflow (Motley Fool):

```bash
# Read-only audit of fetched Motley Fool rows
py-earnings-calls transcripts audit-datetime --provider motley_fool --limit 100

# Export a bounded suspect manifest for selective re-backfill
py-earnings-calls transcripts audit-datetime --provider motley_fool --limit 100 --write-manifest ./data/motley_fool_datetime_suspects.csv

# Re-backfill only suspect rows (uses existing backfill pipeline)
py-earnings-calls transcripts backfill --manifest ./data/motley_fool_datetime_suspects.csv

# Rebuild local lookup artifacts if needed
py-earnings-calls lookup refresh
```

`transcripts audit-datetime` is non-mutating except for optional `--write-manifest`.

Archive path behavior with corrected datetime:

- archive bundle/accession placement remains stable (derived from canonical identity, not call date)
- corrected datetime updates normalized metadata fields without moving archive bundle paths

## Transcript Query API

`GET /transcripts` supports:

- `ticker`
- `cik`
- `start`
- `end`
- `limit`
- `offset`

Semantics:

- default sort: newest first by `call_datetime`
- `start` is inclusive from the start of the given day
- `end` is inclusive through the end of the given day
- when both `ticker` and `cik` are supplied, filters are applied as an intersection
- if ticker/CIK do not resolve to the same issuer universe, response is empty (not an error)

Provider-aware record resolution is intentionally narrow in this phase:

- `GET /transcripts/{call_id}` supports explicit `resolution_mode`:
  - `local_only` (default)
  - `resolve_if_missing` (explicit remote resolution if provider policy allows)
- `GET /transcripts/{call_id}/content` supports the same explicit modes
- list/query endpoint `GET /transcripts` remains local-only (no remote side effects)

CIK behavior:

- CIK resolution uses canonical issuer mapping from `refdata/normalized/issuers.parquet`
- ticker queries still work when issuer mapping is absent/incomplete
- CIK-only queries degrade to empty results when mapping is unavailable

## Canonical Docs

Core architecture and storage docs live under `docs/`:

- `docs/TARGET_ARCHITECTURE.md`
- `docs/STORAGE_LAYOUT.md`
- `docs/REFDATA_SCHEMA.md`
- `docs/DATA_SOURCES.md`
- `docs/FUTURE_DISTRIBUTED_STORAGE_PRINCIPLES.md`

## Forecast Refresh Modes

`py-earnings-calls forecasts refresh-daily` supports two provider modes:

- `single` mode: use exactly one provider (`--provider`)
- `fallback` mode: use an ordered provider list (`--provider-priority ...`) and try providers in order per symbol

Fallback behavior is deterministic:

- providers are attempted in the explicit priority order for each symbol
- once a provider returns usable normalized points for a symbol, lower-priority providers are not used for that symbol
- this avoids duplicate logical normalized rows across providers unless explicitly chosen through separate runs

Snapshot and point semantics:

- snapshot rows represent provider+symbol+as_of_date snapshot identity
- point rows represent normalized metric/stat estimates inside a snapshot
- empty/malformed payloads do not create bogus normalized point rows

Deterministic snapshot read path:

- `GET /forecasts/snapshots/{provider}/{symbol}/{as_of_date}`
  - canonical key: `provider + symbol + as_of_date`
  - supports explicit `resolution_mode` (`local_only` default, `resolve_if_missing` optional by policy)
- `GET /forecasts/{symbol}/latest` remains local-only convenience view in this phase
- `GET /forecasts/by-cik/{cik}` is a local-first derived lookup read path backed by `local_lookup_forecasts_by_cik.parquet`
  - preserves provider provenance rows (no consensus collapse)
  - optional filters: `as_of_date`, `limit`, `offset`
  - no remote resolution side effects

Forecast CIK enrichment behavior:

- new/updated forecast snapshot and point writes persist `cik` when issuer mapping exists
- existing historical rows are not broadly rewritten in place by this pass
- lookup refresh still derives CIK-grouped issuer views deterministically from normalized data + issuer mapping

## Provider Registry and Resolution Events

Normalized policy authority:

- `refdata/normalized/provider_registry.parquet`
- materialized deterministically from code-defined provider specs
- optional local override file:
  - `refdata/inputs/provider_registry_overrides.parquet` (or `.csv`)

Stable provenance/events artifact:

- `refdata/normalized/resolution_events.parquet`
- fields:
  - `event_at`
  - `content_domain`
  - `canonical_key`
  - `resolution_mode`
  - `provider_requested`
  - `provider_used`
  - `method_used`
  - `served_from`
  - `success`
  - `reason_code`
  - `message`
  - `persisted_locally`

## Archive Storage Layout (v12)

Archive/domain roots:

- `.earnings_cache/transcripts/`
- `.earnings_cache/forecasts/`

Each domain root now uses:

- `data/` for artifact payloads
- `full-index/` for archive tracking manifests (non-authoritative)

Transcript archive layout:

- `.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/raw.html`
- `.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/parsed.txt`
- `.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/parsed.json`

Forecast archive layout:

- `.earnings_cache/forecasts/data/provider=<provider>/symbol=<SYMBOL>/as_of_date=<YYYY-MM-DD>/fs-<hash16>/raw.json`

Archive accession derivation (stable):

- transcript accession: `tr-<hash16>` from canonical transcript identity (`call_id`)
- forecast accession: `fs-<hash16>` from canonical forecast snapshot identity (`provider + symbol + as_of_date`)

Runtime authority remains unchanged:

- `refdata/normalized/*.parquet`
- rebuildable `local_lookup_*.parquet`

`full-index/` manifests are archive tracking only; they do not replace runtime authority.

## Storage Migration and Verification

Copy-first migration command:

```bash
py-earnings-calls storage migrate-layout
```

Behavior:

- computes deterministic target archive paths
- copies artifacts to new archive bundles
- verifies copied targets
- updates normalized parquet path fields and writes `full-index` manifests
- **does not delete legacy artifacts by default**

Dry-run:

```bash
py-earnings-calls storage migrate-layout --dry-run
```

Verification:

```bash
py-earnings-calls storage verify-layout
```

Verification reports:

- normalized rows updated/present
- archive bundles present
- manifest rows written
- missing legacy artifacts
- unresolved conflicts

Legacy cleanup command:

```bash
py-earnings-calls storage cleanup-legacy --dry-run
py-earnings-calls storage cleanup-legacy
```

Cleanup deletion safety contract:

- normalized runtime row points to canonical archive target path
- archive target exists
- legacy artifact hash matches archive target hash

Stable cleanup skip reason codes:

- `missing_target`
- `no_runtime_mapping`
- `ambiguous_identity`
- `content_mismatch`
- `target_not_canonical`
- `legacy_only_no_archive_copy`

Directory-level cleanup reporting:

- `empty_dirs_removed`
- `nonempty_legacy_dirs_remaining`
- `legacy_roots_still_present`

Transition note:

- all new writes use new archive layout
- low-friction legacy-path read fallback is supported during transition
- migration run makes new layout canonical for normalized path fields
- legacy roots may still exist after migration until `storage cleanup-legacy` is run
- runtime init no longer eagerly recreates cleaned legacy shallow roots
- normal runtime operator paths (`lookup refresh`, API startup, local-first reads) should not recreate legacy shallow roots

## Monitor and Reconciliation (Phase 7)

Operator commands:

- `py-earnings-calls monitor poll --date YYYY-MM-DD [--warm]`
- `py-earnings-calls monitor loop --date YYYY-MM-DD --interval-seconds N --max-iterations M [--warm]`
- `py-earnings-calls reconcile run --date YYYY-MM-DD [--catch-up-warm]`

All three support `--summary-json` with stable top-level keys:

- `mode`
- `iterations`
- `targets_considered`
- `actions_taken`
- `skipped`
- `failures`
- `lookup_updates`
- `artifacts_written`

Durable normalized artifacts:

- `refdata/normalized/monitor_seen_keys.parquet`
- `refdata/normalized/monitor_events.parquet`
- `refdata/normalized/reconciliation_discrepancies.parquet`
- `refdata/normalized/reconciliation_events.parquet`

Stable discrepancy codes:

- `missing_transcript_parsed`
- `missing_transcript_raw`
- `missing_forecast_snapshot`
- `missing_forecast_points`
- `lookup_visibility_mismatch`
- `stale_forecast_snapshot`
- `retryable_transcript_failure`

## Repo Intent

This repository is ingestion-first, local-first, and intended to produce deterministic parquet-first artifacts for downstream research and retrieval workflows.

## Migration Note

- `refdata refresh` now writes both:
  - `refdata/normalized/issuers.parquet`
  - `refdata/normalized/provider_registry.parquet`
- explicit provider-aware resolution events are persisted to:
  - `refdata/normalized/resolution_events.parquet`
- archive layout migration is explicit and copy-first via:
  - `py-earnings-calls storage migrate-layout`
