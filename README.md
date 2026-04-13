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
m-cache --help
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

Wave 1 output-shape strategy:

- legacy CLI (`py-earnings-calls ...`) keeps legacy JSON/progress payload shapes by default
- canonical CLI (`m-cache earnings ...`) uses canonical Wave 1 summary/progress shapes by default
- `m-cache earnings ...` supports `--output-schema legacy|canonical` for explicit selection

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

Canonical Wave 1 command surface (additive):

```bash
m-cache earnings refdata refresh
m-cache earnings transcripts import-bulk --dataset ./data/motley_fool_kaggle.csv
m-cache earnings forecasts refresh-daily --provider finnhub --date 2026-03-27 --symbol AAPL
m-cache earnings providers list --summary-json
m-cache earnings providers show --provider finnhub --summary-json
m-cache earnings resolve transcript --call-id c1 --resolution-mode resolve_if_missing --summary-json
m-cache earnings aug list-types --summary-json
m-cache earnings aug inspect-target --resource-family transcripts --call-id c1 --summary-json
m-cache earnings aug submit-run --input-json ./producer_run_submission.json --summary-json
m-cache earnings aug submit-artifact --input-json ./producer_artifact_submission.json --summary-json
m-cache earnings aug status --run-id aug-run-1001 --summary-json
m-cache earnings aug events --resource-family transcripts --limit 20 --summary-json
# compatibility alias:
m-cache earnings aug target-descriptor --call-id c1 --summary-json
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
- `docs/WAVE1_MIGRATION_NOTE.md`
- `docs/WAVE2_MIGRATION_NOTE.md`
- `docs/WAVE3_MIGRATION_NOTE.md`
- `docs/WAVE4_MIGRATION_NOTE.md`

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
  - `domain` (`earnings`)
  - `event_at`
  - `content_domain`
  - `canonical_key`
  - `resolution_mode`
  - `provider_requested`
  - `provider_used`
  - `method_used`
  - `served_from`
  - `remote_attempted`
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

Wave 1 canonical now:

- `m-cache earnings ...` canonical command surface (additive)
- canonical `m-cache.toml` loader (`--config`, `M_CACHE_CONFIG`, `./m-cache.toml`, then compatibility env/defaults)
- expanded canonical provider registry fields in `refdata/normalized/provider_registry.parquet`
- additive canonical fields in persisted resolution/reconciliation event artifacts
- canonical summary/progress JSON shape on `m-cache earnings ...` by default

Aliased/compatibility surfaces kept:

- `py-earnings-calls ...` remains supported for operators
- legacy summary/progress JSON payload shapes remain default on `py-earnings-calls ...`
- `m-cache earnings ... --output-schema legacy` is available for compatibility scripts

Reserved for later waves:

- cross-repo shared package extraction
- deep pipeline rewrites for command unification
- broad historical event-row migrations
- any transcript/forecast identity flattening

- `refdata refresh` now writes both:
  - `refdata/normalized/issuers.parquet`
  - `refdata/normalized/provider_registry.parquet`
- explicit provider-aware resolution events are persisted to:
  - `refdata/normalized/resolution_events.parquet`
- archive layout migration is explicit and copy-first via:
  - `py-earnings-calls storage migrate-layout`

Wave 2 canonical now:

- canonical provider read surface on `m-cache earnings`:
  - `providers list`
  - `providers show --provider <provider_id>`
- canonical additive resolve surface on `m-cache earnings`:
  - `resolve transcript --call-id ... --resolution-mode ...`
  - `resolve forecast-snapshot --provider ... --symbol ... --date ... --resolution-mode ...`
- additive provider/rate-limit/defer transparency on:
  - canonical summary/progress JSON for remote-capable `m-cache earnings` paths
  - `resolution_events.parquet` new rows
  - detail/content API response metadata (and content headers for transcript text)

Wave 3 canonical now:

- transcript-only read-only augmentation planning surface on `m-cache earnings`:
  - `aug list-types`
  - `aug inspect-target`
  - `aug status`
  - `aug events`
- transcript augmentation targeting remains `call_id`-scoped with source-text-version aware metadata mapping
- transcript detail/content API surfaces expose additive augmentation metadata
- numeric forecast snapshots/points remain non-augmentation resources in Wave 3

Wave 4 canonical now (pilot in this repo):

- this repo is a Wave 4 producer-protocol pilot implementation
- additive transcript producer protocol surfaces:
  - inspect target (`m-cache earnings aug inspect-target`, `GET /transcripts/{call_id}/augmentation-target`)
  - run submission envelope (`m-cache earnings aug submit-run`, `POST /augmentations/runs`)
  - artifact submission envelope (`m-cache earnings aug submit-artifact`, `POST /augmentations/artifacts`)
  - run status (`m-cache earnings aug status`)
  - event timeline (`m-cache earnings aug events`)
- idempotent replay-safe handling:
  - run submissions keyed by `run_id`
  - artifact submissions keyed by explicit/derived idempotency key
- payload schema ownership stays external/service-owned; repo validates only outer metadata envelopes
- bounded payload handling supports locator-backed artifacts and optional bounded inline payload materialization

Compatibility aliases preserved:

- `m-cache earnings aug target-descriptor` (alias for target descriptor reads; canonical family uses `inspect-target`)
- `m-cache earnings aug inspect-runs` (read-detail compatibility surface)
- `m-cache earnings aug inspect-artifacts` (artifact inspection compatibility surface)

Wave 4 reserved for later:

- broad shared-package extraction rollout across repos
- augmentation execution orchestration beyond this pilot write path
- universal augmentation payload schema standardization

Wave 5 canonical now (first extraction cut, in-repo):

- `m_cache_shared` is introduced as an in-repo package for shared outer protocol/helpers.
- extracted slice is intentionally minimal:
  - augmentation enums/vocabularies
  - shared protocol/internal view models
  - outer-envelope validators
  - pure metadata packers/builders
  - thin helper plumbing
- route-specific API request/response models remain local in `py_earnings_calls.api.*`.
- transcript identity, target building, text retrieval, storage placement, and live write-path orchestration remain local.
- transcript-only applicability remains unchanged; forecasts remain non-augmentation.
- pilot write behavior remains unchanged:
  - `m-cache earnings aug submit-run`
  - `m-cache earnings aug submit-artifact`

Wave 6.1 canonical now (convergence only):

- canonical external identity:
  - distribution: `m-cache-shared-ext`
  - import root: `m_cache_shared_ext.augmentation`
  - pin file: `requirements/m_cache_shared_external.txt`
  - shared RC tag baseline: `v0.1.0-rc1`
- canonical facade source-mode contract via `py_earnings_calls.augmentation_shared`:
  - `M_CACHE_SHARED_SOURCE={auto|external|local}`
  - `M_CACHE_SHARED_EXTERNAL_ROOT` (default `m_cache_shared_ext.augmentation`)
  - compatibility alias retained: `PY_EARNINGS_CALLS_SHARED_SOURCE`
- no extraction scope changes, no runtime/CLI/API semantic changes, no pilot-role changes.

Wave 7 canonical now (lifecycle hardening only):

- no new extraction scope.
- no runtime behavior changes.
- no CLI/API semantic changes.
- no shared public API broadening.
- this repo remains pilot consumer-validator / release blocker for transcript write-path regressions.
- this repo is not an external-package governance owner and not a public-API broadening authority.
- transcript-only applicability and pilot behavior remain frozen:
  - transcript write-path behavior unchanged.
  - forecasts remain non-augmentation.
  - transcript identity/targeting/text retrieval/storage/API wire-shape/live orchestration stay local.
- lifecycle artifacts are additive and repo-local:
  - `docs/WAVE7_MIGRATION_NOTE.md`
  - `docs/standardization/wave7_repo_lifecycle/`
- explicit Wave 7 user-testing policy:
  - cross-application user testing is mandatory only for compatibility-impacting stable releases.
  - it is not required for every routine stable release.
  - it never replaces maintainer/developer validation or RC matrix validation.
- deferred cleanup remains deferred:
  - no public API expansion.
  - no immediate shim/fallback removal.
  - no import-root collapse/removal wave.
  - no local ownership reduction.

Wave 7.1 canonical now (package-side release-execution hardening only):

- no runtime behavior changes.
- no CLI/API semantic changes.
- no shared public API broadening.
- no cleanup/removal work in this pass.
- this repo remains pilot consumer-validator / release blocker for transcript write-path safety.
- this repo contributes required validation/signoff evidence into the shared package-side release cycle.
- this repo does not define external package governance ownership.
- evidence artifacts remain lightweight and obligation-only, and are central-bundle inputs:
  - `docs/WAVE7_1_MIGRATION_NOTE.md`
  - `docs/standardization/wave7_1_repo_release_lifecycle/`
- first-hop facade and local ownership boundaries remain frozen:
  - `py_earnings_calls.augmentation_shared` remains first-hop.
  - transcript identity/targeting/text retrieval/storage/API wire-shape/live write orchestration stay local.
  - transcript-only applicability remains unchanged.
  - forecasts remain non-augmentation.
- comprehensive user-testing start gate is explicit:
  - only after Wave 7.1 implementation, one shared RC fully validated across all repos, end-to-end evidence/signoff flow operation, rollback verification, and no open blocking lifecycle incident.
- cleanup/removal remains explicitly deferred:
  - no public API broadening.
  - no shim/fallback removal.
  - no env alias removal.
  - no import-root collapse.
  - no local ownership reduction.

Wave 7.2 companion canonical now (minimal RC participation only):

- companion scope stays minimal and participation-only:
  - no runtime behavior changes.
  - no CLI/API semantic changes.
  - no shared API broadening.
  - no cleanup/removal work.
- this repo remains pilot consumer-validator / release blocker for transcript write-path safety.
- first local RC consumption step is explicit and portable:
  - canonical pin reference remains `requirements/m_cache_shared_external.txt`.
  - default local RC consumption method in active virtualenv:
    - `export M_CACHE_SHARED_EXT_LOCAL_REPO="${M_CACHE_SHARED_EXT_LOCAL_REPO:-../m-cache-shared-ext}"`
    - `python -m pip install -e "$M_CACHE_SHARED_EXT_LOCAL_REPO"`
- machine-readable signoff input aligns to package-side `SIGNOFF.json` fields exactly:
  - `candidate_tag`, `repo`, `release_role`, `pin_confirmed`, `validation_status`,
  - `signoff_state`, `blockers`, `warnings`, `rollback_ready`
  - canonical terminal decision vocabulary: `signoff_state = pass | warn | block`.
- all artifacts remain central-bundle inputs only:
  - `docs/standardization/wave7_2_repo_companion/`
  - no earnings-local release process and no separate governance system.
- local boundaries and applicability remain frozen:
  - `py_earnings_calls.augmentation_shared` first-hop facade unchanged.
  - transcript-only applicability unchanged.
  - forecasts remain non-augmentation.
  - local transcript identity/target/text/storage/API wire-shape/live orchestration unchanged.
