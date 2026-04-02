
## Purpose

Define canonical local paths for transcript and forecast artifacts.

## Roots

### Archive domain roots
- `.earnings_cache/transcripts/`
- `.earnings_cache/forecasts/`

### Archive data/index split
- `.earnings_cache/transcripts/data/`
- `.earnings_cache/transcripts/full-index/`
- `.earnings_cache/forecasts/data/`
- `.earnings_cache/forecasts/full-index/`

### Normalized outputs
- `refdata/normalized/`

## Canonical transcript paths

### Raw HTML
`.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/raw.html`

### Parsed text
`.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/parsed.txt`

### Optional parsed JSON sidecar
`.earnings_cache/transcripts/data/cik=<CIK-or-UNKNOWN>/tr-<hash16>/parsed.json`

### Transcript full-index manifest
`.earnings_cache/transcripts/full-index/transcript_archive_manifest.parquet`

## Canonical forecast paths

### Provider raw snapshot payload
`.earnings_cache/forecasts/data/provider=<provider>/symbol=<SYMBOL>/as_of_date=<YYYY-MM-DD>/fs-<hash16>/raw.json`

### Forecast full-index manifest
`.earnings_cache/forecasts/full-index/forecast_archive_manifest.parquet`

## Canonical normalized artifacts

- `refdata/normalized/issuers.parquet`
- `refdata/normalized/source_coverage.parquet`
- `refdata/normalized/transcript_calls.parquet`
- `refdata/normalized/transcript_artifacts.parquet`
- `refdata/normalized/forecast_snapshots.parquet`
- `refdata/normalized/forecast_points.parquet`
- `refdata/normalized/reference_file_manifest.parquet`
- `refdata/normalized/local_lookup_transcripts.parquet`
- `refdata/normalized/local_lookup_forecasts.parquet`
- `refdata/normalized/local_lookup_forecasts_by_cik.parquet` (derived issuer-grouped forecast lookup)
- `refdata/normalized/monitor_events.parquet`
- `refdata/normalized/monitor_seen_keys.parquet`
- `refdata/normalized/reconciliation_discrepancies.parquet`
- `refdata/normalized/reconciliation_events.parquet`
- `refdata/normalized/provider_registry.parquet`
- `refdata/normalized/resolution_events.parquet`

## Design rules

- Archive payloads/index and normalized parquet outputs must remain separate.
- Path derivation must be centralized.
- Lookup artifacts must be rebuildable from normalized data plus local cache state.
- `full-index/` manifests are archive tracking only, not runtime query authority.
- Transcript datetime corrections do not re-home archive bundles; bundle/accession paths remain identity-stable.
- Forecast raw archive layout remains provider-centric; issuer-centric (`cik`) views are derived lookup artifacts.

## Migration and Cleanup Notes

- `storage migrate-layout` is copy-first and non-destructive by default.
- legacy shallow roots may still exist after successful migration.
- `storage cleanup-legacy` is an explicit operator step that removes legacy files only after verified canonical archive correspondence and content match.
- runtime directory initialization no longer eagerly recreates legacy shallow roots after cleanup.
