
## Canonical normalized tables

### issuers
Suggested columns:

- `symbol`
- `cik` (10-digit zero-padded canonical form when available)
- `company_name`
- `exchange`
- `is_active`
- `primary_source`
- `source_updated_at`

Issuer refresh contract:

- always write `refdata/normalized/issuers.parquet` with canonical column order
- if no issuer inputs are present, write an empty parquet table with this schema
- source roots:
  - `refdata/sec_sources/` for fetched official SEC issuer references
  - `refdata/inputs/` for operator overrides/manual inputs
  - `--universe` is an explicit run-scoped sole-input replacement

### source_coverage
Suggested columns:

- `source_type` (`transcript` or `forecast`)
- `provider`
- `symbol`
- `coverage_kind` (`historical_bulk`, `incremental_fetch`, `daily_snapshot`)
- `notes`
- `source_updated_at`

### provider_registry
Suggested columns:

- `provider_id`
- `provider_type`
- `content_domain`
- `supports_bulk_history`
- `supports_freshness_fetch`
- `supports_direct_resolution`
- `supports_public_resolve_if_missing`
- `supports_admin_refresh_if_stale`
- `base_url`
- `auth_type`
- `env_key_name`
- `rate_limit_policy`
- `retrieval_policy`
- `preferred_resolution_order`
- `direct_uri_allowed`
- `capability_level`
- `notes`
- `is_active`

Materialization contract:

- deterministic local-authoritative build from code-defined provider specs
- optional local overrides from `refdata/inputs/provider_registry_overrides.parquet` (or `.csv`)
- no network fetch dependency for registry materialization

### monitor_seen_keys
Suggested columns:

- `seen_key`
- `target_type`
- `provider`
- `url`
- `symbol`
- `target_date`
- `last_status`
- `last_reason_code`
- `last_action`
- `last_seen_at`
- `expected_raw_path`
- `expected_parsed_path`

### monitor_events
Suggested columns:

- `event_at`
- `mode`
- `target_type`
- `seen_key`
- `event_code`
- `reason_code`
- `action`
- `message`
- `provider`
- `url`
- `symbol`
- `target_date`

### reconciliation_discrepancies
Suggested columns:

- `discrepancy_key`
- `discrepancy_code`
- `target_type`
- `seen_key`
- `symbol`
- `provider`
- `target_date`
- `details`
- `observed_at`

Stable discrepancy codes:

- `missing_transcript_parsed`
- `missing_transcript_raw`
- `missing_forecast_snapshot`
- `missing_forecast_points`
- `lookup_visibility_mismatch`
- `stale_forecast_snapshot`
- `retryable_transcript_failure`

### reconciliation_events
Suggested columns:

- `event_at`
- `event_code`
- `target_date`
- `discrepancy_count`
- `catch_up_warm`

### transcript_calls
Suggested columns:

- `call_id`
- `provider`
- `provider_call_id`
- `symbol`
- `company_name`
- `call_datetime`
- `fiscal_year`
- `fiscal_period`
- `title`
- `source_url`
- `transcript_path`
- `raw_html_path`
- `storage_cik`
- `archive_accession_id`
- `archive_bundle_path`
- `speaker_count`
- `imported_at`

Lookup/query note:
- `local_lookup_transcripts.parquet` may include optional `cik` enrichment via issuer mapping (`symbol -> cik`) when `issuers.parquet` contains usable CIK data.

### transcript_artifacts
Suggested columns:

- `call_id`
- `artifact_type`
- `artifact_path`
- `provider`
- `symbol`
- `call_date`
- `exists_locally`
- `storage_cik`
- `archive_accession_id`

### forecast_snapshots
Suggested columns:

- `snapshot_id`
- `provider`
- `symbol`
- `cik` (optional enrichment from issuer mapping when available)
- `as_of_date`
- `source_url`
- `raw_payload_path`
- `archive_accession_id`
- `archive_bundle_path`
- `imported_at`

Identity/dedupe:
- `provider`
- `symbol`
- `as_of_date`

Canonical snapshot resolution key:
- `provider + symbol + as_of_date`

### forecast_points
Suggested columns:

- `snapshot_id`
- `provider`
- `symbol`
- `cik` (optional enrichment from issuer mapping when available)
- `as_of_date`
- `fiscal_year`
- `fiscal_period`
- `metric_name`
- `stat_name`
- `value`
- `currency`
- `analyst_count`

Identity/dedupe:
- `provider`
- `symbol`
- `as_of_date`
- `fiscal_year`
- `fiscal_period`
- `metric_name`
- `stat_name`

### local_lookup_forecasts_by_cik
Suggested derived lookup columns:

- `cik`
- `symbol`
- `provider`
- `as_of_date`
- `snapshot_id`
- `fiscal_year`
- `fiscal_period`
- `metric_name`
- `stat_name`
- `value`

Design note:
- this is a derived/read artifact for issuer-centric querying
- provider snapshot identity and raw archive provenance remain provider-centric

## Output format preference

Use parquet for canonical normalized tables.
CSV exports are optional convenience outputs only.

## Dedupe guidance

### transcript_calls
Primary dedupe key:

- `provider`
- `provider_call_id`

Fallback identity if provider id is missing:

- `symbol`
- `call_datetime`
- normalized `title`

Implementation note:
- when `provider_call_id` is missing, adapters derive a deterministic fallback identity string from normalized `symbol` + `call_datetime` + `title`
- `call_id` is then derived from the final normalized identity (primary or fallback)

### forecast_snapshots
Primary dedupe key:

- `provider`
- `symbol`
- `as_of_date`

## Resolution modes

Explicit modes:

- `local_only` (default local-first behavior)
- `resolve_if_missing` (policy-governed remote resolution path)
- `refresh_if_stale` (operator/admin-gated; not public-read behavior in this phase)

## Resolution events

### resolution_events
Suggested columns:

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

## Archive full-index manifests (non-authoritative)

### transcript_archive_manifest
Suggested columns:

- `call_id`
- `archive_accession_id`
- `storage_cik`
- `provider`
- `raw_html_path`
- `parsed_text_path`
- `parsed_json_path`
- `raw_html_exists`
- `parsed_text_exists`
- `parsed_json_exists`
- `updated_at`

### forecast_archive_manifest
Suggested columns:

- `snapshot_id`
- `archive_accession_id`
- `provider`
- `symbol`
- `as_of_date`
- `raw_payload_path`
- `raw_payload_exists`
- `updated_at`

### forecast_points
Primary dedupe key:

- `provider`
- `symbol`
- `as_of_date`
- `fiscal_year`
- `fiscal_period`
- `metric_name`
- `stat_name`
