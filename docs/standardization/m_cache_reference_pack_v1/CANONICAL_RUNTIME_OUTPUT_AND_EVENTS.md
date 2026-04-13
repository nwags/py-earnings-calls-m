# Canonical Runtime Output and Events

## Goal

Standardize machine-readable runtime outputs before deep internal refactors.

This slice covers:

- summary JSON
- progress NDJSON
- resolution provenance events
- reconciliation discrepancy/events

## Summary JSON

### Rule

`--summary-json` must emit one clean JSON object on stdout and nothing else.

### Required top-level fields

- `status`
- `domain`
- `command_path`
- `started_at`
- `finished_at`
- `elapsed_seconds`
- `resolution_mode` (nullable if not relevant)
- `remote_attempted`
- `provider_requested`
- `provider_used`
- `rate_limited`
- `retry_count`
- `persisted_locally`
- `counters`
- `warnings`
- `errors`

### Counters object

Counters is intentionally extensible.
Each domain may add domain-specific counters, but should reuse shared names where possible:

- `candidate_count`
- `attempted_count`
- `succeeded_count`
- `failed_count`
- `skipped_count`
- `persisted_count`
- `lookup_refreshed_count`
- `discrepancy_count`

## Progress NDJSON

### Rule

`--progress-json` emits one NDJSON event per line on stderr.

### Required fields

- `event`
- `domain`
- `command_path`
- `phase`
- `elapsed_seconds`
- `counters`

### Optional fields

- `detail`
- `window_date`
- `window_index`
- `window_total`
- `provider`
- `canonical_key`
- `rate_limit_state`

### Event values

Allowed first-wave values:

- `started`
- `progress`
- `heartbeat`
- `warning`
- `completed`
- `failed`
- `interrupted`
- `deferred`

## Resolution provenance event

A shared resolution event should be persisted whenever an API or CLI resolve path performs meaningful local/remote resolution work.

### Canonical artifact name

```text
refdata/normalized/resolution_events.parquet
```

A domain may keep a domain-specific alias or companion artifact during migration, but `resolution_events.parquet` is the shared target name.

### Required fields

- `event_at`
- `domain`
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

### Recommended fields

- `http_status`
- `retry_count`
- `rate_limited`
- `deferred_until`
- `latency_ms`

### `served_from` values

Allowed first-wave values:

- `local_cache`
- `local_normalized`
- `remote_then_persisted`
- `remote_ephemeral`
- `none`

## Reconciliation discrepancies

### Canonical artifact name

```text
refdata/normalized/reconciliation_discrepancies.parquet
```

### Required fields

- `discrepancy_key`
- `domain`
- `target_type`
- `seen_key`
- `discrepancy_code`
- `target_date`
- `details`
- `observed_at`

### Domain-specific selector fields

Each domain may include its own selector columns such as:

- `accession_number`
- `call_id`
- `symbol`
- `collection_id`
- `dataset_id`
- `series_id`
- `article_id`

## Reconciliation events

### Canonical artifact name

```text
refdata/normalized/reconciliation_events.parquet
```

### Required fields

- `event_at`
- `domain`
- `event_code`
- `target_date`
- `discrepancy_count`
- `catch_up_warm`

### Optional fields

- `provider`
- `window_from`
- `window_to`
- `lookup_refreshed`

## Shared discrepancy code vocabulary

Shared cross-domain codes that should be reused when applicable:

- `missing_raw_artifact`
- `missing_parsed_artifact`
- `missing_snapshot`
- `missing_points`
- `lookup_visibility_mismatch`
- `retryable_remote_failure`
- `stale_local_state`

Domain-specific codes are still allowed.

## API transparency rule

If a detail/content API endpoint performs remote resolution, the response body should expose:

- whether remote resolution was attempted
- which provider was used
- which resolution mode was used
- whether the result was persisted locally
- whether a rate limit affected behavior

## CLI/API consistency rule

For the same underlying resolution behavior, CLI summaries, API response metadata, and persisted resolution events should agree on:

- `resolution_mode`
- `provider_used`
- `served_from`
- `persisted_locally`
- `reason_code`

## Acceptance criteria for this slice

All four repos pass this slice when:

1. summary-json stays clean and script-safe
2. progress-json stays on stderr and uses a stable NDJSON schema
3. resolution provenance fields align with this document
4. reconcile discrepancy and event artifacts align with this document
5. domain-specific counters/selectors extend rather than replace the shared shape
