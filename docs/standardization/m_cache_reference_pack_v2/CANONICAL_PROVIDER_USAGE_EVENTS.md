# Canonical Provider Usage Events

## Goal

Define the additive machine-readable event/reporting shape for provider selection, skips, rate-limit outcomes, and defer outcomes.

## Scope

This document does not require every repo to add a brand-new persisted provider-usage artifact in Wave 2.
It does require that canonical provider/rate-limit/defer outcomes are visible in:
- summary JSON,
- progress NDJSON,
- resolution events when resolve work occurs.

A repo may optionally persist:

```text
refdata/normalized/provider_usage_events.parquet
```

if it already has a clean place to do so.

## Canonical provider-usage event fields

Required fields:
- `event_at`
- `domain`
- `command_path`
- `content_domain`
- `provider_requested`
- `provider_used`
- `selection_outcome`
- `reason_code`
- `remote_attempted`
- `rate_limited`
- `retry_count`
- `deferred`

Recommended optional fields:
- `deferred_until`
- `provider_skip_reasons`
- `served_from`
- `canonical_key`
- `http_status`
- `latency_ms`
- `message`

## `selection_outcome` values

Allowed values:
- `used_requested_provider`
- `used_fallback_provider`
- `served_locally`
- `deferred`
- `failed`
- `policy_denied`
- `mode_unsupported`

## Compatibility rule

If a repo already records resolution events, Wave 2 may satisfy provider-usage visibility by enriching `resolution_events.parquet` with these fields when appropriate, rather than inventing a second artifact immediately.

## Acceptance criteria for this slice

All four repos pass this slice when:
1. provider/rate-limit/defer outcomes are inspectable in machine-readable outputs
2. provider-skip and defer reasons are explicit
3. no repo silently hides quota-driven behavior on remote-capable paths
