# Canonical Rate Limit and Degradation Semantics

## Goal

Standardize how repos describe and report quota, retries, deferrals, and graceful degradation.

## Canonical vocabulary

### Rate-limit policy
Allowed values:
- `unknown`
- `fixed_window`
- `sliding_window`
- `token_bucket`
- `per_minute`
- `per_hour`
- `per_day`
- `custom`

### Graceful degradation policy
Allowed values:
- `return_local_stale_if_available`
- `return_local_metadata_only`
- `defer_and_report`
- `fail_fast`

## Semantics

### `rate_limited`
Set `rate_limited=true` when quota or provider throttling materially constrained execution for the selected provider path.

### `deferred`
A deferred result means the repo did not complete the desired remote work now, but the outcome is transparent and machine-readable.

### `deferred_until`
Nullable timestamp that indicates when work may reasonably be retried, if known.

### `retry_count`
Number of retries actually attempted on the chosen path.

### `provider_skip_reason`
Reason a provider was skipped before or during selection. Suggested values:
- `inactive`
- `policy_denied`
- `quota_limited`
- `missing_auth`
- `mode_unsupported`
- `content_domain_mismatch`
- `unavailable`
- `error_after_retries`

## CLI summary requirements

When remote-capable work is involved, canonical summary payloads should be able to answer:
- `provider_requested`
- `provider_used`
- `remote_attempted`
- `rate_limited`
- `retry_count`
- `persisted_locally`
- `warnings`
- `errors`

If work was deferred, the summary should also expose:
- `deferred`
- `deferred_until`
- `defer_reason`

## Progress event requirements

Canonical progress NDJSON may include:
- `provider`
- `rate_limit_state`
- `detail`
- `canonical_key`

If a provider was skipped or deferred, emit explicit progress events with machine-readable detail rather than silently hiding the transition.

## API transparency requirements

If a detail/content API endpoint performs or attempts remote work, its response metadata should expose:
- whether remote access was attempted,
- whether it was rate limited,
- whether the result was deferred,
- retry count,
- selected provider,
- degradation mode used, if any.

## What Wave 2 does not require

Wave 2 does **not** require:
- a shared global quota planner,
- shared background deferred-work execution,
- historical event rewrites,
- aggressive new retry engines.

Wave 2 **does** require making current behavior inspectable and aligned.
