# Canonical Provider Registry

## Goal

Define one shared provider registry abstraction used by all domains.

A domain may currently have one effective upstream source, but it should still model that source as a provider.

Examples:

- SEC models the SEC as a provider
- earnings models transcript and forecast providers
- Fed models document, release, and series providers
- news models article metadata/content providers

## Canonical authority

The shared canonical normalized artifact name is:

```text
refdata/normalized/provider_registry.parquet
```

A domain may also keep domain-specific provider/support artifacts, but `provider_registry.parquet` is the common operator authority.

## Materialization rules

- registry materialization is deterministic
- code-defined provider specs are the default source of truth
- optional local overrides are allowed from `refdata/inputs/provider_registry_overrides.parquet` or `.csv`
- registry materialization must not require live network access

## Canonical fields

Required fields:

- `provider_id`
- `domain`
- `content_domain`
- `provider_type`
- `display_name`
- `base_url`
- `auth_type`
- `auth_env_var`
- `rate_limit_policy`
- `soft_limit`
- `hard_limit`
- `burst_limit`
- `retry_budget`
- `backoff_policy`
- `direct_resolution_allowed`
- `browse_discovery_allowed`
- `supports_bulk_history`
- `supports_incremental_refresh`
- `supports_direct_resolution`
- `supports_public_resolve_if_missing`
- `supports_admin_refresh_if_stale`
- `graceful_degradation_policy`
- `free_tier_notes`
- `fallback_priority`
- `is_active`
- `notes`

Recommended fields:

- `default_timeout_seconds`
- `quota_window_seconds`
- `quota_reset_hint`
- `expected_error_modes`
- `user_agent_required`
- `contact_requirement`
- `terms_url`

## Field semantics

### `domain`
Owning `m-cache` domain: `sec`, `earnings`, `fed`, or `news`.

### `content_domain`
What kind of records the provider serves within the domain.
Examples:

- `filing`
- `transcript`
- `forecast`
- `document`
- `release`
- `series`
- `article`

### `provider_type`
High-level source type.
Allowed values in the first wave:

- `official_api`
- `official_site`
- `rss_feed`
- `bulk_dataset`
- `partner_api`
- `local_dataset`
- `manual_input`

### `auth_type`
Allowed first-wave values:

- `none`
- `api_key_header`
- `api_key_query`
- `bearer_token`
- `basic_auth`
- `session_cookie`
- `custom`

### `rate_limit_policy`
Allowed first-wave values:

- `unknown`
- `fixed_window`
- `sliding_window`
- `token_bucket`
- `per_minute`
- `per_hour`
- `per_day`
- `custom`

### `graceful_degradation_policy`
Allowed first-wave values:

- `return_local_stale_if_available`
- `return_local_metadata_only`
- `defer_and_report`
- `fail_fast`

## Provider selection rules

### Rule 1
Provider selection must be deterministic.

### Rule 2
Explicit provider request wins over fallback order when policy allows.

### Rule 3
Fallback order is evaluated only among active providers eligible for the selector and resolution mode.

### Rule 4
A provider that cannot satisfy the requested resolution mode must be skipped with an explicit recorded reason.

## Rate-limit coordination rule

The provider registry is not just descriptive metadata.
It is an input to the shared rate-limit planner/tracker.

A command or API path that can trigger remote access must be able to answer:

- which provider was requested
- which provider was actually used
- whether a provider was skipped for policy reasons
- whether a provider was skipped for quota reasons
- whether work was deferred due to quota

## Domain examples

### SEC

Use a provider row for the SEC itself and optional sub-surface metadata if needed.

### Earnings

Provider rows should exist for at least:

- `motley_fool`
- `finnhub`
- `fmp`
- `kaggle_motley_fool`
- `local_tabular`
- `motley_fool_pickle`

### Fed

Provider rows should exist for at least:

- document source families
- release providers
- `fred`

### News

Provider rows should exist for at least:

- `nyt_archive`
- `gdelt_recent`
- `newsdata`
- `local_tabular`

## Acceptance criteria for this slice

All four repos pass this slice when:

1. a canonical provider registry artifact exists or is reserved
2. at least one provider row exists for each active content domain
3. provider selection, direct-resolution policy, and rate-limit policy can be explained from normalized registry state
4. the registry can be queried without live network access
