# Canonical Config Schema

## Goal

Define one shared configuration document and loader model that works across all domains while preserving domain-specific storage semantics.

## Preferred file name

```text
m-cache.toml
```

A repo may temporarily support legacy env vars and project-specific config files, but `m-cache.toml` is the canonical shared format.

## Loader precedence

1. explicit CLI `--config PATH` if provided
2. `M_CACHE_CONFIG` env var if provided
3. `./m-cache.toml` in project root
4. domain/project compatibility env vars
5. built-in defaults

## Canonical structure

```toml
[global]
app_root = "."
log_level = "INFO"
default_summary_json = false
default_progress_json = false

[domains.sec]
enabled = true
cache_root = ".sec_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"

[domains.earnings]
enabled = true
cache_root = ".earnings_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"

[domains.fed]
enabled = true
cache_root = ".fed_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"

[domains.news]
enabled = true
cache_root = ".news_cache"
normalized_refdata_root = "refdata/normalized"
lookup_root = "refdata/normalized"
default_resolution_mode = "local_only"
```

## Required concepts

### Global

Global settings apply to all domains unless a domain overrides them.

Required global keys:

- `app_root`
- `log_level`
- `default_summary_json`
- `default_progress_json`

Recommended global keys:

- `default_progress_heartbeat_seconds`
- `default_http_timeout_seconds`
- `default_retry_budget`
- `default_user_agent`

### Domain block

Each domain block must support:

- `enabled`
- `cache_root`
- `normalized_refdata_root`
- `lookup_root`
- `default_resolution_mode`
- `providers.*`
- `runtime.*`

## Resolution modes

Allowed values:

- `local_only`
- `resolve_if_missing`
- `refresh_if_stale`

### Rule

A repo may implement only a subset in its first wave, but the enum should still be shared.
Unimplemented modes must fail transparently, not silently downgrade.

## Provider config shape

Provider configuration lives under the owning domain.

Example:

```toml
[domains.earnings.providers.finnhub]
enabled = true
auth_type = "api_key_header"
api_key_env = "FINNHUB_API_KEY"
rate_limit_policy = "per_minute"
soft_limit = 60
hard_limit = 60
burst_limit = 5
retry_budget = 2
direct_resolution_allowed = true
browse_discovery_allowed = true
fallback_priority = 10
free_tier_notes = "Free tier may throttle aggressively near market open."
```

## Domain-specific path rule

This schema standardizes **roots**, not every subdirectory.

Examples:

- SEC may keep cache internals under `.sec_cache/Archives/...`
- Earnings may keep `.earnings_cache/transcripts/...` and `.earnings_cache/forecasts/...`
- Fed may keep `.fed_cache/documents/...`, `.fed_cache/releases/...`, `.fed_cache/series/...`
- News may keep publisher-centered article folders under `.news_cache/...`

These sub-layouts remain domain contracts.

## Extensibility rule

A future domain should be addable by introducing:

1. a new `[domains.<name>]` block
2. a provider registry materializer for that domain
3. domain adapters and schema contracts
4. `m-cache <name>` dispatch registration

No shared loader change should be required for a new domain beyond registering the domain name.

## Compatibility env vars

Legacy env vars may continue temporarily.
They should be translated into canonical config fields during load.

Examples:

- `PY_SEC_EDGAR_DOWNLOAD_ROOT` -> `domains.sec.cache_root` plus domain-specific path mapping
- `PY_SEC_EDGAR_NORMALIZED_REFDATA_ROOT` -> `domains.sec.normalized_refdata_root`
- `PY_FED_CACHE_ROOT` -> `domains.fed.cache_root`
- `PY_NEWS_CACHE_ROOT` -> `domains.news.cache_root`

## Validation rules

### Rule 1
Every enabled domain must have a cache root and normalized refdata root.

### Rule 2
Every configured provider must declare auth type, rate-limit policy, and direct-resolution policy.

### Rule 3
Unknown top-level keys should be rejected unless explicitly allowed by an extension namespace.

### Rule 4
Config loading must produce one resolved effective config object that can be emitted in service runtime summaries.
