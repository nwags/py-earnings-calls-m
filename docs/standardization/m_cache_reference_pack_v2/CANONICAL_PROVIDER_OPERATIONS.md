# Canonical Provider Operations

## Goal

Standardize the operator and runtime semantics for provider-aware behavior across all domains.

## Canonical provider surfaces

Every repo should converge on these core commands:

```bash
m-cache <domain> providers list
m-cache <domain> providers show --provider <provider_id>
```

Optional Wave 2 read-only commands are allowed when they directly help operators understand strategy without causing remote side effects:

```bash
m-cache <domain> providers explain-resolution --provider <provider_id>
m-cache <domain> providers validate-config
```

### Compatibility rule

- Existing project-specific provider commands may remain.
- `m-cache <domain> providers list` and `providers show` are the canonical shared surfaces.
- New synonyms should not be invented if `show` can express the same meaning.

## `providers list`

Purpose:
- list available providers from local normalized authority
- support offline inspection
- expose enough information to understand eligibility and fallback order

Minimum fields to display or emit in JSON:
- `provider_id`
- `domain`
- `content_domain`
- `display_name`
- `provider_type`
- `is_active`
- `fallback_priority`
- `rate_limit_policy`
- `direct_resolution_allowed`
- `supports_direct_resolution`
- `supports_incremental_refresh`

Optional filters:
- `--content-domain`
- `--active-only`
- `--provider-type`

## `providers show`

Purpose:
- show one provider's effective policy bundle
- include canonical registry data plus effective config overlay
- work without live network access

Minimum effective fields:
- all required Wave 1 registry fields
- `default_timeout_seconds`
- `quota_window_seconds`
- `quota_reset_hint`
- `expected_error_modes`
- `user_agent_required`
- `contact_requirement`
- `terms_url`
- `effective_auth_present`
- `effective_enabled`

## `providers explain-resolution` (optional)

This is allowed in Wave 2 as an additive read-only surface.
It should explain:
- whether a provider is eligible for a content domain / representation / resolution mode,
- why it would be used or skipped,
- what degradation policy would apply.

It must not perform remote access.

## Provider selection rules

### Rule 1
Provider selection must be deterministic.

### Rule 2
An explicit provider request wins over fallback order when policy allows.

### Rule 3
Fallback order is evaluated only among active providers eligible for the selector and resolution mode.

### Rule 4
A skipped provider should have an inspectable reason, such as:
- `inactive`
- `policy_denied`
- `quota_limited`
- `missing_auth`
- `content_domain_mismatch`
- `mode_unsupported`
- `unavailable`

## Domain-specific notes

### SEC
Model the SEC as the provider, but preserve SEC-specific companion metadata like source surfaces.

### Earnings
Keep transcript and forecast provider families separate even when they are shown through one shared provider surface.

### Fed
Keep `documents`, `releases`, and `series` provider semantics distinct.

### News
Preserve provider-specific fetch/content behavior and do not drift into augmentation scope.

## Acceptance criteria for this slice

All four repos pass this slice when:
1. `providers list` exists on `m-cache <domain>`
2. `providers show --provider` exists on `m-cache <domain>`
3. provider inspection is offline and deterministic
4. provider skip/eligibility reasoning is inspectable without remote access
