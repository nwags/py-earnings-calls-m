# Canonical Resolution Semantics

## Goal

Standardize explicit resolve behavior across all domains without flattening domain identities.

## Canonical resolution modes

Allowed shared modes:
- `local_only`
- `resolve_if_missing`
- `refresh_if_stale`

### Rule
If a repo does not implement one of these modes in a given path, it must fail transparently rather than silently downgrading.

## Shared meanings

### `local_only`
- use local normalized/index/cache state only
- do not perform remote work
- expose transparent local-miss metadata when relevant

### `resolve_if_missing`
- attempt local resolution first
- if local state is insufficient and policy allows, perform bounded remote resolution
- persist locally when the path contract says the result is canonical and storable

### `refresh_if_stale`
- only valid when the repo can actually evaluate and act on staleness
- if not implemented on that path, return an explicit unsupported-mode response

## Canonical CLI resolve surface

Each repo should converge on explicit resolve verbs for canonical identities.
Examples:

```bash
m-cache earnings resolve transcript --call-id <id>
m-cache fed resolve document --document-id <id>
m-cache news resolve article --article-id <id>
m-cache sec resolve filing --accession-number <id>
```

### Compatibility rule
Existing repo-specific flags may remain, but canonical `--resolution-mode` should be supported on the `m-cache` surface.

## Canonical resolution result metadata

A resolve-capable CLI/API path should be able to expose:
- `resolution_mode`
- `provider_requested`
- `provider_used`
- `served_from`
- `remote_attempted`
- `success`
- `reason_code`
- `persisted_locally`
- `rate_limited`
- `retry_count`
- `deferred_until`

## `served_from` values

Allowed values:
- `local_cache`
- `local_normalized`
- `remote_then_persisted`
- `remote_ephemeral`
- `none`

## Shared reason-code guidance

Recommended shared codes when applicable:
- `local_hit`
- `local_miss`
- `remote_fetched`
- `provider_not_configured`
- `policy_denied`
- `mode_unsupported`
- `quota_limited`
- `deferred`
- `retry_exhausted`
- `not_found_remote`

Domain-specific reason codes remain allowed.

## Endpoint class rule

### Browse/list endpoints
Browse/list endpoints should remain local-only by default and should not trigger remote side effects.

### Detail/content endpoints
Detail/content endpoints may support explicit remote resolution when policy allows.

## Acceptance criteria for this slice

All four repos pass this slice when:
1. canonical resolution-mode vocabulary is consistent
2. unsupported modes fail transparently
3. list/browse endpoints remain local-only by default
4. detail/content resolution metadata is aligned
