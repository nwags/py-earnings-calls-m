# Canonical API Resolution Conventions

## Goal

Standardize how API responses expose provider and resolve metadata while preserving existing endpoint shapes and defaults.

## General rules

1. Keep existing endpoint paths whenever possible in Wave 2.
2. Prefer additive metadata fields over breaking response changes.
3. Browse/list endpoints remain local-only by default.
4. Detail/content endpoints may expose explicit remote resolution controls.
5. Do not add a brand-new dedicated resolve endpoint unless the repo truly needs it and the existing detail/content endpoints cannot express the behavior cleanly.

## Canonical API metadata envelope

When an endpoint is remote-capable, response metadata should be able to expose:
- `resolution_mode`
- `remote_attempted`
- `provider_requested`
- `provider_used`
- `served_from`
- `persisted_locally`
- `rate_limited`
- `retry_count`
- `deferred_until`
- `reason_code`

These may be surfaced either:
- as top-level additive response fields, or
- in a dedicated metadata object,

as long as the repo stays internally consistent.

## Canonical query controls

Repos may keep existing endpoint-specific controls for backward compatibility.
Canonical convergence target is:
- explicit resolution mode or explicit remote-resolution flag on detail/content endpoints only
- no remote-side-effect flag on list endpoints

### Compatibility examples
- earnings may keep `resolution_mode`
- fed may keep `resolve_on_miss=true`
- news may keep `resolve_remote=true`

Wave 2 should document how these map onto the canonical resolution vocabulary.

## Remote-side-effect rule

If an API request performs remote work, the response should clearly expose:
- that remote work occurred,
- which provider path was used,
- whether the result was persisted,
- whether rate limit or policy affected the result.

## Status-code rule

Wave 2 should preserve current endpoint behavior/status codes unless a repo already has a clean additive path to canonicalization.
Do not redesign response codes broadly in this wave.

## Future convergence note

Long-term `/v1/{domain}/...` namespacing may still happen later, but Wave 2 does not require it.
Wave 2 standardizes metadata semantics first.
