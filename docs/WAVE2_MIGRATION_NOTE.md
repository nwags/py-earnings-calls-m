# Wave 2 Migration Note (py-earnings-calls-m)

This note documents Wave 2 provider/resolution/API standardization for
`py-earnings-calls-m` only.

Scope:

- standalone repo only,
- additive and compatibility-first,
- no transcript/forecast identity flattening,
- no broad artifact rewrite.

## Canonical Wave 2 Surfaces

- `m-cache earnings providers list`
- `m-cache earnings providers show --provider <provider_id>`
- `m-cache earnings resolve transcript --call-id <id> --resolution-mode ...`
- `m-cache earnings resolve forecast-snapshot --provider ... --symbol ... --date ... --resolution-mode ...`

## Compatibility and Aliases

- `py-earnings-calls ...` remains the operator compatibility surface.
- Legacy defaults remain unchanged on `py-earnings-calls ...`.
- `m-cache earnings ...` remains the canonical additive shared surface.
- Transcript and forecast identities remain distinct:
  - transcript: `call_id` scoped
  - forecast snapshot: `provider + symbol + as_of_date` scoped

## Provider and Rate-Limit Transparency

- Provider registry keeps Wave 1 fields and adds Wave 2 policy fields:
  - `default_timeout_seconds`
  - `quota_window_seconds`
  - `quota_reset_hint`
  - `expected_error_modes`
  - `user_agent_required`
  - `contact_requirement`
  - `terms_url`
- `providers show` includes effective overlay fields:
  - `effective_auth_present`
  - `effective_enabled`
- Canonical summary/progress/resolution outputs now expose additive provider
  selection and quota/defer metadata where relevant:
  - `provider_requested`
  - `provider_used`
  - `selection_outcome`
  - `rate_limited`
  - `retry_count`
  - `deferred_until`
  - `provider_skip_reasons`

## API Resolution Transparency (Additive)

Remote-capable detail/content paths preserve existing routes and status behavior
while adding metadata transparency fields:

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

Browse/list endpoints remain local-only by default.

## Resolution Events (Additive)

`refdata/normalized/resolution_events.parquet` is enriched additively for new
rows with Wave 2 provider-usage metadata fields (no historical rewrite).

## Reserved for Later Waves

- shared cross-repo package extraction,
- broad historical event migration/backfill,
- deep pipeline refactors beyond additive Wave 2 alignment,
- endpoint path redesign or major status-code redesign.
