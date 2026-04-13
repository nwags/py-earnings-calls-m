# Wave 3 Migration Note (py-earnings-calls-m)

This note documents the Wave 3 implementation scope for
`py-earnings-calls-m` only.

Wave 3 remains additive, compatibility-first, and standalone.

## What Became Canonical in Wave 3

- canonical additive augmentation family on `m-cache earnings`:
  - `m-cache earnings aug list-types`
  - `m-cache earnings aug inspect-target ...`
  - `m-cache earnings aug inspect-runs ...`
  - `m-cache earnings aug inspect-artifacts ...`
- transcript augmentation targeting is `call_id`-scoped.
- transcript augmentation metadata is source-text-version aware through
  `source_text_version` handling in run/artifact/API inspection metadata.
- shared outer augmentation metadata contracts are materialized as local
  canonical metadata artifacts:
  - `refdata/normalized/augmentation_runs.parquet`
  - `refdata/normalized/augmentation_events.parquet`
  - `refdata/normalized/augmentation_artifacts.parquet`
- additive transcript API metadata:
  - transcript detail responses include `augmentation_meta`
  - transcript content responses include additive augmentation headers

## Explicit Wave 3 Resource Applicability

- augmentation-eligible in Wave 3:
  - transcript text resources
- excluded in Wave 3:
  - numeric forecast snapshots/points
- future-only exception:
  - if a persisted narrative-text forecast family is introduced later, that
    new family may become augmentation-eligible without changing numeric
    forecast contracts.

## Compatibility-Preserved Surfaces

- `py-earnings-calls ...` remains the operator compatibility surface.
- `m-cache earnings ...` remains the additive canonical shared surface.
- transcript and forecast identities remain distinct.
- no shared-package extraction in this wave.

## Domain-Local by Design (Unchanged)

- transcript/forecast adapters
- identity logic
- storage layout/path rules
- provider resolution internals
- domain augmentation payload bodies

Wave 3 standardizes only the outer metadata contract, not a universal payload
schema.

## Reserved for Later Waves

- augmentation execution orchestration (`submit/status/events` runtime flows)
- shared package extraction and cross-repo pin/version rollouts
- broad historical augmentation artifact rewrites
- any forecast model flattening or deep pipeline rewrites
