# Wave 4 Migration Note (py-earnings-calls-m)

This document covers the Wave 4 implementation in this repo only.

`py-earnings-calls-m` is treated as a **Wave 4 producer-protocol pilot repo**.

## What Became Canonical in Wave 4

- additive producer-protocol pilot surfaces for transcript augmentation:
  - transcript target descriptor
  - producer run submission envelope
  - producer artifact submission envelope
- transcript targeting remains `call_id`-scoped using the repo’s established
  canonical identity shape (`transcript:{call_id}`).
- source-text-version-aware producer flow is explicit:
  - target descriptor includes `source_text_version`
  - run/artifact submissions require `source_text_version`
- idempotent replay-safe handling is additive:
  - run submissions are idempotent by `run_id`
  - artifact submissions are idempotent by explicit or derived idempotency key
- read-back/inspection is enriched through additive run/artifact/event fields:
  - `producer_version`
  - `payload_schema_name`
  - `payload_schema_version`
  - `producer_run_key` (runs)
  - `idempotency_key`, `payload_sha256`, `payload_bytes` (artifacts)

## Implemented Producer Protocol Surfaces

- canonical additive CLI surfaces on `m-cache earnings aug`:
  - `inspect-target --call-id <call_id>`
  - `submit-run --input-json <path>`
  - `submit-artifact --input-json <path>`
  - `status --run-id <run_id>` (primary selector) with optional `--idempotency-key`
  - `events --resource-family transcripts ...` (timeline/audit inspection)
- additive API pilot surfaces:
  - `GET /transcripts/{call_id}/augmentation-target`
  - `POST /augmentations/runs`
  - `POST /augmentations/artifacts`
- existing transcript detail/content APIs remain valid producer text retrieval
  paths (`/transcripts/{call_id}`, `/transcripts/{call_id}/content`).

## Compatibility-Preserved and Repo-Local

- `py-earnings-calls ...` remains the operator compatibility surface.
- `m-cache earnings ...` remains the additive canonical shared surface.
- transcript and forecast identities remain distinct.
- payload schema ownership remains external/service-owned:
  - repo validates only outer metadata envelopes,
  - payload body is treated as opaque and may be locator-backed.
- inline payload handling is bounded; locator-backed artifacts are fully
  supported.
- adapters, storage/path derivation, transcript/forecast family internals,
  and execution engines remain repo-local.
- compatibility aliases remain available:
  - `target-descriptor`
  - `inspect-runs`
  - `inspect-artifacts`

## Explicit Applicability Boundary

- augmentation remains transcript-only in Wave 4.
- numeric forecast snapshots/points remain non-augmentation resources.
- future expansion is reserved for a new persisted narrative-text forecast
  family only.

## Reserved for Later Waves

- broad cross-repo shared-package extraction/adoption rollout
- augmentation execution orchestration (`submit/status/events` runners,
  scheduling, retries across services)
- universal payload schema/ontology standardization
- broad historical artifact rewrites or backfills
