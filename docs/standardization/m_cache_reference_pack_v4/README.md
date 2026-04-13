# M-Cache Wave 4 Reference Pack Starter

This starter pack is for **Wave 4 planning**, not direct implementation approval.

Wave 4 builds on the now-closed earlier waves:

- Wave 1 standardized the shared command/config/runtime spine.
- Wave 2 standardized provider/rate-limit/resolve/API transparency semantics.
- Wave 3 standardized the textual augmentation plane at the **outer metadata contract** level while preserving repo-local execution internals.
- Wave 4 plans the next controlled step:
  1. bounded extraction of a **shared internal package** for stable contracts/helpers,
  2. a shared **external augmentation producer protocol** for text-bearing resources.

## Wave 4 goals

1. Define the minimum viable shared internal package.
2. Define the producer protocol for external augmentation services.
3. Preserve standalone repos, local identities, local adapters, and local execution engines.
4. Keep payload schemas producer-owned while shared outer metadata stays standardized.
5. Stop short of a formal merge.

## Wave 4 is planning-first

Use this pack to get comparable Wave 4 plans from:
- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

Do not treat this starter pack as implementation approval by itself.

## Read in this order

1. `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
2. `CANONICAL_SHARED_MODELS_AND_HELPERS.md`
3. `CANONICAL_EXTERNAL_AUGMENTATION_PRODUCER_PROTOCOL.md`
4. `CANONICAL_ANNOTATION_TARGETING_AND_PAYLOAD_GUIDANCE.md`
5. `CANONICAL_ROLLOUT_AND_VERSIONING.md`
6. `MIGRATION_CHECKLIST.md`
7. `REFERENCE_MODELS.py`
8. `schemas/*`
9. `examples/*`

## In scope

- shared internal package boundary
- initial extraction candidates
- shared model/validator/helper boundaries
- explicit augmentation producer protocol
- stand-off annotation targeting rules
- idempotency/versioning/replay rules
- multi-producer coexistence rules
- repo-specific adoption plans

## Out of scope

- formal merge
- extracting adapters
- extracting domain identity logic
- extracting storage/path rules
- extracting augmentation execution engines
- broad historical rewrites
- forcing a universal augmentation payload schema
- redesigning monitor/reconcile internals

## Core rule carried from Wave 3

**Text-bearing resources are augmentation-eligible.**
**Numeric-only resources are not.**
