# M-Cache Wave 3 Reference Pack Starter

This starter pack is for **Wave 3 planning**, not direct implementation approval.

Wave 3 builds on the now-closed Wave 1 and Wave 2 layers:

- Wave 1 standardized the shared command/config/provider-registry/runtime-event spine.
- Wave 2 standardized provider operations, rate-limit/degradation semantics, explicit resolve semantics, and additive API transparency.
- Wave 3 plans the next step:
  1. identify what common code is now stable enough to extract or share,
  2. define the shared textual augmentation plane,
  3. preserve domain-specific storage, identity, and adapter logic.

## Wave 3 goals

1. Define **shared extraction boundaries** for common code that is now stable across repos.
2. Define a **shared augmentation architecture** for text-bearing records.
3. Keep the repos separate while planning shared internal modules/packages.
4. Avoid a formal merge until the shared layer is proven stable.

## Wave 3 is planning-only

Use this pack to get consistent plans from:
- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

Do not use this pack as implementation approval by itself.

## Read in this order

1. `CANONICAL_SHARED_EXTRACTION_BOUNDARIES.md`
2. `CANONICAL_AUGMENTATION_APPLICABILITY_AND_MODEL.md`
3. `CANONICAL_AUGMENTATION_CLI_AND_API_SURFACES.md`
4. `CANONICAL_AUGMENTATION_ARTIFACTS_AND_EVENTS.md`
5. `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
6. `MIGRATION_CHECKLIST.md`
7. `REFERENCE_MODELS.py`
8. `schemas/*`
9. `examples/*`

## Explicit Wave 3 scope

### In scope
- shared internal package / module candidates
- extraction boundary definition
- common models for provider detail / rate-limit / resolution / augmentation
- common augmentation request/result/event/artifact contracts
- augmentation applicability matrix for text-bearing versus numeric-only resources
- shared CLI/API vocabulary planning for augmentation

### Out of scope
- formal merge into one application
- broad historical artifact rewrites
- flattening domain identities
- replacing domain-specific adapters
- redesigning monitor/reconcile beyond what earlier waves already covered
- implementing broad augmentation execution across all repos in one shot

## Core rule for augmentation

**If a resource retrieves and persists meaningful text, it should support augmentation.**
**If a resource is numeric-only, it should not be forced into augmentation.**

Examples:
- SEC filings -> yes
- News articles/content -> yes
- Fed speeches/releases/statements -> yes
- Fed numeric series points -> no
- Earnings transcripts -> yes
- Earnings numeric forecast snapshots/points -> no unless associated narrative text is present
