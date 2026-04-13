# M-Cache Wave 5 Reference Pack Starter

This starter pack is for **Wave 5 planning**, not direct implementation approval.

Wave 5 is the first real shared-package extraction wave. The prior waves standardized:
- shared command/config/runtime structure,
- provider/rate-limit/resolve semantics,
- textual augmentation outer metadata,
- producer-protocol concepts,
- canonical augmentation command family,
- Wave 4.1 normalization of naming and workflow shape.

Wave 5 now plans the first extraction of a real internal shared package for the stable outer protocol/helper layer.

## Wave 5 goals

1. Define the minimum viable first version of `m_cache_shared`.
2. Bound exactly what moves into the shared package now.
3. Keep domain-specific retrieval, identity, storage, and execution logic repo-local.
4. Preserve pilot vs non-pilot runtime behavior.
5. Keep repos independently runnable and independently testable.

## Wave 5 is planning-first

Use this pack to get comparable Wave 5 plans from:
- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

Do not treat this starter pack as implementation approval by itself.

## Read in this order

1. `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
2. `CANONICAL_EXTRACTABLE_MODELS_ENUMS_AND_HELPERS.md`
3. `CANONICAL_PROTOCOL_COMPATIBILITY_AND_ROLE_PRESERVATION.md`
4. `CANONICAL_PACKAGE_ADOPTION_AND_ROLLBACK.md`
5. `CANONICAL_TESTING_AND_INTEGRATION_EXPECTATIONS.md`
6. `MIGRATION_CHECKLIST.md`
7. `REFERENCE_MODELS.py`
8. `schemas/*`
9. `examples/*`

## In scope

- first real shared package boundary
- exact extractable models/enums/validators/packers/helpers
- module mapping from repo-local code to shared package candidates
- role-preserving adoption strategy
- rollback and versioning strategy
- testing expectations for shared package and repo integrations

## Out of scope

- formal merge
- extracting adapters
- extracting domain identity logic
- extracting storage/path derivation
- extracting execution/orchestration engines
- extracting parser/extractor internals
- redesigning producer protocol semantics
- broad runtime behavior changes
- broad authority model changes

## Core Wave 5 rule

**Extract only the normalized outer protocol/helper layer.**
Keep all domain-specific retrieval, identity, storage, and execution logic repo-local.

## Expected Wave 5 plan deliverables

Every repo's Wave 5 plan should answer:
1. which exact local modules/classes/functions can move into `m_cache_shared` now,
2. which exact local modules/classes/functions must remain repo-local,
3. what import/adoption sequence the repo will follow,
4. how pilot vs non-pilot behavior remains unchanged,
5. what tests prove behavior did not change,
6. what rollback/versioning plan applies.
