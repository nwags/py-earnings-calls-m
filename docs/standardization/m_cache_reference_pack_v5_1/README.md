# M-Cache Wave 5.1 Reference Pack Starter

This starter pack is for **Wave 5.1 planning**, not direct implementation approval.

Wave 5 proved the first real in-repo `m_cache_shared` extraction boundary.
Wave 5.1 is a short normalization pass focused on **`m_cache_shared` itself**.

## Wave 5.1 goals

1. Normalize `m_cache_shared` package layout across all repos.
2. Normalize the public export surface of `m_cache_shared.augmentation`.
3. Normalize shared symbol naming where behavior is already intentionally shared.
4. Normalize shared-package test naming and docs structure.
5. Preserve all current runtime behavior exactly:
   - pilot repos stay pilot,
   - non-pilot repos stay non-pilot,
   - CLI/API/operator behavior stays stable,
   - no additional extraction scope is introduced.

## Wave 5.1 is not

- a new extraction wave,
- a producer protocol redesign,
- an external package publication wave,
- a merge wave,
- a runtime behavior change wave.

## Read in this order

1. `WAVE5_1_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_SHARED_PACKAGE_LAYOUT.md`
3. `CANONICAL_SHARED_PACKAGE_EXPORT_SURFACE.md`
4. `CANONICAL_SHARED_PACKAGE_TEST_AND_DOC_NORMALIZATION.md`
5. `CANONICAL_ROLE_AND_BEHAVIOR_FREEZE.md`
6. `MIGRATION_CHECKLIST.md`
7. `REFERENCE_MODELS.py`
8. `schemas/*`
9. `examples/*`

## Core Wave 5.1 rule

Normalize `m_cache_shared` **shape and exports**, not extraction scope.
