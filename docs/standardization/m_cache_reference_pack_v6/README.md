# M-Cache Wave 6 Reference Pack Starter

This starter pack is for **Wave 6 planning**, not direct implementation approval.

Wave 6 is the **externalization planning wave** for `m_cache_shared`.

Waves 5 and 5.1 proved and normalized the in-repo shared package boundary. Wave 6 should now plan how `m_cache_shared` becomes a **single maintained source of truth** across repos.

## Wave 6 goals

1. Define how `m_cache_shared` becomes an external shared package source of truth.
2. Define the **first external public API** as the strict proven common subset only.
3. Preserve pilot vs non-pilot runtime behavior.
4. Preserve all domain-local identity, storage, adapter, and execution logic.
5. Define versioning, pinning, adoption, compatibility, and rollback strategy.

## Wave 6 is not

- a broad new extraction wave,
- a merge wave,
- a runtime behavior redesign wave,
- a domain-logic externalization wave.

## Read in this order

1. `WAVE6_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_EXTERNAL_PACKAGE_STRATEGY.md`
3. `CANONICAL_FIRST_PUBLIC_API_BOUNDARY.md`
4. `CANONICAL_ADOPTION_VERSIONING_AND_COMPATIBILITY.md`
5. `CANONICAL_ROLE_AND_AUTHORITY_PRESERVATION.md`
6. `CANONICAL_TESTING_AND_RELEASE_EXPECTATIONS.md`
7. `MIGRATION_CHECKLIST.md`
8. `REFERENCE_MODELS.py`
9. `schemas/*`
10. `examples/*`

## Core Wave 6 rule

Externalize `m_cache_shared` as a single source of truth, but expose only the strict proven common subset as the first public API.
