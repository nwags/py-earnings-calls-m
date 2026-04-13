# M-Cache Wave 6.1 Reference Pack Starter

This starter pack is for **Wave 6.1 planning**, not direct implementation approval.

Wave 6 proved shim-first external adoption can work without changing repo behavior.
Wave 6.1 is the short convergence pass that should make external adoption **operationally uniform** across all four repos.

## Wave 6.1 goals

1. Choose one canonical external package identity.
2. Choose one canonical external import root.
3. Choose one canonical Git-tag pinning convention.
4. Choose one canonical shim/fallback contract.
5. Validate one shared external release-candidate tag across all four repos.
6. Preserve all current pilot/non-pilot, applicability, and authority behavior.

## Wave 6.1 is not

- a new extraction wave,
- a runtime behavior redesign wave,
- a domain-logic externalization wave,
- a broad cleanup/removal wave.

## Core Wave 6.1 decision

Use a **distinct external import root** during early externalization so repos do not depend on import-precedence tricks while local `m_cache_shared` packages still exist.

Recommended canonical external identity for the convergence pass:
- distribution name: `m-cache-shared-ext`
- import root: `m_cache_shared_ext.augmentation`
- pin file path: `requirements/m_cache_shared_external.txt`
- first shared release-candidate validation tag: one common tag across all repos

## Read in this order

1. `WAVE6_1_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_EXTERNAL_PACKAGE_IDENTITY.md`
3. `CANONICAL_SHIM_AND_FALLBACK_CONTRACT.md`
4. `CANONICAL_RELEASE_CANDIDATE_AND_PINNING.md`
5. `CANONICAL_ROLE_AND_AUTHORITY_FREEZE.md`
6. `CANONICAL_CROSS_REPO_RC_VALIDATION.md`
7. `MIGRATION_CHECKLIST.md`
8. `REFERENCE_MODELS.py`
9. `schemas/*`
10. `examples/*`
