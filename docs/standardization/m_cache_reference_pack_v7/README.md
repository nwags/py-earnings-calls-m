# M-Cache Wave 7 Reference Pack Starter

This starter pack is for **Wave 7 planning**, not direct implementation approval.

Wave 7 is the **external shared package source-of-truth realization and release-management hardening wave**.

Waves 5 and 5.1 established and normalized the in-repo shared package shape.
Wave 6 established first-pass external adoption scaffolding.
Wave 6.1 converged the external package identity, import root, pinning contract, and shim/fallback behavior across repos.

Wave 7 should now define how the external package itself is:
- maintained,
- validated,
- promoted,
- versioned,
- rolled back,
- and eventually allowed to replace more temporary local compatibility layers.

## Wave 7 goals

1. Define governance for `m-cache-shared-ext` as the canonical external package source of truth.
2. Define formal RC creation, validation, promotion, and rollback rules.
3. Define semver and compatibility promises for the first external public API.
4. Define the cross-repo validation matrix against one RC/stable lifecycle.
5. Define the cross-application user testing gate.
6. Define when temporary local compatibility layers may be reduced later.

## Wave 7 is not

- a shared API expansion wave,
- a runtime behavior redesign wave,
- a domain-logic externalization wave,
- a repo merge wave,
- an immediate shim-removal wave.

## Read in this order

1. `WAVE7_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_EXTERNAL_PACKAGE_GOVERNANCE.md`
3. `CANONICAL_RELEASE_CANDIDATE_WORKFLOW.md`
4. `CANONICAL_PROMOTION_AND_STABLE_RELEASE_POLICY.md`
5. `CANONICAL_SEMVER_AND_COMPATIBILITY_POLICY.md`
6. `CANONICAL_CROSS_REPO_VALIDATION_MATRIX.md`
7. `CANONICAL_USER_TESTING_GATE.md`
8. `CANONICAL_ROLLBACK_AND_INCIDENT_POLICY.md`
9. `CANONICAL_LOCAL_SHIM_RETIREMENT_POLICY.md`
10. `MIGRATION_CHECKLIST.md`
11. `REFERENCE_MODELS.py`
12. `schemas/*`
13. `examples/*`

## Core Wave 7 rule

Treat `m-cache-shared-ext` as a managed release artifact before broadening the shared API or removing local safety layers.
