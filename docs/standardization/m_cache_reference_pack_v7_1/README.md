# M-Cache Wave 7.1 Reference Pack Starter

This starter pack is for **Wave 7.1 planning**, not direct implementation approval.

Wave 7 closed the **consumer-repo lifecycle hardening** pass.
Wave 7.1 now shifts focus to the **external package repo/process itself**.

Wave 7.1 is the **external package repo hardening and shared release execution wave**.

## Wave 7.1 goals

1. Define governance for `m-cache-shared-ext` as the canonical external package source of truth.
2. Define the actual RC creation, validation, promotion, and stable release execution workflow.
3. Define the central release artifact and evidence-bundle requirements.
4. Define how the four consumer repos participate in one shared RC/stable execution cycle.
5. Define the package-side rollback and incident process.
6. Define the exact start gate for the comprehensive cross-application user-testing program.
7. Keep the shared public API frozen and keep cleanup/removal work deferred.

## Wave 7.1 is not

- a public API expansion wave,
- a runtime behavior redesign wave,
- a domain-logic externalization wave,
- a repo merge wave,
- an immediate shim-removal wave.

## Read in this order

1. `WAVE7_1_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_EXTERNAL_PACKAGE_REPO_GOVERNANCE.md`
3. `CANONICAL_SHARED_RELEASE_EXECUTION_WORKFLOW.md`
4. `CANONICAL_RELEASE_ARTIFACTS_AND_EVIDENCE_BUNDLE.md`
5. `CANONICAL_PROMOTION_ROLLBACK_AND_INCIDENT_FLOW.md`
6. `CANONICAL_CROSS_REPO_RELEASE_EXECUTION_MATRIX.md`
7. `CANONICAL_USER_TESTING_START_GATE.md`
8. `CANONICAL_CLEANUP_DEFERRAL_AND_ENTRY_CRITERIA.md`
9. `MIGRATION_CHECKLIST.md`
10. `REFERENCE_MODELS.py`
11. `schemas/*`
12. `examples/*`

## Core Wave 7.1 rule

Harden the external package repo and the shared release execution lifecycle before running the comprehensive cross-application user-testing program or removing temporary local compatibility layers.
