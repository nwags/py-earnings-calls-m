# M-Cache Wave 7.2 Reference Pack Starter

This starter pack is for **Wave 7.2 planning**, not direct implementation approval.

Wave 7 closed the consumer-repo lifecycle-hardening pass.
Wave 7.1 closed the consumer-repo package-side release-execution participation pass.
Wave 7.2 now shifts focus to the **external package repo/process itself**.

Wave 7.2 is the **`m-cache-shared-ext` repo/process implementation and first real shared RC cycle wave**.

## Wave 7.2 goals

1. Define and implement the package-repo-side governance and release workflow for `m-cache-shared-ext`.
2. Define the first real RC creation, evidence-bundle, signoff-ingestion, promotion/rejection, and rollback flow.
3. Execute one real shared RC cycle across all four consumer repos.
4. Keep the strict common v1 public API frozen.
5. Keep cleanup/removal work deferred.
6. Reach the start gate for the comprehensive cross-application user-testing program.

## Wave 7.2 is not

- a public API expansion wave,
- a runtime behavior redesign wave,
- a cleanup/removal wave,
- a domain-logic externalization wave,
- a repo merge wave.

## Read in this order

1. `WAVE7_2_REFERENCE_PACK_OUTLINE.md`
2. `CANONICAL_EXTERNAL_PACKAGE_REPO_IMPLEMENTATION.md`
3. `CANONICAL_FIRST_REAL_RC_EXECUTION.md`
4. `CANONICAL_EVIDENCE_BUNDLE_AND_SIGNOFF_INGESTION.md`
5. `CANONICAL_PROMOTION_REJECTION_AND_ROLLBACK_EXECUTION.md`
6. `CANONICAL_CONSUMER_REPO_RC_COMPANION_OBLIGATIONS.md`
7. `CANONICAL_USER_TESTING_HANDOFF.md`
8. `MIGRATION_CHECKLIST.md`
9. `REFERENCE_MODELS.py`
10. `schemas/*`
11. `examples/*`

## Core Wave 7.2 rule

Wave 7.2 is centered on the **external package repo/process**. Consumer repos participate only as validators/signoff contributors for the first real RC cycle.
