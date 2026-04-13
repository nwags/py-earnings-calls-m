# Wave 7.1 Reference-Pack Outline

## Purpose

Wave 7.1 is the **external package repo hardening and shared release execution wave**.

Wave 7 closed the repo-side lifecycle hardening problem:
- the consumer repos now know their validator/signoff role,
- RC/stable evidence expectations are documented,
- rollback and cleanup deferral are documented,
- cross-application user testing is placed in the lifecycle.

Wave 7.1 now addresses the remaining gap:
- the external package repo/process itself must become operationally real and managed.

## Core Wave 7.1 Goal

Make `m-cache-shared-ext` not just a converged dependency target, but a **fully managed release artifact** with:
- package-repo governance,
- explicit RC creation workflow,
- shared release execution process,
- promotion/rejection rules,
- central evidence bundle requirements,
- package-side rollback/incident flow,
- explicit entry criteria for the cross-application user-testing program.

## Core Principle

### Harden package-side lifecycle before broadening scope or removing safety layers

Wave 7.1 should prioritize:
- package-side release discipline,
- shared execution discipline,
- evidence discipline,
- operational trust.

Wave 7.1 should **not** broaden the public API or begin cleanup/removal work.

## Where User Testing Fits

Wave 7.1 should define the exact **start gate** for the comprehensive user-testing program.

Recommended placement:
- after Wave 7.1 planning and implementation,
- after at least one RC/stable execution path is operational,
- after one shared release candidate has been built and validated through the package-side lifecycle,
- before any cleanup/shim-retirement wave.

So user testing remains:
- a **post-Wave-7.1 stabilization gate**, and
- a **pre-cleanup / pre-shim-retirement gate**.

## Suggested Wave 7.1 Scope

### In scope
- external package repo governance
- package-side RC/stable execution workflow
- central release artifact requirements
- central evidence bundle format
- cross-repo release execution matrix
- package-side rollback/incident flow
- user-testing start criteria
- continued cleanup deferral policy

### Out of scope
- broadening the public API
- removing local shims/fallbacks
- removing one-cycle env aliases
- changing runtime behavior
- externalizing domain-local logic
- merging repos

## Workstream 1: External Package Repo Governance

Define:
- the authoritative external package repository,
- ownership/approval structure,
- public API freeze policy,
- release manager role,
- required reviews for RC/stable promotion,
- documentation/changelog ownership.

## Workstream 2: Shared Release Execution Workflow

Define the real execution lifecycle:
1. package change lands,
2. package tests run,
3. RC is cut,
4. shared evidence bundle is prepared,
5. all four consumer repos validate the same RC,
6. signoffs are collected,
7. promotion/rejection decision is made,
8. stable release is cut or RC is rejected.

## Workstream 3: Release Artifacts and Evidence Bundle

Define required artifacts:
- changelog entry,
- release notes,
- compatibility notes,
- API freeze statement,
- RC/stable validation evidence,
- signoff records,
- rollback notes,
- incident links if relevant.

## Workstream 4: Promotion / Rejection / Rollback Flow

Define:
- who can propose promotion,
- who can block promotion,
- who can declare rollback,
- what evidence is required,
- how failed RCs are recorded,
- how stable rollback is handled.

## Workstream 5: Cross-Repo Release Execution Matrix

Define how the four repos participate in one shared release cycle:
- earnings = pilot consumer-validator,
- news = pilot consumer-validator,
- fed = validator + signoff for non-pilot/applicability safety,
- SEC = conservative validator/signoff for no-dual-authority and non-pilot safety.

## Workstream 6: User-Testing Start Gate

Define the exact criteria for beginning the comprehensive user-testing program, for example:
- one RC/stable execution cycle operational,
- one shared RC validated across all repos,
- evidence bundle format working,
- rollback path verified,
- no open blocking lifecycle issues.

## Workstream 7: Cleanup Deferral and Entry Criteria

Define what must still remain deferred and what conditions must be satisfied before a later cleanup wave may start.

## Recommended Deliverables

Wave 7.1 reference pack should include:
- `WAVE7_1_REFERENCE_PACK_OUTLINE.md`
- `CANONICAL_EXTERNAL_PACKAGE_REPO_GOVERNANCE.md`
- `CANONICAL_SHARED_RELEASE_EXECUTION_WORKFLOW.md`
- `CANONICAL_RELEASE_ARTIFACTS_AND_EVIDENCE_BUNDLE.md`
- `CANONICAL_PROMOTION_ROLLBACK_AND_INCIDENT_FLOW.md`
- `CANONICAL_CROSS_REPO_RELEASE_EXECUTION_MATRIX.md`
- `CANONICAL_USER_TESTING_START_GATE.md`
- `CANONICAL_CLEANUP_DEFERRAL_AND_ENTRY_CRITERIA.md`
- `MIGRATION_CHECKLIST.md`

And then four repo-specific Wave 7.1 plan-mode prompts.

## Acceptance Criteria

Wave 7.1 planning is complete when:
1. external package repo governance is explicit,
2. the package-side RC/stable execution workflow is explicit,
3. release artifact/evidence requirements are explicit,
4. promotion/rejection/rollback flow is explicit,
5. user-testing start criteria are explicit,
6. cleanup remains explicitly deferred,
7. no public API broadening is introduced.
