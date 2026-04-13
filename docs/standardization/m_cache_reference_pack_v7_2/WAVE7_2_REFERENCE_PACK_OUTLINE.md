# Wave 7.2 Reference-Pack Outline

## Purpose

Wave 7.2 is the **`m-cache-shared-ext` repo/process implementation and first real shared RC cycle wave**.

Wave 7 and Wave 7.1 completed the consumer-repo side of lifecycle hardening:
- validator/signoff roles are documented,
- rollback and cleanup deferral are documented,
- release participation artifacts exist,
- user-testing start-gate prerequisites are placed in the lifecycle.

Wave 7.2 now solves the remaining gap:
- the external package repo/process itself must become the operational source of truth.

## Core Wave 7.2 Goal

Make `m-cache-shared-ext` a **real executed release system**, not just a planned one, by implementing:
- package-repo governance,
- package-side RC workflow,
- central evidence-bundle structure,
- consumer signoff ingestion,
- promotion/rejection decision records,
- rollback mechanics,
- one real shared RC cycle across all four consumer repos.

## Core Principle

### Execute the package-side lifecycle before user testing or cleanup

Wave 7.2 should:
- implement the package-side release system,
- run one real RC cycle,
- prove the evidence/signoff/rollback flow,
- then hand off to the comprehensive user-testing program.

Wave 7.2 should not:
- broaden the public API,
- remove shims/fallbacks,
- reduce local safety layers.
