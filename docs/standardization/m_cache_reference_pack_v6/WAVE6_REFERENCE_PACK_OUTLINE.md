# Wave 6 Reference Pack Outline

## Purpose

Wave 6 is the planning wave for externalizing `m_cache_shared` into a true shared package source of truth.

Wave 5 created the first real in-repo shared package.
Wave 5.1 normalized that package layout and export surface across repos.

Wave 6 should now plan:
- where the shared package lives,
- what the first public API is,
- how repos adopt it safely,
- how rollback/versioning work,
- how role and authority boundaries remain intact.

## Key decision

The first external public API should be the **strict proven common subset**, not the union of everything exposed in all repos.

## Why this matters

A narrow first public API reduces the risk of:
- hard-coding repo-specific behavior into the external package,
- freezing unstable or partially shared symbols too early,
- forcing one repo to adopt semantics that are not yet truly common.

## Acceptance criteria

Wave 6 planning is complete when:
1. the external package source-of-truth model is explicit,
2. the first public API is explicitly bounded,
3. adoption/pinning/rollback strategy is explicit,
4. role and authority preservation is explicit,
5. no domain-local logic is pulled into the external package.
