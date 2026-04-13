# Wave 7 Migration Note (py-earnings-calls-m)

## Scope Freeze (Lifecycle Hardening Only)

Wave 7 in `py-earnings-calls-m` is a lifecycle-hardening pass for external package release-management operations.

This wave explicitly does **not**:

- introduce new extraction scope,
- change runtime behavior,
- change CLI/API semantics,
- broaden the shared public API.

The strict-common v1 external surface remains the supported shared public API boundary.

## Repo Role in Wave 7

This repo remains:

- a **pilot consumer-validator** for transcript producer write-path behavior,
- a **release blocker** for transcript write-path regressions in RC/stable validation,
- **not** an external-package governance owner,
- **not** a public-API broadening authority.

Compatibility/canonical surfaces remain unchanged:

- `py-earnings-calls ...` remains the compatibility surface,
- `m-cache earnings ...` remains the additive canonical surface.

## Frozen Local Ownership and Applicability

Wave 7 keeps these boundaries unchanged:

- transcript-only augmentation applicability,
- forecast resources remain non-augmentation,
- transcript identity, target building, text retrieval, storage placement, API wire-shape ownership, and live write-path orchestration remain local.

## Required RC/Stable Evidence in This Repo

Every RC/stable candidate validated in this repo must provide evidence for:

- canonical external pin in `requirements/m_cache_shared_external.txt`,
- first-hop facade use through `py_earnings_calls.augmentation_shared`,
- source-mode contract behavior (`auto`, `external`, `local`) with local fallback available,
- strict-common v1 surface usage only,
- unchanged CLI/API/operator behavior,
- unchanged pilot transcript write-path behavior,
- unchanged applicability/authority behavior.

Minimum repo test command:

- `pytest -q`

## Blocker Conditions (RC/Stable)

Any of the following blocks RC/stable acceptance for this repo:

- transcript write-path regression,
- source-mode contract regression,
- missing or incomplete strict-common v1 external symbols,
- CLI/API semantic drift,
- role/applicability/authority drift,
- accidental dependency on non-public external symbols.

## Rollback and Incident Steps

Wave 7 rollback remains pin/facade driven:

1. Repin `requirements/m_cache_shared_external.txt` to a prior known-good tag.
2. Force local facade mode with `M_CACHE_SHARED_SOURCE=local` when needed.
3. Keep public CLI/API behavior unchanged during rollback.

Incident handling requirements in this repo:

- incident note summarizing trigger/evidence/impact,
- explicit recovery action (repin and/or facade mode switch),
- follow-up task for prevention and re-validation.

## Local Shims/Fallbacks That Must Remain in Wave 7

- `py_earnings_calls.augmentation_shared` as first-hop facade,
- canonical source-mode env contract and local fallback behavior,
- one-cycle compatibility alias `PY_EARNINGS_CALLS_SHARED_SOURCE`,
- local wrappers/helpers that intentionally remain outside strict-common v1,
- local ownership of transcript write-path orchestration and API wire-shapes.

## User-Testing Gate Policy (Explicit)

Cross-application user testing is:

- mandatory for **compatibility-impacting** stable releases,
- not mandatory for every routine stable release,
- never a replacement for maintainer/developer validation,
- never a replacement for cross-repo RC matrix validation.

Compatibility-impacting includes changes to facade/import behavior, external identity/pinning behavior, shim/fallback semantics, and upgrade/rollback operator experience.

## Explicitly Deferred Cleanup

Wave 7 defers all cleanup/removal actions:

- no public API expansion,
- no immediate shim/fallback removal,
- no import-root collapse/removal wave,
- no local ownership reduction.

Retirement discussion can only start after:

- multiple successful stable cycles,
- consistent cross-repo green validation,
- compatible user-testing outcomes for compatibility-impacting releases,
- demonstrated rollback confidence.
