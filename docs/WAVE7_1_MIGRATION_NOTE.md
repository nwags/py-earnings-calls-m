# Wave 7.1 Migration Note (py-earnings-calls-m)

## Scope Freeze (Package-Side Release-Execution Hardening Only)

Wave 7.1 in `py-earnings-calls-m` is limited to package-side release-execution hardening alignment.

This wave explicitly does **not**:

- change runtime behavior,
- change CLI/API semantics,
- broaden shared public API,
- perform cleanup/removal work.

## Repo Role in Shared RC/Stable Execution

This repo remains:

- a pilot consumer-validator for transcript write-path safety,
- a release blocker when transcript write-path safety drifts,
- a required contributor of validation/signoff evidence to the shared package-side RC/stable cycle,
- not the owner of external package governance policy.

Compatibility/canonical surfaces remain unchanged:

- `py-earnings-calls ...` remains the compatibility surface,
- `m-cache earnings ...` remains the additive canonical surface.

## Frozen Local Ownership and Applicability

Wave 7.1 keeps these boundaries unchanged:

- `py_earnings_calls.augmentation_shared` remains first-hop facade,
- transcript-only applicability remains unchanged,
- forecasts remain non-augmentation,
- transcript identity, target building, text retrieval, storage placement, API wire-shape ownership, and live write-path orchestration remain local.

## Repo Validation and Signoff Obligations

For each shared RC/stable candidate, this repo must provide evidence for:

- canonical external pin present in `requirements/m_cache_shared_external.txt`,
- first-hop facade remains `py_earnings_calls.augmentation_shared`,
- source-mode contract coverage (`local`, `auto`, `external`) with local fallback availability,
- strict-common v1 boundary preserved,
- unchanged compatibility and additive canonical surface semantics,
- unchanged pilot transcript write-path behavior and transcript-only applicability.

Minimum validation command:

- `pytest -q`

Required signoff posture:

- explicit repo validator/signoff that transcript write-path safety remains preserved for the candidate.

## Blocker Taxonomy

This repo blocks candidate promotion when any of the following are detected:

- transcript write-path safety regression,
- role/applicability/authority drift,
- facade/source-mode contract regression,
- strict-common v1 surface regression or non-public symbol dependency,
- CLI/API semantic drift,
- unresolved release-lifecycle incident that invalidates this repo's signoff confidence.

## Rollback and Incident Path

Rollback remains simple and package-cycle compatible:

1. repin `requirements/m_cache_shared_external.txt` to prior stable tag,
2. force local facade mode (`M_CACHE_SHARED_SOURCE=local`) if immediate isolation is needed,
3. preserve public CLI/API behavior while recovering.

Incident handling requirements in this repo:

- incident note with trigger, evidence, impact, and recovery steps,
- explicit signoff hold/release record,
- follow-up action before retrying promotion.

## Evidence-Bundle Integration

Repo-local checklists and evidence outputs are inputs to the central shared release evidence bundle.

This repo does **not** define a separate release process competing with the package-side workflow.

## Comprehensive User-Testing Start Gate (Explicit)

Comprehensive cross-application user testing begins only after:

- Wave 7.1 implementation is complete,
- one shared RC is fully validated across all four repos,
- evidence/signoff flow is operational end-to-end,
- rollback path is verified,
- no open blocking lifecycle incident remains.

User testing remains:

- post-Wave-7.1 stabilization gate,
- pre-cleanup / pre-shim-retirement gate,
- never a replacement for maintainer/developer validation or RC-matrix validation.

## Cleanup/Removal Deferral (Explicit)

Wave 7.1 defers all cleanup/removal actions:

- no public API broadening,
- no shim/fallback removal,
- no env alias removal,
- no import-root collapse,
- no local ownership reduction.
