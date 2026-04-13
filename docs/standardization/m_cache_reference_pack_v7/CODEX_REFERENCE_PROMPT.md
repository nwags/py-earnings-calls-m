# Codex Reference Prompt (Wave 7)

Read this entire reference pack before proposing any changes.

Your task is to produce a **Wave 7 plan**, not to implement it yet.

## Required references in this bundle

- `WAVE7_REFERENCE_PACK_OUTLINE.md`
- `CANONICAL_EXTERNAL_PACKAGE_GOVERNANCE.md`
- `CANONICAL_RELEASE_CANDIDATE_WORKFLOW.md`
- `CANONICAL_PROMOTION_AND_STABLE_RELEASE_POLICY.md`
- `CANONICAL_SEMVER_AND_COMPATIBILITY_POLICY.md`
- `CANONICAL_CROSS_REPO_VALIDATION_MATRIX.md`
- `CANONICAL_USER_TESTING_GATE.md`
- `CANONICAL_ROLLBACK_AND_INCIDENT_POLICY.md`
- `CANONICAL_LOCAL_SHIM_RETIREMENT_POLICY.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required planning behavior

1. Preserve the current repo as standalone.
2. Do not broaden the shared public API in this wave.
3. Plan the repo’s role in the external package lifecycle only.
4. Preserve pilot vs non-pilot behavior exactly.
5. Preserve all applicability and authority boundaries.
6. Stop after planning; do not implement.

## Deliverable style

Your response should include:
1. a short assessment of the repo’s current post-Wave-6.1 state relative to Wave 7,
2. the repo’s participation in RC/stable validation lifecycle,
3. repo-specific governance/rollback/user-testing concerns,
4. what local shims must remain for now,
5. what cleanup is explicitly deferred,
6. a smallest-reasonable phased Wave 7 patch sequence.
