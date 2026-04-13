# Codex Reference Prompt (Wave 5)

Read this entire reference pack before proposing any changes.

Your task is to produce a **Wave 5 plan**, not to implement it yet.

## Required references in this bundle

- `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
- `CANONICAL_EXTRACTABLE_MODELS_ENUMS_AND_HELPERS.md`
- `CANONICAL_PROTOCOL_COMPATIBILITY_AND_ROLE_PRESERVATION.md`
- `CANONICAL_PACKAGE_ADOPTION_AND_ROLLBACK.md`
- `CANONICAL_TESTING_AND_INTEGRATION_EXPECTATIONS.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required planning behavior

1. Preserve the current repo as standalone.
2. Do not propose a formal merge.
3. Propose the first real shared-package extraction plan only for the stable outer protocol/helper layer.
4. Identify exact code that can move now and exact code that must remain local.
5. Preserve pilot vs non-pilot runtime behavior.
6. Preserve all applicability boundaries.
7. Stop after planning; do not implement.

## Deliverable style

Your response should include:
1. a short assessment of the repo’s current post-Wave-4.1 state against this pack,
2. the exact shared-package candidates from this repo,
3. the exact local code that must remain local,
4. the proposed import/adoption sequence,
5. repo-specific risks and rollback strategy,
6. a smallest-reasonable phased Wave 5 patch sequence.
