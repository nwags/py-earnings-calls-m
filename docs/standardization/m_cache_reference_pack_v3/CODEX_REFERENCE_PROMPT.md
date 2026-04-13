# Codex Reference Prompt (Wave 3)

Read this entire reference pack before proposing any changes.

Your task is to produce a **Wave 3 plan**, not to implement it yet.

## Required references in this bundle

- `CANONICAL_SHARED_EXTRACTION_BOUNDARIES.md`
- `CANONICAL_AUGMENTATION_APPLICABILITY_AND_MODEL.md`
- `CANONICAL_AUGMENTATION_CLI_AND_API_SURFACES.md`
- `CANONICAL_AUGMENTATION_ARTIFACTS_AND_EVENTS.md`
- `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required planning behavior

1. Preserve the current repo as standalone.
2. Do not propose a formal merge yet.
3. Use the shared-package boundary planning scope plus the textual augmentation plane.
4. Explicitly distinguish text-bearing versus numeric-only resources.
5. Keep domain-specific identity/storage/adapter logic local unless there is a very strong reason otherwise.
6. Produce a compatibility-first, phased plan.
7. Stop after planning; do not implement.

## Deliverable style

Your response should include:
1. a short assessment of the repo’s current post-Wave-2 state against this pack,
2. a proposed Wave 3 scope for that repo,
3. what can be shared,
4. what must remain local,
5. how augmentation should apply to the repo’s resource families,
6. risks and reserved items,
7. a smallest-reasonable phased patch sequence.
