# Codex Reference Prompt (Wave 4)

Read this entire reference pack before proposing any changes.

Your task is to produce a **Wave 4 plan**, not to implement it yet.

## Required references in this bundle

- `CANONICAL_SHARED_PACKAGE_BOUNDARY.md`
- `CANONICAL_SHARED_MODELS_AND_HELPERS.md`
- `CANONICAL_EXTERNAL_AUGMENTATION_PRODUCER_PROTOCOL.md`
- `CANONICAL_ANNOTATION_TARGETING_AND_PAYLOAD_GUIDANCE.md`
- `CANONICAL_ROLLOUT_AND_VERSIONING.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required planning behavior

1. Preserve the current repo as standalone.
2. Do not propose a formal merge yet.
3. Define a bounded shared internal package extraction plan.
4. Define how the repo will expose or consume the external producer protocol.
5. Preserve text-bearing vs numeric-only applicability rules.
6. Keep payload-schema ownership external/service-owned.
7. Keep domain-specific identities/adapters/storage/execution local unless there is a very strong reason otherwise.
8. Produce a phased, compatibility-first plan.
9. Stop after planning; do not implement.
