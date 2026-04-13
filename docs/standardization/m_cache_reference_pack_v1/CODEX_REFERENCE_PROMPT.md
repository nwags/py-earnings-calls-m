# Codex Reference Prompt (Wave 1)

Read this entire reference pack before implementing anything.

Your task is to implement **Wave 1 parallel standardization** in the target repo without forcing a full merge.

## Required references in this bundle

- `CANONICAL_COMMAND_MODEL.md`
- `CANONICAL_CONFIG_SCHEMA.md`
- `CANONICAL_PROVIDER_REGISTRY.md`
- `CANONICAL_RUNTIME_OUTPUT_AND_EVENTS.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required implementation behavior

1. Preserve the current project CLI as a compatibility surface.
2. Introduce or reserve the shared `m-cache <domain>` command model.
3. Implement the canonical config loader and canonical config shape.
4. Implement or reserve canonical provider registry materialization.
5. Standardize runtime summary/progress/event outputs to match this bundle.
6. Do not force domain-specific storage or identity models into one flattened design.
7. Keep changes additive where possible.
8. Add or update tests for each implemented behavior.
9. Update docs in the same patch.

## Constraints

- Keep the repo standalone.
- Do not introduce a hard dependency on a shared external package yet unless explicitly instructed.
- Prefer compatibility wrappers and shared contracts over sweeping internal rewrites.
- If a shared enum/value is not implemented in the repo yet, fail transparently instead of silently downgrading.

## Deliverable style

Produce work in small phases with concise migration notes.
