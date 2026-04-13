# Codex Reference Prompt (Wave 2)

Read this entire reference pack before planning or implementing anything.

Your task is to implement **Wave 2 parallel standardization** in the target repo without forcing a full merge.

## Required references in this bundle

- `CANONICAL_PROVIDER_OPERATIONS.md`
- `CANONICAL_RATE_LIMIT_AND_DEGRADATION.md`
- `CANONICAL_RESOLUTION_SEMANTICS.md`
- `CANONICAL_API_RESOLUTION_CONVENTIONS.md`
- `CANONICAL_PROVIDER_USAGE_EVENTS.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required behavior

1. Preserve the current project CLI as a compatibility surface.
2. Extend `m-cache <domain>` provider/resolve/api semantics additively.
3. Standardize provider inspection, rate-limit visibility, and resolution transparency.
4. Prefer additive metadata over breaking response changes.
5. Keep domain-specific identities and storage semantics intact.
6. Do not silently downgrade unsupported resolution modes.
7. Add or update tests for each implemented behavior.
8. Update docs in the same patch.

## Constraints

- Keep the repo standalone.
- Do not introduce a shared external package yet.
- Prefer thin wrappers and shared contracts over sweeping internal rewrites.
- Do not redesign out-of-scope systems in this wave.
