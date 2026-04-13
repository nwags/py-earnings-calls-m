# Codex Reference Prompt (Wave 6)

Read this entire reference pack before proposing any changes.

Your task is to produce a **Wave 6 plan**, not to implement it yet.

## Required references in this bundle

- `WAVE6_REFERENCE_PACK_OUTLINE.md`
- `CANONICAL_EXTERNAL_PACKAGE_STRATEGY.md`
- `CANONICAL_FIRST_PUBLIC_API_BOUNDARY.md`
- `CANONICAL_ADOPTION_VERSIONING_AND_COMPATIBILITY.md`
- `CANONICAL_ROLE_AND_AUTHORITY_PRESERVATION.md`
- `CANONICAL_TESTING_AND_RELEASE_EXPECTATIONS.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`

## Required planning behavior

1. Preserve the current repo as standalone.
2. Plan externalization of `m_cache_shared` only.
3. Use the strict proven common subset as the first external public API.
4. Preserve pilot vs non-pilot behavior exactly.
5. Preserve all applicability and authority boundaries.
6. Stop after planning; do not implement.
