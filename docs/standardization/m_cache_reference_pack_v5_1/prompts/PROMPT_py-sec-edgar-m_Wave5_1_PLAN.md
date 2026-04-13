First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 5.1 reference pack at `docs/standardization/m_cache_reference_pack_v5_1/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 5.1 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-5 repo state against the Wave 5.1 pack,
2. identify exact package-layout normalization changes needed,
3. identify exact export-surface normalization changes needed,
4. identify which local shims/wrappers remain,
5. identify shared-package test/doc normalization changes needed,
6. preserve runtime behavior exactly,
7. define repo-specific rollback strategy,
8. stop after presenting the plan.


Preserve `py-sec-edgar ...` as the compatibility surface and `m-cache sec ...` as the additive canonical surface.

Repo-specific Wave 5.1 planning constraints:
- This repo remains non-pilot for live producer write-path behavior.
- Keep `aug` canonical and `augmentations` as the compatibility alias.
- Preserve SEC no-dual-authority posture exactly.
- Migrate from the current flatter `m_cache_shared` layout to the canonical nested `m_cache_shared/augmentation/...` layout.
- Preserve `py_sec_edgar.wave4_shared` shim/facade behavior where still needed in the first normalization pass.
- Keep filing identity, authority routing, source-text-version derivation, and SEC provenance/storage/execution internals local.
- Do not propose more extraction scope.
