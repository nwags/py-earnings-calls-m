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


Preserve `py-earnings-calls ...` as the compatibility surface and `m-cache earnings ...` as the additive canonical surface.

Repo-specific Wave 5.1 planning constraints:
- This repo remains a pilot repo for live producer write-path behavior.
- Keep transcript-only applicability exactly unchanged.
- Normalize `m_cache_shared/augmentation/...` exports and tests/docs without changing pilot behavior.
- Identify whether any current export names should be renamed or re-exported to match the canonical shared surface.
- Keep transcript identity, target building, text retrieval, storage placement, and live write-path orchestration local.
- Do not propose more extraction scope.
