First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 6 reference pack at `docs/standardization/m_cache_reference_pack_v6/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 6 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-5.1 repo state against the Wave 6 pack,
2. identify how this repo should adopt the external shared package source of truth,
3. preserve pilot vs non-pilot behavior exactly,
4. preserve all applicability and authority boundaries,
5. use only the strict proven common subset as the first external public API,
6. define repo-specific versioning/pinning/rollback concerns,
7. stop after presenting the plan.


Preserve `py-earnings-calls ...` as the compatibility surface and `m-cache earnings ...` as the additive canonical surface.

Repo-specific Wave 6 planning constraints:
- This repo remains a pilot repo for live producer write-path behavior.
- Keep transcript-only applicability exactly unchanged.
- Plan adoption of the external `m_cache_shared` package through local wrappers/shims first.
- Keep transcript identity, target building, text retrieval, storage placement, API wire-shape ownership, and live write-path orchestration local.
- Use only the strict proven common subset as the first external public API.
- Do not propose expanding the external public API with non-common symbols in this pass.
