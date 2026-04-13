First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 7 reference pack at `docs/standardization/m_cache_reference_pack_v7/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 7 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-6.1 repo state against the Wave 7 pack,
2. identify this repo’s role in the external package governance and RC/stable lifecycle,
3. identify repo-specific validation and user-testing obligations,
4. identify rollback/incident concerns,
5. identify which local shims/fallback layers must remain for now,
6. identify what cleanup is explicitly deferred,
7. stop after presenting the plan.


Preserve `py-fed ...` as the compatibility surface and `m-cache fed ...` as the additive canonical surface.

Repo-specific Wave 7 planning constraints:
- This repo remains non-pilot for live producer write-path behavior.
- Keep strict applicability exactly unchanged:
  - `documents` and `releases` only when explicit persisted text-bearing representation exists
  - `series` always non-augmentation / `not_applicable`
- Plan this repo’s role in RC/stable validation of `m-cache-shared-ext`.
- Treat cross-application user testing as a post-Wave-7 stabilization gate, not as a replacement for maintainer/developer validation.
- Keep descriptor building, source-text-version derivation, applicability enforcement, and authority behavior local.
- Do not propose public API broadening or immediate shim/fallback removal.
