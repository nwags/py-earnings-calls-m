First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 7.1 reference pack at `docs/standardization/m_cache_reference_pack_v7_1/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 7.1 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-7 repo state against the Wave 7.1 pack,
2. identify this repo’s role in the package-side release execution lifecycle,
3. identify required repo validation/signoff/evidence obligations,
4. identify rollback/incident concerns,
5. identify what local shims/fallbacks must remain for now,
6. identify what cleanup is explicitly deferred,
7. identify the repo’s prerequisites for beginning the comprehensive user-testing program,
8. stop after presenting the plan.


Preserve `py-fed ...` as the compatibility surface and `m-cache fed ...` as the additive canonical surface.

Repo-specific Wave 7.1 planning constraints:
- This repo remains validator + signoff for non-pilot reserved behavior and strict applicability.
- Keep strict applicability exactly unchanged:
  - `documents` and `releases` only when explicit persisted text-bearing representation exists
  - `series` always non-augmentation / `not_applicable`
- Plan this repo’s role in package-side RC/stable execution for `m-cache-shared-ext`.
- Treat comprehensive user testing as a post-Wave-7.1 stabilization gate and pre-cleanup gate.
- Keep descriptor building, source-text-version derivation, applicability enforcement, and authority behavior local.
- Do not propose public API broadening or immediate shim/fallback removal.
