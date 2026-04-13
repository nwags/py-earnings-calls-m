First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 5 reference pack at `docs/standardization/m_cache_reference_pack_v5/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 5 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-4.1 repo state against the Wave 5 pack,
2. identify exact local modules/classes/functions that can move into `m_cache_shared` now,
3. identify exact local modules/classes/functions that must remain repo-local,
4. define the import/adoption sequence for this repo,
5. preserve pilot vs non-pilot runtime behavior,
6. preserve all applicability and authority boundaries,
7. define repo-specific rollback strategy,
8. stop after presenting the plan.


Preserve `py-fed ...` as the compatibility surface and `m-cache fed ...` as the additive canonical surface.

Repo-specific Wave 5 planning constraints:
- This repo remains non-pilot for live producer write-path behavior.
- Preserve strict applicability:
  - `documents` and `releases` only when explicit persisted text-bearing representation exists
  - `series` always non-augmentation / `not_applicable`
- Preserve validate-only / reserved write-path behavior.
- Identify exactly which local protocol models/validators/packers/helpers can move into `m_cache_shared` now.
- Identify exactly which resolver/resource-family/storage/execution internals must remain local.
- Do not propose enabling live producer write-path in Wave 5 planning.
