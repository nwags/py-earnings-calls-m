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


Preserve `py-news ...` as the compatibility surface and `m-cache news ...` as the additive canonical surface.

Repo-specific Wave 5 planning constraints:
- This repo remains a pilot repo for live producer write-path behavior.
- `articles` remain the only augmentation-eligible family in scope.
- Operational metadata families remain non-augmentation.
- Preserve current article detail/content API role and current bounded payload behavior.
- Identify exactly which local protocol models/validators/packers/helpers can move into `m_cache_shared` now.
- Identify exactly which article parsing/strategy/storage/execution internals must remain local.
- Do not propose article parsing strategy extraction or monitor/reconcile extraction.
