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


Preserve `py-sec-edgar ...` as the compatibility surface and `m-cache sec ...` as the additive canonical surface.

Repo-specific Wave 5 planning constraints:
- This repo remains non-pilot for live producer write-path behavior.
- Keep `aug` canonical and `augmentations` as the compatibility alias.
- Preserve SEC sidecar/submission/provenance artifacts as the sole operational authority.
- Identify exactly which local protocol models/validators/packers/helpers can move into `m_cache_shared` now.
- Identify exactly which SEC authority/provenance/identity/storage/execution internals must remain local.
- Do not propose any extraction that creates or risks dual authority.
- Do not propose enabling live producer write-path in Wave 5 planning.
