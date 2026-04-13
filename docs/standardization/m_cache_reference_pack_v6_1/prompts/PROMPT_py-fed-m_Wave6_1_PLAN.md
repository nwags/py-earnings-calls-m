First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 6.1 reference pack at `docs/standardization/m_cache_reference_pack_v6_1/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 6.1 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-6 repo state against the Wave 6.1 pack,
2. identify the exact changes needed to converge on the canonical external package identity,
3. identify the exact changes needed to converge on the canonical shim/fallback contract,
4. identify how this repo will validate one shared release-candidate tag,
5. preserve runtime behavior exactly,
6. define repo-specific rollback concerns,
7. stop after presenting the plan.


Preserve `py-fed ...` as the compatibility surface and `m-cache fed ...` as the additive canonical surface.

Repo-specific Wave 6.1 planning constraints:
- This repo remains non-pilot for live producer write-path behavior.
- Keep strict applicability exactly unchanged:
  - `documents` and `releases` only when explicit persisted text-bearing representation exists
  - `series` always non-augmentation / `not_applicable`
- Converge from the current repo-specific facade/distribution-loading approach to the canonical Wave 6.1 external identity and shim contract.
- Preserve `py_fed/augmentation_shared_facade.py` as the first-hop facade during convergence.
- Keep descriptor building, source-text-version derivation, applicability enforcement, and authority behavior local.
- Do not broaden the first external public API.
