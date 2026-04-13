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


Preserve `py-news ...` as the compatibility surface and `m-cache news ...` as the additive canonical surface.

Repo-specific Wave 6.1 planning constraints:
- This repo remains a pilot repo for live producer write-path behavior.
- Keep article-only applicability exactly unchanged.
- Converge from the current `m_cache_shared_ext`-based shim implementation to the canonical Wave 6.1 external identity and shim contract.
- Preserve `py_news/m_cache_shared_shim.py` as the first-hop facade during convergence.
- Keep article target building, text selection, text retrieval, storage placement, idempotency logic, and live write-path persistence local.
- Do not broaden the first external public API.
