First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 3 reference pack at `docs/standardization/m_cache_reference_pack_v3/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 3 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-2 repo state against the Wave 3 pack,
2. identify what common code is realistically shareable now,
3. identify what must remain repo-local,
4. apply the shared augmentation rule to the repo’s resource families,
5. propose the smallest compatibility-first phased Wave 3 sequence,
6. call out risks, domain-specific exceptions, and reserved-for-later items,
7. stop after presenting the plan.


Preserve `py-news ...` as the compatibility surface and `m-cache news ...` as the canonical additive surface.

Repo-specific Wave 3 planning constraints:
- Treat article metadata text and full article content as augmentation-eligible.
- Use the already established news direction of adopting the shared augmentation plane.
- Keep monitor/reconcile redesign out of scope.
- Keep heavy in-process extraction redesign out of scope.
- Identify which provider/rate-limit/resolution/API helpers are strong shared-package candidates.
- Plan how additive augmentation metadata should appear on existing detail/content surfaces.
