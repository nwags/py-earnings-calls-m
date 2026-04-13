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


Preserve `py-sec-edgar ...` as the compatibility surface and `m-cache sec ...` as the canonical additive surface.

Repo-specific Wave 3 planning constraints:
- Treat filings and filing content as augmentation-eligible.
- Keep `aug` canonical and `augmentations` as the compatibility alias.
- Preserve SEC accession identity, filing identity, filing-party behavior, and companion provenance artifacts.
- Treat SEC as the most mature augmentation/provenance implementation and identify what parts of that pattern can safely become shared contracts/helpers.
- Do not propose broad augmentation redesign or flattening away SEC-specific provenance details.
- Be explicit about which SEC augmentation/provenance pieces should remain domain-local even after shared extraction.
