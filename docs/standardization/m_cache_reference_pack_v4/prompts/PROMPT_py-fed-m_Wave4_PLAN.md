First read `AGENTS.md` and the relevant guiding project documents before proposing any changes. Then read the Wave 4 reference pack at `docs/standardization/m_cache_reference_pack_v4/` and use it as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 4 in this repo only, while keeping it fully standalone and backward compatible.

Do not implement. In plan mode:
1. assess the current post-Wave-3 repo state against the Wave 4 pack,
2. identify what modules/helpers are realistically shareable now,
3. identify what must remain repo-local,
4. define how this repo will expose or consume the external producer protocol,
5. preserve text-bearing vs numeric-only applicability,
6. keep payload-schema ownership external/service-owned,
7. propose the smallest compatibility-first phased Wave 4 sequence,
8. call out risks, exceptions, and reserved-for-later items,
9. stop after presenting the plan.


Preserve `py-fed ...` as the compatibility surface and `m-cache fed ...` as the canonical additive surface.

Repo-specific constraints:
- `documents` are augmentation-eligible only when explicit persisted text-bearing representation exists.
- `releases` are augmentation-eligible only when explicit persisted text-bearing representation exists.
- `series` remain fully non-augmentation.
- Keep `documents`, `releases`, and `series` distinct.
- Identify which provider/resolution/augmentation metadata models/helpers are ready for shared extraction.
- Define how an external producer would retrieve text-bearing document/release text and submit entity/temporal annotations back.
- Do not propose resolver extraction, adapter extraction, or storage-path extraction.
