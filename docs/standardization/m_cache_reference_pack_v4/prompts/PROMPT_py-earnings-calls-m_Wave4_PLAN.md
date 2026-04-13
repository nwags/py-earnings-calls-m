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


Preserve `py-earnings-calls ...` as the compatibility surface and `m-cache earnings ...` as the canonical additive surface.

Repo-specific constraints:
- `transcripts` remain the only augmentation-eligible family in scope.
- `forecasts` snapshots/points remain non-augmentation unless a future persisted narrative-text family is introduced.
- Keep transcript augmentation `call_id`-scoped and `source_text_version` aware.
- Identify which local models/helpers around provider/resolution/augmentation metadata can move into the shared package now.
- Define how an external producer would retrieve transcript text and submit entity/temporal annotations back.
- Do not propose forecast model flattening, adapter extraction, or deep pipeline rewrites.
