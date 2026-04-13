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


Preserve `py-sec-edgar ...` as the compatibility surface and `m-cache sec ...` as the canonical additive surface.

Repo-specific constraints:
- Keep `aug` canonical and `augmentations` as the compatibility alias.
- Preserve SEC accession identity, filing identity, filing-party behavior, and authoritative sidecar/submission/provenance artifacts.
- Treat SEC as the most mature augmentation/provenance implementation.
- Identify which outer models/validators/metadata helpers can now move into the shared package, and which SEC-specific richness must remain local.
- Define how an external producer protocol can coexist with authoritative SEC-local augmentation artifacts without creating dual authority.
- Do not propose extraction of SEC overlay/lifecycle/governance/provenance execution internals.
