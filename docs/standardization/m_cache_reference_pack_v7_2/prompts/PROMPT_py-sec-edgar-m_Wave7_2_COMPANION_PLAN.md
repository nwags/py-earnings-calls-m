First read `AGENTS.md` and the relevant guiding project documents, then read the Wave 7.2 reference pack and use it as the canonical source of truth for this planning pass.

You are not implementing yet. This is a **lightweight companion Wave 7.2 planning pass** for this consumer repo only.

Do not implement. In plan mode:
1. assess the repo’s current post-Wave-7.1 state against the Wave 7.2 pack,
2. identify the minimum repo actions needed to participate in the first real shared RC cycle,
3. identify required validation/signoff/evidence inputs into the central bundle,
4. identify rollback-readiness obligations,
5. preserve runtime behavior exactly,
6. keep cleanup/removal out of scope,
7. stop after presenting the plan.


Repo-specific Wave 7.2 companion constraints:
- This repo remains a conservative validator/signoff consumer for non-pilot and no-dual-authority safety.
- Keep `aug` canonical and `augmentations` as the compatibility alias.
- Preserve SEC no-dual-authority posture exactly.
- Focus only on this repo’s minimum RC participation obligations:
  - pin shared RC,
  - run required validations,
  - emit signoff/evidence inputs,
  - confirm rollback readiness.
- Do not propose governance ownership, API broadening, or shim/fallback removal.

For this first real local RC cycle, explicitly include the minimum local package-consumption step needed for this repo to validate the shared RC in its active virtualenv (for example, installing the sibling `m-cache-shared-ext` repo or equivalent local RC-consumption setup), while keeping the existing facade/shim contract unchanged and without introducing runtime redesign, cleanup, or public API broadening.