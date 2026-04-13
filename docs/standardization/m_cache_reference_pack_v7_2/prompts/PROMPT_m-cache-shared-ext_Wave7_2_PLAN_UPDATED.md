If `AGENTS.md` exists in this repo, read it first. Then read the relevant guiding project documents that exist in this repo, plus the Wave 7.2 reference pack at `docs/standardization/m_cache_reference_pack_v7_2/`, and use that pack as the canonical source of truth for this planning pass.

You are not implementing yet. You are planning Wave 7.2 for the **external package repo/process itself**: `m-cache-shared-ext`.

Do not implement. In plan mode:
1. assess the external package repo/process requirements implied by Waves 7, 7.1, and the Wave 7.2 pack,
2. define the package-repo governance and operational release workflow,
3. define the first real shared RC execution path,
4. define the central evidence bundle and signoff-ingestion model,
5. define promotion/rejection/rollback decision mechanics,
6. define exactly how the first RC cycle will collect and consume evidence from the four consumer repos,
7. preserve the strict common v1 public API boundary,
8. keep cleanup/removal out of scope,
9. stop after presenting the plan.

Your response should include:
- the package-side artifact set to add,
- the first real RC execution sequence,
- the evidence bundle structure,
- release-manager decision records,
- rollback/incident handling,
- explicit handoff criteria for beginning the comprehensive cross-application user-testing program.

Additional constraints:
- Do not broaden the shared public API in this wave.
- Do not propose consumer-repo cleanup/removal in this wave.
- Use the four consumer repos only as lightweight RC/signoff companions.
- Preserve pilot vs non-pilot behavior exactly.
- Preserve all applicability and authority boundaries in the consumer repos.
