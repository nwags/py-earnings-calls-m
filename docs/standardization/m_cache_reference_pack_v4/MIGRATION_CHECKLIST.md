# Migration Checklist (Wave 4)

This is a planning-first checklist.

## Shared package boundary
- [ ] Identify exact shared model candidates
- [ ] Identify exact shared validator/schema candidates
- [ ] Identify exact shared metadata packer/helper candidates
- [ ] Identify exact repo-local modules that must remain local
- [ ] Define extraction order
- [ ] Define rollback strategy

## External producer protocol
- [ ] Define target descriptor contract
- [ ] Define source text retrieval expectations
- [ ] Define run submission envelope
- [ ] Define artifact submission envelope
- [ ] Define inspection/read-back expectations
- [ ] Define idempotency behavior
- [ ] Define replay/staleness behavior
- [ ] Define multi-producer coexistence behavior

## Annotation guidance
- [ ] Confirm stand-off annotation rule
- [ ] Confirm redundant span anchoring rule
- [ ] Confirm entity-tagging minimum payload expectations
- [ ] Confirm temporal-tagging minimum payload expectations
- [ ] Confirm producer-owned payload schema rule

## Pause point before implementation
Do not approve Wave 4 implementation until:
- all four plans are compared side by side,
- shared package contents are explicitly bounded,
- the producer protocol is concrete enough to test,
- applicability remains consistent across repos,
- no plan extracts domain-local adapters/identities/execution engines prematurely.
