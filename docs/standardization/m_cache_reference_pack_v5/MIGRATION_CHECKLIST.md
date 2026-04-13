# Migration Checklist (Wave 5)

This is a planning-first checklist.

## Package boundary
- [ ] Identify exact extractable models
- [ ] Identify exact extractable enums/vocabularies
- [ ] Identify exact extractable validators/schema loaders
- [ ] Identify exact extractable packers/helpers
- [ ] Identify exact repo-local wrappers that must remain local
- [ ] Confirm adapters/identities/storage/execution internals stay out

## Role preservation
- [ ] Preserve pilot repo live write-path behavior
- [ ] Preserve non-pilot repo validate-only/reserved behavior
- [ ] Preserve SEC no-dual-authority posture
- [ ] Preserve Fed applicability strictness

## Adoption and rollback
- [ ] Define package location/versioning approach
- [ ] Define adoption order
- [ ] Define rollback path
- [ ] Define partial-adoption stop points

## Testing
- [ ] Define shared package tests
- [ ] Define repo integration tests
- [ ] Define cross-repo consistency checks

## Pause point before implementation
Do not approve Wave 5 implementation until:
- all four plans are compared side by side,
- the first package contents are explicitly bounded,
- role preservation is explicit,
- rollback/versioning is explicit,
- no plan tries to extract domain-local logic prematurely.
