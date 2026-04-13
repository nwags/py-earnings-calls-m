# Migration Checklist (Wave 6)

This is a planning-first checklist.

## External package strategy
- [ ] Define where the external package lives
- [ ] Define the initial public API boundary
- [ ] Define package versioning/pinning strategy
- [ ] Define repo-local shim adoption strategy
- [ ] Define rollback strategy

## Public API boundary
- [ ] Confirm strict common-subset exports
- [ ] Exclude non-common or unstable symbols
- [ ] Keep compatibility aliases local unless absolutely necessary

## Role and authority preservation
- [ ] Preserve pilot repo live write-path behavior
- [ ] Preserve non-pilot repo reserved/validate-only behavior
- [ ] Preserve SEC no-dual-authority posture
- [ ] Preserve Fed strict applicability

## Testing
- [ ] Define external package tests
- [ ] Define repo integration tests
- [ ] Define cross-repo import/API consistency checks
