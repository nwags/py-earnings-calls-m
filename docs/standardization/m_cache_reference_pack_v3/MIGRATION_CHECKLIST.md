# Migration Checklist (Wave 3)

This is a planning-first checklist.

## Shared extraction boundary
- [ ] Identify common models stable enough to share
- [ ] Identify common schema validators stable enough to share
- [ ] Identify common CLI/API helper code stable enough to share
- [ ] Identify code that must remain repo-local

## Augmentation applicability
- [ ] Mark each resource family as text-bearing or numeric-only
- [ ] Mark augmentation-eligible resource families
- [ ] Mark explicitly excluded numeric-only resource families
- [ ] Explain any mixed families where only some records are augmentable

## Augmentation shared contract
- [ ] Define planned shared augmentation types
- [ ] Define shared augmentation request/result/event metadata
- [ ] Define relationship to existing SEC/news augmentation/provenance patterns
- [ ] Define additive API augmentation metadata strategy

## Shared package planning
- [ ] Identify minimum viable shared package contents
- [ ] Define extraction order
- [ ] Define compatibility/versioning plan
- [ ] Define test strategy across standalone repos

## Pause point before implementation
Do not approve Wave 3 implementation until:
- all four plans are compared side by side,
- the shared package boundary is judged stable enough,
- the augmentation applicability matrix is consistent across repos,
- repo-specific exceptions are explicitly named and accepted.
