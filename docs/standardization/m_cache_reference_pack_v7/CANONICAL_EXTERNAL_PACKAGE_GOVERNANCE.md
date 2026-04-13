# Canonical External Package Governance

## Goal

Define how `m-cache-shared-ext` is governed as the authoritative external shared package.

## Governance expectations

Wave 7 should define:
- the canonical external repository for `m-cache-shared-ext`,
- who owns the package and who may approve changes,
- what counts as public API versus internal implementation detail,
- what review level is required for:
  - bug fixes,
  - additive API changes,
  - breaking changes,
  - release workflow changes.

## Public API boundary rule

The first external public API remains the strict common v1 subset already proven across repos.
Wave 7 should not broaden that API by default.

## Required release artifacts

Each RC/stable release should have:
- tagged version,
- changelog entry,
- public API notes,
- compatibility/upgrade notes,
- rollback notes,
- validation evidence references.

## Documentation expectations

Public API changes should require explicit docs updates before release promotion.
