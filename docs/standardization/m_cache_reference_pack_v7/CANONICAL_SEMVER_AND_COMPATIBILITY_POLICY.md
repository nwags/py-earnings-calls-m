# Canonical Semver and Compatibility Policy

## Goal

Define versioning and compatibility promises for `m-cache-shared-ext`.

## Semver guidance

- patch release = bug fix, no public API break
- minor release = additive public API change, backward compatible
- major release = breaking public API or semantic change

## Public API versus internal implementation

Wave 7 should require explicit documentation of what is public and supported.
Anything not in the documented public API should be considered internal and may change without compatibility promise.

## Compatibility window guidance

Wave 7 should define:
- how long old repo-side compatibility aliases may remain,
- how long older stable package versions are considered supported,
- whether any overlap period is guaranteed for migration-sensitive changes.

## Strict first-public-API rule

Do not expand the public API unless it is deliberately approved and semver/documentation consequences are handled explicitly.
