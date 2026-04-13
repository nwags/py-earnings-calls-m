# Canonical External Package Repo Governance

## Goal

Define governance for the external `m-cache-shared-ext` package repository itself.

## Governance expectations

Wave 7.1 should define:
- the canonical external package repository,
- package maintainers / release managers,
- review requirements for:
  - patch bugfixes,
  - additive compatible changes,
  - compatibility-affecting changes,
  - release workflow changes,
- what counts as public API versus internal implementation,
- who may approve RC cuts and stable promotion proposals.

## Public API freeze rule

The strict common v1 public API remains frozen in Wave 7.1.
No API broadening should be assumed or introduced in this wave.

## Required package-side docs

At minimum:
- changelog / release notes policy,
- compatibility notes policy,
- release workflow doc,
- rollback/incident doc,
- public API support statement.
