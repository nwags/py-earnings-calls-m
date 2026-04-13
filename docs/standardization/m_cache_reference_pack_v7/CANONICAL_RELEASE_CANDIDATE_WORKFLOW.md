# Canonical Release Candidate Workflow

## Goal

Define the standard RC lifecycle for `m-cache-shared-ext`.

## Canonical RC flow

1. Change lands in external package repo.
2. External package tests pass.
3. RC tag is created (e.g. `vX.Y.Z-rcN`).
4. All four repos pin that exact RC.
5. Cross-repo validation matrix runs.
6. Required operator/developer checks pass.
7. User-testing gate decision is made when applicable.
8. RC is promoted or rejected.

## RC naming guidance

Use semver-compatible RC tags, for example:
- `v0.1.0-rc1`
- `v0.1.0-rc2`

## RC invalidation guidance

A new RC is required if:
- public API surface changes,
- validation reveals a bug requiring package change,
- release metadata/changelog is incorrect in a material way,
- repo validation requires code change in the external package.

## Evidence required for promotion consideration

- external package tests green,
- all repo validation checks green against the same RC,
- required docs/changelog prepared,
- no blocked incidents for the RC.
