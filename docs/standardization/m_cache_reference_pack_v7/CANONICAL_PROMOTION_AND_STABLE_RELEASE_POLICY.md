# Canonical Promotion and Stable Release Policy

## Goal

Define when an RC becomes a stable release.

## Suggested promotion requirements

An RC may be promoted only when:
- external package test suite is green,
- all four repos validate the exact same RC successfully,
- no role drift is observed,
- no CLI/API drift is observed,
- no authority drift is observed,
- no accidental dependency on non-public symbols is introduced,
- required release notes/changelog are complete,
- required user-testing gate is completed or explicitly waived under policy.

## Stable release outcome

Promotion should produce:
- stable semver tag,
- release notes,
- validated compatibility window statement,
- rollback guidance referencing the prior stable release.

## Rejection outcome

If an RC fails:
- it is not promoted,
- the failure reason is documented,
- rollback/repin guidance remains available,
- a new RC is cut only after fixes are in place.
