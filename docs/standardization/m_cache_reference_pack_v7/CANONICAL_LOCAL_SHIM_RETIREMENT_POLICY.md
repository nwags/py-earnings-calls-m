# Canonical Local Shim Retirement Policy

## Goal

Define when temporary local compatibility layers may be reduced later.

## Candidate cleanup targets

Potential later cleanup may include:
- one-cycle env aliases,
- flat compatibility shims,
- redundant fallback helpers,
- some duplicated local shared wrappers.

## Retirement prerequisites

Cleanup should only be considered after:
- multiple stable release cycles succeed,
- cross-repo validation remains consistently green,
- user testing passes consistently,
- rollback confidence remains high,
- repo-specific safety constraints still permit cleanup.

## Conservatism rule

SEC should remain the most conservative retirement case because its local facade continues to support important authority-boundary safety properties.

## Important note

Wave 7 should define retirement criteria, not necessarily execute retirement.
