# Canonical Cleanup Deferral and Entry Criteria

## Goal

Define what remains deferred and when a later cleanup wave may begin.

## Still deferred in Wave 7.1

- public API broadening,
- local shim/fallback removal,
- one-cycle env alias removal,
- import-root collapse,
- domain-local logic externalization,
- local safety-layer reduction.

## Cleanup may be considered only after

- multiple stable release cycles succeed,
- cross-repo validation stays green consistently,
- user testing passes consistently,
- rollback confidence is high,
- repo-specific safety constraints still permit cleanup.
