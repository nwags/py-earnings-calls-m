# Canonical Shared Release Execution Workflow

## Goal

Define the actual package-side execution path for RC and stable releases.

## Canonical workflow

1. Package change lands in `m-cache-shared-ext`.
2. Package tests and package-side validation pass.
3. RC tag is created.
4. Central evidence bundle is opened for that RC.
5. All four consumer repos pin and validate the same RC.
6. Consumer-repo validator/signoff evidence is collected.
7. Release manager decides:
   - promote to stable,
   - reject RC,
   - cut new RC.
8. Stable release tag is cut if approved.

## Required execution properties

- one RC tag per candidate,
- one evidence bundle per candidate,
- one promotion/rejection decision record per candidate,
- one rollback path per candidate.
