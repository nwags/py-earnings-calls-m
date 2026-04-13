# Canonical User Testing Gate

## Goal

Define where comprehensive cross-application user testing fits in the lifecycle.

## Placement in lifecycle

User testing should occur:
- after Wave 7 implementation puts lifecycle hardening in place,
- after at least one RC/stable candidate is ready for realistic validation,
- before any aggressive cleanup/removal wave for local shims, env aliases, or fallback layers.

## Suggested user-testing categories

- install/setup testing
- upgrade testing from prior pin to new RC/stable
- rollback testing
- CLI operator flows
- API consumer flows
- augmentation submission/status/events flows
- fallback-mode behavior (`local`, `auto`, `external`)
- documentation/operator usability testing

## Suggested outputs

- cross-application user-test checklist
- user-test result template
- release signoff criteria
- issue triage and release-blocking rules

## Policy question Wave 7 should answer

Should user testing be:
- mandatory for every stable release, or
- mandatory only for releases that affect facade/import/compatibility behavior?
