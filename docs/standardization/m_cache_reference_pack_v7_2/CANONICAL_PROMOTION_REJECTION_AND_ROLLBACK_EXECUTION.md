# Canonical Promotion, Rejection, and Rollback Execution

## Goal

Define the actual package-side decision mechanics for RCs/stable releases.

## Promotion prerequisites

Promotion may occur only when:
- package-side tests pass,
- required consumer-repo validations pass,
- required signoffs are present,
- evidence bundle is complete,
- no blocking incident remains unresolved.

## Rejection prerequisites

An RC should be rejected if:
- package-side validation fails,
- any required consumer repo blocks,
- evidence bundle is incomplete,
- role/applicability/authority drift is detected,
- rollback readiness cannot be demonstrated.

## Rollback rule

Stable rollback should remain simple:
- repin to prior known-good stable tag,
- preserve consumer-repo facade/fallback safety,
- record the incident and recovery steps,
- do not require CLI/API redesign.
