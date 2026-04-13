# Canonical Promotion, Rollback, and Incident Flow

## Goal

Define how RCs/stable releases are promoted, rejected, and rolled back.

## Promotion rule

Promotion may occur only when:
- package-side tests pass,
- all required repo validations pass,
- required signoffs are present,
- required docs/artifacts are complete,
- no blocking incident remains unresolved.

## Rejection rule

An RC should be rejected if:
- package-side validation fails,
- any required repo validator blocks,
- evidence is incomplete,
- role/applicability/authority drift is detected.

## Rollback rule

Stable rollback should remain simple:
- repin to prior stable tag,
- preserve consumer-repo facade fallback behavior,
- document incident and recovery steps,
- do not require CLI/API redesign.

## Incident rule

Wave 7.1 should define:
- who can file/block,
- what incident severity levels exist,
- what follow-up is required before retrying promotion.
