# Canonical Protocol Compatibility and Role Preservation

## Goal

Wave 5 must extract shared code without flattening role differences that are still intentional.

## Role preservation rule

The shared package owns protocol representation and helper behavior.
Each repo keeps its current runtime role:
- pilot repos may keep live write paths,
- non-pilot repos may keep validate-only or reserved behavior.

The shared package must not force identical runtime execution behavior across repos.

## Canonical command family semantics

The shared package should assume the canonical family:
- `inspect-target`
- `submit-run`
- `submit-artifact`
- `status`
- `events`

Compatibility aliases may remain repo-local where needed.

## Shared package versus runtime role

### Shared package should handle
- model definitions
- validator logic
- canonical read/write envelope shape
- packaging helpers
- canonical field naming

### Repo-local code should handle
- whether write-path is live
- whether write-path is validate-only
- whether write-path is placeholder-only
- authority routing and storage behavior
- applicability enforcement beyond shared outer vocabulary

## SEC-specific guardrail

Wave 5 must preserve SEC’s no-dual-authority posture.
The shared package must not introduce a second authority mechanism.

## Fed-specific guardrail

Wave 5 must preserve strict applicability and non-pilot write posture.

## Earnings/news-specific guardrail

Wave 5 must preserve existing pilot write-path behavior rather than forcing a redesign.

## Acceptance criteria for this slice

Each repo plan should answer:
1. how the shared package will preserve current runtime role,
2. what wrapper code stays local to enforce that role,
3. what must not move because it would blur role boundaries.
