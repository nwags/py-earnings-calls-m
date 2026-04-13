# Canonical Shared Package Boundary

## Goal

Plan a shared internal package boundary without forcing the merge yet.

## Proposed extraction phases

### Phase A - shared models and schemas
Potential extractions:
- provider detail model
- provider usage event model
- rate-limit state model
- resolution result metadata model
- API resolution metadata model
- augmentation request/result/event/run metadata models
- schema validation helpers

### Phase B - shared helper logic
Potential extractions:
- CLI helper builders
- additive API metadata packers
- common machine-output packaging
- common provider-selection vocabulary helpers
- common augmentation target metadata helpers

### Phase C - shared execution helpers only if proven stable
Potential extractions:
- provider-policy evaluation helpers
- defer / rate-limit helper logic
- augmentation run bookkeeping helpers

Do not plan to extract provider adapters or resource-specific resolvers in Wave 3.

## Repo-separate rule

Even after Wave 3, repos should remain separately runnable and separately testable.
The shared package should be introduced only if:
- its contracts are stable,
- standalone compatibility remains intact,
- there is a low-risk path to versioning changes.

## Versioning and compatibility planning

Wave 3 plans should answer:
- how the shared package would be versioned,
- how repos would pin it,
- how changes would roll out without breaking all repos at once,
- how to test local-only repo development if the shared package exists.

## Acceptance criteria for this slice

Plans should identify:
1. the minimum viable shared package contents,
2. the code files that would remain local,
3. the extraction order,
4. the rollback / compatibility strategy.
