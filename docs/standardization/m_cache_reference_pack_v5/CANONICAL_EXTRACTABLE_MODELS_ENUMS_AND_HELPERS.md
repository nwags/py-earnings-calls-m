# Canonical Extractable Models, Enums, and Helpers

## Goal

Lock the stable code-level building blocks that are candidates for the first extraction.

## Extractable model families

### Protocol models
- `ProducerTargetDescriptor`
- `ProducerRunSubmission`
- `ProducerArtifactSubmission`

### View/output models
- `RunStatusView`
- `EventsViewRow`
- `ApiAugmentationMeta`

### Shared enums
- `AugmentationType`
- `ProducerKind`
- `RunStatus`
- canonical surface identifiers / semantic constants where stable

## Extractable validator families

- target descriptor validation
- run submission validation
- artifact submission validation
- canonical option validation
- compatibility alias normalization helpers

## Extractable packer/builder families

- status view packaging
- event row packaging
- additive `augmentation_meta` packaging
- producer/schema/source-text-version enrichment
- stale computation packaging

## Extractable thin helper families

- canonical command-family option parsing helpers
- alias-registration helpers
- common CLI result formatting helpers
- common API helper packaging where behavior is already aligned

## Explicit non-extractable helpers

Keep local:
- text retrieval helpers
- applicability resolution beyond shared vocabulary
- authority write-path helpers
- payload schema interpretation
- artifact storage placement rules

## Acceptance criteria for this slice

Each repo plan should answer:
1. what the repo’s current local equivalents are,
2. whether they are ready to move directly or need a local shim first,
3. what tests prove the extracted semantics are stable.
