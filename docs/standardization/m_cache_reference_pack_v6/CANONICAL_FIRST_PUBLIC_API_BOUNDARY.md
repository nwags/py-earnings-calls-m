# Canonical First Public API Boundary

## Goal

Define the narrow first public API for the external `m_cache_shared` package.

## Include in the first public API

### Models
- `ProducerTargetDescriptor`
- `ProducerRunSubmission`
- `ProducerArtifactSubmission`
- `RunStatusView`
- `EventsViewRow`
- `ApiAugmentationMeta`

### Enums
- `AugmentationType`
- `ProducerKind`
- `RunStatus`

### Validators
- `validate_producer_target_descriptor`
- `validate_producer_run_submission`
- `validate_producer_artifact_submission`
- `validate_run_submission_envelope`
- `validate_artifact_submission_envelope`

### Schema loader
- `load_json_schema`

### Packers
- `pack_run_status_view`
- `pack_events_view`

### CLI helper
- `parse_json_input_payload`

## Explicitly exclude from the first public API

- `pack_additive_augmentation_meta`
- compatibility alias names
- flat-module shims
- descriptor builders
- source-text-version derivation
- applicability enforcement
- authority routing
- write orchestration
- adapter/storage/path helpers

## Rule

The first public API is the **strict intersection** already proven across repos, not the largest common-ish set.
