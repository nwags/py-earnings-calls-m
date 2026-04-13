# Canonical Shared Package Export Surface

## Goal

Define the canonical shared public API of `m_cache_shared.augmentation`.

## Canonical exports

### Models
- `ProducerTargetDescriptor`
- `ProducerRunSubmission`
- `ProducerArtifactSubmission`
- `RunStatusView`
- `EventsViewRow`
- `ApiAugmentationMeta`

### Enums / vocab
- `AugmentationType`
- `ProducerKind`
- `RunStatus`

Compatibility convenience value lists may also exist, but should be clearly secondary.

### Validators
- `validate_producer_target_descriptor`
- `validate_producer_run_submission`
- `validate_producer_artifact_submission`
- `validate_run_submission_envelope`
- `validate_artifact_submission_envelope`

### Schema loaders
- `load_json_schema`

### Packers
- `pack_run_status_view`
- `pack_events_view`
- `pack_additive_augmentation_meta`

### CLI helpers
- `parse_json_input_payload`

## Export rules

1. The same import paths should work in all repos.
2. Repo-local wrappers may continue to call into these exports.
3. Export names should be semantic and consistent, not repo-shaped.
4. Compatibility aliases should remain local where possible rather than bloating the shared package API.
