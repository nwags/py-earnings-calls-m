# Wave 5 Migration Note (py-earnings-calls-m)

This document covers the Wave 5 extraction in this repo only.

`py-earnings-calls-m` remains a pilot repo for live producer write-path behavior.

## Wave 5 Scope in This Repo

- first in-repo shared package introduction: `m_cache_shared`
- minimum extraction cut only:
  - models
  - enums/vocabularies
  - validators/schema loader helper
  - pure metadata packers/builders
  - thin helper plumbing
- standalone repo behavior remains unchanged

## Symbols Moved Into `m_cache_shared`

Package root:

- `m_cache_shared/augmentation/enums.py`
  - `AUGMENTATION_TYPES`
  - `PRODUCER_KINDS`
  - `PRODUCER_RUN_STATUSES`
  - `CANONICAL_AUG_COMMAND_FAMILY`
  - `CANONICAL_AUG_COMMAND_ALIASES`
- `m_cache_shared/augmentation/models.py`
  - `ProducerTargetDescriptor`
  - `ProducerRunSubmission`
  - `ProducerArtifactSubmission`
  - `RunStatusView`
  - `EventsViewRow`
  - `ApiAugmentationMeta`
- `m_cache_shared/augmentation/validators.py`
  - `validate_run_submission_envelope`
  - `validate_artifact_submission_envelope`
- `m_cache_shared/augmentation/packers.py`
  - `build_artifact_idempotency_key`
  - `pack_run_event_row`
  - `pack_artifact_event_row`
  - `pack_run_status_view`
  - `pack_run_status_not_found`
  - `pack_events_view`
  - `pack_additive_augmentation_meta`
- `m_cache_shared/augmentation/helpers.py`
  - `coerce_bool`
  - `max_nonempty_text`
  - `to_int_or_none`
  - `normalize_aug_command_name`
  - `load_json_schema`

## Intentionally Left Repo-Local

- identity and targeting:
  - `transcript_canonical_key`
  - `parse_transcript_call_id`
  - `lookup_transcript_path_for_call_id`
  - `transcript_target_descriptor`
  - `transcript_source_text_version_from_path`
- live pilot write-path orchestration and storage behavior:
  - `submit_producer_run`
  - `submit_producer_artifact`
  - `_materialize_inline_payload`
- transcript-only applicability enforcement and local read wrappers:
  - `m-cache earnings aug inspect-target|status|events` handlers/wrappers in `py_earnings_calls.m_cache_cli`
- API route and wire-shape ownership:
  - `py_earnings_calls.api.app`
  - `py_earnings_calls.api.models`
  - `py_earnings_calls.api.service`
- adapters/storage/execution internals remain local

## Compatibility and Role Preservation

- `py-earnings-calls ...` remains the compatibility operator surface.
- `m-cache earnings ...` remains the additive canonical surface.
- existing producer API routes remain unchanged:
  - `GET /transcripts/{call_id}/augmentation-target`
  - `POST /augmentations/runs`
  - `POST /augmentations/artifacts`
- transcript-only augmentation applicability remains unchanged.
- payload schemas remain producer/service-owned; shared code validates only outer envelopes.

## Wave 5.1 Normalization (Shape/Exports Only)

Wave 5.1 in this repo is normalization-only:

- no extraction scope broadening,
- no pilot-role changes,
- no CLI/API semantic changes.

### Canonical package layout

`m_cache_shared/augmentation/` now exposes the canonical nested layout:

- `enums.py`
- `models.py`
- `validators.py`
- `schema_loaders.py` (added in Wave 5.1)
- `packers.py`
- `cli_helpers.py` (added in Wave 5.1)

### Canonical shared export surface

`m_cache_shared.augmentation` now provides additive canonical exports:

- Models:
  - `ProducerTargetDescriptor`
  - `ProducerRunSubmission`
  - `ProducerArtifactSubmission`
  - `RunStatusView`
  - `EventsViewRow`
  - `ApiAugmentationMeta`
- Enums/vocab:
  - `AugmentationType`
  - `ProducerKind`
  - `RunStatus`
  - compatibility value lists retained:
    - `AUGMENTATION_TYPES`
    - `PRODUCER_KINDS`
    - `PRODUCER_RUN_STATUSES`
- Validators:
  - `validate_producer_target_descriptor`
  - `validate_producer_run_submission`
  - `validate_producer_artifact_submission`
  - compatibility envelope validators retained:
    - `validate_run_submission_envelope`
    - `validate_artifact_submission_envelope`
- Schema loader:
  - `load_json_schema`
- Packers:
  - `pack_run_status_view`
  - `pack_events_view`
  - `pack_additive_augmentation_meta`
- CLI helper:
  - `parse_json_input_payload`

### What remained local

- non-canonical/internal packers and helper plumbing remain module-local:
  - `build_artifact_idempotency_key`
  - `pack_run_event_row`
  - `pack_artifact_event_row`
  - `pack_run_status_not_found`
  - `normalize_aug_command_name`
  - `coerce_bool`
  - `max_nonempty_text`
  - `to_int_or_none`
- transcript identity, transcript target building, transcript text retrieval, storage placement, API route wire-shape ownership, and live write-path orchestration all remain local and unchanged.

### Compatibility shims retained

- `m_cache_shared.augmentation.helpers.load_json_schema` remains as a compatibility shim to `schema_loaders.load_json_schema`.
- local CLI/API wrappers keep ownership and call shared packers/helpers without changing operator-visible behavior.

## Wave 6 Externalization Adoption Seam (Facade-First)

Wave 6 in this repo keeps runtime behavior unchanged while introducing an explicit facade-first external adoption seam.

### Centralized Git-tag pin

- external pin file: `requirements/m_cache_shared_external.txt`
- current pin:
  - package: `m-cache-shared`
  - git URL: `https://github.com/m-cache/m_cache_shared.git`
  - tag: `v0.1.0`

### Shadowing-safe import strategy

- all adoption flows through `py_earnings_calls.augmentation_shared`.
- the facade supports `PY_EARNINGS_CALLS_SHARED_SOURCE=external` for first-cycle external trials.
- when external mode is requested, the facade imports `m_cache_shared.augmentation` and rejects it if import origin resolves to in-repo `m_cache_shared/` (explicit shadowing guard).
- if external import is unavailable or shadowed, the facade falls back to local shared code (`local_fallback`) without import-order tricks.

### First external public API boundary (strict subset)

Facade externalized subset:

- models: `ProducerTargetDescriptor`, `ProducerRunSubmission`, `ProducerArtifactSubmission`, `RunStatusView`, `EventsViewRow`, `ApiAugmentationMeta`
- enums: `AugmentationType`, `ProducerKind`, `RunStatus`
- validators: `validate_producer_target_descriptor`, `validate_producer_run_submission`, `validate_producer_artifact_submission`, `validate_run_submission_envelope`, `validate_artifact_submission_envelope`
- schema loader: `load_json_schema`
- packers: `pack_run_status_view`, `pack_events_view`
- CLI helper: `parse_json_input_payload`

Intentionally kept local in first external cycle:

- `pack_additive_augmentation_meta`
- `pack_run_status_not_found`
- `pack_run_event_row`
- `pack_artifact_event_row`
- `build_artifact_idempotency_key`
- compatibility alias/value-list shaping and repo-local helper plumbing

### Rollback

- primary rollback: repin to an earlier git tag in `requirements/m_cache_shared_external.txt`.
- secondary rollback: keep facade on local mode (or automatic local fallback) with no CLI/API behavior changes.

## Wave 6.1 Convergence (Canonical Identity + Shim Contract)

Wave 6.1 keeps this repo convergence-only:

- no extraction scope changes,
- no CLI/API semantic changes,
- no pilot-role changes.

### Canonical external identity

- distribution: `m-cache-shared-ext`
- import root: `m_cache_shared_ext.augmentation`
- centralized pin file: `requirements/m_cache_shared_external.txt`
- shared RC tag: `v0.1.0-rc1`

### Canonical facade source-mode contract

`py_earnings_calls/augmentation_shared.py` remains the sole first-hop adoption seam and now follows:

- `M_CACHE_SHARED_SOURCE={auto|external|local}`
- `M_CACHE_SHARED_EXTERNAL_ROOT` defaulting to `m_cache_shared_ext.augmentation`

Semantics:

- `auto`: try external first, verify strict v1 symbol set, fallback to local if unavailable/incomplete.
- `external`: require external module + strict v1 symbol set; fail loudly otherwise.
- `local`: bypass external and use local in-repo shared implementation.

One-cycle compatibility alias retained:

- `PY_EARNINGS_CALLS_SHARED_SOURCE` remains compatibility-only input for one cycle.
- if both are set, canonical `M_CACHE_SHARED_SOURCE` wins.
- deprecation note: remove `PY_EARNINGS_CALLS_SHARED_SOURCE` after the Wave 6.1 stabilization cycle.

### Ambiguity handling posture

- distinct external root (`m_cache_shared_ext.augmentation`) is now the primary ambiguity solution.
- origin-path/shadow detection is no longer the primary mechanism.
- explicit local fallback remains behind the facade.
