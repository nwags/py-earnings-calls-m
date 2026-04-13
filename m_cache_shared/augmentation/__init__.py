"""Canonical shared augmentation exports (Wave 5.1 normalized surface)."""

from m_cache_shared.augmentation.enums import (
    AUGMENTATION_TYPES,
    AugmentationType,
    CANONICAL_AUG_COMMAND_FAMILY,
    PRODUCER_KINDS,
    PRODUCER_RUN_STATUSES,
    ProducerKind,
    RunStatus,
)
from m_cache_shared.augmentation.models import (
    ApiAugmentationMeta,
    EventsViewRow,
    ProducerArtifactSubmission,
    ProducerRunSubmission,
    ProducerTargetDescriptor,
    RunStatusView,
)
from m_cache_shared.augmentation.schema_loaders import load_json_schema
from m_cache_shared.augmentation.cli_helpers import parse_json_input_payload
from m_cache_shared.augmentation.packers import (
    pack_additive_augmentation_meta,
    pack_events_view,
    pack_run_status_view,
)
from m_cache_shared.augmentation.validators import (
    validate_artifact_submission_envelope,
    validate_producer_artifact_submission,
    validate_producer_run_submission,
    validate_producer_target_descriptor,
    validate_run_submission_envelope,
)

__all__ = [
    "ProducerTargetDescriptor",
    "ProducerRunSubmission",
    "ProducerArtifactSubmission",
    "RunStatusView",
    "EventsViewRow",
    "ApiAugmentationMeta",
    "AugmentationType",
    "ProducerKind",
    "RunStatus",
    "AUGMENTATION_TYPES",
    "CANONICAL_AUG_COMMAND_FAMILY",
    "PRODUCER_KINDS",
    "PRODUCER_RUN_STATUSES",
    "validate_producer_target_descriptor",
    "validate_producer_run_submission",
    "validate_producer_artifact_submission",
    "pack_additive_augmentation_meta",
    "pack_events_view",
    "pack_run_status_view",
    "load_json_schema",
    "parse_json_input_payload",
    "validate_artifact_submission_envelope",
    "validate_run_submission_envelope",
]
