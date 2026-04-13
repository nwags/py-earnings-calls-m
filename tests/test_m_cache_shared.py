from __future__ import annotations

from pathlib import Path

from m_cache_shared import augmentation as aug_shared
from m_cache_shared.augmentation import (
    AUGMENTATION_TYPES,
    AugmentationType,
    ProducerKind,
    RunStatus,
    load_json_schema,
    parse_json_input_payload,
    validate_producer_artifact_submission,
    validate_producer_run_submission,
    validate_producer_target_descriptor,
)
from m_cache_shared.augmentation.enums import CANONICAL_AUG_COMMAND_ALIASES
from m_cache_shared.augmentation.helpers import normalize_aug_command_name
from m_cache_shared.augmentation.packers import (
    build_artifact_idempotency_key,
    pack_artifact_event_row,
    pack_events_view,
    pack_run_event_row,
    pack_run_status_not_found,
    pack_run_status_view,
)
from m_cache_shared.augmentation.validators import (
    validate_artifact_submission_envelope,
    validate_run_submission_envelope,
)


def _is_transcript_key(value: str) -> bool:
    return value.startswith("transcript:")


def test_shared_run_validator_accepts_outer_envelope() -> None:
    payload = validate_producer_run_submission(
        {
            "run_id": "run-1",
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "augmentation_type": "entity_tagging",
            "source_text_version": "sha256:abc",
            "producer_kind": "rules",
            "producer_name": "tagger",
            "producer_version": "1.0.0",
            "payload_schema_name": "entity.v1",
            "payload_schema_version": "1.0.0",
            "status": "completed",
            "success": True,
            "reason_code": "OK",
        },
        expected_domain="earnings",
        expected_resource_family="transcripts",
        canonical_key_validator=_is_transcript_key,
        canonical_key_error="bad key",
        resource_family_context="Wave 4 in earnings",
    )
    assert payload["run_id"] == "run-1"
    assert payload["augmentation_type"] in set(AUGMENTATION_TYPES)


def test_shared_run_validator_rejects_bad_resource_family() -> None:
    try:
        validate_producer_run_submission(
            {
                "run_id": "run-1",
                "domain": "earnings",
                "resource_family": "forecasts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
            },
            expected_domain="earnings",
            expected_resource_family="transcripts",
            canonical_key_validator=_is_transcript_key,
            canonical_key_error="bad key",
            resource_family_context="Wave 4 in earnings",
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "resource_family must be 'transcripts'" in str(exc)


def test_shared_artifact_validator_requires_locator_or_payload() -> None:
    try:
        validate_producer_artifact_submission(
            {
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
            },
            expected_domain="earnings",
            expected_resource_family="transcripts",
            canonical_key_validator=_is_transcript_key,
            canonical_key_error="bad key",
            resource_family_context="Wave 4 in earnings",
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "Provide either artifact_locator or payload." in str(exc)


def test_shared_packers_are_pure_and_shape_stable() -> None:
    event = pack_run_event_row(
        {
            "run_id": "run-1",
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "augmentation_type": "entity_tagging",
            "source_text_version": "sha256:abc",
            "producer_name": "tagger",
            "producer_version": "1.0.0",
            "status": "completed",
            "success": True,
            "reason_code": "OK",
        },
        event_at="2026-04-10T10:00:00Z",
    )
    assert event["run_id"] == "run-1"
    assert event["event_id"]

    artifact = pack_artifact_event_row(
        {
            "idempotency_key": "idem-1",
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "augmentation_type": "entity_tagging",
            "source_text_version": "sha256:abc",
            "producer_name": "tagger",
            "producer_version": "1.0.0",
            "success": True,
        },
        event_at="2026-04-10T10:00:01Z",
    )
    assert artifact["reason_code"] == "ARTIFACT_SUBMITTED"

    found = pack_run_status_view(
        domain="earnings",
        resource_family="transcripts",
        run_id="run-1",
        idempotency_key="idem-1",
        canonical_key="transcript:c1",
        augmentation_type="entity_tagging",
        source_text_version="sha256:abc",
        producer_name="tagger",
        producer_version="1.0.0",
        status="completed",
        success=True,
        reason_code="OK",
        persisted_locally=True,
        augmentation_stale=False,
        last_updated_at="2026-04-10T10:00:00Z",
    )
    missing = pack_run_status_not_found(
        domain="earnings",
        resource_family="transcripts",
        run_id="run-2",
        idempotency_key=None,
        reason_code="RUN_NOT_FOUND",
    )
    assert found["found"] is True
    assert missing["found"] is False

    events_payload = pack_events_view(
        domain="earnings",
        resource_family="transcripts",
        augmentation_applicable=True,
        reason_code=None,
        message=None,
        records=[event, artifact],
    )
    assert events_payload["record_count"] == 2

    idem = build_artifact_idempotency_key(
        {
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "augmentation_type": "entity_tagging",
            "source_text_version": "sha256:abc",
            "producer_name": "tagger",
            "producer_version": "1.0.0",
            "payload_schema_name": "entity.v1",
            "payload_schema_version": "1.0.0",
        },
        artifact_locator="s3://bucket/a.json",
        payload_sha256=None,
    )
    assert idem


def test_shared_thin_helpers_load_schema_and_normalize_alias() -> None:
    normalized = normalize_aug_command_name("inspect-runs")
    assert normalized == CANONICAL_AUG_COMMAND_ALIASES["inspect-runs"]
    schema = load_json_schema("docs/standardization/m_cache_reference_pack_v5/schemas/run-status-view.schema.json")
    assert schema["type"] == "object"


def test_shared_cli_helper_parse_json_input_payload(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text('{"run_id":"run-1"}', encoding="utf-8")
    payload = parse_json_input_payload(payload_path)
    assert payload["run_id"] == "run-1"


def test_shared_target_descriptor_validator_wrapper() -> None:
    descriptor = validate_producer_target_descriptor(
        {
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "text_source": "/tmp/c1.txt",
            "source_text_version": "sha256:abc",
            "producer_hints": {"hint": "v"},
        }
    )
    assert descriptor["domain"] == "earnings"
    assert descriptor["producer_hints"]["hint"] == "v"


def test_shared_canonical_export_surface() -> None:
    assert aug_shared.ProducerTargetDescriptor.__name__ == "ProducerTargetDescriptor"
    assert aug_shared.ProducerRunSubmission.__name__ == "ProducerRunSubmission"
    assert aug_shared.ProducerArtifactSubmission.__name__ == "ProducerArtifactSubmission"
    assert aug_shared.RunStatusView.__name__ == "RunStatusView"
    assert aug_shared.EventsViewRow.__name__ == "EventsViewRow"
    assert aug_shared.ApiAugmentationMeta.__name__ == "ApiAugmentationMeta"
    assert aug_shared.AugmentationType == AugmentationType
    assert aug_shared.ProducerKind == ProducerKind
    assert aug_shared.RunStatus == RunStatus
    assert callable(aug_shared.validate_producer_target_descriptor)
    assert callable(aug_shared.validate_producer_run_submission)
    assert callable(aug_shared.validate_producer_artifact_submission)
    assert callable(aug_shared.validate_run_submission_envelope)
    assert callable(aug_shared.validate_artifact_submission_envelope)
    assert callable(aug_shared.load_json_schema)
    assert callable(aug_shared.parse_json_input_payload)
    assert callable(aug_shared.pack_run_status_view)
    assert callable(aug_shared.pack_events_view)
    assert callable(aug_shared.pack_additive_augmentation_meta)
    assert not hasattr(aug_shared, "pack_run_event_row")
    assert not hasattr(aug_shared, "pack_artifact_event_row")
    assert not hasattr(aug_shared, "pack_run_status_not_found")


def test_shared_compatibility_validator_exports_remain() -> None:
    payload = validate_run_submission_envelope(
        {
            "run_id": "run-1",
            "domain": "earnings",
            "resource_family": "transcripts",
            "canonical_key": "transcript:c1",
            "augmentation_type": "entity_tagging",
            "source_text_version": "sha256:abc",
            "producer_kind": "rules",
            "producer_name": "tagger",
            "producer_version": "1.0.0",
            "payload_schema_name": "entity.v1",
            "payload_schema_version": "1.0.0",
            "status": "completed",
            "success": True,
            "reason_code": "OK",
        },
        expected_domain="earnings",
        expected_resource_family="transcripts",
        canonical_key_validator=_is_transcript_key,
        canonical_key_error="bad key",
        resource_family_context="Wave 4 in earnings",
    )
    assert payload["run_id"] == "run-1"
