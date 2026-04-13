from __future__ import annotations

from typing import Any, Callable

from m_cache_shared.augmentation.enums import AUGMENTATION_TYPES, PRODUCER_KINDS, PRODUCER_RUN_STATUSES
from m_cache_shared.augmentation.helpers import to_int_or_none
from m_cache_shared.augmentation.models import ProducerTargetDescriptor


CanonicalKeyValidator = Callable[[str], bool]


def validate_producer_target_descriptor(payload: dict[str, Any]) -> dict[str, Any]:
    descriptor = ProducerTargetDescriptor(
        domain=str(payload.get("domain") or "").strip(),
        resource_family=str(payload.get("resource_family") or "").strip(),
        canonical_key=str(payload.get("canonical_key") or "").strip(),
        text_source=str(payload.get("text_source") or "").strip() or None,
        source_text_version=str(payload.get("source_text_version") or "").strip() or None,
        language=str(payload.get("language") or "").strip() or None,
        document_time_reference=str(payload.get("document_time_reference") or "").strip() or None,
        producer_hints=dict(payload.get("producer_hints") or {}),
    )
    return {
        "domain": descriptor.domain,
        "resource_family": descriptor.resource_family,
        "canonical_key": descriptor.canonical_key,
        "text_source": descriptor.text_source,
        "source_text_version": descriptor.source_text_version,
        "language": descriptor.language,
        "document_time_reference": descriptor.document_time_reference,
        "producer_hints": descriptor.producer_hints,
    }


def validate_run_submission_envelope(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: CanonicalKeyValidator,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    run_id = str(envelope.get("run_id") or "").strip()
    if not run_id:
        raise ValueError("run_id is required.")

    domain = str(envelope.get("domain") or "").strip().lower()
    if domain != expected_domain:
        raise ValueError(f"domain must be '{expected_domain}'.")

    resource_family = str(envelope.get("resource_family") or "").strip().lower()
    if resource_family != expected_resource_family:
        raise ValueError(f"resource_family must be '{expected_resource_family}' for {resource_family_context}.")

    canonical_key = str(envelope.get("canonical_key") or "").strip()
    if not canonical_key_validator(canonical_key):
        raise ValueError(canonical_key_error)

    augmentation_type = str(envelope.get("augmentation_type") or "").strip().lower()
    if augmentation_type not in set(AUGMENTATION_TYPES):
        raise ValueError("unsupported augmentation_type.")

    source_text_version = str(envelope.get("source_text_version") or "").strip()
    if not source_text_version:
        raise ValueError("source_text_version is required.")

    producer_kind = str(envelope.get("producer_kind") or "").strip().lower()
    if producer_kind not in set(PRODUCER_KINDS):
        raise ValueError("unsupported producer_kind.")

    producer_name = str(envelope.get("producer_name") or "").strip()
    producer_version = str(envelope.get("producer_version") or "").strip()
    payload_schema_name = str(envelope.get("payload_schema_name") or "").strip()
    payload_schema_version = str(envelope.get("payload_schema_version") or "").strip()
    status = str(envelope.get("status") or "").strip().lower()
    if status not in set(PRODUCER_RUN_STATUSES):
        raise ValueError("unsupported run status.")
    if not producer_name:
        raise ValueError("producer_name is required.")
    if not producer_version:
        raise ValueError("producer_version is required.")
    if not payload_schema_name:
        raise ValueError("payload_schema_name is required.")
    if not payload_schema_version:
        raise ValueError("payload_schema_version is required.")

    return {
        "run_id": run_id,
        "producer_run_key": str(envelope.get("producer_run_key") or "").strip(),
        "event_at": str(envelope.get("event_at") or "").strip(),
        "domain": domain,
        "resource_family": resource_family,
        "canonical_key": canonical_key,
        "augmentation_type": augmentation_type,
        "source_text_version": source_text_version,
        "producer_kind": producer_kind,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "payload_schema_name": payload_schema_name,
        "payload_schema_version": payload_schema_version,
        "status": status,
        "success": bool(envelope.get("success", True)),
        "reason_code": str(envelope.get("reason_code") or "").strip() or "OK",
        "message": str(envelope.get("message") or "").strip(),
        "persisted_locally": bool(envelope.get("persisted_locally", False)),
        "latency_ms": to_int_or_none(envelope.get("latency_ms")),
        "rate_limited": bool(envelope.get("rate_limited", False)),
        "retry_count": max(0, int(envelope.get("retry_count", 0) or 0)),
        "deferred_until": str(envelope.get("deferred_until") or "").strip(),
    }


def validate_producer_run_submission(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: CanonicalKeyValidator,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    # Compatibility alias: keep existing shared envelope validator behavior.
    return validate_run_submission_envelope(
        envelope,
        expected_domain=expected_domain,
        expected_resource_family=expected_resource_family,
        canonical_key_validator=canonical_key_validator,
        canonical_key_error=canonical_key_error,
        resource_family_context=resource_family_context,
    )


def validate_artifact_submission_envelope(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: CanonicalKeyValidator,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    domain = str(envelope.get("domain") or "").strip().lower()
    if domain != expected_domain:
        raise ValueError(f"domain must be '{expected_domain}'.")

    resource_family = str(envelope.get("resource_family") or "").strip().lower()
    if resource_family != expected_resource_family:
        raise ValueError(f"resource_family must be '{expected_resource_family}' for {resource_family_context}.")

    canonical_key = str(envelope.get("canonical_key") or "").strip()
    if not canonical_key_validator(canonical_key):
        raise ValueError(canonical_key_error)

    augmentation_type = str(envelope.get("augmentation_type") or "").strip().lower()
    if augmentation_type not in set(AUGMENTATION_TYPES):
        raise ValueError("unsupported augmentation_type.")

    source_text_version = str(envelope.get("source_text_version") or "").strip()
    producer_name = str(envelope.get("producer_name") or "").strip()
    producer_version = str(envelope.get("producer_version") or "").strip()
    payload_schema_name = str(envelope.get("payload_schema_name") or "").strip()
    payload_schema_version = str(envelope.get("payload_schema_version") or "").strip()
    if not source_text_version:
        raise ValueError("source_text_version is required.")
    if not producer_name:
        raise ValueError("producer_name is required.")
    if not producer_version:
        raise ValueError("producer_version is required.")
    if not payload_schema_name:
        raise ValueError("payload_schema_name is required.")
    if not payload_schema_version:
        raise ValueError("payload_schema_version is required.")

    artifact_locator = str(envelope.get("artifact_locator") or "").strip()
    payload = envelope.get("payload")
    if not artifact_locator and payload is None:
        raise ValueError("Provide either artifact_locator or payload.")

    return {
        "idempotency_key": str(envelope.get("idempotency_key") or "").strip(),
        "event_at": str(envelope.get("event_at") or "").strip(),
        "domain": domain,
        "resource_family": resource_family,
        "canonical_key": canonical_key,
        "augmentation_type": augmentation_type,
        "artifact_locator": artifact_locator,
        "payload_schema_name": payload_schema_name,
        "payload_schema_version": payload_schema_version,
        "source_text_version": source_text_version,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "payload": payload,
        "success": bool(envelope.get("success", True)),
    }


def validate_producer_artifact_submission(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: CanonicalKeyValidator,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    # Compatibility alias: keep existing shared envelope validator behavior.
    return validate_artifact_submission_envelope(
        envelope,
        expected_domain=expected_domain,
        expected_resource_family=expected_resource_family,
        canonical_key_validator=canonical_key_validator,
        canonical_key_error=canonical_key_error,
        resource_family_context=resource_family_context,
    )
