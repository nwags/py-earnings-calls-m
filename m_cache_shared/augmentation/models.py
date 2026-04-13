from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ProducerTargetDescriptor:
    domain: str
    resource_family: str
    canonical_key: str
    text_source: str | None
    source_text_version: str | None
    language: str | None = None
    document_time_reference: str | None = None
    producer_hints: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProducerRunSubmission:
    run_id: str
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: str
    source_text_version: str
    producer_kind: str
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    status: str
    success: bool
    reason_code: str
    producer_run_key: str = ""
    event_at: str = ""
    message: str = ""
    persisted_locally: bool = False
    latency_ms: int | None = None
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: str = ""


@dataclass(frozen=True)
class ProducerArtifactSubmission:
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: str
    source_text_version: str
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    idempotency_key: str = ""
    event_at: str = ""
    artifact_locator: str = ""
    payload: dict[str, Any] | None = None
    success: bool = True


@dataclass(frozen=True)
class RunStatusView:
    domain: str
    resource_family: str
    found: bool
    run_id: str | None
    idempotency_key: str | None
    status: str | None
    success: bool | None
    reason_code: str | None
    canonical_key: str | None = None
    augmentation_type: str | None = None
    source_text_version: str | None = None
    producer_name: str | None = None
    producer_version: str | None = None
    persisted_locally: bool | None = None
    augmentation_stale: bool | None = None
    last_updated_at: str | None = None


@dataclass(frozen=True)
class EventsViewRow:
    event_at: str
    event_code: str
    canonical_key: str
    augmentation_type: str | None = None
    run_id: str | None = None
    producer_name: str | None = None
    producer_version: str | None = None
    reason_code: str | None = None
    success: bool | None = None


@dataclass(frozen=True)
class ApiAugmentationMeta:
    augmentation_available: bool
    augmentation_types_present: list[str]
    last_augmented_at: str | None = None
    augmentation_stale: bool | None = None
    inspect_path: str | None = None

