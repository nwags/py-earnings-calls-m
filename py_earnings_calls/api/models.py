from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class AugmentationMetaResponse(BaseModel):
    augmentation_available: bool
    augmentation_types_present: list[str]
    last_augmented_at: str | None = None
    augmentation_stale: bool | None = None
    inspect_path: str | None = None


class ProducerTargetDescriptorResponse(BaseModel):
    domain: str
    resource_family: str
    canonical_key: str
    call_id: str
    text_source: str | None = None
    source_text_version: str | None = None
    language: str | None = None
    document_time_reference: str | None = None
    producer_hints: dict


class ProducerRunSubmissionRequest(BaseModel):
    run_id: str
    producer_run_key: str | None = None
    event_at: str | None = None
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
    success: bool = True
    reason_code: str
    message: str | None = None
    persisted_locally: bool = False
    latency_ms: int | None = None
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: str | None = None


class ProducerArtifactSubmissionRequest(BaseModel):
    idempotency_key: str | None = None
    event_at: str | None = None
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: str
    artifact_locator: str | None = None
    payload_schema_name: str
    payload_schema_version: str
    source_text_version: str
    producer_name: str
    producer_version: str
    payload: dict | None = None
    success: bool = True


class ProducerSubmissionResponse(BaseModel):
    accepted: bool
    idempotent_replay: bool
    run_id: str | None = None
    event_id: str | None = None
    run_path: str | None = None
    event_path: str | None = None
    idempotency_key: str | None = None
    artifact_locator: str | None = None
    artifact_path: str | None = None


class TranscriptMetadataResponse(BaseModel):
    call_id: str
    provider: str | None = None
    symbol: str | None = None
    cik: str | None = None
    title: str | None = None
    call_datetime: str | None = None
    transcript_path: str | None = None
    served_from: str | None = None
    resolution_mode: str | None = None
    remote_attempted: bool | None = None
    provider_requested: str | None = None
    provider_used: str | None = None
    method_used: str | None = None
    success: bool | None = None
    reason_code: str | None = None
    persisted_locally: bool | None = None
    rate_limited: bool | None = None
    retry_count: int | None = None
    deferred_until: str | None = None
    augmentation_meta: AugmentationMetaResponse | None = None


class ForecastLatestResponse(BaseModel):
    symbol: str
    as_of_date: str | None = None
    points: list[dict]


class ForecastSnapshotResponse(BaseModel):
    provider: str
    symbol: str
    as_of_date: str
    points: list[dict]
    served_from: str | None = None
    resolution_mode: str | None = None
    remote_attempted: bool | None = None
    provider_requested: str | None = None
    provider_used: str | None = None
    method_used: str | None = None
    success: bool | None = None
    reason_code: str | None = None
    persisted_locally: bool | None = None
    rate_limited: bool | None = None
    retry_count: int | None = None
    deferred_until: str | None = None


class ForecastByCikItemResponse(BaseModel):
    cik: str | None = None
    symbol: str | None = None
    provider: str | None = None
    as_of_date: str | None = None
    snapshot_id: str | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    metric_name: str | None = None
    stat_name: str | None = None
    value: float | None = None


class ForecastByCikResponse(BaseModel):
    cik: str
    as_of_date: str | None = None
    items: list[ForecastByCikItemResponse]
    limit: int
    offset: int
    total: int


class TranscriptListItemResponse(BaseModel):
    call_id: str
    provider: str | None = None
    symbol: str | None = None
    cik: str | None = None
    title: str | None = None
    call_datetime: str | None = None
    transcript_path: str | None = None


class TranscriptListResponse(BaseModel):
    items: list[TranscriptListItemResponse]
    limit: int
    offset: int
    total: int
