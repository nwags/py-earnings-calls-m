from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Literal

AugmentationType = Literal["entity_tagging", "temporal_expression_tagging"]
ProducerKind = Literal["llm", "rules", "hybrid", "manual"]
RunStatus = Literal["queued", "running", "completed", "failed", "deferred", "skipped"]

@dataclass
class SharedPackageCandidate:
    module_name: str
    category: str
    rationale: str
    safe_to_extract_now: bool
    notes: Optional[str] = None

@dataclass
class ProducerTargetDescriptor:
    domain: str
    resource_family: str
    canonical_key: str
    text_source: str
    source_text_version: str
    language: Optional[str] = None
    document_time_reference: Optional[str] = None
    producer_hints: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ProducerRunSubmission:
    run_id: str
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    source_text_version: str
    producer_kind: ProducerKind
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    status: RunStatus
    success: bool
    reason_code: str
    persisted_locally: bool = False

@dataclass
class ProducerArtifactSubmission:
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    source_text_version: str
    producer_name: str
    producer_version: str
    payload_schema_name: str
    payload_schema_version: str
    artifact_locator: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    success: bool = True

@dataclass
class EntityAnnotation:
    span_start: int
    span_end: int
    text: str
    label: str
    confidence: Optional[float] = None
    normalized_text: Optional[str] = None
    canonical_entity_id: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TemporalAnnotation:
    span_start: int
    span_end: int
    text: str
    timex_type: str
    normalized_value: str
    document_time_reference: Optional[str] = None
    anchor_time: Optional[str] = None
    confidence: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
