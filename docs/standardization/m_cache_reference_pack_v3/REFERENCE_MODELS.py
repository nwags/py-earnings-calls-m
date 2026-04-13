from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal

AugmentationType = Literal["entity_tagging", "temporal_expression_tagging"]
ProducerKind = Literal["llm", "rules", "hybrid", "manual"]
AugmentationStatus = Literal["queued", "running", "completed", "failed", "deferred", "skipped"]

@dataclass
class SharedExtractionCandidate:
    module_name: str
    category: str
    rationale: str
    safe_to_extract_now: bool
    notes: Optional[str] = None

@dataclass
class AugmentationApplicability:
    domain: str
    resource_family: str
    text_bearing: bool
    augmentation_applicable: bool
    rationale: str

@dataclass
class AugmentationRunMeta:
    run_id: str
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    source_text_version: Optional[str]
    producer_kind: ProducerKind
    producer_name: str
    status: AugmentationStatus
    success: bool
    reason_code: str
    message: Optional[str] = None
    persisted_locally: bool = False
    latency_ms: Optional[int] = None
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: Optional[str] = None

@dataclass
class AugmentationArtifactMeta:
    domain: str
    resource_family: str
    canonical_key: str
    augmentation_type: AugmentationType
    artifact_locator: str
    source_text_version: Optional[str]
    producer_name: str
    event_at: str
    success: bool

@dataclass
class ApiAugmentationMeta:
    augmentation_available: bool
    augmentation_types_present: List[str] = field(default_factory=list)
    last_augmented_at: Optional[str] = None
    augmentation_stale: Optional[bool] = None
    inspect_path: Optional[str] = None
