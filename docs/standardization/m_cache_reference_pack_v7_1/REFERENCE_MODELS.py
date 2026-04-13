from dataclasses import dataclass
from typing import Optional

@dataclass
class PackageReleasePlan:
    repo: str
    release_role: str
    facade_module: str
    required_validation: str
    evidence_artifact: Optional[str] = None

@dataclass
class PromotionPlan:
    repo: str
    required_signoff: str
    blocker_conditions: str
    rollback_path: str

@dataclass
class UserTestingStartPlan:
    repo: str
    requires_wave7_1_completion: bool
    requires_shared_rc_validation: bool
    key_start_gate_note: str

@dataclass
class CleanupDeferralPlan:
    repo: str
    deferred_items: str
    earliest_entry_condition: str
    notes: Optional[str] = None
