from dataclasses import dataclass
from typing import Optional

@dataclass
class ExternalRepoExecutionPlan:
    repo_name: str
    rc_workflow: str
    evidence_bundle_root: str
    promotion_authority: str
    rollback_authority: str

@dataclass
class RcCandidatePlan:
    candidate_tag: str
    public_api_frozen: bool
    required_consumer_repos: str
    decision_record_path: str

@dataclass
class ConsumerCompanionPlan:
    repo: str
    release_role: str
    required_validation: str
    evidence_input: str
    blocker_scope: str

@dataclass
class UserTestingHandoffPlan:
    requires_wave7_2_completion: bool
    requires_real_shared_rc_cycle: bool
    requires_evidence_bundle_operational: bool
    notes: Optional[str] = None
