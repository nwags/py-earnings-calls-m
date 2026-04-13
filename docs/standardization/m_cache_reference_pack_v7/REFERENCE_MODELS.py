from dataclasses import dataclass
from typing import Optional

@dataclass
class GovernancePlan:
    repo: str
    external_repo_name: str
    public_api_policy: str
    required_release_artifacts: str

@dataclass
class RcWorkflowPlan:
    repo: str
    facade_module: str
    rc_validation_role: str
    full_test_command: str
    evidence_artifact: Optional[str] = None

@dataclass
class UserTestingPlan:
    repo: str
    user_testing_required: bool
    key_user_flows: str
    release_gate_notes: Optional[str] = None

@dataclass
class ShimRetirementPlan:
    repo: str
    local_shims_that_must_remain: str
    earliest_cleanup_condition: str
    notes: Optional[str] = None
