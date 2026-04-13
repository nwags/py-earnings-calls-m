from dataclasses import dataclass
from typing import Optional

@dataclass
class ExternalIdentityPlan:
    repo: str
    distribution_name: str
    import_root: str
    pin_file_path: str
    release_candidate_tag: str

@dataclass
class ShimContractPlan:
    repo: str
    facade_module: str
    source_mode_env_var: str
    external_root_env_var: str
    default_external_root: str
    fallback_behavior: str

@dataclass
class RcValidationPlan:
    repo: str
    strict_public_api_only: bool
    full_test_command: str
    rollback_mechanism: str
    notes: Optional[str] = None
