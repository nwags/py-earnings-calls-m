from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ExternalPublicApiSymbol:
    module_path: str
    symbol_name: str
    category: str
    included_in_v1: bool
    reason: str
    notes: Optional[str] = None

@dataclass
class ExternalizationTarget:
    repo: str
    local_shim_module: str
    target_external_import: str
    keep_local_wrapper: bool
    notes: Optional[str] = None

@dataclass
class VersioningPlan:
    package_name: str
    versioning_scheme: str
    pinning_strategy: str
    rollback_strategy: str

@dataclass
class RolePreservationPlan:
    repo: str
    pilot_role: bool
    write_path_behavior: str
    authority_behavior: str
    applicability_behavior: str
