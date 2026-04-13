from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class SharedExportTarget:
    module_path: str
    symbol_name: str
    category: str
    canonical_name: str
    compatibility_aliases: List[str] = field(default_factory=list)

@dataclass
class SharedLayoutTarget:
    repo: str
    current_layout: str
    target_layout: str
    requires_compat_shim: bool
    notes: Optional[str] = None

@dataclass
class SharedTestNormalizationTarget:
    repo: str
    current_test_module: str
    target_test_module: str
    notes: Optional[str] = None

@dataclass
class RoleFreezeAssertion:
    repo: str
    pilot_role: bool
    write_path_behavior: str
    authority_behavior: str
    applicability_behavior: str
