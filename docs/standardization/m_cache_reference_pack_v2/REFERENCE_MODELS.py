from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ProviderDetail:
    provider_id: str
    domain: str
    content_domain: str
    display_name: str
    provider_type: str
    auth_type: str
    rate_limit_policy: str
    direct_resolution_allowed: bool
    browse_discovery_allowed: bool
    supports_bulk_history: bool
    supports_incremental_refresh: bool
    supports_direct_resolution: bool
    supports_public_resolve_if_missing: bool
    supports_admin_refresh_if_stale: bool
    graceful_degradation_policy: str
    is_active: bool
    soft_limit: Optional[int] = None
    hard_limit: Optional[int] = None
    burst_limit: Optional[int] = None
    retry_budget: int = 0
    backoff_policy: Optional[str] = None
    fallback_priority: Optional[int] = None
    default_timeout_seconds: Optional[int] = None
    quota_window_seconds: Optional[int] = None
    quota_reset_hint: Optional[str] = None
    expected_error_modes: list[str] = field(default_factory=list)
    user_agent_required: Optional[bool] = None
    contact_requirement: Optional[str] = None
    terms_url: Optional[str] = None
    effective_auth_present: Optional[bool] = None
    effective_enabled: Optional[bool] = None
    notes: Optional[str] = None


@dataclass
class ProviderSelectionDecision:
    provider_requested: Optional[str]
    provider_used: Optional[str]
    selection_outcome: str
    reason_code: str
    remote_attempted: bool
    rate_limited: bool
    retry_count: int
    deferred: bool
    deferred_until: Optional[str] = None
    provider_skip_reasons: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class RateLimitState:
    provider_id: str
    rate_limit_policy: str
    soft_limit: Optional[int] = None
    hard_limit: Optional[int] = None
    burst_limit: Optional[int] = None
    quota_window_seconds: Optional[int] = None
    retry_budget: Optional[int] = None
    rate_limited: bool = False
    deferred: bool = False
    deferred_until: Optional[str] = None
    message: Optional[str] = None


@dataclass
class ResolutionResultMeta:
    resolution_mode: str
    provider_requested: Optional[str]
    provider_used: Optional[str]
    served_from: str
    remote_attempted: bool
    persisted_locally: Optional[bool]
    success: bool
    reason_code: str
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: Optional[str] = None
    http_status: Optional[int] = None
    latency_ms: Optional[int] = None
    message: Optional[str] = None


@dataclass
class ApiResolutionMeta:
    resolution_mode: str
    remote_attempted: bool
    provider_requested: Optional[str]
    provider_used: Optional[str]
    served_from: str
    persisted_locally: Optional[bool]
    rate_limited: bool = False
    retry_count: int = 0
    deferred_until: Optional[str] = None
    reason_code: Optional[str] = None
