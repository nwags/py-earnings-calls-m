from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

ResolutionMode = Literal["local_only", "resolve_if_missing", "refresh_if_stale"]
ServedFrom = Literal["local_cache", "local_normalized", "remote_then_persisted", "remote_ephemeral", "none"]


@dataclass(slots=True)
class ProviderConfig:
    provider_id: str
    domain: str
    content_domain: str
    provider_type: str
    display_name: str
    base_url: str | None
    auth_type: str
    auth_env_var: str | None
    rate_limit_policy: str
    soft_limit: int | None = None
    hard_limit: int | None = None
    burst_limit: int | None = None
    retry_budget: int = 0
    backoff_policy: str = "exponential"
    direct_resolution_allowed: bool = False
    browse_discovery_allowed: bool = False
    supports_bulk_history: bool = False
    supports_incremental_refresh: bool = False
    supports_direct_resolution: bool = False
    supports_public_resolve_if_missing: bool = False
    supports_admin_refresh_if_stale: bool = False
    graceful_degradation_policy: str = "defer_and_report"
    free_tier_notes: str | None = None
    fallback_priority: int | None = None
    is_active: bool = True
    notes: str | None = None


@dataclass(slots=True)
class DomainConfig:
    enabled: bool
    cache_root: str
    normalized_refdata_root: str
    lookup_root: str
    default_resolution_mode: ResolutionMode = "local_only"
    providers: dict[str, ProviderConfig] = field(default_factory=dict)
    runtime: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class GlobalConfig:
    app_root: str = "."
    log_level: str = "INFO"
    default_summary_json: bool = False
    default_progress_json: bool = False


@dataclass(slots=True)
class EffectiveConfig:
    global_config: GlobalConfig
    domains: dict[str, DomainConfig]


@dataclass(slots=True)
class RuntimeSummary:
    status: str
    domain: str
    command_path: list[str]
    started_at: str
    finished_at: str
    elapsed_seconds: float
    resolution_mode: ResolutionMode | None
    remote_attempted: bool
    provider_requested: str | None
    provider_used: str | None
    rate_limited: bool
    retry_count: int
    persisted_locally: bool | None
    counters: dict[str, int | float]
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ProgressEvent:
    event: str
    domain: str
    command_path: list[str]
    phase: str
    elapsed_seconds: float
    counters: dict[str, int | float]
    detail: str | None = None
    window_date: str | None = None
    window_index: int | None = None
    window_total: int | None = None
    provider: str | None = None
    canonical_key: str | None = None
    rate_limit_state: str | None = None


@dataclass(slots=True)
class ResolutionEvent:
    event_at: str
    domain: str
    content_domain: str
    canonical_key: str
    resolution_mode: ResolutionMode
    provider_requested: str | None
    provider_used: str | None
    method_used: str
    served_from: ServedFrom
    remote_attempted: bool
    success: bool
    reason_code: str
    message: str | None
    persisted_locally: bool
    http_status: int | None = None
    retry_count: int = 0
    rate_limited: bool = False
    deferred_until: str | None = None
    latency_ms: int | None = None


@dataclass(slots=True)
class ReconciliationDiscrepancy:
    discrepancy_key: str
    domain: str
    target_type: str
    seen_key: str | None
    discrepancy_code: str
    target_date: str | None
    details: dict[str, Any]
    observed_at: str


@dataclass(slots=True)
class ReconciliationEvent:
    event_at: str
    domain: str
    event_code: str
    target_date: str | None
    discrepancy_count: int
    catch_up_warm: bool
    provider: str | None = None
    window_from: str | None = None
    window_to: str | None = None
    lookup_refreshed: bool | None = None
