from __future__ import annotations

from pathlib import Path

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path

PROVIDER_REGISTRY_COLUMNS = [
    "provider_id",
    "domain",
    "content_domain",
    "provider_type",
    "display_name",
    "base_url",
    "auth_type",
    "auth_env_var",
    "rate_limit_policy",
    "soft_limit",
    "hard_limit",
    "burst_limit",
    "retry_budget",
    "backoff_policy",
    "direct_resolution_allowed",
    "browse_discovery_allowed",
    "supports_bulk_history",
    "supports_incremental_refresh",
    "supports_direct_resolution",
    "supports_public_resolve_if_missing",
    "supports_admin_refresh_if_stale",
    "graceful_degradation_policy",
    "free_tier_notes",
    "fallback_priority",
    "is_active",
    "notes",
    "default_timeout_seconds",
    "quota_window_seconds",
    "quota_reset_hint",
    "expected_error_modes",
    "user_agent_required",
    "contact_requirement",
    "terms_url",
    # Additive repo-specific compatibility fields.
    "supports_freshness_fetch",
    "env_key_name",
    "retrieval_policy",
    "preferred_resolution_order",
    "direct_uri_allowed",
    "capability_level",
]

_BOOL_FIELDS = {
    "direct_resolution_allowed",
    "browse_discovery_allowed",
    "supports_bulk_history",
    "supports_incremental_refresh",
    "supports_direct_resolution",
    "supports_public_resolve_if_missing",
    "supports_admin_refresh_if_stale",
    "is_active",
    "user_agent_required",
    # Legacy compatibility fields.
    "supports_freshness_fetch",
    "direct_uri_allowed",
}

_INT_FIELDS = {
    "soft_limit",
    "hard_limit",
    "burst_limit",
    "retry_budget",
    "fallback_priority",
    "preferred_resolution_order",
    "default_timeout_seconds",
    "quota_window_seconds",
}


def default_provider_registry() -> pd.DataFrame:
    rows = [
        {
            "provider_id": "motley_fool",
            "domain": "earnings",
            "content_domain": "transcript",
            "provider_type": "official_site",
            "display_name": "Motley Fool",
            "base_url": "https://www.fool.com",
            "auth_type": "none",
            "auth_env_var": "",
            "rate_limit_policy": "unknown",
            "soft_limit": 0,
            "hard_limit": 0,
            "burst_limit": 0,
            "retry_budget": 2,
            "backoff_policy": "exponential",
            "direct_resolution_allowed": True,
            "browse_discovery_allowed": False,
            "supports_bulk_history": False,
            "supports_incremental_refresh": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "graceful_degradation_policy": "defer_and_report",
            "free_tier_notes": "Use manifest-driven backfill and bounded retries.",
            "fallback_priority": 10,
            "is_active": True,
            "notes": "Manifest/backfill and targeted direct URI resolution.",
            "default_timeout_seconds": 30,
            "quota_window_seconds": 60,
            "quota_reset_hint": "",
            "expected_error_modes": ["http_429", "retry_exhausted", "non_transcript_page"],
            "user_agent_required": True,
            "contact_requirement": "Declared contactable user agent is required.",
            "terms_url": "https://www.fool.com",
            "supports_freshness_fetch": True,
            "env_key_name": "",
            "retrieval_policy": "uri_fetch_parse",
            "preferred_resolution_order": 1,
            "direct_uri_allowed": True,
            "capability_level": "full_content",
        },
        {
            "provider_id": "finnhub",
            "domain": "earnings",
            "content_domain": "forecast",
            "provider_type": "partner_api",
            "display_name": "Finnhub",
            "base_url": "https://finnhub.io/api/v1",
            "auth_type": "api_key_header",
            "auth_env_var": "FINNHUB_API_KEY",
            "rate_limit_policy": "per_minute",
            "soft_limit": 60,
            "hard_limit": 60,
            "burst_limit": 5,
            "retry_budget": 2,
            "backoff_policy": "exponential",
            "direct_resolution_allowed": True,
            "browse_discovery_allowed": True,
            "supports_bulk_history": False,
            "supports_incremental_refresh": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "graceful_degradation_policy": "defer_and_report",
            "free_tier_notes": "Free tier may throttle near quota boundaries.",
            "fallback_priority": 10,
            "is_active": True,
            "notes": "Forecast snapshot API provider.",
            "default_timeout_seconds": 30,
            "quota_window_seconds": 60,
            "quota_reset_hint": "Free tier resets on minute boundaries.",
            "expected_error_modes": ["http_401", "http_403", "http_429", "request_exception"],
            "user_agent_required": True,
            "contact_requirement": "Declared contactable user agent is required.",
            "terms_url": "https://finnhub.io/terms-of-use",
            "supports_freshness_fetch": True,
            "env_key_name": "FINNHUB_API_KEY",
            "retrieval_policy": "symbol_snapshot_api",
            "preferred_resolution_order": 1,
            "direct_uri_allowed": False,
            "capability_level": "partial_content",
        },
        {
            "provider_id": "fmp",
            "domain": "earnings",
            "content_domain": "forecast",
            "provider_type": "partner_api",
            "display_name": "FinancialModelingPrep",
            "base_url": "https://financialmodelingprep.com/api/v3",
            "auth_type": "api_key_query",
            "auth_env_var": "FMP_API_KEY",
            "rate_limit_policy": "per_minute",
            "soft_limit": 0,
            "hard_limit": 0,
            "burst_limit": 0,
            "retry_budget": 2,
            "backoff_policy": "exponential",
            "direct_resolution_allowed": True,
            "browse_discovery_allowed": True,
            "supports_bulk_history": False,
            "supports_incremental_refresh": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "graceful_degradation_policy": "defer_and_report",
            "free_tier_notes": "Rate limits vary by plan.",
            "fallback_priority": 20,
            "is_active": True,
            "notes": "Forecast snapshot API provider.",
            "default_timeout_seconds": 30,
            "quota_window_seconds": 60,
            "quota_reset_hint": "Plan-dependent quota windows.",
            "expected_error_modes": ["http_401", "http_403", "http_429", "request_exception"],
            "user_agent_required": True,
            "contact_requirement": "Declared contactable user agent is required.",
            "terms_url": "https://site.financialmodelingprep.com/terms-of-service",
            "supports_freshness_fetch": True,
            "env_key_name": "FMP_API_KEY",
            "retrieval_policy": "symbol_snapshot_api",
            "preferred_resolution_order": 2,
            "direct_uri_allowed": False,
            "capability_level": "partial_content",
        },
        {
            "provider_id": "kaggle_motley_fool",
            "domain": "earnings",
            "content_domain": "transcript",
            "provider_type": "bulk_dataset",
            "display_name": "Kaggle Motley Fool Bulk",
            "base_url": None,
            "auth_type": "none",
            "auth_env_var": "",
            "rate_limit_policy": "custom",
            "soft_limit": 0,
            "hard_limit": 0,
            "burst_limit": 0,
            "retry_budget": 0,
            "backoff_policy": "none",
            "direct_resolution_allowed": False,
            "browse_discovery_allowed": False,
            "supports_bulk_history": True,
            "supports_incremental_refresh": False,
            "supports_direct_resolution": False,
            "supports_public_resolve_if_missing": False,
            "supports_admin_refresh_if_stale": False,
            "graceful_degradation_policy": "return_local_metadata_only",
            "free_tier_notes": "Local operator-supplied bulk dataset import.",
            "fallback_priority": None,
            "is_active": True,
            "notes": "Bulk bootstrap adapter.",
            "default_timeout_seconds": None,
            "quota_window_seconds": None,
            "quota_reset_hint": "",
            "expected_error_modes": [],
            "user_agent_required": False,
            "contact_requirement": "",
            "terms_url": "",
            "supports_freshness_fetch": False,
            "env_key_name": "",
            "retrieval_policy": "local_bulk_import",
            "preferred_resolution_order": 999,
            "direct_uri_allowed": False,
            "capability_level": "bulk_history",
        },
        {
            "provider_id": "local_tabular",
            "domain": "earnings",
            "content_domain": "transcript",
            "provider_type": "local_dataset",
            "display_name": "Local Tabular Transcript Import",
            "base_url": None,
            "auth_type": "none",
            "auth_env_var": "",
            "rate_limit_policy": "custom",
            "soft_limit": 0,
            "hard_limit": 0,
            "burst_limit": 0,
            "retry_budget": 0,
            "backoff_policy": "none",
            "direct_resolution_allowed": False,
            "browse_discovery_allowed": False,
            "supports_bulk_history": True,
            "supports_incremental_refresh": False,
            "supports_direct_resolution": False,
            "supports_public_resolve_if_missing": False,
            "supports_admin_refresh_if_stale": False,
            "graceful_degradation_policy": "return_local_metadata_only",
            "free_tier_notes": "Local CSV/JSONL/parquet import adapter.",
            "fallback_priority": None,
            "is_active": True,
            "notes": "Neutral local tabular import.",
            "default_timeout_seconds": None,
            "quota_window_seconds": None,
            "quota_reset_hint": "",
            "expected_error_modes": [],
            "user_agent_required": False,
            "contact_requirement": "",
            "terms_url": "",
            "supports_freshness_fetch": False,
            "env_key_name": "",
            "retrieval_policy": "local_bulk_import",
            "preferred_resolution_order": 999,
            "direct_uri_allowed": False,
            "capability_level": "bulk_history",
        },
        {
            "provider_id": "motley_fool_pickle",
            "domain": "earnings",
            "content_domain": "transcript",
            "provider_type": "local_dataset",
            "display_name": "Local Motley Fool Pickle Import",
            "base_url": None,
            "auth_type": "none",
            "auth_env_var": "",
            "rate_limit_policy": "custom",
            "soft_limit": 0,
            "hard_limit": 0,
            "burst_limit": 0,
            "retry_budget": 0,
            "backoff_policy": "none",
            "direct_resolution_allowed": False,
            "browse_discovery_allowed": False,
            "supports_bulk_history": True,
            "supports_incremental_refresh": False,
            "supports_direct_resolution": False,
            "supports_public_resolve_if_missing": False,
            "supports_admin_refresh_if_stale": False,
            "graceful_degradation_policy": "return_local_metadata_only",
            "free_tier_notes": "Explicit local pickle import path.",
            "fallback_priority": None,
            "is_active": True,
            "notes": "Explicit local pickle adapter.",
            "default_timeout_seconds": None,
            "quota_window_seconds": None,
            "quota_reset_hint": "",
            "expected_error_modes": [],
            "user_agent_required": False,
            "contact_requirement": "",
            "terms_url": "",
            "supports_freshness_fetch": False,
            "env_key_name": "",
            "retrieval_policy": "local_bulk_import",
            "preferred_resolution_order": 999,
            "direct_uri_allowed": False,
            "capability_level": "bulk_history",
        },
    ]
    return _normalize_registry(pd.DataFrame(rows))


def materialize_provider_registry(config: AppConfig) -> Path:
    config.ensure_runtime_dirs()
    registry = default_provider_registry()
    overrides = _load_local_overrides(config)
    if not overrides.empty:
        merged = registry.set_index("provider_id")
        for row in overrides.to_dict(orient="records"):
            provider_id = str(row.get("provider_id") or "").strip()
            if not provider_id:
                continue
            provider_id = provider_id.lower()
            current = merged.loc[provider_id].to_dict() if provider_id in merged.index else {}
            current.update({key: value for key, value in row.items() if key in PROVIDER_REGISTRY_COLUMNS})
            merged.loc[provider_id] = pd.Series(current)
        registry = _normalize_registry(merged.reset_index())

    path = normalized_path(config, "provider_registry")
    registry.to_parquet(path, index=False)
    return path


def load_provider_registry(config: AppConfig) -> pd.DataFrame:
    path = normalized_path(config, "provider_registry")
    if not path.exists():
        return default_provider_registry()
    return _normalize_registry(pd.read_parquet(path))


def provider_resolution_candidates(
    registry: pd.DataFrame,
    *,
    content_domain: str,
    provider_requested: str | None = None,
) -> list[dict]:
    out = _normalize_registry(registry)
    out = out[out["content_domain"] == content_domain]
    out = out[out["is_active"]]
    if provider_requested:
        out = out[out["provider_id"] == provider_requested.lower()]
    out = out.sort_values(
        ["fallback_priority", "preferred_resolution_order", "provider_id"],
        ascending=[True, True, True],
        na_position="last",
    )
    return out.to_dict(orient="records")


def _normalize_registry(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in PROVIDER_REGISTRY_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out[PROVIDER_REGISTRY_COLUMNS]

    for field in _BOOL_FIELDS:
        out[field] = out[field].map(_coerce_bool)

    for field in _INT_FIELDS:
        out[field] = pd.to_numeric(out[field], errors="coerce")

    out["provider_id"] = out["provider_id"].astype("string").str.strip().str.lower()
    out["domain"] = out["domain"].astype("string").fillna("earnings").str.strip().str.lower()
    out["content_domain"] = out["content_domain"].astype("string").str.strip().str.lower()
    out["provider_type"] = out["provider_type"].astype("string").str.strip().str.lower()
    out["display_name"] = out["display_name"].astype("string").fillna("").str.strip()
    out["base_url"] = out["base_url"].astype("string").fillna("").str.strip()
    out["auth_type"] = out["auth_type"].astype("string").fillna("").str.strip().str.lower()
    out["auth_env_var"] = out["auth_env_var"].astype("string").fillna("").str.strip()
    out["rate_limit_policy"] = out["rate_limit_policy"].astype("string").fillna("").str.strip().str.lower()
    out["backoff_policy"] = out["backoff_policy"].astype("string").fillna("").str.strip().str.lower()
    out["graceful_degradation_policy"] = out["graceful_degradation_policy"].astype("string").fillna("").str.strip().str.lower()
    out["free_tier_notes"] = out["free_tier_notes"].astype("string").fillna("").str.strip()
    out["notes"] = out["notes"].astype("string").fillna("").str.strip()
    out["quota_reset_hint"] = out["quota_reset_hint"].astype("string").fillna("").str.strip()
    out["contact_requirement"] = out["contact_requirement"].astype("string").fillna("").str.strip()
    out["terms_url"] = out["terms_url"].astype("string").fillna("").str.strip()
    out["expected_error_modes"] = out["expected_error_modes"].map(_normalize_expected_error_modes)

    # Backfill additive legacy compatibility fields from canonical values.
    out["supports_freshness_fetch"] = out["supports_freshness_fetch"].where(
        out["supports_freshness_fetch"].notna(),
        out["supports_incremental_refresh"],
    ).map(_coerce_bool)
    out["env_key_name"] = out["env_key_name"].astype("string").fillna("").str.strip()
    out["env_key_name"] = out["env_key_name"].where(out["env_key_name"].astype(str) != "", out["auth_env_var"])
    out["retrieval_policy"] = out["retrieval_policy"].astype("string").fillna("").str.strip()
    out["preferred_resolution_order"] = out["preferred_resolution_order"].where(
        out["preferred_resolution_order"].notna(),
        out["fallback_priority"],
    )
    out["direct_uri_allowed"] = out["direct_uri_allowed"].where(
        out["direct_uri_allowed"].notna(),
        out["direct_resolution_allowed"],
    ).map(_coerce_bool)
    out["capability_level"] = out["capability_level"].astype("string").fillna("").str.strip().str.lower()

    for field in _INT_FIELDS:
        out[field] = pd.to_numeric(out[field], errors="coerce").fillna(999).astype(int)

    out = out.dropna(subset=["provider_id", "content_domain"])
    out = out[out["provider_id"] != ""]
    out = out[out["content_domain"] != ""]
    out = out.drop_duplicates(subset=["provider_id"], keep="last")
    out = out.sort_values(
        ["content_domain", "fallback_priority", "preferred_resolution_order", "provider_id"],
        ascending=[True, True, True, True],
        na_position="last",
    )
    return out.reset_index(drop=True)


def _load_local_overrides(config: AppConfig) -> pd.DataFrame:
    parquet_path = config.refdata_inputs_root / "provider_registry_overrides.parquet"
    csv_path = config.refdata_inputs_root / "provider_registry_overrides.csv"
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame(columns=["provider_id"])


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"true", "t", "1", "yes", "y"}


def _normalize_expected_error_modes(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    else:
        text = str(value).strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1]
        items = [part.strip() for part in text.split(",")]
    out: list[str] = []
    for item in items:
        normalized = str(item).strip().strip("'").strip('"').lower()
        if normalized:
            out.append(normalized)
    return out
