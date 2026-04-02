from __future__ import annotations

from pathlib import Path

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path

PROVIDER_REGISTRY_COLUMNS = [
    "provider_id",
    "provider_type",
    "content_domain",
    "supports_bulk_history",
    "supports_freshness_fetch",
    "supports_direct_resolution",
    "supports_public_resolve_if_missing",
    "supports_admin_refresh_if_stale",
    "base_url",
    "auth_type",
    "env_key_name",
    "rate_limit_policy",
    "retrieval_policy",
    "preferred_resolution_order",
    "direct_uri_allowed",
    "capability_level",
    "notes",
    "is_active",
]

_BOOL_FIELDS = {
    "supports_bulk_history",
    "supports_freshness_fetch",
    "supports_direct_resolution",
    "supports_public_resolve_if_missing",
    "supports_admin_refresh_if_stale",
    "direct_uri_allowed",
    "is_active",
}


def default_provider_registry() -> pd.DataFrame:
    rows = [
        {
            "provider_id": "motley_fool",
            "provider_type": "web",
            "content_domain": "transcript",
            "supports_bulk_history": True,
            "supports_freshness_fetch": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "base_url": "https://www.fool.com",
            "auth_type": "none",
            "env_key_name": "",
            "rate_limit_policy": "shared_http_client_default",
            "retrieval_policy": "uri_fetch_parse",
            "preferred_resolution_order": 1,
            "direct_uri_allowed": True,
            "capability_level": "full_content",
            "notes": "Manifest/backfill and targeted direct URI resolution.",
            "is_active": True,
        },
        {
            "provider_id": "finnhub",
            "provider_type": "api",
            "content_domain": "forecast",
            "supports_bulk_history": False,
            "supports_freshness_fetch": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "base_url": "https://finnhub.io",
            "auth_type": "api_key",
            "env_key_name": "FINNHUB_API_KEY",
            "rate_limit_policy": "shared_http_client_default",
            "retrieval_policy": "symbol_snapshot_api",
            "preferred_resolution_order": 1,
            "direct_uri_allowed": False,
            "capability_level": "partial_content",
            "notes": "Forecast snapshot API provider.",
            "is_active": True,
        },
        {
            "provider_id": "fmp",
            "provider_type": "api",
            "content_domain": "forecast",
            "supports_bulk_history": False,
            "supports_freshness_fetch": True,
            "supports_direct_resolution": True,
            "supports_public_resolve_if_missing": True,
            "supports_admin_refresh_if_stale": True,
            "base_url": "https://financialmodelingprep.com",
            "auth_type": "api_key",
            "env_key_name": "FMP_API_KEY",
            "rate_limit_policy": "shared_http_client_default",
            "retrieval_policy": "symbol_snapshot_api",
            "preferred_resolution_order": 2,
            "direct_uri_allowed": False,
            "capability_level": "partial_content",
            "notes": "Forecast snapshot API provider.",
            "is_active": True,
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
    out = out.sort_values(["preferred_resolution_order", "provider_id"], ascending=[True, True], na_position="last")
    return out.to_dict(orient="records")


def _normalize_registry(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in PROVIDER_REGISTRY_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out[PROVIDER_REGISTRY_COLUMNS]

    for field in _BOOL_FIELDS:
        out[field] = out[field].map(_coerce_bool)
    out["provider_id"] = out["provider_id"].astype("string").str.strip().str.lower()
    out["provider_type"] = out["provider_type"].astype("string").str.strip().str.lower()
    out["content_domain"] = out["content_domain"].astype("string").str.strip().str.lower()
    out["base_url"] = out["base_url"].astype("string").fillna("").str.strip()
    out["auth_type"] = out["auth_type"].astype("string").fillna("").str.strip().str.lower()
    out["env_key_name"] = out["env_key_name"].astype("string").fillna("").str.strip()
    out["rate_limit_policy"] = out["rate_limit_policy"].astype("string").fillna("").str.strip()
    out["retrieval_policy"] = out["retrieval_policy"].astype("string").fillna("").str.strip()
    out["capability_level"] = out["capability_level"].astype("string").fillna("").str.strip().str.lower()
    out["notes"] = out["notes"].astype("string").fillna("").str.strip()
    out["preferred_resolution_order"] = pd.to_numeric(out["preferred_resolution_order"], errors="coerce").fillna(999).astype(int)
    out = out.dropna(subset=["provider_id", "content_domain"])
    out = out[out["provider_id"] != ""]
    out = out[out["content_domain"] != ""]
    out = out.drop_duplicates(subset=["provider_id"], keep="last")
    out = out.sort_values(["content_domain", "preferred_resolution_order", "provider_id"], ascending=[True, True, True], na_position="last")
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
