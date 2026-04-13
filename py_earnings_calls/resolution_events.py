from __future__ import annotations

from pathlib import Path

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path

RESOLUTION_EVENT_COLUMNS = [
    "event_at",
    "domain",
    "content_domain",
    "canonical_key",
    "resolution_mode",
    "provider_requested",
    "provider_used",
    "method_used",
    "served_from",
    "remote_attempted",
    "success",
    "reason_code",
    "message",
    "persisted_locally",
    "selection_outcome",
    "rate_limited",
    "retry_count",
    "deferred",
    "deferred_until",
    "provider_skip_reasons",
]


def append_resolution_event(config: AppConfig, event: dict[str, object]) -> Path:
    path = normalized_path(config, "resolution_events")
    incoming = _normalize_events(pd.DataFrame([event]))
    if path.exists():
        existing = _normalize_events(pd.read_parquet(path))
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming
    combined = combined[RESOLUTION_EVENT_COLUMNS]
    combined.to_parquet(path, index=False)
    return path


def read_resolution_events(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=RESOLUTION_EVENT_COLUMNS)
    return _normalize_events(pd.read_parquet(path))


def _normalize_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in RESOLUTION_EVENT_COLUMNS:
        if column not in out.columns:
            out[column] = None
    out = out[RESOLUTION_EVENT_COLUMNS]
    out["domain"] = out["domain"].astype("string").fillna("earnings").str.strip().str.lower()
    out["content_domain"] = out["content_domain"].astype("string").fillna("").str.strip().str.lower()
    out["canonical_key"] = out["canonical_key"].astype("string").fillna("").str.strip()
    out["resolution_mode"] = out["resolution_mode"].astype("string").fillna("").str.strip().str.lower()
    out["provider_requested"] = out["provider_requested"].astype("string").fillna("").str.strip().str.lower()
    out["provider_used"] = out["provider_used"].astype("string").fillna("").str.strip().str.lower()
    out["method_used"] = out["method_used"].astype("string").fillna("").str.strip().str.lower()
    out["served_from"] = out["served_from"].astype("string").fillna("").str.strip().str.lower()
    out["remote_attempted"] = out["remote_attempted"].map(_coerce_bool)
    out["reason_code"] = out["reason_code"].astype("string").fillna("").str.strip().str.upper()
    out["message"] = out["message"].astype("string").fillna("").str.strip()
    out["success"] = out["success"].map(_coerce_bool)
    out["persisted_locally"] = out["persisted_locally"].map(_coerce_bool)
    out["selection_outcome"] = out["selection_outcome"].astype("string").fillna("").str.strip().str.lower()
    out["rate_limited"] = out["rate_limited"].map(_coerce_bool)
    out["retry_count"] = pd.to_numeric(out["retry_count"], errors="coerce").fillna(0).astype(int)
    out["deferred"] = out["deferred"].map(_coerce_bool)
    out["deferred_until"] = out["deferred_until"].astype("string").fillna("").str.strip()
    out["provider_skip_reasons"] = out["provider_skip_reasons"].astype("string").fillna("").str.strip()
    return out.reset_index(drop=True)


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y"}
