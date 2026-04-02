from __future__ import annotations

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.provider_registry import (
    PROVIDER_REGISTRY_COLUMNS,
    load_provider_registry,
    materialize_provider_registry,
    provider_resolution_candidates,
)
from py_earnings_calls.storage.paths import normalized_path


def test_provider_registry_materializes_deterministically(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    path = materialize_provider_registry(config)
    df = pd.read_parquet(path)

    assert list(df.columns) == PROVIDER_REGISTRY_COLUMNS
    assert set(df["provider_id"].tolist()) >= {"motley_fool", "finnhub", "fmp"}
    assert set(df["content_domain"].tolist()) >= {"transcript", "forecast"}


def test_provider_registry_local_override(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    overrides = pd.DataFrame(
        [
            {
                "provider_id": "fmp",
                "supports_public_resolve_if_missing": False,
                "notes": "disabled for test",
            }
        ]
    )
    overrides.to_parquet(config.refdata_inputs_root / "provider_registry_overrides.parquet", index=False)

    materialize_provider_registry(config)
    registry = load_provider_registry(config)
    fmp = registry[registry["provider_id"] == "fmp"].iloc[0].to_dict()
    assert fmp["supports_public_resolve_if_missing"] is False
    assert "disabled for test" in str(fmp["notes"])


def test_provider_resolution_candidates_filter_by_domain_and_provider(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)
    registry = load_provider_registry(config)

    forecast_candidates = provider_resolution_candidates(registry, content_domain="forecast")
    assert forecast_candidates[0]["provider_id"] == "finnhub"
    assert all(item["content_domain"] == "forecast" for item in forecast_candidates)

    transcript_candidate = provider_resolution_candidates(
        registry,
        content_domain="transcript",
        provider_requested="motley_fool",
    )
    assert [item["provider_id"] for item in transcript_candidate] == ["motley_fool"]
    assert normalized_path(config, "provider_registry").exists()
