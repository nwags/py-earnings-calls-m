from __future__ import annotations

from py_earnings_calls.config import AppConfig
from py_earnings_calls.provider_registry import materialize_provider_registry
from py_earnings_calls.refdata import ISSUERS_COLUMNS, build_issuers_table, load_issuer_inputs
from py_earnings_calls.storage.paths import normalized_path


def run_refdata_refresh(config: AppConfig, universe_path: str | None = None) -> dict[str, object]:
    config.ensure_runtime_dirs()
    issuer_inputs = load_issuer_inputs(
        sec_sources_root=config.sec_sources_root,
        inputs_root=config.refdata_inputs_root,
        universe_path=universe_path,
    )
    issuers = build_issuers_table(issuer_inputs)
    issuers = issuers.reindex(columns=ISSUERS_COLUMNS)

    path = normalized_path(config, "issuers")
    issuers.to_parquet(path, index=False)
    provider_registry_path = materialize_provider_registry(config)
    return {
        "artifact_count": 2,
        "artifact_paths": [str(path), str(provider_registry_path)],
        "issuer_count": int(len(issuers.index)),
        "issuer_input_mode": issuer_inputs.mode,
        "issuer_source_files": issuer_inputs.source_files,
        "issuer_source_resolution": {
            "used_universe_only": issuer_inputs.mode == "universe_only",
            "used_sec_sources": issuer_inputs.used_sec_sources,
            "used_inputs": issuer_inputs.used_inputs,
            "used_inputs_overrides": issuer_inputs.used_input_overrides,
            "no_usable_raw_sources": issuer_inputs.no_usable_raw_sources,
        },
    }
