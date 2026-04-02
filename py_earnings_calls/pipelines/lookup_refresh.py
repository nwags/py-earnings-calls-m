from __future__ import annotations

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.storage.paths import normalized_path


def run_lookup_refresh(config: AppConfig) -> dict[str, object]:
    return run_lookup_refresh_scoped(config, include_transcripts=True, include_forecasts=True)


def run_lookup_refresh_scoped(
    config: AppConfig, *, include_transcripts: bool, include_forecasts: bool
) -> dict[str, object]:
    config.ensure_runtime_dirs()

    transcript_calls_path = normalized_path(config, "transcript_calls")
    forecast_points_path = normalized_path(config, "forecast_points")
    forecast_snapshots_path = normalized_path(config, "forecast_snapshots")
    local_lookup_transcripts = normalized_path(config, "local_lookup_transcripts")
    local_lookup_forecasts = normalized_path(config, "local_lookup_forecasts")
    local_lookup_forecasts_by_cik = normalized_path(config, "local_lookup_forecasts_by_cik")

    artifact_paths: list[str] = []
    transcript_row_count = 0
    forecast_row_count = 0

    if include_transcripts:
        if transcript_calls_path.exists():
            transcript_df = pd.read_parquet(transcript_calls_path)
        else:
            transcript_df = pd.DataFrame(columns=["call_id", "provider", "symbol", "call_datetime", "title", "transcript_path"])

        transcript_lookup = transcript_df.copy()
        if not transcript_lookup.empty:
            transcript_lookup["call_date"] = pd.to_datetime(transcript_lookup["call_datetime"], errors="coerce").dt.date.astype("string")
        issuers_df = load_issuers_dataframe(config)
        symbol_to_cik = build_symbol_to_cik_map(issuers_df)
        if "symbol" in transcript_lookup.columns:
            transcript_lookup["cik"] = transcript_lookup["symbol"].astype(str).str.upper().map(symbol_to_cik)
        elif "cik" not in transcript_lookup.columns:
            transcript_lookup["cik"] = pd.Series(dtype="string")
        transcript_lookup.to_parquet(local_lookup_transcripts, index=False)
        transcript_row_count = int(len(transcript_lookup.index))
        artifact_paths.append(str(local_lookup_transcripts))

    if include_forecasts:
        issuers_df = load_issuers_dataframe(config)
        symbol_to_cik = build_symbol_to_cik_map(issuers_df)
        if forecast_points_path.exists():
            forecast_df = pd.read_parquet(forecast_points_path)
        else:
            forecast_df = pd.DataFrame(columns=["provider", "symbol", "as_of_date", "metric_name", "stat_name", "value"])
        forecast_lookup = forecast_df.copy()
        if "symbol" in forecast_lookup.columns:
            mapped = forecast_lookup["symbol"].astype(str).str.upper().map(symbol_to_cik)
            if "cik" in forecast_lookup.columns:
                current = forecast_lookup["cik"]
                forecast_lookup["cik"] = current.where(current.notna() & (current.astype(str).str.strip() != ""), mapped)
            else:
                forecast_lookup["cik"] = mapped
        elif "cik" not in forecast_lookup.columns:
            forecast_lookup["cik"] = pd.Series(dtype="string")
        forecast_lookup = forecast_lookup.sort_values(
            ["symbol", "as_of_date", "provider", "metric_name", "stat_name"],
            ascending=[True, False, True, True, True],
            na_position="last",
        )
        forecast_lookup.to_parquet(local_lookup_forecasts, index=False)
        forecast_row_count = int(len(forecast_lookup.index))
        artifact_paths.append(str(local_lookup_forecasts))

        if forecast_snapshots_path.exists():
            snapshots_df = pd.read_parquet(forecast_snapshots_path)
        else:
            snapshots_df = pd.DataFrame(columns=["snapshot_id", "provider", "symbol", "as_of_date"])
        by_cik_lookup = _build_forecasts_by_cik_lookup(forecast_lookup, snapshots_df=snapshots_df, symbol_to_cik=symbol_to_cik)
        by_cik_lookup.to_parquet(local_lookup_forecasts_by_cik, index=False)
        artifact_paths.append(str(local_lookup_forecasts_by_cik))

    return {
        "artifact_paths": artifact_paths,
        "transcript_row_count": transcript_row_count,
        "forecast_row_count": forecast_row_count,
        "scope": {
            "include_transcripts": include_transcripts,
            "include_forecasts": include_forecasts,
        },
    }


def _build_forecasts_by_cik_lookup(
    forecast_lookup: pd.DataFrame,
    *,
    snapshots_df: pd.DataFrame,
    symbol_to_cik: dict[str, str],
) -> pd.DataFrame:
    if forecast_lookup.empty:
        return pd.DataFrame(
            columns=[
                "cik",
                "symbol",
                "provider",
                "as_of_date",
                "snapshot_id",
                "fiscal_year",
                "fiscal_period",
                "metric_name",
                "stat_name",
                "value",
            ]
        )

    out = forecast_lookup.copy()
    if "cik" in out.columns:
        mapped = out["symbol"].astype(str).str.upper().map(symbol_to_cik)
        out["cik"] = out["cik"].where(out["cik"].notna() & (out["cik"].astype(str).str.strip() != ""), mapped)
    else:
        out["cik"] = out["symbol"].astype(str).str.upper().map(symbol_to_cik)

    snapshot_cols = [name for name in ["snapshot_id", "provider", "symbol", "as_of_date"] if name in snapshots_df.columns]
    if "snapshot_id" not in out.columns and len(snapshot_cols) == 4:
        snapshot_join = snapshots_df[snapshot_cols].drop_duplicates(subset=["provider", "symbol", "as_of_date"], keep="last")
        out = out.merge(snapshot_join, on=["provider", "symbol", "as_of_date"], how="left")
    elif "snapshot_id" not in out.columns:
        out["snapshot_id"] = pd.Series(dtype="string")

    for column in ["fiscal_year", "fiscal_period", "metric_name", "stat_name", "value"]:
        if column not in out.columns:
            out[column] = pd.Series(dtype="object")

    out = out[
        [
            "cik",
            "symbol",
            "provider",
            "as_of_date",
            "snapshot_id",
            "fiscal_year",
            "fiscal_period",
            "metric_name",
            "stat_name",
            "value",
        ]
    ]
    out = out.sort_values(
        ["cik", "as_of_date", "provider", "symbol", "metric_name", "stat_name", "snapshot_id"],
        ascending=[True, False, True, True, True, True, True],
        na_position="last",
    )
    return out.reset_index(drop=True)
