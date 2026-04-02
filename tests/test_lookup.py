from datetime import date

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import normalize_cik
from py_earnings_calls.lookup import (
    build_symbol_to_cik_map,
    load_lookup_dataframe,
    query_forecasts_by_cik,
    query_transcripts,
    resolve_symbols_for_cik,
)
from py_earnings_calls.pipelines.lookup_refresh import run_lookup_refresh
from py_earnings_calls.storage.paths import normalized_path


def test_query_transcripts(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    path = normalized_path(config, "local_lookup_transcripts")
    pd.DataFrame(
        [
            {"call_id": "c1", "provider": "motley_fool", "symbol": "AAPL", "title": "Call", "transcript_path": "/tmp/x.txt"}
        ]
    ).to_parquet(path, index=False)

    df = load_lookup_dataframe(config, "transcripts")
    out = query_transcripts(df, symbol="AAPL")

    assert len(out.index) == 1
    assert out.iloc[0]["call_id"] == "c1"


def test_normalize_cik():
    assert normalize_cik("320193") == "0000320193"
    assert normalize_cik("0000320193") == "0000320193"
    assert normalize_cik("CIK: 320193") == "0000320193"
    assert normalize_cik("x") is None


def test_query_transcripts_with_cik_intersection_and_date_range(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    transcripts = pd.DataFrame(
        [
            {"call_id": "c3", "provider": "motley_fool", "symbol": "AAPL", "call_datetime": "2024-01-10T12:00:00", "title": "T3"},
            {"call_id": "c2", "provider": "motley_fool", "symbol": "AAPL", "call_datetime": "2024-01-05T12:00:00", "title": "T2"},
            {"call_id": "c1", "provider": "motley_fool", "symbol": "MSFT", "call_datetime": "2024-01-01T12:00:00", "title": "T1"},
        ]
    )
    issuers = pd.DataFrame(
        [
            {"symbol": "AAPL", "cik": "320193"},
            {"symbol": "MSFT", "cik": "789019"},
        ]
    )

    t_path = normalized_path(config, "local_lookup_transcripts")
    i_path = normalized_path(config, "issuers")
    transcripts.to_parquet(t_path, index=False)
    issuers.to_parquet(i_path, index=False)

    df = load_lookup_dataframe(config, "transcripts")
    issuers_df = pd.read_parquet(i_path)

    out = query_transcripts(
        df,
        symbol="AAPL",
        cik="0000320193",
        start=date(2024, 1, 5),
        end=date(2024, 1, 10),
        limit=10,
        offset=0,
        issuers_df=issuers_df,
    )
    assert list(out["call_id"]) == ["c3", "c2"]

    mismatch = query_transcripts(df, symbol="AAPL", cik="0000789019", issuers_df=issuers_df)
    assert mismatch.empty


def test_cik_resolution_degrades_when_issuers_missing():
    df = pd.DataFrame([{"call_id": "c1", "provider": "motley_fool", "symbol": "AAPL", "call_datetime": "2024-01-01T12:00:00"}])

    ticker_only = query_transcripts(df, symbol="AAPL", issuers_df=pd.DataFrame())
    assert len(ticker_only.index) == 1

    cik_only = query_transcripts(df, cik="0000320193", issuers_df=pd.DataFrame())
    assert cik_only.empty


def test_symbol_cik_mapping_helpers():
    issuers_df = pd.DataFrame([{"symbol": "aapl", "cik": "320193"}, {"symbol": "msft", "cik": "789019"}])
    mapping = build_symbol_to_cik_map(issuers_df)
    assert mapping["AAPL"] == "0000320193"
    assert resolve_symbols_for_cik(issuers_df, "320193") == {"AAPL"}


def test_lookup_refresh_enriches_cik_when_issuers_present(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    pd.DataFrame(
        [
            {"call_id": "c1", "provider": "motley_fool", "symbol": "AAPL", "call_datetime": "2024-01-01T10:00:00"},
            {"call_id": "c2", "provider": "motley_fool", "symbol": "MSFT", "call_datetime": "2024-01-01T10:00:00"},
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)

    run_lookup_refresh(config)
    lookup_df = pd.read_parquet(normalized_path(config, "local_lookup_transcripts"))
    cik_by_symbol = {row["symbol"]: row.get("cik") for row in lookup_df.to_dict(orient="records")}
    assert cik_by_symbol["AAPL"] == "0000320193"
    assert pd.isna(cik_by_symbol["MSFT"])
    cik_filtered = query_transcripts(lookup_df, cik="0000320193", issuers_df=pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]))
    assert len(cik_filtered.index) == 1
    assert cik_filtered.iloc[0]["symbol"] == "AAPL"


def test_lookup_refresh_without_issuers_keeps_ticker_queries_usable(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame(
        [{"call_id": "c1", "provider": "motley_fool", "symbol": "AAPL", "call_datetime": "2024-01-01T10:00:00"}]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    run_lookup_refresh(config)
    lookup_df = pd.read_parquet(normalized_path(config, "local_lookup_transcripts"))
    out = query_transcripts(lookup_df, symbol="AAPL")
    assert len(out.index) == 1
    cik_out = query_transcripts(lookup_df, cik="0000320193", issuers_df=pd.DataFrame())
    assert cik_out.empty


def test_lookup_refresh_builds_forecasts_by_cik_artifact_and_query(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)
    pd.DataFrame(
        [
            {
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-27",
                "snapshot_id": "s2",
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.5,
            },
            {
                "provider": "fmp",
                "symbol": "AAPL",
                "as_of_date": "2026-03-26",
                "snapshot_id": "s1",
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.4,
            },
        ]
    ).to_parquet(normalized_path(config, "forecast_points"), index=False)
    pd.DataFrame(
        [
            {"snapshot_id": "s1", "provider": "fmp", "symbol": "AAPL", "as_of_date": "2026-03-26"},
            {"snapshot_id": "s2", "provider": "finnhub", "symbol": "AAPL", "as_of_date": "2026-03-27"},
        ]
    ).to_parquet(normalized_path(config, "forecast_snapshots"), index=False)

    run_lookup_refresh(config)
    by_cik = pd.read_parquet(normalized_path(config, "local_lookup_forecasts_by_cik"))
    assert set(["cik", "provider", "symbol", "as_of_date", "snapshot_id", "metric_name", "stat_name", "value"]).issubset(
        set(by_cik.columns)
    )

    queried = query_forecasts_by_cik(by_cik, cik="0000320193")
    assert len(queried.index) == 2
    assert queried.iloc[0]["as_of_date"] == "2026-03-27"
    assert queried.iloc[0]["provider"] == "finnhub"
