from datetime import date

import pandas as pd
from fastapi.testclient import TestClient

from py_earnings_calls.api.app import create_app
from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import legacy_transcript_text_path, normalized_path
from py_earnings_calls.storage.writes import write_text


def test_api_health(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    app = create_app(config)
    client = TestClient(app)

    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_api_latest_forecast(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    forecast_path = normalized_path(config, "local_lookup_forecasts")
    pd.DataFrame(
        [
            {
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-26",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.23,
            }
        ]
    ).to_parquet(forecast_path, index=False)

    transcript_path = normalized_path(config, "local_lookup_transcripts")
    pd.DataFrame(columns=["call_id"]).to_parquet(transcript_path, index=False)

    app = create_app(config)
    client = TestClient(app)

    response = client.get("/forecasts/AAPL/latest")
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "AAPL"
    assert payload["as_of_date"] == "2026-03-26"


def test_api_forecast_snapshot_local_only_endpoint(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    pd.DataFrame(
        [
            {
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-26",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.23,
            }
        ]
    ).to_parquet(normalized_path(config, "forecast_points"), index=False)
    pd.DataFrame(columns=["call_id"]).to_parquet(normalized_path(config, "local_lookup_transcripts"), index=False)
    pd.DataFrame(columns=["symbol", "as_of_date"]).to_parquet(normalized_path(config, "local_lookup_forecasts"), index=False)

    app = create_app(config)
    client = TestClient(app)

    response = client.get("/forecasts/snapshots/finnhub/AAPL/2026-03-26", params={"resolution_mode": "local_only"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["provider"] == "finnhub"
    assert payload["symbol"] == "AAPL"
    assert payload["as_of_date"] == "2026-03-26"
    assert payload["served_from"] == "local_hit"


def test_api_transcript_metadata_invalid_resolution_mode_returns_422(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)
    response = client.get("/transcripts/c1", params={"resolution_mode": "invalid_mode"})
    assert response.status_code == 422


def _seed_transcript_lookup(config: AppConfig) -> None:
    transcript_path = normalized_path(config, "local_lookup_transcripts")
    pd.DataFrame(
        [
            {
                "call_id": "c1",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2024-01-01T10:00:00",
                "title": "Apple Jan",
                "transcript_path": "/tmp/c1.txt",
            },
            {
                "call_id": "c2",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2024-02-01T10:00:00",
                "title": "Apple Feb",
                "transcript_path": "/tmp/c2.txt",
            },
            {
                "call_id": "c3",
                "provider": "motley_fool",
                "symbol": "MSFT",
                "call_datetime": "2024-01-15T10:00:00",
                "title": "Microsoft Jan",
                "transcript_path": "/tmp/c3.txt",
            },
        ]
    ).to_parquet(transcript_path, index=False)


def _seed_forecast_lookup(config: AppConfig) -> None:
    pd.DataFrame(columns=["symbol", "as_of_date"]).to_parquet(normalized_path(config, "local_lookup_forecasts"), index=False)
    pd.DataFrame(
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
    ).to_parquet(normalized_path(config, "local_lookup_forecasts_by_cik"), index=False)


def test_api_forecasts_by_cik_reads_derived_lookup_and_is_deterministic(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame(columns=["call_id"]).to_parquet(normalized_path(config, "local_lookup_transcripts"), index=False)
    pd.DataFrame(columns=["symbol", "as_of_date"]).to_parquet(normalized_path(config, "local_lookup_forecasts"), index=False)
    pd.DataFrame(
        [
            {
                "cik": "0000320193",
                "symbol": "AAPL",
                "provider": "fmp",
                "as_of_date": "2026-03-26",
                "snapshot_id": "s1",
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.4,
            },
            {
                "cik": "0000320193",
                "symbol": "AAPL",
                "provider": "finnhub",
                "as_of_date": "2026-03-27",
                "snapshot_id": "s2",
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.5,
            },
        ]
    ).to_parquet(normalized_path(config, "local_lookup_forecasts_by_cik"), index=False)

    app = create_app(config)
    client = TestClient(app)

    response = client.get("/forecasts/by-cik/320193")
    assert response.status_code == 200
    payload = response.json()
    assert payload["cik"] == "0000320193"
    assert payload["total"] == 2
    assert payload["items"][0]["provider"] == "finnhub"
    assert payload["items"][0]["as_of_date"] == "2026-03-27"

    filtered = client.get("/forecasts/by-cik/0000320193", params={"as_of_date": "2026-03-26"})
    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1
    assert filtered.json()["items"][0]["provider"] == "fmp"


def test_api_forecasts_by_cik_invalid_cik_returns_422(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame(columns=["call_id"]).to_parquet(normalized_path(config, "local_lookup_transcripts"), index=False)
    _seed_forecast_lookup(config)
    app = create_app(config)
    client = TestClient(app)
    response = client.get("/forecasts/by-cik/ABCDEFGHIJKL")
    assert response.status_code == 422


def test_api_transcripts_query_by_ticker_and_limit(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)

    response = client.get("/transcripts", params={"ticker": "AAPL", "limit": 1})
    assert response.status_code == 200
    payload = response.json()
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert payload["total"] == 2
    assert len(payload["items"]) == 1
    assert payload["items"][0]["call_id"] == "c2"


def test_api_transcripts_query_ticker_date_range_and_newest_first(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)

    response = client.get("/transcripts", params={"ticker": "AAPL", "start": "2024-01-01", "end": "2024-01-31"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["call_id"] == "c1"


def test_api_transcripts_query_by_cik_with_mapping_and_without_mapping(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)

    mapped = client.get("/transcripts", params={"cik": "0000320193"})
    assert mapped.status_code == 200
    assert mapped.json()["total"] == 2

    mismatch = client.get("/transcripts", params={"ticker": "MSFT", "cik": "0000320193"})
    assert mismatch.status_code == 200
    assert mismatch.json()["total"] == 0

    # Missing/incomplete mapping: ticker works, CIK degrades to empty.
    pd.DataFrame([{"symbol": "MSFT"}]).to_parquet(normalized_path(config, "issuers"), index=False)
    no_mapping_ticker = client.get("/transcripts", params={"ticker": "AAPL"})
    assert no_mapping_ticker.status_code == 200
    assert no_mapping_ticker.json()["total"] == 2
    no_mapping_cik = client.get("/transcripts", params={"cik": "0000320193"})
    assert no_mapping_cik.status_code == 200
    assert no_mapping_cik.json()["total"] == 0


def test_api_transcripts_query_malformed_cik_returns_422(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)
    response = client.get("/transcripts", params={"cik": "ABCDEFGHIJKL"})
    assert response.status_code == 422


def test_api_transcripts_query_malformed_date_returns_422(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    _seed_transcript_lookup(config)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)
    response = client.get("/transcripts", params={"start": "2024-13-99"})
    assert response.status_code == 422


def test_api_transcripts_query_handles_nan_cik_without_500(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    transcript_path = normalized_path(config, "local_lookup_transcripts")
    pd.DataFrame(
        [
            {
                "call_id": "c1",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "cik": float("nan"),
                "call_datetime": "2024-01-01T10:00:00",
                "title": "Apple Jan",
                "transcript_path": "/tmp/c1.txt",
            }
        ]
    ).to_parquet(transcript_path, index=False)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)
    response = client.get("/transcripts", params={"ticker": "AAPL"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["cik"] is None


def test_api_transcript_content_falls_back_to_legacy_path(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    legacy_text = legacy_transcript_text_path(
        config,
        provider="motley_fool",
        symbol="AAPL",
        call_date=date(2024, 1, 1),
        call_id="clegacy",
    )
    write_text(legacy_text, "legacy content")
    pd.DataFrame(
        [
            {
                "call_id": "clegacy",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2024-01-01T10:00:00",
                "transcript_path": "/tmp/missing-new-layout.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "local_lookup_transcripts"), index=False)
    _seed_forecast_lookup(config)

    app = create_app(config)
    client = TestClient(app)
    response = client.get("/transcripts/clegacy/content")
    assert response.status_code == 200
    assert "legacy content" in response.text
