from __future__ import annotations

from click.testing import CliRunner
import pandas as pd
from fastapi.testclient import TestClient

from py_earnings_calls.api.app import create_app
from py_earnings_calls.cli import main
from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path


def _legacy_roots(config: AppConfig):
    return [
        config.legacy_transcript_raw_root,
        config.legacy_transcript_parsed_root,
        config.legacy_forecast_raw_root,
    ]


def _assert_legacy_absent(config: AppConfig, *, stage: str) -> None:
    existing_roots = [str(path) for path in _legacy_roots(config) if path.exists()]
    legacy_files: list[str] = []
    for root in _legacy_roots(config):
        if not root.exists():
            continue
        for item in root.rglob("*"):
            if item.is_file():
                legacy_files.append(str(item))
    if existing_roots or legacy_files:
        details = {
            "stage": stage,
            "existing_roots": existing_roots,
            "legacy_files": legacy_files,
        }
        raise AssertionError(f"Legacy shallow roots were recreated or touched: {details}")


def test_operator_surfaces_do_not_recreate_legacy_roots(tmp_path, monkeypatch):
    config = AppConfig.from_project_root(tmp_path)
    _assert_legacy_absent(config, stage="initial")

    # Stage 1: runtime init.
    config.ensure_runtime_dirs()
    _assert_legacy_absent(config, stage="ensure_runtime_dirs")

    # Seed minimal normalized authority for realistic lookup/API reads.
    pd.DataFrame(
        [
            {
                "call_id": "c1",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-27T10:00:00",
                "title": "Apple Call",
                "transcript_path": "/tmp/missing.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)
    pd.DataFrame(
        [
            {
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-27",
                "snapshot_id": "snap-1",
                "fiscal_year": 2026,
                "fiscal_period": "Q2",
                "metric_name": "eps",
                "stat_name": "estimate",
                "value": 1.23,
            }
        ]
    ).to_parquet(normalized_path(config, "forecast_points"), index=False)
    pd.DataFrame(
        [
            {
                "snapshot_id": "snap-1",
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-27",
            }
        ]
    ).to_parquet(normalized_path(config, "forecast_snapshots"), index=False)
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)

    # Stage 2: real operator surface via CLI.
    monkeypatch.setenv("PY_EARNINGS_CALLS_PROJECT_ROOT", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(main, ["lookup", "refresh"])
    assert result.exit_code == 0, result.output
    _assert_legacy_absent(config, stage="cli_lookup_refresh")

    # Stage 3: app creation/startup path and local-first reads.
    app = create_app(config)
    _assert_legacy_absent(config, stage="api_create_app")
    client = TestClient(app)

    transcript_response = client.get("/transcripts", params={"ticker": "AAPL", "limit": 1})
    assert transcript_response.status_code == 200
    by_cik_response = client.get("/forecasts/by-cik/0000320193")
    assert by_cik_response.status_code == 200
    payload = by_cik_response.json()
    assert payload["total"] >= 1
    _assert_legacy_absent(config, stage="api_local_reads")
