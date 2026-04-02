from __future__ import annotations

import json

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.pipelines.refdata_refresh import run_refdata_refresh
from py_earnings_calls.refdata.normalize import normalize_cik, normalize_ticker
from py_earnings_calls.refdata.schema import ISSUERS_COLUMNS
from py_earnings_calls.storage.paths import normalized_path


def test_refdata_normalizers():
    assert normalize_ticker(" aapl ") == "AAPL"
    assert normalize_ticker("") is None
    assert normalize_cik("CIK: 320193") == "0000320193"
    assert normalize_cik("1234567890123") == "4567890123"
    assert normalize_cik("x") is None


def test_refdata_refresh_writes_empty_schema_when_no_inputs(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    result = run_refdata_refresh(config)
    path = normalized_path(config, "issuers")
    provider_registry_path = normalized_path(config, "provider_registry")
    df = pd.read_parquet(path)
    provider_registry = pd.read_parquet(provider_registry_path)

    assert result["issuer_count"] == 0
    assert result["issuer_input_mode"] == "sec_local_inputs"
    assert result["issuer_source_resolution"]["no_usable_raw_sources"] is True
    assert result["issuer_source_resolution"]["used_sec_sources"] is False
    assert result["issuer_source_resolution"]["used_inputs_overrides"] is False
    assert result["artifact_count"] == 2
    assert str(provider_registry_path) in result["artifact_paths"]
    assert list(df.columns) == ISSUERS_COLUMNS
    assert df.empty
    assert not provider_registry.empty
    assert "provider_id" in provider_registry.columns


def test_refdata_refresh_uses_sec_sources_by_default(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    sec_sources_root = config.sec_sources_root
    sec_sources_root.mkdir(parents=True, exist_ok=True)

    (sec_sources_root / "company_tickers_exchange.json").write_text(
        json.dumps(
            {
                "fields": ["cik", "name", "ticker", "exchange"],
                "data": [
                    [320193, "Apple Inc Official", "aapl", "NASDAQ"],
                ],
            }
        ),
        encoding="utf-8",
    )
    (sec_sources_root / "ticker.txt").write_text("AAPL 320193\nMSFT 789019\n", encoding="utf-8")
    (sec_sources_root / "cik-lookup-data.txt").write_text("MICROSOFT CORP:0000789019:\n", encoding="utf-8")

    result = run_refdata_refresh(config)
    df = pd.read_parquet(normalized_path(config, "issuers"))
    rows = df.to_dict(orient="records")

    assert result["issuer_source_resolution"]["used_sec_sources"] is True
    assert result["issuer_source_resolution"]["used_inputs"] is False
    assert result["issuer_source_resolution"]["used_inputs_overrides"] is False
    assert result["issuer_source_resolution"]["no_usable_raw_sources"] is False

    aapl = next(row for row in rows if row["symbol"] == "AAPL")
    msft = next(row for row in rows if row["symbol"] == "MSFT")
    assert aapl["cik"] == "0000320193"
    assert aapl["company_name"] == "Apple Inc Official"
    assert aapl["primary_source"] == "company_tickers_exchange.json"
    assert msft["cik"] == "0000789019"
    assert msft["company_name"] == "MICROSOFT CORP"


def test_refdata_refresh_universe_is_sole_input(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    inputs_root = config.refdata_inputs_root
    inputs_root.mkdir(parents=True, exist_ok=True)

    # Local SEC-style input exists but should be ignored when --universe is provided.
    (inputs_root / "ticker.txt").write_text("AAPL 320193\n", encoding="utf-8")

    universe_path = tmp_path / "universe.csv"
    pd.DataFrame(
        [
            {
                "symbol": "msft",
                "cik": "789019",
                "company_name": "Microsoft Corp",
                "exchange": "NASDAQ",
                "is_active": True,
            }
        ]
    ).to_csv(universe_path, index=False)

    result = run_refdata_refresh(config, universe_path=str(universe_path))
    df = pd.read_parquet(normalized_path(config, "issuers"))

    assert result["issuer_input_mode"] == "universe_only"
    assert result["issuer_source_resolution"]["used_universe_only"] is True
    assert result["issuer_source_resolution"]["used_sec_sources"] is False
    assert result["issuer_source_resolution"]["used_inputs_overrides"] is False
    assert df["symbol"].tolist() == ["MSFT"]
    assert df["cik"].tolist() == ["0000789019"]
    assert df["primary_source"].tolist() == ["universe_file"]


def test_refdata_refresh_inputs_override_sec_sources(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    (config.sec_sources_root / "ticker.txt").write_text("AAPL 320193\n", encoding="utf-8")
    (config.refdata_inputs_root / "ticker.txt").write_text("MSFT 789019\n", encoding="utf-8")

    result = run_refdata_refresh(config)
    df = pd.read_parquet(normalized_path(config, "issuers"))

    assert result["issuer_source_resolution"]["used_inputs"] is True
    assert result["issuer_source_resolution"]["used_inputs_overrides"] is True
    assert "MSFT" in set(df["symbol"].tolist())
    assert "AAPL" not in set(df["symbol"].tolist())
