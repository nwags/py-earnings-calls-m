from datetime import date

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.models import ForecastPoint, ForecastSnapshot
from py_earnings_calls.pipelines import forecast_refresh
from py_earnings_calls.storage.paths import normalized_path


class FakeAdapter:
    def __init__(self, provider_name: str, by_symbol: dict[str, tuple[list[ForecastSnapshot], list[ForecastPoint]] | Exception], calls: list[tuple[str, str]]) -> None:
        self.provider_name = provider_name
        self.by_symbol = by_symbol
        self.calls = calls

    def fetch_snapshots(self, symbols, as_of_date):
        symbol = list(symbols)[0]
        self.calls.append((self.provider_name, symbol))
        response = self.by_symbol[symbol]
        if isinstance(response, Exception):
            raise response
        return response


def _snapshot(provider: str, symbol: str, as_of: date) -> ForecastSnapshot:
    return ForecastSnapshot(
        snapshot_id=f"{provider}-{symbol}-{as_of.isoformat()}",
        provider=provider,
        symbol=symbol,
        as_of_date=as_of,
        source_url=f"https://example.com/{provider}/{symbol}",
        raw_payload={"provider": provider, "symbol": symbol},
    )


def _point(snapshot_id: str, provider: str, symbol: str, as_of: date, metric: str, value: float) -> ForecastPoint:
    return ForecastPoint(
        snapshot_id=snapshot_id,
        provider=provider,
        symbol=symbol,
        as_of_date=as_of,
        fiscal_year=2026,
        fiscal_period="Q1",
        metric_name=metric,
        stat_name="estimate",
        value=value,
    )


def test_forecast_refresh_single_provider_mode(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)

    s = _snapshot("finnhub", "AAPL", as_of)
    p = _point(s.snapshot_id, "finnhub", "AAPL", as_of, "eps", 1.2)
    adapters = {
        "finnhub": FakeAdapter("finnhub", {"AAPL": ([s], [p])}, calls),
    }

    monkeypatch.setattr(forecast_refresh, "_build_adapter", lambda name, cfg, http: adapters[name])
    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider="finnhub",
        provider_mode="single",
    )

    assert result["provider_mode"] == "single"
    assert result["provider_order"] == ["finnhub"]
    assert result["selected_provider_by_symbol"] == {"AAPL": "finnhub"}
    assert result["snapshot_count"] == 1
    assert result["point_count"] == 1
    assert calls == [("finnhub", "AAPL")]
    snapshots_df = pd.read_parquet(normalized_path(config, "forecast_snapshots"))
    points_df = pd.read_parquet(normalized_path(config, "forecast_points"))
    assert snapshots_df.iloc[0]["cik"] == "0000320193"
    assert points_df.iloc[0]["cik"] == "0000320193"
    assert "/provider=finnhub/" in snapshots_df.iloc[0]["raw_payload_path"]


def test_forecast_refresh_cik_degrades_cleanly_when_mapping_missing(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []

    s = _snapshot("finnhub", "AAPL", as_of)
    p = _point(s.snapshot_id, "finnhub", "AAPL", as_of, "eps", 1.2)
    adapters = {
        "finnhub": FakeAdapter("finnhub", {"AAPL": ([s], [p])}, calls),
    }

    monkeypatch.setattr(forecast_refresh, "_build_adapter", lambda name, cfg, http: adapters[name])
    forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider="finnhub",
        provider_mode="single",
    )
    snapshots_df = pd.read_parquet(normalized_path(config, "forecast_snapshots"))
    points_df = pd.read_parquet(normalized_path(config, "forecast_points"))
    assert "cik" in snapshots_df.columns
    assert "cik" in points_df.columns
    assert pd.isna(snapshots_df.iloc[0]["cik"])
    assert pd.isna(points_df.iloc[0]["cik"])


def test_forecast_refresh_fallback_mode_is_deterministic_and_avoids_duplicates(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []

    finnhub_aapl = _snapshot("finnhub", "AAPL", as_of)
    fmp_aapl = _snapshot("fmp", "AAPL", as_of)
    finnhub_msft = _snapshot("finnhub", "MSFT", as_of)
    fmp_msft = _snapshot("fmp", "MSFT", as_of)

    adapters = {
        "finnhub": FakeAdapter(
            "finnhub",
            {
                "AAPL": ([finnhub_aapl], [_point(finnhub_aapl.snapshot_id, "finnhub", "AAPL", as_of, "eps", 1.0)]),
                "MSFT": ([finnhub_msft], []),  # no usable data, force fallback
            },
            calls,
        ),
        "fmp": FakeAdapter(
            "fmp",
            {
                "AAPL": ([fmp_aapl], [_point(fmp_aapl.snapshot_id, "fmp", "AAPL", as_of, "eps", 9.9)]),
                "MSFT": ([fmp_msft], [_point(fmp_msft.snapshot_id, "fmp", "MSFT", as_of, "eps", 2.0)]),
            },
            calls,
        ),
    }

    monkeypatch.setattr(forecast_refresh, "_build_adapter", lambda name, cfg, http: adapters[name])
    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["MSFT", "AAPL"],
        as_of_date=as_of,
        provider_mode="fallback",
        provider_priority=["finnhub", "fmp"],
    )

    assert result["provider_order"] == ["finnhub", "fmp"]
    assert result["selected_provider_by_symbol"] == {"AAPL": "finnhub", "MSFT": "fmp"}
    assert result["snapshot_count"] == 2
    assert result["point_count"] == 2
    # AAPL should not hit FMP because first provider already returned usable data.
    assert ("fmp", "AAPL") not in calls

    points_df = pd.read_parquet(normalized_path(config, "forecast_points"))
    assert set(points_df["provider"].tolist()) == {"finnhub", "fmp"}


def test_forecast_refresh_idempotent_writes(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []
    s = _snapshot("finnhub", "AAPL", as_of)
    p = _point(s.snapshot_id, "finnhub", "AAPL", as_of, "eps", 1.2)
    adapters = {
        "finnhub": FakeAdapter("finnhub", {"AAPL": ([s], [p])}, calls),
    }
    monkeypatch.setattr(forecast_refresh, "_build_adapter", lambda name, cfg, http: adapters[name])

    forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider="finnhub",
        provider_mode="single",
    )
    forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider="finnhub",
        provider_mode="single",
    )

    snapshots_df = pd.read_parquet(normalized_path(config, "forecast_snapshots"))
    points_df = pd.read_parquet(normalized_path(config, "forecast_points"))
    assert len(snapshots_df.index) == 1
    assert len(points_df.index) == 1


def test_forecast_refresh_total_failure_writes_no_bogus_rows(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []

    adapters = {
        "finnhub": FakeAdapter("finnhub", {"AAPL": RuntimeError("boom")}, calls),
        "fmp": FakeAdapter("fmp", {"AAPL": RuntimeError("boom")}, calls),
    }
    monkeypatch.setattr(forecast_refresh, "_build_adapter", lambda name, cfg, http: adapters[name])
    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider_mode="fallback",
        provider_priority=["finnhub", "fmp"],
    )

    assert result["snapshot_count"] == 0
    assert result["point_count"] == 0
    assert result["no_data_symbols"] == ["AAPL"]
    assert result["provider_failures"]["finnhub"] == 1
    assert result["provider_failures"]["fmp"] == 1
    assert not normalized_path(config, "forecast_snapshots").exists()
    assert not normalized_path(config, "forecast_points").exists()


def test_forecast_refresh_single_provider_unavailable_is_bounded(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)

    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider="fmp",
        provider_mode="single",
    )

    assert result["provider_order"] == ["fmp"]
    assert result["provider_unavailable"] == {
        "fmp": {
            "reason_code": "missing_api_key",
            "message": "Provider unavailable: missing required API key.",
            "missing_key": "FMP_API_KEY",
        }
    }
    assert result["provider_attempts"]["fmp"] == 0
    assert result["snapshot_count"] == 0
    assert result["point_count"] == 0
    assert result["no_data_symbols"] == ["AAPL"]


def test_forecast_refresh_fallback_skips_unavailable_provider(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)
    calls: list[tuple[str, str]] = []

    s = _snapshot("finnhub", "AAPL", as_of)
    p = _point(s.snapshot_id, "finnhub", "AAPL", as_of, "eps", 1.3)

    def _fake_build(name, cfg, http):
        if name == "finnhub":
            return FakeAdapter("finnhub", {"AAPL": ([s], [p])}, calls)
        if name == "fmp":
            raise ValueError("FMP_API_KEY is required for the FMP adapter.")
        raise AssertionError("unexpected provider")

    monkeypatch.setattr(forecast_refresh, "_build_adapter", _fake_build)
    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider_mode="fallback",
        provider_priority=["fmp", "finnhub"],
    )

    assert result["provider_order"] == ["fmp", "finnhub"]
    assert result["provider_unavailable"]["fmp"]["reason_code"] == "missing_api_key"
    assert result["provider_attempts"]["fmp"] == 0
    assert result["selected_provider_by_symbol"] == {"AAPL": "finnhub"}
    assert result["snapshot_count"] == 1
    assert result["point_count"] == 1
    assert calls == [("finnhub", "AAPL")]


def test_forecast_refresh_fallback_all_providers_unavailable(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    as_of = date(2026, 3, 26)

    def _fake_build(name, cfg, http):
        if name == "finnhub":
            raise ValueError("FINNHUB_API_KEY is required for the Finnhub adapter.")
        if name == "fmp":
            raise ValueError("FMP_API_KEY is required for the FMP adapter.")
        raise AssertionError("unexpected provider")

    monkeypatch.setattr(forecast_refresh, "_build_adapter", _fake_build)
    result = forecast_refresh.run_forecast_refresh(
        config,
        symbols=["AAPL"],
        as_of_date=as_of,
        provider_mode="fallback",
        provider_priority=["finnhub", "fmp"],
    )

    assert result["provider_unavailable"] == {
        "finnhub": {
            "reason_code": "missing_api_key",
            "message": "Provider unavailable: missing required API key.",
            "missing_key": "FINNHUB_API_KEY",
        },
        "fmp": {
            "reason_code": "missing_api_key",
            "message": "Provider unavailable: missing required API key.",
            "missing_key": "FMP_API_KEY",
        },
    }
    assert result["provider_attempts"] == {"finnhub": 0, "fmp": 0}
    assert result["snapshot_count"] == 0
    assert result["point_count"] == 0
    assert result["no_data_symbols"] == ["AAPL"]
    assert not normalized_path(config, "forecast_snapshots").exists()
    assert not normalized_path(config, "forecast_points").exists()
