from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.models import ForecastPoint, ForecastSnapshot, TranscriptDocument
from py_earnings_calls.provider_registry import materialize_provider_registry
from py_earnings_calls.resolution import ResolutionMode
from py_earnings_calls.resolution_service import ProviderAwareResolutionService
from py_earnings_calls.storage.paths import normalized_path


class _FakeForecastAdapter:
    def __init__(self, snapshots: list[ForecastSnapshot], points: list[ForecastPoint]) -> None:
        self._snapshots = snapshots
        self._points = points

    def fetch_snapshots(self, symbols, as_of_date):
        return self._snapshots, self._points


@dataclass(frozen=True)
class _FakeTranscriptOutcome:
    document: TranscriptDocument | None
    failure: object | None = None


class _FakeTranscriptAdapter:
    def __init__(self, _http_client) -> None:
        pass

    def fetch_document_outcome(self, url: str, symbol: str | None = None):
        doc = TranscriptDocument(
            call_id="unused-during-resolution",
            provider="motley_fool",
            provider_call_id=url,
            symbol=symbol or "AAPL",
            company_name=None,
            call_datetime=datetime(2026, 3, 26, 12, 0, 0),
            fiscal_year=None,
            fiscal_period=None,
            title="Test transcript",
            source_url=url,
            transcript_text="hello transcript",
            raw_html="<html><body>hello transcript</body></html>",
            speaker_count=None,
        )
        return _FakeTranscriptOutcome(document=doc)


def test_resolution_local_only_miss_is_clean_and_non_mutating(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)
    service = ProviderAwareResolutionService(config)

    result = service.resolve_forecast_snapshot_if_missing(
        provider="finnhub",
        symbol="AAPL",
        as_of_date=date(2026, 3, 26),
        resolution_mode=ResolutionMode.LOCAL_ONLY,
    )

    assert result.success is False
    assert result.reason_code == "LOCAL_ONLY_MISS"
    assert not normalized_path(config, "resolution_events").exists()


def test_resolution_policy_denied_records_truthful_event(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)
    registry = pd.read_parquet(normalized_path(config, "provider_registry"))
    registry.loc[registry["provider_id"] == "fmp", "supports_public_resolve_if_missing"] = False
    registry.to_parquet(normalized_path(config, "provider_registry"), index=False)
    service = ProviderAwareResolutionService(config)

    result = service.resolve_forecast_snapshot_if_missing(
        provider="fmp",
        symbol="AAPL",
        as_of_date=date(2026, 3, 26),
        resolution_mode=ResolutionMode.RESOLVE_IF_MISSING,
    )

    events = pd.read_parquet(normalized_path(config, "resolution_events"))
    assert result.success is False
    assert result.reason_code == "POLICY_DENIED"
    assert len(events.index) == 1
    event = events.iloc[0].to_dict()
    assert event["content_domain"] == "forecast"
    assert event["resolution_mode"] == "resolve_if_missing"
    assert event["provider_requested"] == "fmp"
    assert event["success"] is False
    assert event["persisted_locally"] is False


def test_resolution_forecast_resolve_if_missing_persists_and_records_provenance(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)
    pd.DataFrame([{"symbol": "AAPL", "cik": "320193"}]).to_parquet(normalized_path(config, "issuers"), index=False)
    service = ProviderAwareResolutionService(config)
    as_of = date(2026, 3, 26)

    snapshot = ForecastSnapshot(
        snapshot_id="snap1",
        provider="finnhub",
        symbol="AAPL",
        as_of_date=as_of,
        source_url="https://example.com/snapshot",
        raw_payload={"ok": True},
    )
    point = ForecastPoint(
        snapshot_id="snap1",
        provider="finnhub",
        symbol="AAPL",
        as_of_date=as_of,
        fiscal_year=2026,
        fiscal_period="Q1",
        metric_name="eps",
        stat_name="estimate",
        value=1.23,
    )
    monkeypatch.setattr(
        "py_earnings_calls.resolution_service._build_forecast_adapter",
        lambda provider, cfg, http_client: _FakeForecastAdapter([snapshot], [point]),
    )

    result = service.resolve_forecast_snapshot_if_missing(
        provider="finnhub",
        symbol="AAPL",
        as_of_date=as_of,
        resolution_mode=ResolutionMode.RESOLVE_IF_MISSING,
    )

    assert result.success is True
    assert result.persisted_locally is True
    assert normalized_path(config, "forecast_snapshots").exists()
    assert normalized_path(config, "forecast_points").exists()
    snapshots = pd.read_parquet(normalized_path(config, "forecast_snapshots"))
    points = pd.read_parquet(normalized_path(config, "forecast_points"))
    assert snapshots.iloc[-1]["cik"] == "0000320193"
    assert points.iloc[-1]["cik"] == "0000320193"
    events = pd.read_parquet(normalized_path(config, "resolution_events"))
    assert events.iloc[-1]["method_used"] == "api"
    assert events.iloc[-1]["served_from"] == "resolved_remote"
    assert events.iloc[-1]["reason_code"] == "RESOLVED"


def test_resolution_transcript_resolve_if_missing_persists_and_records_provenance(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)

    pd.DataFrame(
        [
            {
                "call_id": "c123",
                "provider": "motley_fool",
                "provider_call_id": "https://example.com/c123",
                "symbol": "AAPL",
                "source_url": "https://example.com/c123",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    monkeypatch.setattr("py_earnings_calls.resolution_service.MotleyFoolTranscriptAdapter", _FakeTranscriptAdapter)
    service = ProviderAwareResolutionService(config)
    result = service.resolve_transcript_if_missing(
        call_id="c123",
        resolution_mode=ResolutionMode.RESOLVE_IF_MISSING,
    )

    assert result.success is True
    assert result.persisted_locally is True
    calls = pd.read_parquet(normalized_path(config, "transcript_calls"))
    assert "transcript_path" in calls.columns
    events = pd.read_parquet(normalized_path(config, "resolution_events"))
    assert events.iloc[-1]["content_domain"] == "transcript"
    assert events.iloc[-1]["method_used"] == "uri"
    assert events.iloc[-1]["served_from"] == "resolved_remote"


def test_refresh_if_stale_operator_mode_requires_admin_flag(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    materialize_provider_registry(config)
    service = ProviderAwareResolutionService(config)

    denied = service.resolve_forecast_snapshot_if_missing(
        provider="finnhub",
        symbol="AAPL",
        as_of_date=date(2026, 3, 26),
        resolution_mode=ResolutionMode.REFRESH_IF_STALE,
        allow_admin=False,
        public_surface=False,
    )
    assert denied.success is False
    assert denied.reason_code == "ADMIN_REQUIRED"
