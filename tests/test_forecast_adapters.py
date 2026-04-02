from dataclasses import replace
from datetime import date

from py_earnings_calls.adapters.forecasts_finnhub import FinnhubForecastAdapter
from py_earnings_calls.adapters.forecasts_fmp import FmpForecastAdapter
from py_earnings_calls.config import AppConfig


class FakeHttpClient:
    def __init__(self, payload) -> None:
        self._payload = payload

    def request_json(self, url: str, *, params=None, max_attempts: int = 3):
        return self._payload


def test_finnhub_adapter_handles_malformed_payload_without_points(tmp_path):
    config = replace(AppConfig.from_project_root(tmp_path), finnhub_api_key="test-key")
    adapter = FinnhubForecastAdapter(config, FakeHttpClient({"earningsCalendar": "invalid"}))

    snapshots, points = adapter.fetch_snapshots(["AAPL"], date(2026, 3, 26))

    assert len(snapshots) == 1
    assert snapshots[0].symbol == "AAPL"
    assert points == []


def test_finnhub_adapter_handles_partial_rows(tmp_path):
    config = replace(AppConfig.from_project_root(tmp_path), finnhub_api_key="test-key")
    payload = {
        "earningsCalendar": [
            {"fiscalYear": "2026", "quarter": "Q1", "epsEstimate": "", "revenueEstimate": "1000.5"},
            {"fiscalYear": "2026", "quarter": "Q2", "epsEstimate": "1.25", "revenueEstimate": None},
        ]
    }
    adapter = FinnhubForecastAdapter(config, FakeHttpClient(payload))

    snapshots, points = adapter.fetch_snapshots(["AAPL"], date(2026, 3, 26))

    assert len(snapshots) == 1
    assert len(points) == 2
    assert {point.metric_name for point in points} == {"eps", "revenue"}


def test_fmp_adapter_handles_malformed_payload_without_points(tmp_path):
    config = replace(AppConfig.from_project_root(tmp_path), fmp_api_key="test-key")
    adapter = FmpForecastAdapter(config, FakeHttpClient({"not": "a list"}))

    snapshots, points = adapter.fetch_snapshots(["MSFT"], date(2026, 3, 26))

    assert len(snapshots) == 1
    assert snapshots[0].symbol == "MSFT"
    assert points == []


def test_fmp_adapter_handles_partial_rows(tmp_path):
    config = replace(AppConfig.from_project_root(tmp_path), fmp_api_key="test-key")
    payload = [
        {"year": "2026", "period": "Q1", "estimatedEpsAvg": None, "estimatedRevenueAvg": "5500", "numberAnalystEstimatedRevenue": "12"},
        {"year": "2026", "period": "Q2", "estimatedEpsAvg": "2.15", "estimatedRevenueAvg": "", "numberAnalystEstimatedEps": "15"},
    ]
    adapter = FmpForecastAdapter(config, FakeHttpClient(payload))

    snapshots, points = adapter.fetch_snapshots(["MSFT"], date(2026, 3, 26))

    assert len(snapshots) == 1
    assert len(points) == 2
    assert {point.metric_name for point in points} == {"eps", "revenue"}
