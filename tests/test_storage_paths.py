from datetime import date

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import (
    forecast_raw_snapshot_path,
    transcript_html_path,
    transcript_text_path,
)


def test_transcript_paths_are_canonical(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    call_date = date(2026, 3, 26)

    html_path = transcript_html_path(
        config,
        provider="motley_fool",
        symbol="AAPL",
        call_date=call_date,
        call_id="abc123",
        storage_cik="0000320193",
    )
    text_path = transcript_text_path(
        config,
        provider="motley_fool",
        symbol="AAPL",
        call_date=call_date,
        call_id="abc123",
        storage_cik="0000320193",
    )

    assert "/transcripts/data/" in str(html_path)
    assert "cik=0000320193" in str(html_path)
    assert "tr-" in str(text_path)
    assert html_path.suffix == ".html"
    assert text_path.suffix == ".txt"


def test_forecast_snapshot_path(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    path = forecast_raw_snapshot_path(config, provider="finnhub", symbol="MSFT", as_of_date=date(2026, 3, 26))
    assert "provider=finnhub" in str(path)
    assert "symbol=MSFT" in str(path)
    assert "as_of_date=2026-03-26" in str(path)
    assert path.name == "raw.json"
