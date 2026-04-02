from pathlib import Path

import pandas as pd

from py_earnings_calls.adapters.transcripts_motley_fool import (
    FAILURE_MISSING_TRANSCRIPT_BODY,
    FAILURE_NON_TRANSCRIPT_PAGE,
    FAILURE_RETRY_EXHAUSTED,
    MotleyFoolTranscriptAdapter,
)
from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpFailure, HttpRequestError
from py_earnings_calls.pipelines.transcript_backfill import _load_backfill_rows, run_transcript_backfill
from py_earnings_calls.pipelines.transcript_manifest import rows_from_urls
from py_earnings_calls.storage.paths import normalized_path


FIXTURES = Path(__file__).parent / "fixtures" / "motley_fool"


class FakeHttpClient:
    def __init__(self, payloads: dict[str, object]) -> None:
        self._payloads = payloads

    def request_text(self, url: str, *, params=None, max_attempts: int = 3) -> str:
        payload = self._payloads[url]
        if isinstance(payload, Exception):
            raise payload
        return str(payload)


def _fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _html_visible_vs_article_conflict() -> str:
    return """
<html>
  <head>
    <title>Example Earnings Call Transcript</title>
  </head>
  <body>
    <time datetime="2026-03-08T00:00:00">March 8, 2026</time>
    <article>
      <h1>Earnings Call Transcript</h1>
      <p>Thursday, March 12, 2026 at 5 p.m. ET</p>
      <p>Operator: Welcome.</p>
      <p>Prepared remarks follow for this quarter.</p>
      <p>Question-and-answer session and closing remarks.</p>
    </article>
  </body>
</html>
"""


def _html_structured_vs_article_conflict() -> str:
    return """
<html>
  <head>
    <title>Example Earnings Call Transcript</title>
  </head>
  <body>
    <time datetime="2026-03-08T00:00:00">March 8, 2026</time>
    <article>
      <h1>Earnings Call Transcript</h1>
      <time datetime="2026-03-12T17:00:00">March 12, 2026 at 5:00 p.m. ET</time>
      <p>Operator: Welcome.</p>
      <p>Prepared remarks follow for this quarter.</p>
      <p>Question-and-answer session and closing remarks.</p>
    </article>
  </body>
</html>
"""


def _html_eastern_time_visible() -> str:
    return """
<html>
  <head><title>Example Earnings Call Transcript</title></head>
  <body>
    <article>
      <h1>Earnings Call Transcript</h1>
      <p>Thursday, March 12, 2026 at 5 p.m. Eastern Time</p>
      <p>Operator: Welcome.</p>
      <p>Prepared remarks follow for this quarter.</p>
      <p>Question-and-answer session and closing remarks.</p>
    </article>
  </body>
</html>
"""


def _html_no_date_signal() -> str:
    return """
<html>
  <head><title>Earnings Call Transcript</title></head>
  <body>
    <article>
      <h1>Earnings Call Transcript</h1>
      <p>Operator: Welcome.</p>
      <p>Prepared remarks follow for this quarter.</p>
      <p>Question-and-answer session and closing remarks.</p>
    </article>
  </body>
</html>
"""


def test_motley_fool_adapter_extracts_valid_transcript_fixture():
    url = "https://example.com/success"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _fixture_text("success_transcript.html")}))

    outcome = adapter.fetch_document_outcome(url)

    assert outcome.failure is None
    assert outcome.document is not None
    assert outcome.document.symbol == "BC"
    assert outcome.document.call_datetime is not None
    assert outcome.document.call_datetime_source == "transcript_structured"
    assert "Operator:" in outcome.document.transcript_text


def test_motley_fool_datetime_precedence_prefers_visible_transcript_datetime():
    url = "https://example.com/visible-vs-article"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _html_visible_vs_article_conflict()}))
    outcome = adapter.fetch_document_outcome(url)
    assert outcome.document is not None
    assert outcome.document.call_datetime is not None
    assert outcome.document.call_datetime.isoformat().startswith("2026-03-12T17:00:00")
    assert outcome.document.call_datetime_source == "transcript_visible"


def test_motley_fool_datetime_precedence_prefers_transcript_structured_before_article():
    url = "https://example.com/structured-vs-article"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _html_structured_vs_article_conflict()}))
    outcome = adapter.fetch_document_outcome(url)
    assert outcome.document is not None
    assert outcome.document.call_datetime is not None
    assert outcome.document.call_datetime.isoformat().startswith("2026-03-12T17:00:00")
    assert outcome.document.call_datetime_source == "transcript_structured"


def test_motley_fool_datetime_parses_eastern_time_visible_signal():
    url = "https://example.com/eastern-visible"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _html_eastern_time_visible()}))
    outcome = adapter.fetch_document_outcome(url)
    assert outcome.document is not None
    assert outcome.document.call_datetime is not None
    assert outcome.document.call_datetime.isoformat().startswith("2026-03-12T17:00:00")
    assert outcome.document.call_datetime_source == "transcript_visible"


def test_motley_fool_datetime_no_signal_falls_back_to_none():
    url = "https://example.com/no-date"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _html_no_date_signal()}))
    outcome = adapter.fetch_document_outcome(url)
    assert outcome.document is not None
    assert outcome.document.call_datetime is None
    assert outcome.document.call_datetime_source == "none"


def test_motley_fool_adapter_rejects_missing_body_fixture():
    url = "https://example.com/missing-body"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _fixture_text("missing_body.html")}))

    outcome = adapter.fetch_document_outcome(url)

    assert outcome.document is None
    assert outcome.failure is not None
    assert outcome.failure.reason == FAILURE_MISSING_TRANSCRIPT_BODY


def test_motley_fool_adapter_rejects_obvious_error_page_fixture():
    url = "https://example.com/error"
    adapter = MotleyFoolTranscriptAdapter(FakeHttpClient({url: _fixture_text("error_page.html")}))

    outcome = adapter.fetch_document_outcome(url)

    assert outcome.document is None
    assert outcome.failure is not None
    assert outcome.failure.reason == FAILURE_NON_TRANSCRIPT_PAGE


def test_transcript_backfill_manifest_flow_mixed_outcomes(tmp_path):
    success_url = "https://example.com/success"
    error_url = "https://example.com/error"
    payloads = {
        success_url: _fixture_text("success_transcript.html"),
        error_url: _fixture_text("error_page.html"),
    }

    manifest = tmp_path / "manifest.csv"
    pd.DataFrame(
        [
            {"url": success_url, "symbol": "BC"},
            {"url": error_url, "symbol": "BC"},
        ]
    ).to_csv(manifest, index=False)

    config = AppConfig.from_project_root(tmp_path)
    result = run_transcript_backfill(config, manifest_path=str(manifest), http_client=FakeHttpClient(payloads))

    calls_path = normalized_path(config, "transcript_calls")
    failures_path = normalized_path(config, "transcript_backfill_failures")
    calls_df = pd.read_parquet(calls_path)
    failures_df = pd.read_parquet(failures_path)

    assert result["requested_count"] == 2
    assert result["fetched_count"] == 1
    assert result["failed_count"] == 1
    assert len(calls_df.index) == 1
    assert len(failures_df.index) == 1
    assert failures_df.iloc[0]["failure_reason"] == FAILURE_NON_TRANSCRIPT_PAGE
    assert failures_df.iloc[0]["url"] == error_url


def test_transcript_backfill_failure_code_retry_exhausted(tmp_path):
    retry_error = HttpRequestError(
        HttpFailure(url="https://example.com/retry", reason="request_exception"),
        attempts=3,
        max_attempts=3,
    )
    config = AppConfig.from_project_root(tmp_path)
    result = run_transcript_backfill(
        config,
        urls=["https://example.com/retry"],
        http_client=FakeHttpClient({"https://example.com/retry": retry_error}),
    )

    failures_path = normalized_path(config, "transcript_backfill_failures")
    failures_df = pd.read_parquet(failures_path)

    assert result["failed_count"] == 1
    assert failures_df.iloc[0]["failure_reason"] == FAILURE_RETRY_EXHAUSTED


def test_transcript_backfill_refetch_only_overwrites_when_stronger_datetime_signal(tmp_path):
    url = "https://example.com/refetch"
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    # Existing row has transcript-specific datetime confidence.
    pd.DataFrame(
        [
            {
                "provider": "motley_fool",
                "provider_call_id": url,
                "call_id": "existing-call-id",
                "symbol": "BC",
                "source_url": url,
                "call_datetime": "2026-03-12T17:00:00",
                "call_datetime_source": "transcript_visible",
                "transcript_path": "/tmp/old.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    # Incoming page only has weaker article publish metadata.
    weak_article_only = """
<html>
  <head><title>Example Earnings Call Transcript</title></head>
  <body>
    <time datetime="2026-03-08T00:00:00">March 8, 2026</time>
    <article>
      <h1>Earnings Call Transcript</h1>
      <p>Operator: Welcome.</p>
      <p>Prepared remarks follow for this quarter.</p>
      <p>Question-and-answer session and closing remarks.</p>
    </article>
  </body>
</html>
"""
    run_transcript_backfill(config, urls=[url], http_client=FakeHttpClient({url: weak_article_only}))

    calls_df = pd.read_parquet(normalized_path(config, "transcript_calls"))
    row = calls_df[calls_df["provider_call_id"].astype(str) == url].iloc[-1].to_dict()
    assert str(row["call_datetime"]).startswith("2026-03-12T17:00:00")
    assert row["call_datetime_source"] == "transcript_visible"


def test_transcript_backfill_refetch_upgrades_unknown_legacy_to_transcript_specific(tmp_path):
    url = "https://example.com/refetch-upgrade"
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame(
        [
            {
                "provider": "motley_fool",
                "provider_call_id": url,
                "call_id": "existing-upgrade-call-id",
                "symbol": "BC",
                "source_url": url,
                "call_datetime": "2026-03-08T00:00:00",
                "transcript_path": "/tmp/old.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    run_transcript_backfill(config, urls=[url], http_client=FakeHttpClient({url: _html_visible_vs_article_conflict()}))
    calls_df = pd.read_parquet(normalized_path(config, "transcript_calls"))
    row = calls_df[calls_df["provider_call_id"].astype(str) == url].iloc[-1].to_dict()
    assert str(row["call_datetime"]).startswith("2026-03-12T17:00:00")
    assert row["call_datetime_source"] == "transcript_visible"


def test_backfill_url_compatibility_uses_same_manifest_row_model():
    urls = ["https://example.com/b", "https://example.com/a"]

    via_urls = _load_backfill_rows(manifest_path=None, urls=urls, symbol="msft")
    via_manifest_helper = sorted(rows_from_urls(urls, symbol="msft"), key=lambda row: (row.url, row.symbol or ""))

    assert via_urls == via_manifest_helper
