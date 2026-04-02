from __future__ import annotations

from hashlib import sha1

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.pipelines.transcript_datetime_audit import run_transcript_datetime_audit
from py_earnings_calls.pipelines.transcript_manifest import load_manifest_rows
from py_earnings_calls.storage.paths import normalized_path


def _write_calls(config: AppConfig, rows: list[dict]) -> None:
    config.ensure_runtime_dirs()
    pd.DataFrame(rows).to_parquet(normalized_path(config, "transcript_calls"), index=False)


def test_datetime_audit_classifies_sources_and_missing(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    _write_calls(
        config,
        [
            {
                "call_id": "c-visible",
                "provider": "motley_fool",
                "provider_call_id": "https://x/visible",
                "source_url": "https://x/visible",
                "symbol": "AAPL",
                "call_datetime": "2026-03-12T17:00:00",
                "call_datetime_source": "transcript_visible",
            },
            {
                "call_id": "c-structured",
                "provider": "motley_fool",
                "provider_call_id": "https://x/structured",
                "source_url": "https://x/structured",
                "symbol": "MSFT",
                "call_datetime": "2026-03-11T16:00:00",
                "call_datetime_source": "transcript_structured",
            },
            {
                "call_id": "c-article",
                "provider": "motley_fool",
                "provider_call_id": "https://x/article",
                "source_url": "https://x/article",
                "symbol": "NVDA",
                "call_datetime": "2026-03-08T00:00:00",
                "call_datetime_source": "article_published",
            },
            {
                "call_id": "c-none",
                "provider": "motley_fool",
                "provider_call_id": "https://x/none",
                "source_url": "https://x/none",
                "symbol": "TSLA",
                "call_datetime": "2026-03-10T12:00:00",
                "call_datetime_source": "none",
            },
            {
                "call_id": "c-legacy",
                "provider": "motley_fool",
                "provider_call_id": "https://x/legacy",
                "source_url": "https://x/legacy",
                "symbol": "AMZN",
                "call_datetime": "2026-03-09T09:00:00",
                "call_datetime_source": "legacy_tag_v1",
            },
            {
                "call_id": "c-missing",
                "provider": "motley_fool",
                "provider_call_id": "https://x/missing",
                "source_url": "https://x/missing",
                "symbol": "GOOG",
                "call_datetime": None,
                "call_datetime_source": "transcript_visible",
            },
            {
                "call_id": "c-other-provider",
                "provider": "kaggle_motley_fool",
                "provider_call_id": "https://x/other-provider",
                "source_url": "https://x/other-provider",
                "symbol": "META",
                "call_datetime": "2026-03-01T10:00:00",
                "call_datetime_source": "none",
            },
        ],
    )

    result = run_transcript_datetime_audit(config, limit=10)

    assert result["provider"] == "motley_fool"
    assert result["total_fetched_rows_considered"] == 6
    assert result["rows_with_transcript_visible_datetime"] == 2
    assert result["rows_with_transcript_structured_datetime"] == 1
    assert result["rows_with_article_published_datetime"] == 1
    assert result["rows_with_missing_datetime"] == 1
    assert result["rows_with_legacy_unknown_source"] == 1
    assert result["suspect_rows_count"] == 4
    assert len(result["suspect_rows_sample"]) == 4


def test_datetime_audit_orders_suspects_deterministically_and_preserves_raw_source(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    _write_calls(
        config,
        [
            {
                "call_id": "c-article-newer",
                "provider": "motley_fool",
                "provider_call_id": "https://x/article-newer",
                "source_url": "https://x/article-newer",
                "symbol": "AAPL",
                "call_datetime": "2026-03-10T12:00:00",
                "call_datetime_source": "article_published",
            },
            {
                "call_id": "c-article-older",
                "provider": "motley_fool",
                "provider_call_id": "https://x/article-older",
                "source_url": "https://x/article-older",
                "symbol": "AAPL",
                "call_datetime": "2026-03-08T12:00:00",
                "call_datetime_source": "article_published",
            },
            {
                "call_id": "c-none",
                "provider": "motley_fool",
                "provider_call_id": "https://x/none",
                "source_url": "https://x/none",
                "symbol": "MSFT",
                "call_datetime": "2026-03-09T12:00:00",
                "call_datetime_source": "none",
            },
            {
                "call_id": "c-legacy",
                "provider": "motley_fool",
                "provider_call_id": "https://x/legacy",
                "source_url": "https://x/legacy",
                "symbol": "MSFT",
                "call_datetime": "2026-03-11T12:00:00",
                "call_datetime_source": "legacy_custom",
            },
            {
                "call_id": "c-missing",
                "provider": "motley_fool",
                "provider_call_id": "https://x/missing",
                "source_url": "https://x/missing",
                "symbol": "MSFT",
                "call_datetime": None,
                "call_datetime_source": "transcript_visible",
            },
        ],
    )

    result = run_transcript_datetime_audit(config, limit=10)
    sample = result["suspect_rows_sample"]
    assert [item["call_id"] for item in sample] == [
        "c-missing",
        "c-legacy",
        "c-none",
        "c-article-newer",
        "c-article-older",
    ]
    assert sample[0]["audit_bucket"] == "missing_datetime"
    assert sample[1]["audit_bucket"] == "legacy_unknown"
    assert sample[1]["current_call_datetime_source"] == "legacy_custom"


def test_datetime_audit_manifest_export_is_backfill_compatible(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    _write_calls(
        config,
        [
            {
                "call_id": "c-none",
                "provider": "motley_fool",
                "provider_call_id": "https://x/none",
                "source_url": "https://x/none",
                "symbol": "MSFT",
                "call_datetime": "2026-03-09T12:00:00",
                "call_datetime_source": "none",
            },
            {
                "call_id": "c-article",
                "provider": "motley_fool",
                "provider_call_id": "https://x/article",
                "source_url": "https://x/article",
                "symbol": "AAPL",
                "call_datetime": "2026-03-10T12:00:00",
                "call_datetime_source": "article_published",
            },
        ],
    )

    manifest_path = tmp_path / "suspect_manifest.csv"
    result = run_transcript_datetime_audit(config, limit=1, write_manifest_path=str(manifest_path))

    assert result["manifest_written"] is True
    assert result["manifest_path"] == str(manifest_path.resolve())
    exported = pd.read_csv(manifest_path)
    assert exported.columns.tolist() == [
        "url",
        "symbol",
        "call_id",
        "current_call_datetime",
        "current_call_datetime_source",
        "audit_bucket",
    ]
    assert len(exported.index) == 1

    rows = load_manifest_rows(str(manifest_path))
    assert len(rows) == 1
    assert rows[0].url.startswith("https://x/")
    assert rows[0].symbol in {"AAPL", "MSFT"}


def test_datetime_audit_zero_suspect_case(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    _write_calls(
        config,
        [
            {
                "call_id": "c-visible",
                "provider": "motley_fool",
                "provider_call_id": "https://x/visible",
                "source_url": "https://x/visible",
                "symbol": "AAPL",
                "call_datetime": "2026-03-12T17:00:00",
                "call_datetime_source": "transcript_visible",
            },
            {
                "call_id": "c-structured",
                "provider": "motley_fool",
                "provider_call_id": "https://x/structured",
                "source_url": "https://x/structured",
                "symbol": "MSFT",
                "call_datetime": "2026-03-11T16:00:00",
                "call_datetime_source": "transcript_structured",
            },
        ],
    )

    result = run_transcript_datetime_audit(config, limit=10)
    assert result["suspect_rows_count"] == 0
    assert result["suspect_rows_sample"] == []


def test_datetime_audit_is_non_mutating_without_manifest_write(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    _write_calls(
        config,
        [
            {
                "call_id": "c-none",
                "provider": "motley_fool",
                "provider_call_id": "https://x/none",
                "source_url": "https://x/none",
                "symbol": "MSFT",
                "call_datetime": "2026-03-09T12:00:00",
                "call_datetime_source": "none",
            }
        ],
    )
    calls_path = normalized_path(config, "transcript_calls")
    before_digest = sha1(calls_path.read_bytes()).hexdigest()
    before_files = sorted(path.name for path in config.normalized_refdata_root.glob("*.parquet"))

    result = run_transcript_datetime_audit(config, limit=10)

    after_digest = sha1(calls_path.read_bytes()).hexdigest()
    after_files = sorted(path.name for path in config.normalized_refdata_root.glob("*.parquet"))
    assert result["manifest_written"] is False
    assert before_digest == after_digest
    assert before_files == after_files
