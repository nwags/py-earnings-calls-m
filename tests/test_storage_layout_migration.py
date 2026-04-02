from __future__ import annotations

from datetime import date

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.pipelines.storage_layout import (
    SKIP_CONTENT_MISMATCH,
    SKIP_LEGACY_ONLY_NO_ARCHIVE_COPY,
    SKIP_MISSING_TARGET,
    SKIP_TARGET_NOT_CANONICAL,
    run_storage_cleanup_legacy,
    run_storage_migrate_layout,
    run_storage_verify_layout,
)
from py_earnings_calls.storage.paths import (
    legacy_forecast_raw_snapshot_path,
    legacy_transcript_json_path,
    legacy_transcript_text_path,
    normalized_path,
    transcript_archive_paths,
)
from py_earnings_calls.storage.writes import write_json, write_text


def test_storage_migration_copy_first_preserves_legacy_files(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    call_id = "c-1"
    call_date = date(2026, 3, 26)
    legacy_text = legacy_transcript_text_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=call_id)
    legacy_json = legacy_transcript_json_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=call_id)
    write_text(legacy_text, "legacy transcript text")
    write_json(legacy_json, {"call_id": call_id})

    pd.DataFrame(
        [
            {
                "call_id": call_id,
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-26T10:00:00",
                "source_url": "https://example.com/x",
                "transcript_path": str(legacy_text),
                "raw_html_path": None,
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)
    pd.DataFrame(
        [
            {
                "call_id": call_id,
                "artifact_type": "transcript_json",
                "artifact_path": str(legacy_json),
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_artifacts"), index=False)

    result = run_storage_migrate_layout(config, dry_run=False)

    migrated_calls = pd.read_parquet(normalized_path(config, "transcript_calls"))
    row = migrated_calls.iloc[0].to_dict()
    assert row["transcript_path"] != str(legacy_text)
    assert legacy_text.exists()  # copy-first: legacy preserved
    assert legacy_json.exists()
    assert result["mode"] == "apply"
    assert result["normalized_rows_updated"] >= 1


def test_storage_migration_updates_forecast_paths_and_manifests(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    legacy_raw = legacy_forecast_raw_snapshot_path(
        config,
        provider="finnhub",
        symbol="AAPL",
        as_of_date=date(2026, 3, 26),
    )
    write_json(legacy_raw, {"ok": True})
    pd.DataFrame(
        [
            {
                "snapshot_id": "snap1",
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-26",
                "raw_payload_path": str(legacy_raw),
            }
        ]
    ).to_parquet(normalized_path(config, "forecast_snapshots"), index=False)

    result = run_storage_migrate_layout(config, dry_run=False)
    forecasts = pd.read_parquet(normalized_path(config, "forecast_snapshots"))
    assert "/forecasts/data/" in str(forecasts.iloc[0]["raw_payload_path"])
    assert result["forecast_manifest_rows"] >= 1


def test_storage_verify_layout_reports_counts(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    path_info = transcript_archive_paths(config, call_id="c1", storage_cik="UNKNOWN")
    write_text(path_info["parsed_text_path"], "x")
    pd.DataFrame(
        [
            {
                "call_id": "c1",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-26T10:00:00",
                "transcript_path": str(path_info["parsed_text_path"]),
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    report = run_storage_verify_layout(config)
    assert report["normalized_rows_total"] >= 1
    assert report["archive_bundles_present"] >= 1
    assert "operator_note" in report


def test_cleanup_legacy_dry_run_reports_reason_codes_and_dir_keys(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    call_id = "c-skip"
    call_date = date(2026, 3, 26)
    legacy_text = legacy_transcript_text_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=call_id)
    write_text(legacy_text, "legacy content")
    pd.DataFrame(
        [
            {
                "call_id": call_id,
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-26T10:00:00",
                "transcript_path": "/tmp/not-canonical.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    report = run_storage_cleanup_legacy(config, dry_run=True)
    assert report["mode"] == "dry_run"
    assert "skip_reason_counts" in report
    assert SKIP_TARGET_NOT_CANONICAL in report["skip_reason_counts"]
    assert "empty_dirs_removed" in report
    assert "nonempty_legacy_dirs_remaining" in report
    assert "legacy_roots_still_present" in report


def test_cleanup_legacy_apply_deletes_only_verified_match(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    call_id = "c-clean"
    call_date = date(2026, 3, 26)
    legacy_text = legacy_transcript_text_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=call_id)
    write_text(legacy_text, "same content")
    archive_path = transcript_archive_paths(config, call_id=call_id, storage_cik="UNKNOWN")["parsed_text_path"]
    write_text(archive_path, "same content")
    pd.DataFrame(
        [
            {
                "call_id": call_id,
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-26T10:00:00",
                "transcript_path": str(archive_path),
                "storage_cik": "UNKNOWN",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    report = run_storage_cleanup_legacy(config, dry_run=False)
    assert report["deleted_files"] >= 1
    assert not legacy_text.exists()


def test_cleanup_legacy_skips_missing_target_and_mismatch(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    call_date = date(2026, 3, 26)

    missing_id = "c-missing-target"
    missing_legacy = legacy_transcript_text_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=missing_id)
    write_text(missing_legacy, "legacy only")
    missing_archive = transcript_archive_paths(config, call_id=missing_id, storage_cik="UNKNOWN")["parsed_text_path"]
    pd.DataFrame(
        [
            {
                "call_id": missing_id,
                "provider": "motley_fool",
                "symbol": "AAPL",
                "call_datetime": "2026-03-26T10:00:00",
                "transcript_path": str(missing_archive),
                "storage_cik": "UNKNOWN",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    mismatch_id = "c-mismatch"
    mismatch_legacy = legacy_transcript_text_path(config, provider="motley_fool", symbol="AAPL", call_date=call_date, call_id=mismatch_id)
    write_text(mismatch_legacy, "legacy")
    mismatch_archive = transcript_archive_paths(config, call_id=mismatch_id, storage_cik="UNKNOWN")["parsed_text_path"]
    write_text(mismatch_archive, "archive different")
    df = pd.read_parquet(normalized_path(config, "transcript_calls"))
    df = pd.concat(
        [
            df,
            pd.DataFrame(
                [
                    {
                        "call_id": mismatch_id,
                        "provider": "motley_fool",
                        "symbol": "AAPL",
                        "call_datetime": "2026-03-26T10:00:00",
                        "transcript_path": str(mismatch_archive),
                        "storage_cik": "UNKNOWN",
                    }
                ]
            ),
        ],
        ignore_index=True,
        sort=False,
    )
    df.to_parquet(normalized_path(config, "transcript_calls"), index=False)

    report = run_storage_cleanup_legacy(config, dry_run=True)
    assert report["skip_reason_counts"][SKIP_MISSING_TARGET] >= 1
    assert report["skip_reason_counts"][SKIP_CONTENT_MISMATCH] >= 1


def test_cleanup_legacy_forecast_legacy_only_reason(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    legacy_raw = legacy_forecast_raw_snapshot_path(config, provider="finnhub", symbol="AAPL", as_of_date=date(2026, 3, 26))
    write_json(legacy_raw, {"x": 1})
    report = run_storage_cleanup_legacy(config, dry_run=True)
    assert report["skip_reason_counts"][SKIP_LEGACY_ONLY_NO_ARCHIVE_COPY] >= 1


def test_cleanup_legacy_reports_target_not_canonical_for_forecast(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    legacy_raw = legacy_forecast_raw_snapshot_path(config, provider="finnhub", symbol="AAPL", as_of_date=date(2026, 3, 26))
    write_json(legacy_raw, {"x": 1})
    pd.DataFrame(
        [
            {
                "snapshot_id": "snap1",
                "provider": "finnhub",
                "symbol": "AAPL",
                "as_of_date": "2026-03-26",
                "raw_payload_path": "/tmp/not-canonical-forecast.json",
            }
        ]
    ).to_parquet(normalized_path(config, "forecast_snapshots"), index=False)
    report = run_storage_cleanup_legacy(config, dry_run=True)
    assert report["skip_reason_counts"][SKIP_TARGET_NOT_CANONICAL] >= 1
