from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import forecast_full_index_manifest_path, transcript_full_index_manifest_path
from py_earnings_calls.storage.writes import upsert_parquet


def upsert_transcript_archive_manifest(config: AppConfig, rows: list[dict]) -> Path:
    path = transcript_full_index_manifest_path(config)
    payload = []
    for row in rows:
        payload.append(
            {
                "call_id": row.get("call_id"),
                "archive_accession_id": row.get("archive_accession_id"),
                "storage_cik": row.get("storage_cik"),
                "provider": row.get("provider"),
                "raw_html_path": row.get("raw_html_path"),
                "parsed_text_path": row.get("parsed_text_path"),
                "parsed_json_path": row.get("parsed_json_path"),
                "raw_html_exists": bool(row.get("raw_html_exists", False)),
                "parsed_text_exists": bool(row.get("parsed_text_exists", False)),
                "parsed_json_exists": bool(row.get("parsed_json_exists", False)),
                "updated_at": row.get("updated_at") or _utc_now(),
            }
        )
    upsert_parquet(path, payload, dedupe_keys=["call_id", "archive_accession_id"])
    return path


def upsert_forecast_archive_manifest(config: AppConfig, rows: list[dict]) -> Path:
    path = forecast_full_index_manifest_path(config)
    payload = []
    for row in rows:
        payload.append(
            {
                "snapshot_id": row.get("snapshot_id"),
                "archive_accession_id": row.get("archive_accession_id"),
                "provider": row.get("provider"),
                "symbol": row.get("symbol"),
                "as_of_date": row.get("as_of_date"),
                "raw_payload_path": row.get("raw_payload_path"),
                "raw_payload_exists": bool(row.get("raw_payload_exists", False)),
                "updated_at": row.get("updated_at") or _utc_now(),
            }
        )
    upsert_parquet(path, payload, dedupe_keys=["snapshot_id", "archive_accession_id"])
    return path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
