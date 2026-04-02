from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd

from py_earnings_calls.adapters.transcripts_motley_fool import MotleyFoolTranscriptAdapter, TranscriptFetchOutcome
from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpClient
from py_earnings_calls.identifiers import transcript_archive_accession_id
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.pipelines.transcript_manifest import load_manifest_rows, rows_from_urls
from py_earnings_calls.storage.archive_index import upsert_transcript_archive_manifest
from py_earnings_calls.storage.paths import normalized_path, transcript_html_path, transcript_json_path, transcript_text_path
from py_earnings_calls.storage.paths import transcript_storage_cik
from py_earnings_calls.storage.writes import upsert_parquet, write_json, write_text

UNKNOWN_CALL_DATE = date(1970, 1, 1)
FAILURES_DEDUPE_KEYS = ["provider", "url"]
DATETIME_SOURCE_STRENGTH = {
    "none": 0,
    "article_published": 1,
    "unknown_legacy": 1,
    "transcript_structured": 2,
    "transcript_visible": 3,
}


def run_transcript_backfill(
    config: AppConfig,
    *,
    manifest_path: str | None = None,
    urls: list[str] | None = None,
    symbol: str | None = None,
    http_client: HttpClient | None = None,
) -> dict[str, object]:
    config.ensure_runtime_dirs()
    client = http_client or HttpClient(config)
    adapter = MotleyFoolTranscriptAdapter(client)

    call_rows = []
    artifact_rows = []
    failure_rows = []
    archive_manifest_rows = []
    outcomes: list[tuple[str, str, TranscriptFetchOutcome]] = []
    issuers_df = load_issuers_dataframe(config)
    symbol_to_cik = build_symbol_to_cik_map(issuers_df)
    calls_path = normalized_path(config, "transcript_calls")
    existing_calls = _read_calls_by_provider_call_id(calls_path)

    backfill_rows = _load_backfill_rows(manifest_path=manifest_path, urls=urls or [], symbol=symbol)
    observed_at = datetime.now(timezone.utc).isoformat()

    for row in backfill_rows:
        outcome = adapter.fetch_document_outcome(row.url, symbol=row.symbol)
        outcomes.append((row.url, adapter.provider, outcome))
        if outcome.failure is not None or outcome.document is None:
            failure = outcome.failure
            failure_rows.append(
                {
                    "provider": adapter.provider,
                    "url": row.url,
                    "symbol": row.symbol,
                    "failure_reason": failure.reason if failure else "UNKNOWN_FAILURE",
                    "failure_message": failure.message if failure else "Unknown failure.",
                    "http_status": failure.http_status if failure else None,
                    "observed_at": observed_at,
                }
            )
            continue

        doc = outcome.document
        call_date = doc.call_datetime.date() if doc.call_datetime else UNKNOWN_CALL_DATE
        storage_cik = symbol_to_cik.get(doc.symbol.upper()) or "UNKNOWN"
        archive_accession_id = transcript_archive_accession_id(doc.call_id)
        html_path = transcript_html_path(
            config,
            provider=doc.provider,
            symbol=doc.symbol,
            call_date=call_date,
            call_id=doc.call_id,
            storage_cik=storage_cik,
        )
        text_path = transcript_text_path(
            config,
            provider=doc.provider,
            symbol=doc.symbol,
            call_date=call_date,
            call_id=doc.call_id,
            storage_cik=storage_cik,
        )
        json_path = transcript_json_path(
            config,
            provider=doc.provider,
            symbol=doc.symbol,
            call_date=call_date,
            call_id=doc.call_id,
            storage_cik=storage_cik,
        )

        if doc.raw_html is not None:
            write_text(html_path, doc.raw_html)
        write_text(text_path, doc.transcript_text)
        write_json(json_path, doc.to_record())

        record = doc.to_record()
        record["transcript_path"] = str(text_path)
        record["raw_html_path"] = str(html_path) if doc.raw_html is not None else None
        record["storage_cik"] = transcript_storage_cik(storage_cik)
        record["archive_accession_id"] = archive_accession_id
        record["archive_bundle_path"] = str(text_path.parent)
        record["imported_at"] = observed_at
        existing = existing_calls.get((str(record.get("provider") or "").strip(), str(record.get("provider_call_id") or "").strip()))
        record = _merge_datetime_on_refetch(existing=existing, incoming=record)
        call_rows.append(record)

        for artifact_type, artifact_path in [
            ("transcript_html", html_path if doc.raw_html is not None else None),
            ("transcript_text", text_path),
            ("transcript_json", json_path),
        ]:
            if artifact_path is None:
                continue
            artifact_rows.append(
                {
                    "call_id": doc.call_id,
                    "artifact_type": artifact_type,
                    "artifact_path": str(artifact_path),
                    "provider": doc.provider,
                    "symbol": doc.symbol,
                    "call_date": call_date.isoformat(),
                    "exists_locally": True,
                    "storage_cik": transcript_storage_cik(storage_cik),
                    "archive_accession_id": archive_accession_id,
                }
            )
        archive_manifest_rows.append(
            {
                "call_id": doc.call_id,
                "archive_accession_id": archive_accession_id,
                "storage_cik": transcript_storage_cik(storage_cik),
                "provider": doc.provider,
                "raw_html_path": str(html_path) if doc.raw_html is not None else None,
                "parsed_text_path": str(text_path),
                "parsed_json_path": str(json_path),
                "raw_html_exists": doc.raw_html is not None,
                "parsed_text_exists": True,
                "parsed_json_exists": True,
                "updated_at": observed_at,
            }
        )

    artifacts_path = normalized_path(config, "transcript_artifacts")
    failures_path = normalized_path(config, "transcript_backfill_failures")
    upsert_parquet(calls_path, call_rows, dedupe_keys=["provider", "call_id"])
    upsert_parquet(artifacts_path, artifact_rows, dedupe_keys=["call_id", "artifact_type", "artifact_path"])
    _write_latest_failures(failures_path, new_failures=failure_rows, outcomes=outcomes)
    manifest_path = upsert_transcript_archive_manifest(config, archive_manifest_rows)

    failure_reason_counts: dict[str, int] = {}
    for row in failure_rows:
        key = str(row["failure_reason"])
        failure_reason_counts[key] = failure_reason_counts.get(key, 0) + 1
    return {
        "requested_count": len(backfill_rows),
        "fetched_count": len(call_rows),
        "failed_count": len(failure_rows),
        "failure_reason_counts": failure_reason_counts,
        "artifact_paths": [str(calls_path), str(artifacts_path)],
        "archive_manifest_path": str(manifest_path),
        "failures_path": str(failures_path),
    }


def _load_backfill_rows(*, manifest_path: str | None, urls: list[str], symbol: str | None) -> list:
    rows = []
    if manifest_path:
        rows = load_manifest_rows(manifest_path)
    elif urls:
        # Compatibility path: --url still goes through the same manifest row model.
        rows = rows_from_urls(urls, symbol=symbol)
    else:
        raise ValueError("Backfill requires either manifest_path or at least one URL.")
    return sorted(rows, key=lambda row: (row.url, row.symbol or ""))


def _write_latest_failures(failures_path, *, new_failures: list[dict], outcomes: list[tuple[str, str, TranscriptFetchOutcome]]) -> None:
    existing = pd.DataFrame()
    if failures_path.exists():
        existing = pd.read_parquet(failures_path)

    successful_keys = {
        (provider, url)
        for (url, provider, outcome) in outcomes
        if outcome.failure is None and outcome.document is not None
    }
    if not existing.empty:
        existing = existing[~existing.apply(lambda row: (row.get("provider"), row.get("url")) in successful_keys, axis=1)]

    merged = existing
    if new_failures:
        new_df = pd.DataFrame(new_failures)
        merged = pd.concat([existing, new_df], ignore_index=True, sort=False)
    if merged.empty:
        merged = pd.DataFrame(columns=["provider", "url", "symbol", "failure_reason", "failure_message", "http_status", "observed_at"])
    merged = merged.drop_duplicates(subset=FAILURES_DEDUPE_KEYS, keep="last")
    merged.to_parquet(failures_path, index=False)


def _read_calls_by_provider_call_id(calls_path) -> dict[tuple[str, str], dict]:
    if not calls_path.exists():
        return {}
    calls = pd.read_parquet(calls_path)
    if calls.empty:
        return {}
    out: dict[tuple[str, str], dict] = {}
    for row in calls.to_dict(orient="records"):
        provider = str(row.get("provider") or "").strip()
        provider_call_id = str(row.get("provider_call_id") or "").strip()
        if not provider or not provider_call_id:
            continue
        out[(provider, provider_call_id)] = row
    return out


def _merge_datetime_on_refetch(*, existing: dict | None, incoming: dict) -> dict:
    if existing is None:
        incoming["call_datetime_source"] = str(incoming.get("call_datetime_source") or "none")
        return incoming

    existing_dt = existing.get("call_datetime")
    existing_source = str(existing.get("call_datetime_source") or "").strip() or "unknown_legacy"
    incoming_dt = incoming.get("call_datetime")
    incoming_source = str(incoming.get("call_datetime_source") or "").strip() or "none"

    # Keep current value when incoming extraction has no datetime.
    if existing_dt and not incoming_dt:
        incoming["call_datetime"] = existing_dt
        incoming["call_datetime_source"] = existing_source
        return incoming

    if existing_dt and incoming_dt:
        existing_strength = DATETIME_SOURCE_STRENGTH.get(existing_source, 1)
        incoming_strength = DATETIME_SOURCE_STRENGTH.get(incoming_source, 0)
        # Conservative overwrite: only upgrade to a stronger signal.
        if incoming_strength <= existing_strength:
            incoming["call_datetime"] = existing_dt
            incoming["call_datetime_source"] = existing_source
            return incoming

    incoming["call_datetime_source"] = incoming_source
    return incoming
