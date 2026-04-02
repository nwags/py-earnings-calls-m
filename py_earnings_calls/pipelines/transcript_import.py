from __future__ import annotations

from datetime import date, datetime

from py_earnings_calls.adapters.transcripts_kaggle import KaggleMotleyFoolTranscriptAdapter
from py_earnings_calls.adapters.transcripts_local_tabular import LocalTabularTranscriptAdapter
from py_earnings_calls.adapters.transcripts_motley_fool_pickle import MotleyFoolPickleTranscriptAdapter
from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import transcript_archive_accession_id
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.storage.archive_index import upsert_transcript_archive_manifest
from py_earnings_calls.storage.paths import normalized_path, transcript_json_path, transcript_text_path
from py_earnings_calls.storage.paths import transcript_storage_cik
from py_earnings_calls.storage.writes import upsert_parquet, write_json, write_text


# Dedupe identity:
# - primary: provider + provider_call_id
# - fallback: symbol + call_datetime + title (materialized into provider_call_id by adapters)
TRANSCRIPT_CALL_DEDUPE_KEYS = ["provider", "provider_call_id"]
UNKNOWN_CALL_DATE = date(1970, 1, 1)

TRANSCRIPT_BULK_ADAPTERS = {
    "kaggle_motley_fool": KaggleMotleyFoolTranscriptAdapter,
    "local_tabular": LocalTabularTranscriptAdapter,
    "motley_fool_pickle": MotleyFoolPickleTranscriptAdapter,
}


def run_transcript_bulk_import(config: AppConfig, dataset_path: str, *, adapter_name: str = "kaggle_motley_fool") -> dict[str, object]:
    config.ensure_runtime_dirs()

    adapter_class = TRANSCRIPT_BULK_ADAPTERS.get(adapter_name)
    if adapter_class is None:
        raise ValueError(f"Unsupported bulk transcript adapter: {adapter_name}")

    adapter = adapter_class()
    documents = adapter.load_documents(dataset_path)
    documents = sorted(documents, key=lambda doc: (doc.provider, doc.call_id))

    artifact_rows = []
    call_rows = []
    manifest_rows = []
    issuers_df = load_issuers_dataframe(config)
    symbol_to_cik = build_symbol_to_cik_map(issuers_df)

    for doc in documents:
        call_date = doc.call_datetime.date() if doc.call_datetime else UNKNOWN_CALL_DATE
        storage_cik = symbol_to_cik.get(doc.symbol.upper()) or "UNKNOWN"
        archive_accession_id = transcript_archive_accession_id(doc.call_id)
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
        write_text(text_path, doc.transcript_text)
        write_json(json_path, doc.to_record())

        record = doc.to_record()
        record["transcript_path"] = str(text_path)
        record["raw_html_path"] = None
        record["storage_cik"] = transcript_storage_cik(storage_cik)
        record["archive_accession_id"] = archive_accession_id
        record["archive_bundle_path"] = str(text_path.parent)
        # Keep import writes deterministic; call_datetime is the stable timestamp when present.
        record["imported_at"] = record.get("call_datetime")
        call_rows.append(record)

        artifact_rows.append(
            {
                "call_id": doc.call_id,
                "artifact_type": "transcript_text",
                "artifact_path": str(text_path),
                "provider": doc.provider,
                "symbol": doc.symbol,
                "call_date": call_date.isoformat(),
                "exists_locally": True,
                "storage_cik": transcript_storage_cik(storage_cik),
                "archive_accession_id": archive_accession_id,
            }
        )
        artifact_rows.append(
            {
                "call_id": doc.call_id,
                "artifact_type": "transcript_json",
                "artifact_path": str(json_path),
                "provider": doc.provider,
                "symbol": doc.symbol,
                "call_date": call_date.isoformat(),
                "exists_locally": True,
                "storage_cik": transcript_storage_cik(storage_cik),
                "archive_accession_id": archive_accession_id,
            }
        )
        manifest_rows.append(
            {
                "call_id": doc.call_id,
                "archive_accession_id": archive_accession_id,
                "storage_cik": transcript_storage_cik(storage_cik),
                "provider": doc.provider,
                "raw_html_path": None,
                "parsed_text_path": str(text_path),
                "parsed_json_path": str(json_path),
                "raw_html_exists": False,
                "parsed_text_exists": True,
                "parsed_json_exists": True,
            }
        )

    calls_path = normalized_path(config, "transcript_calls")
    artifacts_path = normalized_path(config, "transcript_artifacts")
    upsert_parquet(calls_path, call_rows, dedupe_keys=TRANSCRIPT_CALL_DEDUPE_KEYS)
    upsert_parquet(artifacts_path, artifact_rows, dedupe_keys=["call_id", "artifact_type", "artifact_path"])
    manifest_path = upsert_transcript_archive_manifest(config, manifest_rows)

    return {
        "document_count": len(documents),
        "artifact_paths": [str(calls_path), str(artifacts_path), str(manifest_path)],
    }
