from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

import pandas as pd

from py_earnings_calls.augmentation_shared import (
    AUGMENTATION_TYPES,
    PRODUCER_KINDS,
    PRODUCER_RUN_STATUSES,
    build_artifact_idempotency_key,
    coerce_bool as _shared_coerce_bool,
    max_nonempty_text,
    pack_artifact_event_row,
    pack_additive_augmentation_meta,
    pack_run_event_row,
    to_int_or_none as _shared_to_int_or_none,
    validate_artifact_submission_envelope,
    validate_run_submission_envelope,
)
from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path
from py_earnings_calls.storage.writes import upsert_parquet, write_json

MAX_INLINE_PAYLOAD_BYTES = 262_144

AUGMENTATION_RUN_COLUMNS = [
    "run_id",
    "producer_run_key",
    "event_at",
    "domain",
    "resource_family",
    "canonical_key",
    "augmentation_type",
    "source_text_version",
    "producer_kind",
    "producer_name",
    "producer_version",
    "payload_schema_name",
    "payload_schema_version",
    "status",
    "success",
    "reason_code",
    "message",
    "persisted_locally",
    "latency_ms",
    "rate_limited",
    "retry_count",
    "deferred_until",
]

AUGMENTATION_ARTIFACT_COLUMNS = [
    "idempotency_key",
    "domain",
    "resource_family",
    "canonical_key",
    "augmentation_type",
    "artifact_locator",
    "payload_schema_name",
    "payload_schema_version",
    "source_text_version",
    "producer_name",
    "producer_version",
    "payload_sha256",
    "payload_bytes",
    "event_at",
    "success",
]

AUGMENTATION_EVENT_COLUMNS = [
    "event_id",
    "run_id",
    "event_at",
    "domain",
    "resource_family",
    "canonical_key",
    "augmentation_type",
    "source_text_version",
    "producer_kind",
    "producer_name",
    "producer_version",
    "payload_schema_name",
    "payload_schema_version",
    "status",
    "success",
    "reason_code",
    "message",
    "persisted_locally",
    "latency_ms",
    "rate_limited",
    "retry_count",
    "deferred_until",
]


def transcript_canonical_key(call_id: str) -> str:
    return f"transcript:{str(call_id).strip()}"


def parse_transcript_call_id(canonical_key: str | None) -> str | None:
    text = str(canonical_key or "").strip()
    if not text:
        return None
    if ":" not in text:
        return None
    family, call_id = text.split(":", 1)
    if family.strip().lower() != "transcript":
        return None
    normalized_call_id = call_id.strip()
    if not normalized_call_id:
        return None
    return normalized_call_id


def transcript_source_text_version_from_path(path_like: str | Path | None) -> str | None:
    if path_like is None:
        return None
    path = Path(str(path_like))
    if not path.exists() or not path.is_file():
        return None
    digest = sha256(path.read_bytes()).hexdigest()
    return f"sha256:{digest}"


def read_augmentation_runs(config: AppConfig) -> pd.DataFrame:
    return _read_and_normalize(
        path=normalized_path(config, "augmentation_runs"),
        columns=AUGMENTATION_RUN_COLUMNS,
    )


def read_augmentation_artifacts(config: AppConfig) -> pd.DataFrame:
    return _read_and_normalize(
        path=normalized_path(config, "augmentation_artifacts"),
        columns=AUGMENTATION_ARTIFACT_COLUMNS,
    )


def read_augmentation_events(config: AppConfig) -> pd.DataFrame:
    return _read_and_normalize(
        path=normalized_path(config, "augmentation_events"),
        columns=AUGMENTATION_EVENT_COLUMNS,
    )


def lookup_transcript_path_for_call_id(config: AppConfig, call_id: str) -> str | None:
    lookup_path = normalized_path(config, "local_lookup_transcripts")
    if not lookup_path.exists():
        return None
    try:
        df = pd.read_parquet(lookup_path)
    except Exception:
        return None
    if df.empty or "call_id" not in df.columns:
        return None
    matches = df[df["call_id"].astype(str) == str(call_id)].tail(1)
    if matches.empty:
        return None
    transcript_path = matches.iloc[0].to_dict().get("transcript_path")
    text = str(transcript_path).strip() if transcript_path is not None else ""
    return text or None


def transcript_target_descriptor(config: AppConfig, *, call_id: str) -> dict[str, Any]:
    normalized_call_id = str(call_id).strip()
    canonical_key = transcript_canonical_key(normalized_call_id)
    transcript_path = lookup_transcript_path_for_call_id(config, normalized_call_id)
    source_text_version = transcript_source_text_version_from_path(transcript_path)
    return {
        "domain": "earnings",
        "resource_family": "transcripts",
        "canonical_key": canonical_key,
        "call_id": normalized_call_id,
        "text_source": transcript_path,
        "source_text_version": source_text_version,
        "language": None,
        "document_time_reference": None,
        "producer_hints": {
            "api_detail_path": f"/transcripts/{normalized_call_id}",
            "api_content_path": f"/transcripts/{normalized_call_id}/content",
        },
    }


def submit_producer_run(config: AppConfig, envelope: dict[str, Any]) -> dict[str, Any]:
    validated = _validate_run_envelope(envelope)
    event_at = validated.get("event_at") or _utc_now()
    row = {
        **validated,
        "event_at": event_at,
    }
    config.ensure_runtime_dirs()
    path = normalized_path(config, "augmentation_runs")
    added = upsert_parquet(path, [row], dedupe_keys=["run_id"])
    event = _event_from_run_row(row)
    event_path = normalized_path(config, "augmentation_events")
    upsert_parquet(event_path, [event], dedupe_keys=["event_id"])
    return {
        "accepted": True,
        "idempotent_replay": added == 0,
        "run_id": row["run_id"],
        "event_id": event["event_id"],
        "run_path": str(path),
        "event_path": str(event_path),
    }


def submit_producer_artifact(config: AppConfig, envelope: dict[str, Any]) -> dict[str, Any]:
    validated = _validate_artifact_envelope(envelope)
    payload = validated.pop("payload", None)
    event_at = validated.get("event_at") or _utc_now()
    payload_sha256 = None
    payload_bytes = None
    artifact_locator = str(validated.get("artifact_locator") or "").strip()
    if payload is not None:
        payload_text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        payload_bytes = len(payload_text.encode("utf-8"))
        if payload_bytes > MAX_INLINE_PAYLOAD_BYTES:
            raise ValueError(
                f"Inline payload exceeds limit ({payload_bytes} bytes > {MAX_INLINE_PAYLOAD_BYTES} bytes). "
                "Use artifact_locator for bounded producer submissions."
            )
        payload_sha256 = sha256(payload_text.encode("utf-8")).hexdigest()
        if not artifact_locator:
            artifact_locator = _materialize_inline_payload(
                config=config,
                canonical_key=str(validated["canonical_key"]),
                augmentation_type=str(validated["augmentation_type"]),
                producer_name=str(validated["producer_name"]),
                producer_version=str(validated["producer_version"]),
                payload=payload,
            )
    idempotency_key = validated.get("idempotency_key")
    if not idempotency_key:
        idempotency_key = _artifact_idempotency_key(validated, artifact_locator=artifact_locator, payload_sha256=payload_sha256)

    row = {
        **validated,
        "artifact_locator": artifact_locator,
        "idempotency_key": idempotency_key,
        "payload_sha256": payload_sha256 or "",
        "payload_bytes": payload_bytes,
        "event_at": event_at,
    }
    config.ensure_runtime_dirs()
    path = normalized_path(config, "augmentation_artifacts")
    added = upsert_parquet(path, [row], dedupe_keys=["idempotency_key"])
    event = _event_from_artifact_row(row)
    event_path = normalized_path(config, "augmentation_events")
    upsert_parquet(event_path, [event], dedupe_keys=["event_id"])
    return {
        "accepted": True,
        "idempotent_replay": added == 0,
        "idempotency_key": idempotency_key,
        "artifact_locator": artifact_locator,
        "artifact_path": str(path),
        "event_path": str(event_path),
    }


def transcript_augmentation_meta(
    config: AppConfig,
    *,
    call_id: str,
    source_text_version: str | None,
) -> dict[str, object]:
    canonical_key = transcript_canonical_key(call_id)
    artifacts = read_augmentation_artifacts(config)
    runs = read_augmentation_runs(config)

    if not artifacts.empty:
        artifacts = artifacts[
            (artifacts["domain"].astype(str).str.lower() == "earnings")
            & (artifacts["resource_family"].astype(str).str.lower() == "transcripts")
            & (artifacts["canonical_key"].astype(str) == canonical_key)
            & (artifacts["success"] == True)  # noqa: E712 - explicit bool series match
        ]
    if not runs.empty:
        runs = runs[
            (runs["domain"].astype(str).str.lower() == "earnings")
            & (runs["resource_family"].astype(str).str.lower() == "transcripts")
            & (runs["canonical_key"].astype(str) == canonical_key)
            & (runs["success"] == True)  # noqa: E712 - explicit bool series match
        ]

    types_present = sorted(
        {
            str(value).strip().lower()
            for value in artifacts.get("augmentation_type", pd.Series(dtype="string")).tolist()
            if str(value).strip()
        }
    )
    if not types_present:
        types_present = sorted(
            {
                str(value).strip().lower()
                for value in runs.get("augmentation_type", pd.Series(dtype="string")).tolist()
                if str(value).strip()
            }
        )

    recorded_source_versions = sorted(
        {
            str(value).strip()
            for value in artifacts.get("source_text_version", pd.Series(dtype="string")).tolist()
            if str(value).strip()
        }
    )
    if not recorded_source_versions:
        recorded_source_versions = sorted(
            {
                str(value).strip()
                for value in runs.get("source_text_version", pd.Series(dtype="string")).tolist()
                if str(value).strip()
            }
        )

    return pack_additive_augmentation_meta(
        augmentation_types_present=types_present,
        artifact_event_ats=[
            str(value).strip()
            for value in artifacts.get("event_at", pd.Series(dtype="string")).tolist()
            if str(value).strip()
        ],
        run_event_ats=[
            str(value).strip()
            for value in runs.get("event_at", pd.Series(dtype="string")).tolist()
            if str(value).strip()
        ],
        recorded_source_versions=recorded_source_versions,
        source_text_version=source_text_version,
        inspect_path=f"/transcripts/{str(call_id).strip()}/augmentations",
    )


def _read_and_normalize(*, path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_parquet(path)
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    out = out[columns]
    for text_col in [
        "event_id",
        "domain",
        "resource_family",
        "canonical_key",
        "augmentation_type",
        "source_text_version",
        "producer_run_key",
        "producer_kind",
        "producer_name",
        "producer_version",
        "payload_schema_name",
        "payload_schema_version",
        "idempotency_key",
        "status",
        "reason_code",
        "message",
        "artifact_locator",
        "payload_sha256",
    ]:
        if text_col in out.columns:
            out[text_col] = out[text_col].astype("string").fillna("").str.strip()
    for bool_col in ["success", "persisted_locally", "rate_limited"]:
        if bool_col in out.columns:
            out[bool_col] = out[bool_col].map(_coerce_bool)
    for int_col in ["retry_count", "latency_ms", "payload_bytes"]:
        if int_col in out.columns:
            out[int_col] = pd.to_numeric(out[int_col], errors="coerce")
    return out.reset_index(drop=True)


def _max_timestamp(values: list[pd.Series]) -> str | None:
    bucket: list[str] = []
    for series in values:
        for value in series.tolist():
            bucket.append(str(value))
    return max_nonempty_text(bucket)


def _coerce_bool(value: object) -> bool:
    return _shared_coerce_bool(value)


def _validate_run_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    return validate_run_submission_envelope(
        envelope,
        expected_domain="earnings",
        expected_resource_family="transcripts",
        canonical_key_validator=lambda value: parse_transcript_call_id(value) is not None,
        canonical_key_error="canonical_key must use existing transcript identity format: transcript:{call_id}.",
        resource_family_context="Wave 4 in earnings",
    )


def _validate_artifact_envelope(envelope: dict[str, Any]) -> dict[str, Any]:
    return validate_artifact_submission_envelope(
        envelope,
        expected_domain="earnings",
        expected_resource_family="transcripts",
        canonical_key_validator=lambda value: parse_transcript_call_id(value) is not None,
        canonical_key_error="canonical_key must use existing transcript identity format: transcript:{call_id}.",
        resource_family_context="Wave 4 in earnings",
    )


def _event_from_run_row(row: dict[str, Any]) -> dict[str, Any]:
    return pack_run_event_row(row, event_at=str(row.get("event_at") or _utc_now()))


def _event_from_artifact_row(row: dict[str, Any]) -> dict[str, Any]:
    return pack_artifact_event_row(row, event_at=str(row.get("event_at") or _utc_now()))


def _materialize_inline_payload(
    *,
    config: AppConfig,
    canonical_key: str,
    augmentation_type: str,
    producer_name: str,
    producer_version: str,
    payload: Any,
) -> str:
    safe_key = canonical_key.replace(":", "_")
    safe_type = augmentation_type.replace(":", "_")
    safe_name = producer_name.replace(":", "_").replace(" ", "_")
    safe_version = producer_version.replace(":", "_").replace(" ", "_")
    payload_dir = config.normalized_refdata_root / "augmentation_payloads"
    filename = f"{safe_key}_{safe_type}_{safe_name}_{safe_version}.json"
    path = payload_dir / filename
    write_json(path, payload)
    return str(path)


def _artifact_idempotency_key(
    row: dict[str, Any],
    *,
    artifact_locator: str,
    payload_sha256: str | None,
) -> str:
    return build_artifact_idempotency_key(
        row,
        artifact_locator=artifact_locator,
        payload_sha256=payload_sha256,
    )


def _to_int_or_none(value: object) -> int | None:
    return _shared_to_int_or_none(value)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
