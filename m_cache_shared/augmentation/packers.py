from __future__ import annotations

from hashlib import sha256
from typing import Any, Mapping

from m_cache_shared.augmentation.helpers import max_nonempty_text


def build_artifact_idempotency_key(
    row: Mapping[str, Any],
    *,
    artifact_locator: str,
    payload_sha256: str | None,
) -> str:
    payload_identity = payload_sha256 or artifact_locator
    raw = "|".join(
        [
            str(row.get("domain") or ""),
            str(row.get("resource_family") or ""),
            str(row.get("canonical_key") or ""),
            str(row.get("augmentation_type") or ""),
            str(row.get("source_text_version") or ""),
            str(row.get("producer_name") or ""),
            str(row.get("producer_version") or ""),
            str(row.get("payload_schema_name") or ""),
            str(row.get("payload_schema_version") or ""),
            payload_identity,
        ]
    )
    return sha256(raw.encode("utf-8")).hexdigest()


def pack_run_event_row(row: Mapping[str, Any], *, event_at: str) -> dict[str, Any]:
    event_id = sha256(
        "|".join(
            [
                "run",
                str(row.get("run_id") or ""),
                str(row.get("status") or ""),
                str(row.get("source_text_version") or ""),
                str(row.get("producer_name") or ""),
                str(row.get("producer_version") or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()
    return {
        "event_id": event_id,
        "run_id": row.get("run_id"),
        "event_at": event_at,
        "domain": row.get("domain"),
        "resource_family": row.get("resource_family"),
        "canonical_key": row.get("canonical_key"),
        "augmentation_type": row.get("augmentation_type"),
        "source_text_version": row.get("source_text_version"),
        "producer_kind": row.get("producer_kind"),
        "producer_name": row.get("producer_name"),
        "producer_version": row.get("producer_version"),
        "payload_schema_name": row.get("payload_schema_name"),
        "payload_schema_version": row.get("payload_schema_version"),
        "status": row.get("status"),
        "success": row.get("success"),
        "reason_code": row.get("reason_code"),
        "message": row.get("message"),
        "persisted_locally": row.get("persisted_locally"),
        "latency_ms": row.get("latency_ms"),
        "rate_limited": row.get("rate_limited"),
        "retry_count": row.get("retry_count"),
        "deferred_until": row.get("deferred_until"),
    }


def pack_artifact_event_row(row: Mapping[str, Any], *, event_at: str) -> dict[str, Any]:
    event_id = sha256(
        "|".join(
            [
                "artifact",
                str(row.get("idempotency_key") or ""),
                str(row.get("source_text_version") or ""),
                str(row.get("producer_name") or ""),
                str(row.get("producer_version") or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()
    success = bool(row.get("success", True))
    return {
        "event_id": event_id,
        "run_id": "",
        "event_at": event_at,
        "domain": row.get("domain"),
        "resource_family": row.get("resource_family"),
        "canonical_key": row.get("canonical_key"),
        "augmentation_type": row.get("augmentation_type"),
        "source_text_version": row.get("source_text_version"),
        "producer_kind": "",
        "producer_name": row.get("producer_name"),
        "producer_version": row.get("producer_version"),
        "payload_schema_name": row.get("payload_schema_name"),
        "payload_schema_version": row.get("payload_schema_version"),
        "status": "completed" if success else "failed",
        "success": success,
        "reason_code": "ARTIFACT_SUBMITTED" if success else "ARTIFACT_FAILED",
        "message": "",
        "persisted_locally": True,
        "latency_ms": None,
        "rate_limited": False,
        "retry_count": 0,
        "deferred_until": "",
    }


def pack_run_status_view(
    *,
    domain: str,
    resource_family: str,
    run_id: str | None,
    idempotency_key: str | None,
    canonical_key: str | None,
    augmentation_type: str | None,
    source_text_version: str | None,
    producer_name: str | None,
    producer_version: str | None,
    status: str | None,
    success: bool | None,
    reason_code: str | None,
    persisted_locally: bool | None,
    augmentation_stale: bool | None,
    last_updated_at: str | None,
) -> dict[str, Any]:
    return {
        "domain": domain,
        "resource_family": resource_family,
        "found": True,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "canonical_key": canonical_key,
        "augmentation_type": augmentation_type,
        "source_text_version": source_text_version,
        "producer_name": producer_name,
        "producer_version": producer_version,
        "status": status,
        "success": success,
        "reason_code": reason_code,
        "persisted_locally": persisted_locally,
        "augmentation_stale": augmentation_stale,
        "last_updated_at": last_updated_at,
    }


def pack_run_status_not_found(
    *,
    domain: str,
    resource_family: str,
    run_id: str | None,
    idempotency_key: str | None,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "domain": domain,
        "resource_family": resource_family,
        "found": False,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "status": None,
        "success": None,
        "reason_code": reason_code,
        "augmentation_stale": None,
        "last_updated_at": None,
    }


def pack_events_view(
    *,
    domain: str,
    resource_family: str,
    augmentation_applicable: bool,
    reason_code: str | None,
    message: str | None,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    out = {
        "domain": domain,
        "resource_family": resource_family,
        "augmentation_applicable": augmentation_applicable,
        "record_count": int(len(records)),
        "records": records,
    }
    if reason_code:
        out["reason_code"] = reason_code
    if message:
        out["message"] = message
    return out


def pack_additive_augmentation_meta(
    *,
    augmentation_types_present: list[str],
    artifact_event_ats: list[str],
    run_event_ats: list[str],
    recorded_source_versions: list[str],
    source_text_version: str | None,
    inspect_path: str | None,
) -> dict[str, Any]:
    last_augmented_at = max_nonempty_text(artifact_event_ats + run_event_ats)
    augmentation_stale: bool | None
    if not recorded_source_versions:
        augmentation_stale = None
    elif source_text_version is None:
        augmentation_stale = None
    else:
        augmentation_stale = source_text_version not in set(recorded_source_versions)
    return {
        "augmentation_available": bool(augmentation_types_present),
        "augmentation_types_present": sorted(augmentation_types_present),
        "last_augmented_at": last_augmented_at,
        "augmentation_stale": augmentation_stale,
        "inspect_path": inspect_path,
    }

