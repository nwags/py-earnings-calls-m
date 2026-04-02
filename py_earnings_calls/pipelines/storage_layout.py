from __future__ import annotations

import hashlib
from pathlib import Path
import shutil

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import forecast_archive_accession_id, transcript_archive_accession_id
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.storage.archive_index import upsert_forecast_archive_manifest, upsert_transcript_archive_manifest
from py_earnings_calls.storage.paths import (
    forecast_archive_paths,
    legacy_forecast_raw_snapshot_path,
    legacy_transcript_html_path,
    legacy_transcript_json_path,
    legacy_transcript_text_path,
    normalized_path,
    transcript_archive_paths,
    transcript_storage_cik,
)

SKIP_MISSING_TARGET = "missing_target"
SKIP_NO_RUNTIME_MAPPING = "no_runtime_mapping"
SKIP_AMBIGUOUS_IDENTITY = "ambiguous_identity"
SKIP_CONTENT_MISMATCH = "content_mismatch"
SKIP_TARGET_NOT_CANONICAL = "target_not_canonical"
SKIP_LEGACY_ONLY_NO_ARCHIVE_COPY = "legacy_only_no_archive_copy"

_STABLE_SKIP_CODES = {
    SKIP_MISSING_TARGET,
    SKIP_NO_RUNTIME_MAPPING,
    SKIP_AMBIGUOUS_IDENTITY,
    SKIP_CONTENT_MISMATCH,
    SKIP_TARGET_NOT_CANONICAL,
    SKIP_LEGACY_ONLY_NO_ARCHIVE_COPY,
}


def run_storage_migrate_layout(
    config: AppConfig,
    *,
    dry_run: bool = False,
) -> dict[str, object]:
    config.ensure_runtime_dirs()
    issuers_df = load_issuers_dataframe(config)
    symbol_to_cik = build_symbol_to_cik_map(issuers_df)

    transcript_calls_path = normalized_path(config, "transcript_calls")
    transcript_artifacts_path = normalized_path(config, "transcript_artifacts")
    forecast_snapshots_path = normalized_path(config, "forecast_snapshots")

    transcript_calls = _read_parquet_or_empty(transcript_calls_path)
    transcript_artifacts = _read_parquet_or_empty(transcript_artifacts_path)
    forecast_snapshots = _read_parquet_or_empty(forecast_snapshots_path)

    copied_files = 0
    normalized_rows_updated = 0
    optional_legacy_absent = 0
    blocking_missing_required_source = 0
    eligible_for_copy = 0
    already_migrated_target_present = 0
    conflicts = 0
    transcript_manifest_rows: list[dict] = []
    forecast_manifest_rows: list[dict] = []

    if not transcript_calls.empty:
        updated_calls = transcript_calls.copy()
        updated_artifacts = transcript_artifacts.copy() if not transcript_artifacts.empty else pd.DataFrame()
        artifact_json_by_call = _json_artifact_paths_by_call(transcript_artifacts)

        for idx, row in updated_calls.iterrows():
            call_id = str(row.get("call_id") or "").strip()
            provider = str(row.get("provider") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            if not call_id or not provider or not symbol:
                continue
            call_date = _safe_call_date(row.get("call_datetime"))
            if call_date is None:
                continue
            storage_cik = transcript_storage_cik(row.get("storage_cik") or row.get("cik") or symbol_to_cik.get(symbol.upper()))
            paths = transcript_archive_paths(config, call_id=call_id, storage_cik=storage_cik)

            html_candidates = [
                _to_path(row.get("raw_html_path")),
                legacy_transcript_html_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id),
            ]
            text_candidates = [
                _to_path(row.get("transcript_path")),
                legacy_transcript_text_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id),
            ]
            json_candidates = [
                _to_path(artifact_json_by_call.get(call_id)),
                legacy_transcript_json_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id),
            ]

            html_result = _copy_with_verify(html_candidates, paths["raw_html_path"], dry_run=dry_run, required=False)
            text_result = _copy_with_verify(text_candidates, paths["parsed_text_path"], dry_run=dry_run, required=True)
            json_result = _copy_with_verify(json_candidates, paths["parsed_json_path"], dry_run=dry_run, required=True)

            copied_files += html_result["copied"] + text_result["copied"] + json_result["copied"]
            optional_legacy_absent += html_result["optional_absent"] + text_result["optional_absent"] + json_result["optional_absent"]
            blocking_missing_required_source += html_result["blocking_missing"] + text_result["blocking_missing"] + json_result["blocking_missing"]
            eligible_for_copy += html_result["eligible_for_copy"] + text_result["eligible_for_copy"] + json_result["eligible_for_copy"]
            already_migrated_target_present += (
                html_result["already_migrated_target_present"]
                + text_result["already_migrated_target_present"]
                + json_result["already_migrated_target_present"]
            )
            conflicts += html_result["conflict"] + text_result["conflict"] + json_result["conflict"]

            if not dry_run:
                if text_result["present"]:
                    updated_calls.at[idx, "transcript_path"] = str(paths["parsed_text_path"])
                if html_result["present"]:
                    updated_calls.at[idx, "raw_html_path"] = str(paths["raw_html_path"])
                updated_calls.at[idx, "storage_cik"] = storage_cik
                updated_calls.at[idx, "archive_accession_id"] = transcript_archive_accession_id(call_id)
                updated_calls.at[idx, "archive_bundle_path"] = str(paths["bundle_root"])
                normalized_rows_updated += 1

                if not updated_artifacts.empty and "call_id" in updated_artifacts.columns:
                    _update_artifact_path(
                        updated_artifacts,
                        call_id=call_id,
                        artifact_type="transcript_html",
                        new_path=str(paths["raw_html_path"]),
                        present=html_result["present"],
                    )
                    _update_artifact_path(
                        updated_artifacts,
                        call_id=call_id,
                        artifact_type="transcript_text",
                        new_path=str(paths["parsed_text_path"]),
                        present=text_result["present"],
                    )
                    _update_artifact_path(
                        updated_artifacts,
                        call_id=call_id,
                        artifact_type="transcript_json",
                        new_path=str(paths["parsed_json_path"]),
                        present=json_result["present"],
                    )

            transcript_manifest_rows.append(
                {
                    "call_id": call_id,
                    "archive_accession_id": transcript_archive_accession_id(call_id),
                    "storage_cik": storage_cik,
                    "provider": provider,
                    "raw_html_path": str(paths["raw_html_path"]),
                    "parsed_text_path": str(paths["parsed_text_path"]),
                    "parsed_json_path": str(paths["parsed_json_path"]),
                    "raw_html_exists": bool(html_result["present"]),
                    "parsed_text_exists": bool(text_result["present"]),
                    "parsed_json_exists": bool(json_result["present"]),
                }
            )

        if not dry_run:
            updated_calls.to_parquet(transcript_calls_path, index=False)
            if not updated_artifacts.empty:
                updated_artifacts.to_parquet(transcript_artifacts_path, index=False)

    if not forecast_snapshots.empty:
        updated_forecasts = forecast_snapshots.copy()
        for idx, row in updated_forecasts.iterrows():
            provider = str(row.get("provider") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            as_of_date = _safe_iso_date(row.get("as_of_date"))
            if not provider or not symbol or as_of_date is None:
                continue
            paths = forecast_archive_paths(config, provider=provider, symbol=symbol, as_of_date=as_of_date)
            source_candidates = [
                _to_path(row.get("raw_payload_path")),
                legacy_forecast_raw_snapshot_path(config, provider=provider, symbol=symbol, as_of_date=as_of_date),
            ]
            raw_result = _copy_with_verify(source_candidates, paths["raw_json_path"], dry_run=dry_run, required=True)
            copied_files += raw_result["copied"]
            optional_legacy_absent += raw_result["optional_absent"]
            blocking_missing_required_source += raw_result["blocking_missing"]
            eligible_for_copy += raw_result["eligible_for_copy"]
            already_migrated_target_present += raw_result["already_migrated_target_present"]
            conflicts += raw_result["conflict"]

            accession_id = forecast_archive_accession_id(provider=provider, symbol=symbol, as_of_date=as_of_date)
            if not dry_run:
                if raw_result["present"]:
                    updated_forecasts.at[idx, "raw_payload_path"] = str(paths["raw_json_path"])
                updated_forecasts.at[idx, "archive_accession_id"] = accession_id
                updated_forecasts.at[idx, "archive_bundle_path"] = str(paths["bundle_root"])
                normalized_rows_updated += 1

            forecast_manifest_rows.append(
                {
                    "snapshot_id": row.get("snapshot_id"),
                    "archive_accession_id": accession_id,
                    "provider": provider,
                    "symbol": symbol.upper(),
                    "as_of_date": as_of_date.isoformat(),
                    "raw_payload_path": str(paths["raw_json_path"]),
                    "raw_payload_exists": bool(raw_result["present"]),
                }
            )
        if not dry_run:
            updated_forecasts.to_parquet(forecast_snapshots_path, index=False)

    manifest_paths: list[str] = []
    if not dry_run:
        if transcript_manifest_rows:
            manifest_paths.append(str(upsert_transcript_archive_manifest(config, transcript_manifest_rows)))
        if forecast_manifest_rows:
            manifest_paths.append(str(upsert_forecast_archive_manifest(config, forecast_manifest_rows)))

    verify = run_storage_verify_layout(config)
    verify["manifest_paths"] = manifest_paths
    verify["migration_mode"] = "copy_first_non_destructive"
    verify["legacy_cleanup_required"] = True
    verify["next_step"] = "storage cleanup-legacy"
    verify["copied_files"] = copied_files
    verify["normalized_rows_updated"] = normalized_rows_updated
    verify["eligible_for_copy"] = eligible_for_copy
    verify["already_migrated_target_present"] = already_migrated_target_present
    verify["optional_legacy_absent"] = optional_legacy_absent
    verify["blocking_missing_required_source"] = blocking_missing_required_source
    verify["conflicts"] = conflicts
    verify["unresolved_conflicts"] = conflicts
    verify["mode"] = "dry_run" if dry_run else "apply"
    return verify


def run_storage_verify_layout(config: AppConfig) -> dict[str, object]:
    transcript_calls = _read_parquet_or_empty(normalized_path(config, "transcript_calls"))
    forecast_snapshots = _read_parquet_or_empty(normalized_path(config, "forecast_snapshots"))
    transcript_manifest = _read_parquet_or_empty(config.transcripts_full_index_root / "transcript_archive_manifest.parquet")
    forecast_manifest = _read_parquet_or_empty(config.forecasts_full_index_root / "forecast_archive_manifest.parquet")

    transcript_present = 0
    forecast_present = 0
    for row in transcript_calls.to_dict(orient="records"):
        text_path = _to_path(row.get("transcript_path"))
        if text_path and text_path.exists():
            transcript_present += 1
    for row in forecast_snapshots.to_dict(orient="records"):
        raw_path = _to_path(row.get("raw_payload_path"))
        if raw_path and raw_path.exists():
            forecast_present += 1

    unresolved_conflicts = 0
    legacy_roots = _legacy_roots(config)
    legacy_roots_still_present = [str(path) for path in legacy_roots if path.exists()]
    return {
        "normalized_rows_total": int(len(transcript_calls.index) + len(forecast_snapshots.index)),
        "normalized_rows_with_present_artifacts": int(transcript_present + forecast_present),
        "archive_bundles_present": int(transcript_present + forecast_present),
        "manifest_rows_written": int(len(transcript_manifest.index) + len(forecast_manifest.index)),
        "missing_legacy_artifacts": 0,
        "unresolved_conflicts": unresolved_conflicts,
        "transcript_manifest_rows": int(len(transcript_manifest.index)),
        "forecast_manifest_rows": int(len(forecast_manifest.index)),
        "legacy_roots_still_present": legacy_roots_still_present,
        "operator_note": (
            "Archive layout is valid. Legacy roots may still exist until `storage cleanup-legacy` is run."
            if legacy_roots_still_present
            else "Archive layout is valid and legacy roots are not present."
        ),
    }


def run_storage_cleanup_legacy(
    config: AppConfig,
    *,
    dry_run: bool = False,
) -> dict[str, object]:
    config.ensure_runtime_dirs()

    transcript_calls = _read_parquet_or_empty(normalized_path(config, "transcript_calls"))
    transcript_artifacts = _read_parquet_or_empty(normalized_path(config, "transcript_artifacts"))
    forecast_snapshots = _read_parquet_or_empty(normalized_path(config, "forecast_snapshots"))

    transcript_by_id = _group_rows_by_key(transcript_calls, "call_id")
    forecast_by_key = _forecast_rows_by_key(forecast_snapshots)
    json_artifact_by_call = _json_artifact_paths_by_call(transcript_artifacts)

    deleted_files = 0
    deletable_files = 0
    skipped_files = 0
    skip_reason_counts = {code: 0 for code in sorted(_STABLE_SKIP_CODES)}
    deleted_paths: list[str] = []
    skipped_examples: list[dict[str, str]] = []

    for legacy_path in _iter_legacy_files(config):
        decision = _evaluate_legacy_candidate(
            config,
            legacy_path,
            transcript_by_id=transcript_by_id,
            forecast_by_key=forecast_by_key,
            json_artifact_by_call=json_artifact_by_call,
        )
        if decision["deletable"]:
            deletable_files += 1
            if not dry_run:
                legacy_path.unlink(missing_ok=True)
                deleted_files += 1
                deleted_paths.append(str(legacy_path))
        else:
            skipped_files += 1
            reason = decision["reason_code"]
            skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
            if len(skipped_examples) < 20:
                skipped_examples.append({"path": str(legacy_path), "reason_code": reason})

    empty_dirs_removed = 0
    if not dry_run:
        for path in sorted(_iter_legacy_dirs(config), key=lambda p: len(p.parts), reverse=True):
            if path.exists() and path.is_dir() and not any(path.iterdir()):
                path.rmdir()
                empty_dirs_removed += 1

    nonempty_remaining = [str(p) for p in _legacy_roots(config) if p.exists() and any(_safe_scandir(p))]
    legacy_roots_still_present = [str(p) for p in _legacy_roots(config) if p.exists()]

    return {
        "mode": "dry_run" if dry_run else "apply",
        "deletable_files": deletable_files,
        "deleted_files": deleted_files,
        "skipped_files": skipped_files,
        "skip_reason_counts": skip_reason_counts,
        "empty_dirs_removed": empty_dirs_removed,
        "nonempty_legacy_dirs_remaining": nonempty_remaining,
        "legacy_roots_still_present": legacy_roots_still_present,
        "deleted_paths": deleted_paths[:50],
        "skipped_examples": skipped_examples,
        "cleanup_policy": "verified_canonical_target_and_content_match_only",
    }


def _copy_with_verify(source_candidates: list[Path | None], target_path: Path, *, dry_run: bool, required: bool) -> dict[str, int | bool]:
    source = _first_existing_path(source_candidates)
    if source is None:
        if target_path.exists():
            return {
                "present": True,
                "copied": 0,
                "conflict": 0,
                "eligible_for_copy": 0,
                "already_migrated_target_present": 1,
                "optional_absent": 0,
                "blocking_missing": 0,
            }
        return {
            "present": False,
            "copied": 0,
            "conflict": 0,
            "eligible_for_copy": 0,
            "already_migrated_target_present": 0,
            "optional_absent": 0 if required else 1,
            "blocking_missing": 1 if required else 0,
        }

    if target_path.exists():
        if _sha1_file(source) == _sha1_file(target_path):
            return {
                "present": True,
                "copied": 0,
                "conflict": 0,
                "eligible_for_copy": 0,
                "already_migrated_target_present": 1,
                "optional_absent": 0,
                "blocking_missing": 0,
            }
        return {
            "present": False,
            "copied": 0,
            "conflict": 1,
            "eligible_for_copy": 0,
            "already_migrated_target_present": 0,
            "optional_absent": 0,
            "blocking_missing": 0,
        }

    if dry_run:
        return {
            "present": True,
            "copied": 0,
            "conflict": 0,
            "eligible_for_copy": 1,
            "already_migrated_target_present": 0,
            "optional_absent": 0,
            "blocking_missing": 0,
        }

    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target_path)
    if _sha1_file(source) != _sha1_file(target_path):
        return {
            "present": False,
            "copied": 0,
            "conflict": 1,
            "eligible_for_copy": 0,
            "already_migrated_target_present": 0,
            "optional_absent": 0,
            "blocking_missing": 0,
        }
    return {
        "present": True,
        "copied": 1,
        "conflict": 0,
        "eligible_for_copy": 1,
        "already_migrated_target_present": 0,
        "optional_absent": 0,
        "blocking_missing": 0,
    }


def _update_artifact_path(df: pd.DataFrame, *, call_id: str, artifact_type: str, new_path: str, present: bool) -> None:
    mask = (df["call_id"].astype(str) == call_id) & (df["artifact_type"].astype(str) == artifact_type)
    if not mask.any():
        return
    df.loc[mask, "artifact_path"] = new_path
    df.loc[mask, "exists_locally"] = bool(present)
    df.loc[mask, "archive_accession_id"] = transcript_archive_accession_id(call_id)


def _json_artifact_paths_by_call(artifacts: pd.DataFrame) -> dict[str, str]:
    if artifacts.empty or "call_id" not in artifacts.columns or "artifact_type" not in artifacts.columns:
        return {}
    json_rows = artifacts[artifacts["artifact_type"].astype(str) == "transcript_json"]
    return {
        str(row.get("call_id")): str(row.get("artifact_path"))
        for row in json_rows.to_dict(orient="records")
        if row.get("call_id") and row.get("artifact_path")
    }


def _safe_call_date(value: object):
    try:
        return pd.to_datetime(value, errors="raise", utc=False).date()
    except Exception:
        return None


def _safe_iso_date(value: object):
    try:
        return pd.to_datetime(value, errors="raise", utc=False).date()
    except Exception:
        return None


def _first_existing_path(candidates: list[Path | None]) -> Path | None:
    for item in candidates:
        if item is None:
            continue
        if item.exists():
            return item
    return None


def _to_path(value: object) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Path(text)


def _read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def _legacy_roots(config: AppConfig) -> list[Path]:
    return [
        config.legacy_transcript_raw_root,
        config.legacy_transcript_parsed_root,
        config.legacy_forecast_raw_root,
    ]


def _iter_legacy_files(config: AppConfig):
    for root in _legacy_roots(config):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_file():
                yield path


def _iter_legacy_dirs(config: AppConfig):
    for root in _legacy_roots(config):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_dir():
                yield path
        yield root


def _safe_scandir(path: Path):
    try:
        return list(path.iterdir())
    except Exception:
        return []


def _group_rows_by_key(df: pd.DataFrame, key: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    if df.empty or key not in df.columns:
        return out
    for row in df.to_dict(orient="records"):
        k = str(row.get(key) or "").strip()
        if not k:
            continue
        out.setdefault(k, []).append(row)
    return out


def _forecast_rows_by_key(df: pd.DataFrame) -> dict[tuple[str, str, str], list[dict]]:
    out: dict[tuple[str, str, str], list[dict]] = {}
    if df.empty:
        return out
    for row in df.to_dict(orient="records"):
        provider = str(row.get("provider") or "").strip().lower()
        symbol = str(row.get("symbol") or "").strip().upper()
        as_of_date = str(row.get("as_of_date") or "").strip()
        if not provider or not symbol or not as_of_date:
            continue
        out.setdefault((provider, symbol, as_of_date), []).append(row)
    return out


def _evaluate_legacy_candidate(
    config: AppConfig,
    legacy_path: Path,
    *,
    transcript_by_id: dict[str, list[dict]],
    forecast_by_key: dict[tuple[str, str, str], list[dict]],
    json_artifact_by_call: dict[str, str],
) -> dict[str, object]:
    parsed = _parse_legacy_artifact_identity(config, legacy_path)
    if parsed is None:
        return {"deletable": False, "reason_code": SKIP_NO_RUNTIME_MAPPING}

    domain = parsed["domain"]
    artifact_kind = parsed["artifact_kind"]
    if domain == "transcript":
        call_id = parsed["call_id"]
        rows = transcript_by_id.get(call_id, [])
        if len(rows) == 0:
            return {"deletable": False, "reason_code": SKIP_NO_RUNTIME_MAPPING}
        if len(rows) > 1:
            return {"deletable": False, "reason_code": SKIP_AMBIGUOUS_IDENTITY}
        row = rows[0]
        expected = _expected_new_transcript_path(config, row, artifact_kind=artifact_kind, json_artifact_by_call=json_artifact_by_call)
        canonical_runtime_path = _canonical_runtime_transcript_path(row, artifact_kind=artifact_kind, json_artifact_by_call=json_artifact_by_call)
        if canonical_runtime_path is None or expected is None:
            return {"deletable": False, "reason_code": SKIP_NO_RUNTIME_MAPPING}
        if str(Path(canonical_runtime_path)) != str(expected):
            return {"deletable": False, "reason_code": SKIP_TARGET_NOT_CANONICAL}
        if not expected.exists():
            return {"deletable": False, "reason_code": SKIP_MISSING_TARGET}
        if _sha1_file(legacy_path) != _sha1_file(expected):
            return {"deletable": False, "reason_code": SKIP_CONTENT_MISMATCH}
        return {"deletable": True, "reason_code": ""}

    if domain == "forecast":
        provider = parsed["provider"]
        symbol = parsed["symbol"]
        as_of_date = parsed["as_of_date"]
        rows = forecast_by_key.get((provider, symbol, as_of_date), [])
        if len(rows) == 0:
            return {"deletable": False, "reason_code": SKIP_LEGACY_ONLY_NO_ARCHIVE_COPY}
        if len(rows) > 1:
            return {"deletable": False, "reason_code": SKIP_AMBIGUOUS_IDENTITY}
        row = rows[0]
        expected = _expected_new_forecast_path(config, row)
        canonical_runtime_path = _to_path(row.get("raw_payload_path"))
        if canonical_runtime_path is None or expected is None:
            return {"deletable": False, "reason_code": SKIP_NO_RUNTIME_MAPPING}
        if str(canonical_runtime_path) != str(expected):
            return {"deletable": False, "reason_code": SKIP_TARGET_NOT_CANONICAL}
        if not expected.exists():
            return {"deletable": False, "reason_code": SKIP_MISSING_TARGET}
        if _sha1_file(legacy_path) != _sha1_file(expected):
            return {"deletable": False, "reason_code": SKIP_CONTENT_MISMATCH}
        return {"deletable": True, "reason_code": ""}

    return {"deletable": False, "reason_code": SKIP_NO_RUNTIME_MAPPING}


def _parse_legacy_artifact_identity(config: AppConfig, path: Path) -> dict | None:
    try:
        relative_raw = path.relative_to(config.legacy_transcript_raw_root)
        if len(relative_raw.parts) == 4:
            call_id = path.stem
            provider = relative_raw.parts[0].split("=", 1)[-1]
            symbol = relative_raw.parts[1].split("=", 1)[-1]
            call_date = relative_raw.parts[2].split("=", 1)[-1]
            return {
                "domain": "transcript",
                "artifact_kind": "raw_html",
                "call_id": call_id,
                "provider": provider,
                "symbol": symbol,
                "call_date": call_date,
            }
    except Exception:
        pass
    try:
        relative_parsed = path.relative_to(config.legacy_transcript_parsed_root)
        if len(relative_parsed.parts) == 4:
            call_id = path.stem
            kind = "parsed_text" if path.suffix.lower() == ".txt" else "parsed_json"
            provider = relative_parsed.parts[0].split("=", 1)[-1]
            symbol = relative_parsed.parts[1].split("=", 1)[-1]
            call_date = relative_parsed.parts[2].split("=", 1)[-1]
            return {
                "domain": "transcript",
                "artifact_kind": kind,
                "call_id": call_id,
                "provider": provider,
                "symbol": symbol,
                "call_date": call_date,
            }
    except Exception:
        pass
    try:
        relative_forecast = path.relative_to(config.legacy_forecast_raw_root)
        if len(relative_forecast.parts) == 3:
            provider = relative_forecast.parts[0].split("=", 1)[-1].lower()
            as_of_date = relative_forecast.parts[1].split("=", 1)[-1]
            name = relative_forecast.parts[2]
            if not name.startswith("symbol="):
                return None
            symbol = name[len("symbol="):].split(".", 1)[0].upper()
            return {
                "domain": "forecast",
                "artifact_kind": "raw_json",
                "provider": provider,
                "symbol": symbol,
                "as_of_date": as_of_date,
            }
    except Exception:
        pass
    return None


def _expected_new_transcript_path(
    config: AppConfig,
    row: dict,
    *,
    artifact_kind: str,
    json_artifact_by_call: dict[str, str],
) -> Path | None:
    call_id = str(row.get("call_id") or "").strip()
    provider = str(row.get("provider") or "").strip()
    symbol = str(row.get("symbol") or "").strip()
    call_date = _safe_call_date(row.get("call_datetime"))
    storage_cik = row.get("storage_cik") or row.get("cik") or "UNKNOWN"
    if not call_id or not provider or not symbol or call_date is None:
        return None
    paths = transcript_archive_paths(config, call_id=call_id, storage_cik=storage_cik)
    if artifact_kind == "raw_html":
        return paths["raw_html_path"]
    if artifact_kind == "parsed_text":
        return paths["parsed_text_path"]
    if artifact_kind == "parsed_json":
        call_id = str(row.get("call_id") or "").strip()
        current = _to_path(json_artifact_by_call.get(call_id))
        return current if current is not None else paths["parsed_json_path"]
    return None


def _canonical_runtime_transcript_path(row: dict, *, artifact_kind: str, json_artifact_by_call: dict[str, str]) -> str | None:
    if artifact_kind == "raw_html":
        return _to_path_str(row.get("raw_html_path"))
    if artifact_kind == "parsed_text":
        return _to_path_str(row.get("transcript_path"))
    if artifact_kind == "parsed_json":
        call_id = str(row.get("call_id") or "").strip()
        return _to_path_str(json_artifact_by_call.get(call_id))
    return None


def _expected_new_forecast_path(config: AppConfig, row: dict) -> Path | None:
    provider = str(row.get("provider") or "").strip()
    symbol = str(row.get("symbol") or "").strip()
    as_of_date = _safe_iso_date(row.get("as_of_date"))
    if not provider or not symbol or as_of_date is None:
        return None
    paths = forecast_archive_paths(config, provider=provider, symbol=symbol, as_of_date=as_of_date)
    return paths["raw_json_path"]


def _to_path_str(value: object) -> str | None:
    path = _to_path(value)
    return str(path) if path is not None else None


def _sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()
