from __future__ import annotations

from datetime import date
from pathlib import Path
import re

from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import forecast_archive_accession_id, transcript_archive_accession_id
from py_earnings_calls.refdata.normalize import normalize_cik


_slug_re = re.compile(r"[^a-z0-9]+")


def slugify(value: str) -> str:
    text = str(value or "").strip().lower()
    text = _slug_re.sub("-", text).strip("-")
    return text or "unknown"


def transcript_html_path(
    config: AppConfig,
    *,
    provider: str,
    symbol: str,
    call_date: date,
    call_id: str,
    storage_cik: str | None = None,
) -> Path:
    return (
        transcript_bundle_root(config, call_id=call_id, storage_cik=storage_cik)
        / "raw.html"
    )


def transcript_text_path(
    config: AppConfig,
    *,
    provider: str,
    symbol: str,
    call_date: date,
    call_id: str,
    storage_cik: str | None = None,
) -> Path:
    return (
        transcript_bundle_root(config, call_id=call_id, storage_cik=storage_cik)
        / "parsed.txt"
    )


def transcript_json_path(
    config: AppConfig,
    *,
    provider: str,
    symbol: str,
    call_date: date,
    call_id: str,
    storage_cik: str | None = None,
) -> Path:
    return (
        transcript_bundle_root(config, call_id=call_id, storage_cik=storage_cik)
        / "parsed.json"
    )


def forecast_raw_snapshot_path(config: AppConfig, *, provider: str, symbol: str, as_of_date: date) -> Path:
    accession_id = forecast_archive_accession_id(provider=provider, symbol=symbol, as_of_date=as_of_date)
    return (
        config.forecasts_data_root
        / f"provider={slugify(provider)}"
        / f"symbol={symbol.upper()}"
        / f"as_of_date={as_of_date.isoformat()}"
        / accession_id
        / "raw.json"
    )


def normalized_path(config: AppConfig, name: str) -> Path:
    return config.normalized_refdata_root / f"{name}.parquet"


def transcript_storage_cik(value: object | None) -> str:
    normalized = normalize_cik(value)
    return normalized or "UNKNOWN"


def transcript_bundle_root(config: AppConfig, *, call_id: str, storage_cik: str | None) -> Path:
    cik_group = transcript_storage_cik(storage_cik)
    accession_id = transcript_archive_accession_id(call_id)
    return config.transcripts_data_root / f"cik={cik_group}" / accession_id


def transcript_archive_paths(config: AppConfig, *, call_id: str, storage_cik: str | None) -> dict[str, Path]:
    bundle_root = transcript_bundle_root(config, call_id=call_id, storage_cik=storage_cik)
    return {
        "bundle_root": bundle_root,
        "raw_html_path": bundle_root / "raw.html",
        "parsed_text_path": bundle_root / "parsed.txt",
        "parsed_json_path": bundle_root / "parsed.json",
    }


def forecast_bundle_root(config: AppConfig, *, provider: str, symbol: str, as_of_date: date) -> Path:
    accession_id = forecast_archive_accession_id(provider=provider, symbol=symbol, as_of_date=as_of_date)
    return (
        config.forecasts_data_root
        / f"provider={slugify(provider)}"
        / f"symbol={symbol.upper()}"
        / f"as_of_date={as_of_date.isoformat()}"
        / accession_id
    )


def forecast_archive_paths(config: AppConfig, *, provider: str, symbol: str, as_of_date: date) -> dict[str, Path]:
    bundle_root = forecast_bundle_root(config, provider=provider, symbol=symbol, as_of_date=as_of_date)
    return {
        "bundle_root": bundle_root,
        "raw_json_path": bundle_root / "raw.json",
    }


def transcript_full_index_manifest_path(config: AppConfig) -> Path:
    return config.transcripts_full_index_root / "transcript_archive_manifest.parquet"


def forecast_full_index_manifest_path(config: AppConfig) -> Path:
    return config.forecasts_full_index_root / "forecast_archive_manifest.parquet"


def legacy_transcript_html_path(config: AppConfig, *, provider: str, symbol: str, call_date: date, call_id: str) -> Path:
    return (
        config.legacy_transcript_raw_root
        / f"source={slugify(provider)}"
        / f"symbol={symbol.upper()}"
        / f"date={call_date.isoformat()}"
        / f"{call_id}.html"
    )


def legacy_transcript_text_path(config: AppConfig, *, provider: str, symbol: str, call_date: date, call_id: str) -> Path:
    return (
        config.legacy_transcript_parsed_root
        / f"source={slugify(provider)}"
        / f"symbol={symbol.upper()}"
        / f"date={call_date.isoformat()}"
        / f"{call_id}.txt"
    )


def legacy_transcript_json_path(config: AppConfig, *, provider: str, symbol: str, call_date: date, call_id: str) -> Path:
    return (
        config.legacy_transcript_parsed_root
        / f"source={slugify(provider)}"
        / f"symbol={symbol.upper()}"
        / f"date={call_date.isoformat()}"
        / f"{call_id}.json"
    )


def legacy_forecast_raw_snapshot_path(config: AppConfig, *, provider: str, symbol: str, as_of_date: date) -> Path:
    return (
        config.legacy_forecast_raw_root
        / f"provider={slugify(provider)}"
        / f"as_of_date={as_of_date.isoformat()}"
        / f"symbol={symbol.upper()}.json"
    )
