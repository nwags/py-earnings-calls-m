from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.storage.paths import normalized_path


AUDIT_PROVIDER_MOTLEY_FOOL = "motley_fool"

BUCKET_TRANSCRIPT_VISIBLE = "transcript_visible"
BUCKET_TRANSCRIPT_STRUCTURED = "transcript_structured"
BUCKET_ARTICLE_PUBLISHED = "article_published"
BUCKET_NONE = "none"
BUCKET_LEGACY_UNKNOWN = "legacy_unknown"
BUCKET_MISSING_DATETIME = "missing_datetime"

_WEAKNESS_ORDER = {
    BUCKET_MISSING_DATETIME: 0,
    BUCKET_LEGACY_UNKNOWN: 1,
    BUCKET_NONE: 2,
    BUCKET_ARTICLE_PUBLISHED: 3,
    BUCKET_TRANSCRIPT_STRUCTURED: 4,
    BUCKET_TRANSCRIPT_VISIBLE: 5,
}

_KNOWN_SOURCE_BUCKETS = {
    BUCKET_TRANSCRIPT_VISIBLE,
    BUCKET_TRANSCRIPT_STRUCTURED,
    BUCKET_ARTICLE_PUBLISHED,
    BUCKET_NONE,
}


def run_transcript_datetime_audit(
    config: AppConfig,
    *,
    provider: str = AUDIT_PROVIDER_MOTLEY_FOOL,
    limit: int = 50,
    write_manifest_path: str | None = None,
) -> dict[str, Any]:
    if provider != AUDIT_PROVIDER_MOTLEY_FOOL:
        raise ValueError(f"Unsupported provider for datetime audit: {provider}")
    if limit <= 0:
        raise ValueError("`limit` must be greater than 0.")

    calls_path = normalized_path(config, "transcript_calls")
    if not calls_path.exists():
        return _empty_summary(provider=provider, limit=limit, write_manifest_path=write_manifest_path)

    calls_df = pd.read_parquet(calls_path)
    if calls_df.empty:
        return _empty_summary(provider=provider, limit=limit, write_manifest_path=write_manifest_path)

    fetched_df = _filter_fetched_provider_rows(calls_df, provider=provider)
    if fetched_df.empty:
        return _empty_summary(provider=provider, limit=limit, write_manifest_path=write_manifest_path)

    prepared = _prepare_audit_rows(fetched_df)
    suspect = prepared[prepared["is_suspect"] == True].copy()  # noqa: E712
    suspect = _sort_suspects(suspect)
    suspect = suspect.head(limit).copy()

    suspect_rows = [
        {
            "url": str(row.get("url") or ""),
            "symbol": str(row.get("symbol") or ""),
            "call_id": str(row.get("call_id") or ""),
            "current_call_datetime": str(row.get("current_call_datetime") or ""),
            "current_call_datetime_source": row.get("current_call_datetime_source"),
            "audit_bucket": str(row.get("audit_bucket") or ""),
        }
        for row in suspect.to_dict(orient="records")
    ]

    manifest_written = False
    manifest_path = None
    if write_manifest_path:
        manifest_path = _write_manifest(write_manifest_path, suspect_rows)
        manifest_written = True

    total = int(len(prepared.index))
    bucket_series = prepared["normalized_source_bucket"].astype(str)
    missing_series = prepared["missing_datetime"].astype(bool)
    summary = {
        "provider": provider,
        "limit": int(limit),
        "total_fetched_rows_considered": total,
        "rows_with_transcript_visible_datetime": int((bucket_series == BUCKET_TRANSCRIPT_VISIBLE).sum()),
        "rows_with_transcript_structured_datetime": int((bucket_series == BUCKET_TRANSCRIPT_STRUCTURED).sum()),
        "rows_with_article_published_datetime": int((bucket_series == BUCKET_ARTICLE_PUBLISHED).sum()),
        "rows_with_missing_datetime": int(missing_series.sum()),
        "rows_with_legacy_unknown_source": int((bucket_series == BUCKET_LEGACY_UNKNOWN).sum()),
        "suspect_rows_count": int((prepared["is_suspect"] == True).sum()),  # noqa: E712
        "suspect_rows_sample": suspect_rows,
        "manifest_written": manifest_written,
    }
    if manifest_path is not None:
        summary["manifest_path"] = str(manifest_path)
    return summary


def _empty_summary(*, provider: str, limit: int, write_manifest_path: str | None) -> dict[str, Any]:
    manifest_written = False
    manifest_path = None
    if write_manifest_path:
        manifest_path = _write_manifest(write_manifest_path, [])
        manifest_written = True
    summary: dict[str, Any] = {
        "provider": provider,
        "limit": int(limit),
        "total_fetched_rows_considered": 0,
        "rows_with_transcript_visible_datetime": 0,
        "rows_with_transcript_structured_datetime": 0,
        "rows_with_article_published_datetime": 0,
        "rows_with_missing_datetime": 0,
        "rows_with_legacy_unknown_source": 0,
        "suspect_rows_count": 0,
        "suspect_rows_sample": [],
        "manifest_written": manifest_written,
    }
    if manifest_path is not None:
        summary["manifest_path"] = str(manifest_path)
    return summary


def _filter_fetched_provider_rows(calls_df: pd.DataFrame, *, provider: str) -> pd.DataFrame:
    required = calls_df.copy()
    required["provider"] = _column_as_string(required, "provider").str.lower()
    required["source_url"] = _column_as_string(required, "source_url")
    required["provider_call_id"] = _column_as_string(required, "provider_call_id")
    provider_rows = required[required["provider"] == provider].copy()
    if provider_rows.empty:
        return provider_rows
    has_url = provider_rows["source_url"].str.lower().str.startswith(("http://", "https://"))
    has_provider_url = provider_rows["provider_call_id"].str.lower().str.startswith(("http://", "https://"))
    return provider_rows[has_url | has_provider_url].copy()


def _prepare_audit_rows(rows: pd.DataFrame) -> pd.DataFrame:
    prepared = rows.copy()
    prepared["call_datetime"] = pd.to_datetime(_column_nullable(prepared, "call_datetime"), errors="coerce")
    prepared["current_call_datetime"] = prepared["call_datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    prepared["current_call_datetime"] = prepared["current_call_datetime"].fillna("")
    prepared["current_call_datetime_source"] = _column_nullable(prepared, "call_datetime_source")
    prepared["normalized_source_bucket"] = prepared["current_call_datetime_source"].apply(_normalize_source_bucket)
    prepared["missing_datetime"] = prepared["call_datetime"].isna()
    prepared["audit_bucket"] = prepared.apply(_audit_bucket_for_row, axis=1)
    prepared["is_suspect"] = prepared.apply(_is_suspect_row, axis=1)
    prepared["url"] = prepared.apply(_extract_row_url, axis=1)
    prepared["symbol"] = _column_as_string(prepared, "symbol").str.upper()
    prepared["call_id"] = _column_as_string(prepared, "call_id")
    return prepared


def _normalize_source_bucket(raw: object) -> str:
    source = str(raw or "").strip()
    if not source:
        return BUCKET_LEGACY_UNKNOWN
    if source in _KNOWN_SOURCE_BUCKETS:
        return source
    return BUCKET_LEGACY_UNKNOWN


def _audit_bucket_for_row(row: pd.Series) -> str:
    if bool(row.get("missing_datetime")):
        return BUCKET_MISSING_DATETIME
    return str(row.get("normalized_source_bucket") or BUCKET_LEGACY_UNKNOWN)


def _is_suspect_row(row: pd.Series) -> bool:
    bucket = str(row.get("audit_bucket") or "")
    return bucket in {BUCKET_MISSING_DATETIME, BUCKET_LEGACY_UNKNOWN, BUCKET_NONE, BUCKET_ARTICLE_PUBLISHED}


def _extract_row_url(row: pd.Series) -> str:
    source_url = str(row.get("source_url") or "").strip()
    if source_url:
        return source_url
    provider_call_id = str(row.get("provider_call_id") or "").strip()
    return provider_call_id


def _sort_suspects(suspect: pd.DataFrame) -> pd.DataFrame:
    if suspect.empty:
        return suspect
    ordered = suspect.copy()
    ordered["bucket_rank"] = ordered["audit_bucket"].map(_WEAKNESS_ORDER).fillna(99).astype(int)
    ordered["has_datetime"] = ordered["call_datetime"].notna()
    # Newest first for rows with datetime; missing datetime rows remain deterministic.
    ordered["datetime_rank"] = ordered["call_datetime"].apply(
        lambda value: -int(value.value) if pd.notna(value) else 2**63 - 1
    )
    ordered["call_id_rank"] = ordered["call_id"].astype(str)
    ordered = ordered.sort_values(
        by=["bucket_rank", "datetime_rank", "call_id_rank"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    return ordered


def _write_manifest(path_value: str, suspect_rows: list[dict[str, Any]]) -> Path:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "url",
        "symbol",
        "call_id",
        "current_call_datetime",
        "current_call_datetime_source",
        "audit_bucket",
    ]
    rows = []
    for item in suspect_rows:
        rows.append({column: item.get(column) for column in columns})
    manifest_df = pd.DataFrame(rows, columns=columns)
    manifest_df.to_csv(path, index=False)
    return path.resolve()


def _column_as_string(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([""] * len(df.index), index=df.index, dtype="object")
    return df[column].fillna("").astype(str).str.strip()


def _column_nullable(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([None] * len(df.index), index=df.index, dtype="object")
    return df[column]
