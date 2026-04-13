from __future__ import annotations

from datetime import date
from pathlib import Path
from hashlib import sha256

import pandas as pd

from py_earnings_calls.augmentation import transcript_augmentation_meta
from py_earnings_calls.augmentation import (
    submit_producer_artifact,
    submit_producer_run,
    transcript_target_descriptor,
)
from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import normalize_cik
from py_earnings_calls.lookup import (
    load_issuers_dataframe,
    load_lookup_dataframe,
    query_forecasts,
    query_forecasts_by_cik,
    query_transcripts,
)
from py_earnings_calls.resolution import ResolutionMode
from py_earnings_calls.resolution_service import ProviderAwareResolutionService
from py_earnings_calls.storage.paths import legacy_transcript_text_path, normalized_path


class LocalLookupService:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._resolver = ProviderAwareResolutionService(config)

    def get_transcript_metadata(self, call_id: str) -> dict | None:
        df = load_lookup_dataframe(self._config, "transcripts")
        matches = query_transcripts(df, call_id=call_id)
        if matches.empty:
            return None
        return _sanitize_transcript_payload(matches.iloc[0].to_dict())

    def get_transcript_target_descriptor(self, call_id: str) -> dict[str, object]:
        return transcript_target_descriptor(self._config, call_id=call_id)

    def submit_augmentation_run(self, payload: dict[str, object]) -> dict[str, object]:
        return submit_producer_run(self._config, payload)

    def submit_augmentation_artifact(self, payload: dict[str, object]) -> dict[str, object]:
        return submit_producer_artifact(self._config, payload)

    def get_transcript_metadata_with_resolution(
        self,
        call_id: str,
        *,
        resolution_mode: ResolutionMode = ResolutionMode.LOCAL_ONLY,
    ) -> tuple[dict | None, dict[str, object]]:
        metadata = self.get_transcript_metadata(call_id)
        if metadata is not None:
            source_text_version = _source_text_version_from_metadata(metadata)
            return metadata, {
                "served_from": "local_hit",
                "resolution_mode": resolution_mode.value,
                "remote_attempted": False,
                "provider_requested": metadata.get("provider"),
                "provider_used": metadata.get("provider"),
                "method_used": "local_lookup",
                "success": True,
                "reason_code": "LOCAL_HIT",
                "persisted_locally": False,
                "rate_limited": False,
                "retry_count": 0,
                "deferred_until": None,
                "augmentation_meta": transcript_augmentation_meta(
                    self._config,
                    call_id=call_id,
                    source_text_version=source_text_version,
                ),
            }
        result = self._resolver.resolve_transcript_if_missing(call_id=call_id, resolution_mode=resolution_mode)
        metadata = self.get_transcript_metadata(call_id)
        source_text_version = _source_text_version_from_metadata(metadata) if metadata is not None else None
        return metadata, {
            "served_from": result.served_from,
            "resolution_mode": result.resolution_mode,
            "remote_attempted": resolution_mode != ResolutionMode.LOCAL_ONLY,
            "provider_requested": result.provider_requested,
            "provider_used": result.provider_used,
            "method_used": result.method_used,
            "success": result.success,
            "reason_code": result.reason_code,
            "persisted_locally": result.persisted_locally,
            "rate_limited": result.rate_limited,
            "retry_count": result.retry_count,
            "deferred_until": result.deferred_until,
            "augmentation_meta": transcript_augmentation_meta(
                self._config,
                call_id=call_id,
                source_text_version=source_text_version,
            ),
        }

    def list_transcripts(
        self,
        *,
        ticker: str | None = None,
        cik: str | None = None,
        start: date | None = None,
        end: date | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, object]:
        df = load_lookup_dataframe(self._config, "transcripts")
        issuers_df = load_issuers_dataframe(self._config)

        # Total is computed before pagination so the envelope can support paging clients.
        total_matches = query_transcripts(
            df,
            symbol=ticker,
            cik=cik,
            start=start,
            end=end,
            limit=None,
            offset=0,
            issuers_df=issuers_df,
        )
        page = query_transcripts(
            df,
            symbol=ticker,
            cik=cik,
            start=start,
            end=end,
            limit=limit,
            offset=offset,
            issuers_df=issuers_df,
        )
        return {
            "items": [_sanitize_transcript_payload(item) for item in page.to_dict(orient="records")],
            "limit": limit,
            "offset": offset,
            "total": int(len(total_matches.index)),
        }

    def get_transcript_content(self, call_id: str) -> str | None:
        metadata = self.get_transcript_metadata(call_id)
        if metadata is None:
            return None
        transcript_path = metadata.get("transcript_path")
        if transcript_path:
            path = Path(str(transcript_path))
            if path.exists():
                return path.read_text(encoding="utf-8")

        # Transition fallback: allow legacy path reads until migration has run.
        provider = str(metadata.get("provider") or "").strip()
        symbol = str(metadata.get("symbol") or "").strip()
        call_datetime = metadata.get("call_datetime")
        if provider and symbol and call_datetime:
            try:
                call_date = pd.to_datetime(call_datetime, errors="raise", utc=False).date()
                legacy = legacy_transcript_text_path(
                    self._config,
                    provider=provider,
                    symbol=symbol,
                    call_date=call_date,
                    call_id=call_id,
                )
                if legacy.exists():
                    return legacy.read_text(encoding="utf-8")
            except Exception:
                return None
        return None

    def get_transcript_content_with_resolution(
        self,
        call_id: str,
        *,
        resolution_mode: ResolutionMode = ResolutionMode.LOCAL_ONLY,
    ) -> tuple[str | None, dict[str, object]]:
        content = self.get_transcript_content(call_id)
        if content is not None:
            source_text_version = f"sha256:{sha256(content.encode('utf-8')).hexdigest()}"
            return content, {
                "served_from": "local_hit",
                "resolution_mode": resolution_mode.value,
                "remote_attempted": False,
                "provider_requested": None,
                "provider_used": None,
                "method_used": "local_file",
                "success": True,
                "reason_code": "LOCAL_HIT",
                "persisted_locally": False,
                "rate_limited": False,
                "retry_count": 0,
                "deferred_until": None,
                "augmentation_meta": transcript_augmentation_meta(
                    self._config,
                    call_id=call_id,
                    source_text_version=source_text_version,
                ),
            }
        result = self._resolver.resolve_transcript_if_missing(call_id=call_id, resolution_mode=resolution_mode)
        content = self.get_transcript_content(call_id)
        source_text_version = f"sha256:{sha256(content.encode('utf-8')).hexdigest()}" if content is not None else None
        return content, {
            "served_from": result.served_from,
            "resolution_mode": result.resolution_mode,
            "remote_attempted": resolution_mode != ResolutionMode.LOCAL_ONLY,
            "provider_requested": result.provider_requested,
            "provider_used": result.provider_used,
            "method_used": result.method_used,
            "success": result.success and content is not None,
            "reason_code": result.reason_code,
            "persisted_locally": result.persisted_locally,
            "rate_limited": result.rate_limited,
            "retry_count": result.retry_count,
            "deferred_until": result.deferred_until,
            "augmentation_meta": transcript_augmentation_meta(
                self._config,
                call_id=call_id,
                source_text_version=source_text_version,
            ),
        }

    def get_latest_forecast(self, symbol: str) -> dict | None:
        df = load_lookup_dataframe(self._config, "forecasts")
        matches = query_forecasts(df, symbol=symbol)
        if matches.empty:
            return None
        latest_as_of = matches["as_of_date"].astype(str).iloc[0]
        latest = matches[matches["as_of_date"].astype(str) == latest_as_of]
        return {
            "symbol": symbol.upper(),
            "as_of_date": latest_as_of,
            "points": latest.to_dict(orient="records"),
        }

    def list_forecasts_by_cik(
        self,
        *,
        cik: str,
        as_of_date: date | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, object]:
        normalized_cik = normalize_cik(cik)
        if normalized_cik is None:
            raise ValueError("Invalid cik. Expected up to 10 digits after normalization.")
        df = load_lookup_dataframe(self._config, "forecasts_by_cik")
        total_matches = query_forecasts_by_cik(
            df,
            cik=normalized_cik,
            as_of_date=as_of_date,
            limit=None,
            offset=0,
        )
        page = query_forecasts_by_cik(
            df,
            cik=normalized_cik,
            as_of_date=as_of_date,
            limit=limit,
            offset=offset,
        )
        return {
            "cik": normalized_cik,
            "as_of_date": as_of_date.isoformat() if as_of_date is not None else None,
            "items": page.to_dict(orient="records"),
            "limit": limit,
            "offset": offset,
            "total": int(len(total_matches.index)),
        }

    def get_forecast_snapshot(
        self,
        *,
        provider: str,
        symbol: str,
        as_of_date: date,
        resolution_mode: ResolutionMode = ResolutionMode.LOCAL_ONLY,
    ) -> tuple[dict | None, dict[str, object]]:
        payload = self._local_forecast_snapshot(provider=provider, symbol=symbol, as_of_date=as_of_date)
        if payload is not None:
            return payload, {
                "served_from": "local_hit",
                "resolution_mode": resolution_mode.value,
                "remote_attempted": False,
                "provider_requested": provider.lower(),
                "provider_used": provider.lower(),
                "method_used": "local_parquet",
                "success": True,
                "reason_code": "LOCAL_HIT",
                "persisted_locally": False,
                "rate_limited": False,
                "retry_count": 0,
                "deferred_until": None,
            }

        result = self._resolver.resolve_forecast_snapshot_if_missing(
            provider=provider,
            symbol=symbol,
            as_of_date=as_of_date,
            resolution_mode=resolution_mode,
        )
        payload = self._local_forecast_snapshot(provider=provider, symbol=symbol, as_of_date=as_of_date)
        return payload, {
            "served_from": result.served_from,
            "resolution_mode": result.resolution_mode,
            "remote_attempted": resolution_mode != ResolutionMode.LOCAL_ONLY,
            "provider_requested": result.provider_requested,
            "provider_used": result.provider_used,
            "method_used": result.method_used,
            "success": result.success and payload is not None,
            "reason_code": result.reason_code,
            "persisted_locally": result.persisted_locally,
            "rate_limited": result.rate_limited,
            "retry_count": result.retry_count,
            "deferred_until": result.deferred_until,
        }

    def _local_forecast_snapshot(self, *, provider: str, symbol: str, as_of_date: date) -> dict | None:
        points_path = normalized_path(self._config, "forecast_points")
        if not points_path.exists():
            return None
        points_df = pd.read_parquet(points_path)
        if points_df.empty:
            return None
        mask = (
            points_df["provider"].astype(str).str.lower() == provider.lower()
        ) & (
            points_df["symbol"].astype(str).str.upper() == symbol.upper()
        ) & (
            points_df["as_of_date"].astype(str) == as_of_date.isoformat()
        )
        matches = points_df[mask]
        if matches.empty:
            return None
        return {
            "provider": provider.lower(),
            "symbol": symbol.upper(),
            "as_of_date": as_of_date.isoformat(),
            "points": matches.to_dict(orient="records"),
        }


def _sanitize_transcript_payload(payload: dict) -> dict:
    """Convert pandas/pyarrow missing markers (NaN, pd.NA, NaT) to JSON-safe None."""
    sanitized: dict = {}
    for key, value in payload.items():
        if pd.isna(value):
            sanitized[key] = None
        else:
            sanitized[key] = value
    return sanitized


def _source_text_version_from_metadata(metadata: dict | None) -> str | None:
    if metadata is None:
        return None
    transcript_path = metadata.get("transcript_path")
    if transcript_path is None:
        return None
    path = Path(str(transcript_path))
    if not path.exists() or not path.is_file():
        return None
    return f"sha256:{sha256(path.read_bytes()).hexdigest()}"
