from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd

from py_earnings_calls.adapters.forecasts_finnhub import FinnhubForecastAdapter
from py_earnings_calls.adapters.forecasts_fmp import FmpForecastAdapter
from py_earnings_calls.adapters.transcripts_motley_fool import MotleyFoolTranscriptAdapter
from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpClient
from py_earnings_calls.identifiers import (
    forecast_archive_accession_id,
    forecast_snapshot_canonical_key,
    normalize_ticker,
    transcript_archive_accession_id,
    transcript_canonical_key,
)
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.pipelines.lookup_refresh import run_lookup_refresh_scoped
from py_earnings_calls.provider_registry import load_provider_registry, provider_resolution_candidates
from py_earnings_calls.resolution import ResolutionMode
from py_earnings_calls.resolution_events import append_resolution_event
from py_earnings_calls.storage.archive_index import upsert_forecast_archive_manifest, upsert_transcript_archive_manifest
from py_earnings_calls.storage.paths import (
    forecast_bundle_root,
    forecast_raw_snapshot_path,
    normalized_path,
    transcript_html_path,
    transcript_json_path,
    transcript_storage_cik,
    transcript_text_path,
)
from py_earnings_calls.storage.writes import upsert_parquet, write_json, write_text

_UNKNOWN_CALL_DATE = date(1970, 1, 1)


@dataclass(frozen=True)
class ResolutionResult:
    found: bool
    served_from: str
    resolution_mode: str
    provider_requested: str | None
    provider_used: str | None
    method_used: str | None
    success: bool
    reason_code: str
    message: str
    persisted_locally: bool


class ProviderAwareResolutionService:
    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def resolve_transcript_if_missing(
        self,
        *,
        call_id: str,
        resolution_mode: ResolutionMode,
        allow_admin: bool = False,
        public_surface: bool = True,
    ) -> ResolutionResult:
        canonical_key = transcript_canonical_key(call_id)
        mode_allowed, deny_reason = self._is_mode_allowed(resolution_mode, allow_admin=allow_admin, public_surface=public_surface)
        if not mode_allowed:
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=None,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code=deny_reason,
                message="Resolution mode is not allowed on this surface.",
                persisted_locally=False,
            )

        if resolution_mode == ResolutionMode.LOCAL_ONLY:
            return ResolutionResult(
                found=False,
                served_from="local_miss",
                resolution_mode=resolution_mode.value,
                provider_requested=None,
                provider_used=None,
                method_used=None,
                success=False,
                reason_code="LOCAL_ONLY_MISS",
                message="Local-only mode does not perform remote resolution.",
                persisted_locally=False,
            )

        calls = self._read_parquet_or_empty(normalized_path(self._config, "transcript_calls"))
        if calls.empty or "call_id" not in calls.columns:
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=None,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="NO_LOCAL_METADATA",
                message="No local canonical transcript metadata available for this call_id.",
                persisted_locally=False,
            )

        row_matches = calls[calls["call_id"].astype(str) == str(call_id)].tail(1)
        if row_matches.empty:
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=None,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="CALL_ID_NOT_FOUND",
                message="Canonical transcript call_id was not found locally.",
                persisted_locally=False,
            )

        row = row_matches.iloc[0].to_dict()
        provider = str(row.get("provider") or "").strip().lower()
        url = str(row.get("source_url") or "").strip()
        symbol = normalize_ticker(row.get("symbol")) or "UNKNOWN"
        if not provider or not url:
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider or None,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="MISSING_SOURCE_METADATA",
                message="Transcript resolution requires local provider and source_url metadata.",
                persisted_locally=False,
            )

        provider_policy = self._resolve_provider_policy(content_domain="transcript", provider_requested=provider)
        if provider_policy is None:
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="PROVIDER_NOT_ACTIVE",
                message="Provider is not active in local provider registry.",
                persisted_locally=False,
            )
        if not bool(provider_policy.get("supports_public_resolve_if_missing", False)):
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider,
                provider_used=provider,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="POLICY_DENIED",
                message="Provider policy does not allow public resolve_if_missing.",
                persisted_locally=False,
            )
        if not bool(provider_policy.get("supports_direct_resolution", False)):
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider,
                provider_used=provider,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="DIRECT_RESOLUTION_UNSUPPORTED",
                message="Provider policy does not support direct record resolution.",
                persisted_locally=False,
            )

        if provider != "motley_fool":
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider,
                provider_used=provider,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="ADAPTER_UNAVAILABLE",
                message="No transcript resolver is implemented for this provider.",
                persisted_locally=False,
            )

        http_client = HttpClient(self._config)
        adapter = MotleyFoolTranscriptAdapter(http_client)
        outcome = adapter.fetch_document_outcome(url, symbol=symbol)
        if outcome.document is None or outcome.failure is not None:
            reason = outcome.failure.reason if outcome.failure else "RESOLUTION_FAILED"
            message = outcome.failure.message if outcome.failure else "Transcript provider resolution failed."
            return self._record_event(
                content_domain="transcript",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=provider,
                provider_used=provider,
                method_used="uri",
                served_from="local_miss",
                success=False,
                reason_code=reason,
                message=message,
                persisted_locally=False,
            )

        document = outcome.document
        call_date = document.call_datetime.date() if document.call_datetime else _UNKNOWN_CALL_DATE
        issuers_df = load_issuers_dataframe(self._config)
        symbol_to_cik = build_symbol_to_cik_map(issuers_df)
        storage_cik = symbol_to_cik.get(document.symbol.upper()) or row.get("storage_cik") or "UNKNOWN"
        archive_accession_id = transcript_archive_accession_id(call_id)
        html_path = transcript_html_path(
            self._config,
            provider=document.provider,
            symbol=document.symbol,
            call_date=call_date,
            call_id=call_id,
            storage_cik=storage_cik,
        )
        text_path = transcript_text_path(
            self._config,
            provider=document.provider,
            symbol=document.symbol,
            call_date=call_date,
            call_id=call_id,
            storage_cik=storage_cik,
        )
        json_path = transcript_json_path(
            self._config,
            provider=document.provider,
            symbol=document.symbol,
            call_date=call_date,
            call_id=call_id,
            storage_cik=storage_cik,
        )

        if document.raw_html:
            write_text(html_path, document.raw_html)
        write_text(text_path, document.transcript_text)
        document_record = document.to_record()
        document_record["call_id"] = call_id
        write_json(json_path, document_record)

        call_row = document_record.copy()
        call_row["transcript_path"] = str(text_path)
        call_row["raw_html_path"] = str(html_path) if document.raw_html else None
        call_row["storage_cik"] = transcript_storage_cik(storage_cik)
        call_row["archive_accession_id"] = archive_accession_id
        call_row["archive_bundle_path"] = str(text_path.parent)
        call_row["imported_at"] = datetime.now(timezone.utc).isoformat()
        upsert_parquet(normalized_path(self._config, "transcript_calls"), [call_row], dedupe_keys=["provider", "call_id"])

        artifact_rows = [
            {
                "call_id": call_id,
                "artifact_type": "transcript_text",
                "artifact_path": str(text_path),
                "provider": document.provider,
                "symbol": document.symbol,
                "call_date": call_date.isoformat(),
                "exists_locally": True,
                "storage_cik": transcript_storage_cik(storage_cik),
                "archive_accession_id": archive_accession_id,
            },
            {
                "call_id": call_id,
                "artifact_type": "transcript_json",
                "artifact_path": str(json_path),
                "provider": document.provider,
                "symbol": document.symbol,
                "call_date": call_date.isoformat(),
                "exists_locally": True,
                "storage_cik": transcript_storage_cik(storage_cik),
                "archive_accession_id": archive_accession_id,
            },
        ]
        if document.raw_html:
            artifact_rows.append(
                {
                    "call_id": call_id,
                    "artifact_type": "transcript_html",
                    "artifact_path": str(html_path),
                    "provider": document.provider,
                    "symbol": document.symbol,
                    "call_date": call_date.isoformat(),
                    "exists_locally": True,
                    "storage_cik": transcript_storage_cik(storage_cik),
                    "archive_accession_id": archive_accession_id,
                }
            )
        upsert_parquet(
            normalized_path(self._config, "transcript_artifacts"),
            artifact_rows,
            dedupe_keys=["call_id", "artifact_type", "artifact_path"],
        )
        upsert_transcript_archive_manifest(
            self._config,
            [
                {
                    "call_id": call_id,
                    "archive_accession_id": archive_accession_id,
                    "storage_cik": transcript_storage_cik(storage_cik),
                    "provider": document.provider,
                    "raw_html_path": str(html_path) if document.raw_html else None,
                    "parsed_text_path": str(text_path),
                    "parsed_json_path": str(json_path),
                    "raw_html_exists": bool(document.raw_html),
                    "parsed_text_exists": True,
                    "parsed_json_exists": True,
                }
            ],
        )
        run_lookup_refresh_scoped(self._config, include_transcripts=True, include_forecasts=False)
        return self._record_event(
            content_domain="transcript",
            canonical_key=canonical_key,
            resolution_mode=resolution_mode,
            provider_requested=provider,
            provider_used=provider,
            method_used="uri",
            served_from="resolved_remote",
            success=True,
            reason_code="RESOLVED",
            message="Transcript resolved from provider and persisted locally.",
            persisted_locally=True,
        )

    def resolve_forecast_snapshot_if_missing(
        self,
        *,
        provider: str,
        symbol: str,
        as_of_date: date,
        resolution_mode: ResolutionMode,
        allow_admin: bool = False,
        public_surface: bool = True,
    ) -> ResolutionResult:
        normalized_provider = str(provider).strip().lower()
        normalized_symbol = normalize_ticker(symbol) or str(symbol).strip().upper()
        canonical_key = forecast_snapshot_canonical_key(
            provider=normalized_provider,
            symbol=normalized_symbol,
            as_of_date=as_of_date,
        )

        mode_allowed, deny_reason = self._is_mode_allowed(resolution_mode, allow_admin=allow_admin, public_surface=public_surface)
        if not mode_allowed:
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code=deny_reason,
                message="Resolution mode is not allowed on this surface.",
                persisted_locally=False,
            )

        if resolution_mode == ResolutionMode.LOCAL_ONLY:
            return ResolutionResult(
                found=False,
                served_from="local_miss",
                resolution_mode=resolution_mode.value,
                provider_requested=normalized_provider,
                provider_used=None,
                method_used=None,
                success=False,
                reason_code="LOCAL_ONLY_MISS",
                message="Local-only mode does not perform remote resolution.",
                persisted_locally=False,
            )

        provider_policy = self._resolve_provider_policy(content_domain="forecast", provider_requested=normalized_provider)
        if provider_policy is None:
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=None,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="PROVIDER_NOT_ACTIVE",
                message="Provider is not active in local provider registry.",
                persisted_locally=False,
            )
        if not bool(provider_policy.get("supports_public_resolve_if_missing", False)):
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="POLICY_DENIED",
                message="Provider policy does not allow public resolve_if_missing.",
                persisted_locally=False,
            )
        if not bool(provider_policy.get("supports_direct_resolution", False)):
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used=None,
                served_from="local_miss",
                success=False,
                reason_code="DIRECT_RESOLUTION_UNSUPPORTED",
                message="Provider policy does not support direct record resolution.",
                persisted_locally=False,
            )

        http_client = HttpClient(self._config)
        try:
            adapter = _build_forecast_adapter(normalized_provider, self._config, http_client)
        except ValueError as exc:
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used="api",
                served_from="local_miss",
                success=False,
                reason_code="PROVIDER_UNAVAILABLE",
                message=str(exc),
                persisted_locally=False,
            )

        try:
            snapshots, points = adapter.fetch_snapshots([normalized_symbol], as_of_date)
        except Exception as exc:  # pragma: no cover - defensive for adapter/runtime errors
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used="api",
                served_from="local_miss",
                success=False,
                reason_code="ADAPTER_ERROR",
                message=str(exc),
                persisted_locally=False,
            )

        selected_snapshot = next(
            (
                snapshot
                for snapshot in snapshots
                if snapshot.provider.lower() == normalized_provider
                and snapshot.symbol.upper() == normalized_symbol
                and snapshot.as_of_date == as_of_date
            ),
            None,
        )
        selected_points = [
            point
            for point in points
            if point.provider.lower() == normalized_provider
            and point.symbol.upper() == normalized_symbol
            and point.as_of_date == as_of_date
        ]

        if selected_snapshot is None:
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used="api",
                served_from="local_miss",
                success=False,
                reason_code="NO_SNAPSHOT",
                message="Provider returned no snapshot for the requested key.",
                persisted_locally=False,
            )
        if not selected_points:
            return self._record_event(
                content_domain="forecast",
                canonical_key=canonical_key,
                resolution_mode=resolution_mode,
                provider_requested=normalized_provider,
                provider_used=normalized_provider,
                method_used="api",
                served_from="local_miss",
                success=False,
                reason_code="NO_POINTS",
                message="Provider returned no usable normalized forecast points.",
                persisted_locally=False,
            )

        raw_path = forecast_raw_snapshot_path(
            self._config,
            provider=selected_snapshot.provider,
            symbol=selected_snapshot.symbol,
            as_of_date=selected_snapshot.as_of_date,
        )
        archive_accession_id = forecast_archive_accession_id(
            provider=selected_snapshot.provider,
            symbol=selected_snapshot.symbol,
            as_of_date=selected_snapshot.as_of_date,
        )
        write_json(raw_path, selected_snapshot.raw_payload)
        snapshot_row = selected_snapshot.to_record()
        issuers_df = load_issuers_dataframe(self._config)
        symbol_to_cik = build_symbol_to_cik_map(issuers_df)
        snapshot_row["cik"] = symbol_to_cik.get(selected_snapshot.symbol.upper())
        snapshot_row["raw_payload_path"] = str(raw_path)
        snapshot_row["archive_accession_id"] = archive_accession_id
        snapshot_row["archive_bundle_path"] = str(
            forecast_bundle_root(
                self._config,
                provider=selected_snapshot.provider,
                symbol=selected_snapshot.symbol,
                as_of_date=selected_snapshot.as_of_date,
            )
        )
        snapshot_row["imported_at"] = datetime.now(timezone.utc).isoformat()
        upsert_parquet(
            normalized_path(self._config, "forecast_snapshots"),
            [snapshot_row],
            dedupe_keys=["provider", "symbol", "as_of_date"],
        )
        upsert_parquet(
            normalized_path(self._config, "forecast_points"),
            [
                {
                    **point.to_record(),
                    "cik": symbol_to_cik.get(point.symbol.upper()),
                }
                for point in selected_points
            ],
            dedupe_keys=[
                "provider",
                "symbol",
                "as_of_date",
                "fiscal_year",
                "fiscal_period",
                "metric_name",
                "stat_name",
            ],
        )
        upsert_forecast_archive_manifest(
            self._config,
            [
                {
                    "snapshot_id": selected_snapshot.snapshot_id,
                    "archive_accession_id": archive_accession_id,
                    "provider": selected_snapshot.provider,
                    "symbol": selected_snapshot.symbol,
                    "as_of_date": selected_snapshot.as_of_date.isoformat(),
                    "raw_payload_path": str(raw_path),
                    "raw_payload_exists": True,
                }
            ],
        )
        run_lookup_refresh_scoped(self._config, include_transcripts=False, include_forecasts=True)
        return self._record_event(
            content_domain="forecast",
            canonical_key=canonical_key,
            resolution_mode=resolution_mode,
            provider_requested=normalized_provider,
            provider_used=normalized_provider,
            method_used="api",
            served_from="resolved_remote",
            success=True,
            reason_code="RESOLVED",
            message="Forecast snapshot resolved from provider and persisted locally.",
            persisted_locally=True,
        )

    def _resolve_provider_policy(self, *, content_domain: str, provider_requested: str) -> dict | None:
        registry = load_provider_registry(self._config)
        candidates = provider_resolution_candidates(
            registry,
            content_domain=content_domain,
            provider_requested=provider_requested,
        )
        if not candidates:
            return None
        return candidates[0]

    def _is_mode_allowed(self, mode: ResolutionMode, *, allow_admin: bool, public_surface: bool) -> tuple[bool, str]:
        if mode == ResolutionMode.REFRESH_IF_STALE and not allow_admin:
            return False, "ADMIN_REQUIRED"
        if public_surface and mode == ResolutionMode.REFRESH_IF_STALE:
            return False, "PUBLIC_MODE_DENIED"
        return True, ""

    def _record_event(
        self,
        *,
        content_domain: str,
        canonical_key: str,
        resolution_mode: ResolutionMode,
        provider_requested: str | None,
        provider_used: str | None,
        method_used: str | None,
        served_from: str,
        success: bool,
        reason_code: str,
        message: str,
        persisted_locally: bool,
    ) -> ResolutionResult:
        append_resolution_event(
            self._config,
            {
                "event_at": datetime.now(timezone.utc).isoformat(),
                "content_domain": content_domain,
                "canonical_key": canonical_key,
                "resolution_mode": resolution_mode.value,
                "provider_requested": provider_requested or "",
                "provider_used": provider_used or "",
                "method_used": method_used or "",
                "served_from": served_from,
                "success": success,
                "reason_code": reason_code,
                "message": message,
                "persisted_locally": persisted_locally,
            },
        )
        return ResolutionResult(
            found=success,
            served_from=served_from,
            resolution_mode=resolution_mode.value,
            provider_requested=provider_requested,
            provider_used=provider_used,
            method_used=method_used,
            success=success,
            reason_code=reason_code,
            message=message,
            persisted_locally=persisted_locally,
        )

    def _read_parquet_or_empty(self, path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)


def _build_forecast_adapter(provider: str, config: AppConfig, http_client: HttpClient):
    if provider == "finnhub":
        return FinnhubForecastAdapter(config, http_client)
    if provider == "fmp":
        return FmpForecastAdapter(config, http_client)
    raise ValueError(f"Unsupported forecast provider: {provider}")
