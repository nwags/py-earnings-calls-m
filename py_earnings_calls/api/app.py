from __future__ import annotations

from datetime import date

from fastapi import FastAPI, HTTPException
from fastapi import Query
from fastapi.responses import PlainTextResponse

from py_earnings_calls.api.models import (
    ForecastByCikItemResponse,
    ForecastByCikResponse,
    ForecastLatestResponse,
    ForecastSnapshotResponse,
    HealthResponse,
    TranscriptListItemResponse,
    TranscriptListResponse,
    TranscriptMetadataResponse,
)
from py_earnings_calls.api.service import LocalLookupService
from py_earnings_calls.config import AppConfig, load_config
from py_earnings_calls.resolution import ResolutionMode, parse_resolution_mode


def create_app(config: AppConfig | None = None) -> FastAPI:
    runtime_config = config or load_config()
    service = LocalLookupService(runtime_config)

    app = FastAPI(
        title="py-earnings-calls API",
        version="0.1.0",
        description="Local-first API for transcript and forecast artifacts.",
    )

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(status="ok", service="py-earnings-calls-api")

    @app.get("/transcripts", response_model=TranscriptListResponse)
    async def transcripts_query(
        ticker: str | None = None,
        cik: str | None = None,
        start: date | None = None,
        end: date | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> TranscriptListResponse:
        try:
            payload = service.list_transcripts(
                ticker=ticker,
                cik=cik,
                start=start,
                end=end,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        items = [TranscriptListItemResponse(**item) for item in payload["items"]]
        return TranscriptListResponse(items=items, limit=payload["limit"], offset=payload["offset"], total=payload["total"])

    @app.get("/transcripts/{call_id}", response_model=TranscriptMetadataResponse)
    async def transcript_metadata(
        call_id: str,
        resolution_mode: str = Query(default="local_only"),
    ) -> TranscriptMetadataResponse:
        try:
            mode = parse_resolution_mode(resolution_mode)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if mode == ResolutionMode.REFRESH_IF_STALE:
            raise HTTPException(status_code=422, detail="refresh_if_stale is not available on public read endpoints.")
        metadata, resolution = service.get_transcript_metadata_with_resolution(call_id, resolution_mode=mode)
        if metadata is None:
            raise HTTPException(status_code=404, detail="Transcript not found.")
        return TranscriptMetadataResponse(**{
            "call_id": metadata.get("call_id"),
            "provider": metadata.get("provider"),
            "symbol": metadata.get("symbol"),
            "cik": metadata.get("cik"),
            "title": metadata.get("title"),
            "call_datetime": metadata.get("call_datetime"),
            "transcript_path": metadata.get("transcript_path"),
            "served_from": resolution.get("served_from"),
            "resolution_mode": resolution.get("resolution_mode"),
            "provider_used": resolution.get("provider_used"),
            "method_used": resolution.get("method_used"),
            "success": resolution.get("success"),
            "reason_code": resolution.get("reason_code"),
            "persisted_locally": resolution.get("persisted_locally"),
        })

    @app.get("/transcripts/{call_id}/content")
    async def transcript_content(
        call_id: str,
        resolution_mode: str = Query(default="local_only"),
    ) -> PlainTextResponse:
        try:
            mode = parse_resolution_mode(resolution_mode)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if mode == ResolutionMode.REFRESH_IF_STALE:
            raise HTTPException(status_code=422, detail="refresh_if_stale is not available on public read endpoints.")
        content, resolution = service.get_transcript_content_with_resolution(call_id, resolution_mode=mode)
        if content is None:
            raise HTTPException(status_code=404, detail="Transcript content not found.")
        return PlainTextResponse(
            content,
            headers={
                "X-Resolution-Mode": str(resolution.get("resolution_mode") or ""),
                "X-Served-From": str(resolution.get("served_from") or ""),
                "X-Reason-Code": str(resolution.get("reason_code") or ""),
            },
        )

    @app.get("/forecasts/{symbol}/latest", response_model=ForecastLatestResponse)
    async def latest_forecast(symbol: str) -> ForecastLatestResponse:
        payload = service.get_latest_forecast(symbol)
        if payload is None:
            raise HTTPException(status_code=404, detail="Forecast not found.")
        return ForecastLatestResponse(**payload)

    @app.get("/forecasts/by-cik/{cik}", response_model=ForecastByCikResponse)
    async def forecasts_by_cik(
        cik: str,
        as_of_date: date | None = None,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> ForecastByCikResponse:
        try:
            payload = service.list_forecasts_by_cik(
                cik=cik,
                as_of_date=as_of_date,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        items = [ForecastByCikItemResponse(**item) for item in payload["items"]]
        return ForecastByCikResponse(
            cik=str(payload["cik"]),
            as_of_date=payload["as_of_date"],
            items=items,
            limit=payload["limit"],
            offset=payload["offset"],
            total=payload["total"],
        )

    @app.get("/forecasts/snapshots/{provider}/{symbol}/{as_of_date}", response_model=ForecastSnapshotResponse)
    async def forecast_snapshot(
        provider: str,
        symbol: str,
        as_of_date: str,
        resolution_mode: str = Query(default="local_only"),
    ) -> ForecastSnapshotResponse:
        try:
            parsed_as_of_date = date.fromisoformat(as_of_date)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="Invalid as_of_date. Expected YYYY-MM-DD.") from exc
        try:
            mode = parse_resolution_mode(resolution_mode)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if mode == ResolutionMode.REFRESH_IF_STALE:
            raise HTTPException(status_code=422, detail="refresh_if_stale is not available on public read endpoints.")
        payload, resolution = service.get_forecast_snapshot(
            provider=provider,
            symbol=symbol,
            as_of_date=parsed_as_of_date,
            resolution_mode=mode,
        )
        if payload is None:
            raise HTTPException(status_code=404, detail="Forecast snapshot not found.")
        return ForecastSnapshotResponse(**{
            "provider": payload.get("provider"),
            "symbol": payload.get("symbol"),
            "as_of_date": payload.get("as_of_date"),
            "points": payload.get("points"),
            "served_from": resolution.get("served_from"),
            "resolution_mode": resolution.get("resolution_mode"),
            "provider_used": resolution.get("provider_used"),
            "method_used": resolution.get("method_used"),
            "success": resolution.get("success"),
            "reason_code": resolution.get("reason_code"),
            "persisted_locally": resolution.get("persisted_locally"),
        })

    return app
