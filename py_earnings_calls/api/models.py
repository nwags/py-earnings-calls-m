from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class TranscriptMetadataResponse(BaseModel):
    call_id: str
    provider: str | None = None
    symbol: str | None = None
    cik: str | None = None
    title: str | None = None
    call_datetime: str | None = None
    transcript_path: str | None = None
    served_from: str | None = None
    resolution_mode: str | None = None
    provider_used: str | None = None
    method_used: str | None = None
    success: bool | None = None
    reason_code: str | None = None
    persisted_locally: bool | None = None


class ForecastLatestResponse(BaseModel):
    symbol: str
    as_of_date: str | None = None
    points: list[dict]


class ForecastSnapshotResponse(BaseModel):
    provider: str
    symbol: str
    as_of_date: str
    points: list[dict]
    served_from: str | None = None
    resolution_mode: str | None = None
    provider_used: str | None = None
    method_used: str | None = None
    success: bool | None = None
    reason_code: str | None = None
    persisted_locally: bool | None = None


class ForecastByCikItemResponse(BaseModel):
    cik: str | None = None
    symbol: str | None = None
    provider: str | None = None
    as_of_date: str | None = None
    snapshot_id: str | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    metric_name: str | None = None
    stat_name: str | None = None
    value: float | None = None


class ForecastByCikResponse(BaseModel):
    cik: str
    as_of_date: str | None = None
    items: list[ForecastByCikItemResponse]
    limit: int
    offset: int
    total: int


class TranscriptListItemResponse(BaseModel):
    call_id: str
    provider: str | None = None
    symbol: str | None = None
    cik: str | None = None
    title: str | None = None
    call_datetime: str | None = None
    transcript_path: str | None = None


class TranscriptListResponse(BaseModel):
    items: list[TranscriptListItemResponse]
    limit: int
    offset: int
    total: int
