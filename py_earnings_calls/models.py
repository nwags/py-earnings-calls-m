from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, date
from typing import Any


@dataclass(frozen=True)
class TranscriptDocument:
    call_id: str
    provider: str
    provider_call_id: str | None
    symbol: str
    company_name: str | None
    call_datetime: datetime | None
    fiscal_year: int | None
    fiscal_period: str | None
    title: str | None
    source_url: str | None
    transcript_text: str
    call_datetime_source: str | None = None
    raw_html: str | None = None
    speaker_count: int | None = None

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        if self.call_datetime is not None:
            record["call_datetime"] = self.call_datetime.isoformat()
        return record


@dataclass(frozen=True)
class ForecastPoint:
    snapshot_id: str
    provider: str
    symbol: str
    as_of_date: date
    fiscal_year: int | None
    fiscal_period: str | None
    metric_name: str
    stat_name: str
    value: float | None
    currency: str | None = None
    analyst_count: int | None = None

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["as_of_date"] = self.as_of_date.isoformat()
        return record


@dataclass(frozen=True)
class ForecastSnapshot:
    snapshot_id: str
    provider: str
    symbol: str
    as_of_date: date
    source_url: str | None
    raw_payload: dict[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "provider": self.provider,
            "symbol": self.symbol,
            "as_of_date": self.as_of_date.isoformat(),
            "source_url": self.source_url,
        }
