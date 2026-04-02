from __future__ import annotations

from datetime import date
import hashlib

from py_earnings_calls.refdata.normalize import normalize_cik, normalize_ticker


def transcript_canonical_key(call_id: str) -> str:
    return f"transcript|call_id={str(call_id).strip()}"


def forecast_snapshot_canonical_key(*, provider: str, symbol: str, as_of_date: date) -> str:
    return (
        f"forecast_snapshot|provider={str(provider).strip().lower()}"
        f"|symbol={normalize_ticker(symbol) or str(symbol).strip().upper()}"
        f"|as_of_date={as_of_date.isoformat()}"
    )


def transcript_archive_accession_id(call_id: str) -> str:
    canonical = transcript_canonical_key(call_id)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]
    return f"tr-{digest}"


def forecast_archive_accession_id(*, provider: str, symbol: str, as_of_date: date) -> str:
    canonical = forecast_snapshot_canonical_key(provider=provider, symbol=symbol, as_of_date=as_of_date)
    digest = hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:16]
    return f"fs-{digest}"


__all__ = [
    "normalize_ticker",
    "normalize_cik",
    "transcript_canonical_key",
    "forecast_snapshot_canonical_key",
    "transcript_archive_accession_id",
    "forecast_archive_accession_id",
]
