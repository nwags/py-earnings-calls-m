from __future__ import annotations

from datetime import date
import hashlib
from typing import Iterable

from py_earnings_calls.adapters.base import ForecastAdapter
from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpClient
from py_earnings_calls.models import ForecastPoint, ForecastSnapshot


class FinnhubForecastAdapter(ForecastAdapter):
    provider = "finnhub"

    def __init__(self, config: AppConfig, http_client: HttpClient) -> None:
        if not config.finnhub_api_key:
            raise ValueError("FINNHUB_API_KEY is required for the Finnhub adapter.")
        self._config = config
        self._http = http_client
        self._api_key = config.finnhub_api_key

    def fetch_snapshots(self, symbols: Iterable[str], as_of_date: date) -> tuple[list[ForecastSnapshot], list[ForecastPoint]]:
        snapshots: list[ForecastSnapshot] = []
        points: list[ForecastPoint] = []

        for symbol in symbols:
            normalized_symbol = symbol.upper()
            payload = self._http.request_json(
                "https://finnhub.io/api/v1/calendar/earnings",
                params={
                    "from": as_of_date.isoformat(),
                    "to": as_of_date.isoformat(),
                    "token": self._api_key,
                    "symbol": normalized_symbol,
                },
            )
            snapshot_id = hashlib.sha1(f"{self.provider}|{normalized_symbol}|{as_of_date.isoformat()}".encode("utf-8")).hexdigest()[:20]
            snapshots.append(
                ForecastSnapshot(
                    snapshot_id=snapshot_id,
                    provider=self.provider,
                    symbol=normalized_symbol,
                    as_of_date=as_of_date,
                    source_url="https://finnhub.io/api/v1/calendar/earnings",
                    raw_payload=payload,
                )
            )

            entries = payload.get("earningsCalendar", []) if isinstance(payload, dict) else []
            if not isinstance(entries, list):
                entries = []
            for item in entries:
                if not isinstance(item, dict):
                    continue
                fiscal_year = _int_or_none(item.get("fiscalYear"))
                fiscal_period = _normalize_period(item.get("quarter"))
                currency = _str_or_none(item.get("currency"))
                eps_estimate = _float_or_none(item.get("epsEstimate"))
                revenue_estimate = _float_or_none(item.get("revenueEstimate"))
                analyst_count = _int_or_none(item.get("numberOfEstimates"))

                if eps_estimate is not None:
                    points.append(
                        ForecastPoint(
                            snapshot_id=snapshot_id,
                            provider=self.provider,
                            symbol=normalized_symbol,
                            as_of_date=as_of_date,
                            fiscal_year=fiscal_year,
                            fiscal_period=fiscal_period,
                            metric_name="eps",
                            stat_name="estimate",
                            value=eps_estimate,
                            currency=currency,
                            analyst_count=analyst_count,
                        )
                    )
                if revenue_estimate is not None:
                    points.append(
                        ForecastPoint(
                            snapshot_id=snapshot_id,
                            provider=self.provider,
                            symbol=normalized_symbol,
                            as_of_date=as_of_date,
                            fiscal_year=fiscal_year,
                            fiscal_period=fiscal_period,
                            metric_name="revenue",
                            stat_name="estimate",
                            value=revenue_estimate,
                            currency=currency,
                            analyst_count=analyst_count,
                        )
                    )

        points = sorted(
            points,
            key=lambda point: (
                point.symbol,
                str(point.fiscal_year),
                str(point.fiscal_period),
                point.metric_name,
                point.stat_name,
            ),
        )

        return snapshots, points


def _float_or_none(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _int_or_none(value) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _normalize_period(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _str_or_none(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
