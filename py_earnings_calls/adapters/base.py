from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Iterable

from py_earnings_calls.models import ForecastPoint, ForecastSnapshot, TranscriptDocument


class TranscriptBulkAdapter(ABC):
    @abstractmethod
    def load_documents(self, dataset_path: str) -> list[TranscriptDocument]:
        raise NotImplementedError


class TranscriptFetchAdapter(ABC):
    @abstractmethod
    def fetch_document(self, url: str, symbol: str | None = None) -> TranscriptDocument:
        raise NotImplementedError


class ForecastAdapter(ABC):
    @abstractmethod
    def fetch_snapshots(self, symbols: Iterable[str], as_of_date: date) -> tuple[list[ForecastSnapshot], list[ForecastPoint]]:
        raise NotImplementedError
