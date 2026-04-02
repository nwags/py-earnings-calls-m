from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from py_earnings_calls.adapters.transcript_bulk_utils import first_present_column, normalize_symbol, normalize_text


@dataclass(frozen=True)
class TranscriptBackfillManifestRow:
    url: str
    symbol: str | None = None


def load_manifest_rows(manifest_path: str) -> list[TranscriptBackfillManifestRow]:
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix == ".jsonl":
        df = pd.read_json(path, lines=True)
    else:
        raise ValueError("Unsupported manifest format. Use CSV (required) or JSONL.")

    return parse_manifest_rows(df.to_dict(orient="records"))


def rows_from_urls(urls: list[str], *, symbol: str | None = None) -> list[TranscriptBackfillManifestRow]:
    records = [{"url": url, "symbol": symbol} for url in urls]
    return parse_manifest_rows(records)


def parse_manifest_rows(records: list[dict]) -> list[TranscriptBackfillManifestRow]:
    if not records:
        return []

    sample_columns = list(records[0].keys())
    url_key = first_present_column(sample_columns, ["url"])
    symbol_key = first_present_column(sample_columns, ["symbol", "ticker"])
    if url_key is None:
        raise ValueError("Manifest is missing required `url` column.")

    rows: list[TranscriptBackfillManifestRow] = []
    for record in records:
        url = normalize_text(record.get(url_key))
        if not url:
            continue
        symbol = normalize_symbol(record.get(symbol_key)) if symbol_key else None
        rows.append(TranscriptBackfillManifestRow(url=url, symbol=symbol))
    return rows
