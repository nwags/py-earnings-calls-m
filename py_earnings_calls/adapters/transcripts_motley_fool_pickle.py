from __future__ import annotations

from pathlib import Path

import pandas as pd

from py_earnings_calls.adapters.base import TranscriptBulkAdapter
from py_earnings_calls.adapters.transcript_bulk_utils import (
    call_id_from_identity,
    first_present_column,
    infer_symbol_from_title,
    normalize_symbol,
    normalize_text,
    parse_call_datetime,
    stable_identity,
)
from py_earnings_calls.models import TranscriptDocument


_CANDIDATE_PROVIDER_CALL_ID_COLUMNS = ["provider_call_id", "call_id", "id", "transcript_id", "article_id"]
_CANDIDATE_SYMBOL_COLUMNS = ["symbol", "ticker", "ticker_symbol"]
_CANDIDATE_TEXT_COLUMNS = ["transcript", "content", "text", "body", "transcript_text"]
_CANDIDATE_DATE_COLUMNS = ["date", "call_date", "published_at", "published_date", "call_datetime", "datetime"]
_CANDIDATE_TITLE_COLUMNS = ["title", "headline", "call_title", "event_title"]
_CANDIDATE_COMPANY_COLUMNS = ["company", "company_name", "name"]
_CANDIDATE_URL_COLUMNS = ["url", "source_url", "article_url"]


class MotleyFoolPickleTranscriptAdapter(TranscriptBulkAdapter):
    provider = "motley_fool_pickle"

    def load_documents(self, dataset_path: str) -> list[TranscriptDocument]:
        path = Path(dataset_path)
        if not path.exists():
            raise FileNotFoundError(f"Dataset not found: {path}")
        if path.suffix.lower() not in {".pkl", ".pickle"}:
            raise ValueError("motley_fool_pickle adapter only accepts .pkl or .pickle files.")

        df = pd.read_pickle(path)
        if not isinstance(df, pd.DataFrame):
            raise ValueError("motley_fool_pickle adapter expected a pickled pandas DataFrame.")

        provider_call_id_col = first_present_column(df.columns, _CANDIDATE_PROVIDER_CALL_ID_COLUMNS)
        symbol_col = first_present_column(df.columns, _CANDIDATE_SYMBOL_COLUMNS)
        text_col = first_present_column(df.columns, _CANDIDATE_TEXT_COLUMNS)
        date_col = first_present_column(df.columns, _CANDIDATE_DATE_COLUMNS)
        title_col = first_present_column(df.columns, _CANDIDATE_TITLE_COLUMNS)
        company_col = first_present_column(df.columns, _CANDIDATE_COMPANY_COLUMNS)
        url_col = first_present_column(df.columns, _CANDIDATE_URL_COLUMNS)

        if text_col is None:
            raise ValueError("Could not infer required transcript dataset columns (transcript text).")

        documents: list[TranscriptDocument] = []
        for row in df.to_dict(orient="records"):
            transcript_text = normalize_text(row.get(text_col))
            if transcript_text is None:
                continue

            title = normalize_text(row.get(title_col)) if title_col else None
            company_name = normalize_text(row.get(company_col)) if company_col else None
            source_url = normalize_text(row.get(url_col)) if url_col else None

            symbol = normalize_symbol(row.get(symbol_col)) if symbol_col else None
            if symbol is None:
                symbol = infer_symbol_from_title(title)
            symbol = symbol or "UNKNOWN"

            call_datetime = parse_call_datetime(row.get(date_col)) if date_col else None
            provider_call_id, identity = stable_identity(
                provider=self.provider,
                provider_call_id=normalize_text(row.get(provider_call_id_col)) if provider_call_id_col else None,
                symbol=symbol,
                call_datetime=call_datetime,
                title=title,
            )

            documents.append(
                TranscriptDocument(
                    call_id=call_id_from_identity(identity),
                    provider=self.provider,
                    provider_call_id=provider_call_id,
                    symbol=symbol,
                    company_name=company_name,
                    call_datetime=call_datetime,
                    fiscal_year=None,
                    fiscal_period=None,
                    title=title,
                    source_url=source_url,
                    transcript_text=transcript_text,
                    raw_html=None,
                    speaker_count=None,
                )
            )
        return documents
