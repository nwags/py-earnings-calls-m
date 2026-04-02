from __future__ import annotations

from datetime import datetime
import hashlib
from pathlib import Path
import re
from typing import Iterable

import pandas as pd


_COLUMN_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_WHITESPACE_RE = re.compile(r"\s+")
_MONTH_DOT_RE = re.compile(r"\b(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.", flags=re.IGNORECASE)
_SYMBOL_PARENS_RE = re.compile(r"\(([A-Z]{1,6})(?:\s+[-+0-9.,%]+)?\)")
_SYMBOL_EXCHANGE_RE = re.compile(r"\((?:NASDAQ|NYSE|AMEX)\s*:\s*([A-Z]{1,6})\)", flags=re.IGNORECASE)


def normalize_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = _WHITESPACE_RE.sub(" ", str(value)).strip()
    return text or None


def normalize_symbol(value: object | None) -> str | None:
    text = normalize_text(value)
    if text is None:
        return None
    text = re.sub(r"[^A-Za-z0-9.\-]+", "", text).upper()
    return text or None


def infer_symbol_from_title(title: str | None) -> str | None:
    if not title:
        return None
    for regex in (_SYMBOL_EXCHANGE_RE, _SYMBOL_PARENS_RE):
        match = regex.search(title)
        if match:
            inferred = normalize_symbol(match.group(1))
            if inferred:
                return inferred
    return None


def parse_call_datetime(value: object | None) -> datetime | None:
    text = normalize_text(value)
    if text is None:
        return None

    cleaned = text.replace("a.m.", "AM").replace("p.m.", "PM").replace("A.M.", "AM").replace("P.M.", "PM")
    cleaned = _MONTH_DOT_RE.sub(lambda m: m.group(1), cleaned)
    cleaned = re.sub(r"\b(Eastern\s+Time|Eastern|ET|EST|EDT)\b", "", cleaned, flags=re.IGNORECASE).strip(" ,")
    parsed = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def stable_identity(
    *,
    provider: str,
    provider_call_id: str | None,
    symbol: str,
    call_datetime: datetime | None,
    title: str | None,
) -> tuple[str, str]:
    normalized_provider = normalize_text(provider) or "unknown_provider"
    normalized_id = normalize_text(provider_call_id)
    normalized_symbol = normalize_symbol(symbol) or "UNKNOWN"
    normalized_title = normalize_text(title) or ""
    normalized_datetime = call_datetime.isoformat(timespec="seconds") if call_datetime is not None else ""

    # Primary identity is provider + provider_call_id.
    if normalized_id:
        identity = f"primary|provider={normalized_provider}|provider_call_id={normalized_id}"
        return normalized_id, identity

    # Fallback identity is normalized symbol + normalized call_datetime + normalized title.
    fallback = f"fallback|symbol={normalized_symbol}|call_datetime={normalized_datetime}|title={normalized_title}"
    fallback_provider_call_id = fallback
    return fallback_provider_call_id, fallback


def call_id_from_identity(identity: str) -> str:
    digest = hashlib.sha1(identity.encode("utf-8")).hexdigest()
    return digest[:16]


def read_tabular_dataframe(path: Path, *, allow_parquet: bool = True) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    if suffix == ".parquet":
        if not allow_parquet:
            raise ValueError("Parquet input is not supported for this adapter.")
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported dataset format: {suffix}")


def first_present_column(columns: Iterable[str], candidates: list[str]) -> str | None:
    normalized_map = {_normalize_column_name(column): column for column in columns}
    for candidate in candidates:
        matched = normalized_map.get(_normalize_column_name(candidate))
        if matched:
            return matched
    return None


def _normalize_column_name(name: str) -> str:
    return _COLUMN_ALNUM_RE.sub("", name.strip().lower())
