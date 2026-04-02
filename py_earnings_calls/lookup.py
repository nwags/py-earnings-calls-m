from __future__ import annotations

from datetime import date

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.identifiers import normalize_cik, normalize_ticker
from py_earnings_calls.storage.paths import normalized_path


def load_lookup_dataframe(config: AppConfig, scope: str) -> pd.DataFrame:
    if scope == "transcripts":
        path = normalized_path(config, "local_lookup_transcripts")
    elif scope == "forecasts":
        path = normalized_path(config, "local_lookup_forecasts")
    elif scope == "forecasts_by_cik":
        path = normalized_path(config, "local_lookup_forecasts_by_cik")
    else:
        raise ValueError(f"Unsupported lookup scope: {scope}")

    if not path.exists():
        raise FileNotFoundError(f"Missing lookup artifact: {path}")
    return pd.read_parquet(path)


def load_issuers_dataframe(config: AppConfig) -> pd.DataFrame:
    path = normalized_path(config, "issuers")
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def query_transcripts(
    df: pd.DataFrame,
    *,
    symbol: str | None = None,
    cik: str | None = None,
    start: date | None = None,
    end: date | None = None,
    limit: int | None = None,
    offset: int = 0,
    call_id: str | None = None,
    issuers_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out = df.copy()
    ticker = normalize_ticker(symbol)
    if ticker:
        out = out[out["symbol"].astype(str).str.upper() == ticker]

    normalized_cik = normalize_cik(cik)
    if cik is not None and normalized_cik is None:
        raise ValueError("Invalid cik. Expected up to 10 digits after normalization.")
    if normalized_cik:
        symbols_for_cik = resolve_symbols_for_cik(issuers_df, normalized_cik)
        if not symbols_for_cik:
            return out.iloc[0:0].reset_index(drop=True)
        out = out[out["symbol"].astype(str).str.upper().isin(symbols_for_cik)]

    if start is not None or end is not None:
        if "call_datetime" not in out.columns:
            return out.iloc[0:0].reset_index(drop=True)
        call_ts = pd.to_datetime(out["call_datetime"], errors="coerce", utc=False)
        if start is not None:
            start_ts = pd.Timestamp(start)
            out = out[call_ts >= start_ts]
            call_ts = pd.to_datetime(out["call_datetime"], errors="coerce", utc=False)
        if end is not None:
            # Inclusive through end-of-day by applying an exclusive next-day bound.
            end_exclusive = pd.Timestamp(end) + pd.Timedelta(days=1)
            out = out[call_ts < end_exclusive]

    if call_id:
        out = out[out["call_id"].astype(str) == call_id]

    if "call_datetime" in out.columns:
        out = out.assign(_call_dt=pd.to_datetime(out["call_datetime"], errors="coerce", utc=False))
        out = out.sort_values("_call_dt", ascending=False, na_position="last").drop(columns=["_call_dt"])

    if offset > 0:
        out = out.iloc[offset:]
    if limit is not None:
        out = out.iloc[:limit]
    return out.reset_index(drop=True)


def query_forecasts(df: pd.DataFrame, *, symbol: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if symbol:
        out = out[out["symbol"].astype(str).str.upper() == symbol.upper()]
    return out.reset_index(drop=True)


def query_forecasts_by_cik(
    df: pd.DataFrame,
    *,
    cik: str,
    as_of_date: date | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> pd.DataFrame:
    normalized = normalize_cik(cik)
    if normalized is None:
        raise ValueError("Invalid cik. Expected up to 10 digits after normalization.")
    out = df.copy()
    out = out[out["cik"].astype(str) == normalized]
    if as_of_date is not None and "as_of_date" in out.columns:
        out = out[out["as_of_date"].astype(str) == as_of_date.isoformat()]
    out = out.sort_values(
        ["as_of_date", "provider", "symbol", "metric_name", "stat_name", "snapshot_id"],
        ascending=[False, True, True, True, True, True],
        na_position="last",
    )
    if offset > 0:
        out = out.iloc[offset:]
    if limit is not None:
        out = out.iloc[:limit]
    return out.reset_index(drop=True)


def build_symbol_to_cik_map(issuers_df: pd.DataFrame | None) -> dict[str, str]:
    if issuers_df is None or issuers_df.empty:
        return {}
    symbol_col = _find_column(issuers_df.columns, ["symbol", "ticker"])
    cik_col = _find_column(issuers_df.columns, ["cik"])
    if symbol_col is None or cik_col is None:
        return {}

    mapping: dict[str, str] = {}
    for row in issuers_df.to_dict(orient="records"):
        symbol = normalize_ticker(str(row.get(symbol_col) or ""))
        cik = normalize_cik(str(row.get(cik_col) or ""))
        if symbol and cik:
            mapping[symbol] = cik
    return mapping


def resolve_symbols_for_cik(issuers_df: pd.DataFrame | None, cik: str) -> set[str]:
    normalized_cik = normalize_cik(cik)
    if normalized_cik is None:
        return set()
    mapping = build_symbol_to_cik_map(issuers_df)
    return {symbol for symbol, mapped_cik in mapping.items() if mapped_cik == normalized_cik}


def _find_column(columns, candidates: list[str]) -> str | None:
    normalized = {str(col).strip().lower(): str(col) for col in columns}
    for candidate in candidates:
        match = normalized.get(candidate.lower())
        if match:
            return match
    return None
