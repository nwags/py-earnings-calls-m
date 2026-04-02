from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from py_earnings_calls.refdata.normalize import normalize_cik, normalize_ticker
from py_earnings_calls.refdata.schema import ISSUERS_COLUMNS
from py_earnings_calls.refdata.sources import IssuerInputs


def build_issuers_table(inputs: IssuerInputs) -> pd.DataFrame:
    refresh_time = datetime.now(timezone.utc).isoformat()

    frames: list[pd.DataFrame] = []
    for source in inputs.sources:
        source_df = source.dataframe.copy()
        if source_df.empty:
            continue
        source_df["symbol"] = source_df["symbol"].map(normalize_ticker)
        source_df["cik"] = source_df["cik"].map(normalize_cik)
        source_df["source_rank"] = source.rank
        source_df["source_updated_at"] = refresh_time
        frames.append(source_df)

    if not frames:
        return pd.DataFrame(columns=ISSUERS_COLUMNS)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.dropna(subset=["symbol", "cik"])

    if combined.empty:
        return pd.DataFrame(columns=ISSUERS_COLUMNS)

    combined["company_name"] = combined["company_name"].astype("string").str.strip()
    combined["exchange"] = combined["exchange"].astype("string").str.strip()
    combined["is_active"] = combined["is_active"].map(_coerce_bool_or_none)

    # Enrich blank company names from cik-lookup source when we have a CIK.
    if inputs.company_name_by_cik:
        name_map = {normalize_cik(key): value for key, value in inputs.company_name_by_cik.items()}
        name_map = {key: value for key, value in name_map.items() if key and value}
        missing_name_mask = combined["company_name"].isna() | (combined["company_name"] == "")
        combined.loc[missing_name_mask, "company_name"] = combined.loc[missing_name_mask, "cik"].map(name_map)

    combined = combined.sort_values(
        by=["cik", "symbol", "source_rank", "primary_source", "company_name"],
        ascending=[True, True, True, True, True],
        na_position="last",
    )
    combined = combined.drop_duplicates(subset=["symbol", "cik"], keep="first")
    combined = combined[ISSUERS_COLUMNS]
    return combined.reset_index(drop=True)


def _coerce_bool_or_none(value: object) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None

