from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd


def write_text(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")


def upsert_parquet(path: Path, rows: Iterable[dict], dedupe_keys: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(list(rows))
    if new_df.empty and not path.exists():
        return 0
    if path.exists():
        old_df = pd.read_parquet(path)
        combined = pd.concat([old_df, new_df], ignore_index=True, sort=False)
    else:
        combined = new_df
    before = len(combined.index)
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    combined.to_parquet(path, index=False)
    after = len(combined.index)
    return max(0, after - (before - len(new_df.index)))
