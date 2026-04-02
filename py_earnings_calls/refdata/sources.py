from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re

import pandas as pd


@dataclass(frozen=True)
class IssuerSourceFrame:
    name: str
    rank: int
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class IssuerInputs:
    mode: str
    sources: list[IssuerSourceFrame]
    company_name_by_cik: dict[str, str]
    source_files: list[str]
    used_sec_sources: bool
    used_inputs: bool
    used_input_overrides: bool
    no_usable_raw_sources: bool


def load_issuer_inputs(*, sec_sources_root: Path, inputs_root: Path, universe_path: str | None) -> IssuerInputs:
    if universe_path:
        universe = _read_universe_file(Path(universe_path))
        return IssuerInputs(
            mode="universe_only",
            sources=[IssuerSourceFrame(name="universe_file", rank=0, dataframe=universe)],
            company_name_by_cik={},
            source_files=[str(Path(universe_path).resolve())],
            used_sec_sources=False,
            used_inputs=False,
            used_input_overrides=False,
            no_usable_raw_sources=False,
        )

    sec_sources, used_sec_sources, used_inputs, used_input_overrides, source_files = _load_sec_style_sources(
        sec_sources_root=sec_sources_root,
        inputs_root=inputs_root,
    )
    cik_lookup_path = _resolve_source_file(
        "cik-lookup-data.txt",
        sec_sources_root=sec_sources_root,
        inputs_root=inputs_root,
    )
    company_name_by_cik = _load_cik_lookup_company_names(cik_lookup_path) if cik_lookup_path else {}
    if cik_lookup_path is not None and cik_lookup_path.exists():
        source_files.append(str(cik_lookup_path.resolve()))
        if cik_lookup_path.parent == sec_sources_root:
            used_sec_sources = True
        if cik_lookup_path.parent == inputs_root:
            used_inputs = True
            if (sec_sources_root / "cik-lookup-data.txt").exists():
                used_input_overrides = True

    return IssuerInputs(
        mode="sec_local_inputs",
        sources=sec_sources,
        company_name_by_cik=company_name_by_cik,
        source_files=source_files,
        used_sec_sources=used_sec_sources,
        used_inputs=used_inputs,
        used_input_overrides=used_input_overrides,
        no_usable_raw_sources=(not sec_sources and not company_name_by_cik),
    )


def _read_universe_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    return _coerce_to_canonical_columns(df, primary_source="universe_file")


def _load_sec_style_sources(
    *, sec_sources_root: Path, inputs_root: Path
) -> tuple[list[IssuerSourceFrame], bool, bool, bool, list[str]]:
    specs = [
        ("company_tickers_exchange.json", 10, _parse_company_tickers_exchange),
        ("company_tickers.json", 20, _parse_company_tickers),
        ("company_tickers_mf.json", 30, _parse_company_tickers_mf),
        ("ticker.txt", 40, _parse_ticker_txt),
    ]

    frames: list[IssuerSourceFrame] = []
    used_sec_sources = False
    used_inputs = False
    used_input_overrides = False
    source_files: list[str] = []
    for filename, rank, parser in specs:
        path = _resolve_source_file(filename, sec_sources_root=sec_sources_root, inputs_root=inputs_root)
        if path is None:
            continue
        if not path.exists():
            continue
        parsed = parser(path)
        parsed = _coerce_to_canonical_columns(parsed, primary_source=filename)
        frames.append(IssuerSourceFrame(name=filename, rank=rank, dataframe=parsed))
        source_files.append(str(path.resolve()))
        if path.parent == sec_sources_root:
            used_sec_sources = True
        if path.parent == inputs_root:
            used_inputs = True
            if (sec_sources_root / filename).exists():
                used_input_overrides = True
    return frames, used_sec_sources, used_inputs, used_input_overrides, source_files


def _resolve_source_file(filename: str, *, sec_sources_root: Path, inputs_root: Path) -> Path | None:
    input_path = inputs_root / filename
    if input_path.exists():
        return input_path
    sec_path = sec_sources_root / filename
    if sec_path.exists():
        return sec_path
    return None


def _coerce_to_canonical_columns(df: pd.DataFrame, *, primary_source: str) -> pd.DataFrame:
    columns = {str(col).strip().lower(): str(col) for col in df.columns}

    def _get(*names: str):
        for name in names:
            col = columns.get(name.lower())
            if col is not None:
                return df[col]
        return pd.Series([None] * len(df.index))

    out = pd.DataFrame(
        {
            "symbol": _get("symbol", "ticker"),
            "cik": _get("cik", "cik_str", "issuer_cik"),
            "company_name": _get("company_name", "title", "name", "issuer_name"),
            "exchange": _get("exchange"),
            "is_active": _get("is_active"),
        }
    )
    out["primary_source"] = primary_source
    return out


def _parse_company_tickers_exchange(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
        fields = payload.get("fields")
        if isinstance(fields, list) and all(isinstance(item, str) for item in fields):
            records = [dict(zip(fields, row)) for row in payload["data"] if isinstance(row, list)]
            return pd.DataFrame(records)
        return pd.DataFrame(payload["data"])
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    return pd.DataFrame()


def _parse_company_tickers(path: Path) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        records = [value for value in payload.values() if isinstance(value, dict)]
        return pd.DataFrame(records)
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    return pd.DataFrame()


def _parse_company_tickers_mf(path: Path) -> pd.DataFrame:
    return _parse_company_tickers(path)


def _parse_ticker_txt(path: Path) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = re.split(r"\s+", line)
        if len(parts) < 2:
            continue
        rows.append({"ticker": parts[0], "cik": parts[1]})
    return pd.DataFrame(rows)


def _load_cik_lookup_company_names(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    mapping: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" in line:
            left, right = line.split(":", 1)
            digits = re.sub(r"\D+", "", right)
            name = left.strip()
        else:
            parts = re.split(r"\s{2,}|\t", line)
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            digits = re.sub(r"\D+", "", parts[-1])
        if not name or not digits:
            continue
        mapping[digits] = name
    return mapping
