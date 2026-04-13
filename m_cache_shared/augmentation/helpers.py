from __future__ import annotations

from pathlib import Path

from m_cache_shared.augmentation.enums import CANONICAL_AUG_COMMAND_ALIASES
from m_cache_shared.augmentation.schema_loaders import load_json_schema as _load_json_schema


def coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "t", "1", "yes", "y"}


def max_nonempty_text(values: list[str]) -> str | None:
    bucket = [str(value).strip() for value in values if str(value).strip()]
    if not bucket:
        return None
    return max(bucket)


def to_int_or_none(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def normalize_aug_command_name(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in CANONICAL_AUG_COMMAND_ALIASES:
        return CANONICAL_AUG_COMMAND_ALIASES[normalized]
    return normalized


def load_json_schema(schema_path: str | Path) -> dict[str, object]:
    # Compatibility shim: canonical export lives in schema_loaders.py.
    return _load_json_schema(schema_path)
