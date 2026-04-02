from __future__ import annotations

from enum import Enum


class ResolutionMode(str, Enum):
    LOCAL_ONLY = "local_only"
    RESOLVE_IF_MISSING = "resolve_if_missing"
    REFRESH_IF_STALE = "refresh_if_stale"


PUBLIC_READ_RESOLUTION_MODES = {
    ResolutionMode.LOCAL_ONLY,
    ResolutionMode.RESOLVE_IF_MISSING,
}


def parse_resolution_mode(value: str | None, *, default: ResolutionMode = ResolutionMode.LOCAL_ONLY) -> ResolutionMode:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    for mode in ResolutionMode:
        if mode.value == normalized:
            return mode
    raise ValueError(f"Unsupported resolution_mode: {value}")
