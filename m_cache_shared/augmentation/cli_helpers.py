from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def parse_json_input_payload(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        raise ValueError(f"JSON input file not found: {file_path}")
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {file_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object.")
    return payload
