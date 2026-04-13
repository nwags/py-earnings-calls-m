from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json_schema(schema_path: str | Path) -> dict[str, Any]:
    path = Path(schema_path)
    return json.loads(path.read_text(encoding="utf-8"))
