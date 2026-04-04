from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import time
from typing import TextIO

import click


LOG_LEVEL_ORDER = {
    "debug": 10,
    "info": 20,
    "warning": 30,
    "error": 40,
}


def normalize_log_level(value: str) -> str:
    level = str(value or "").strip().lower()
    if level not in LOG_LEVEL_ORDER:
        raise ValueError(f"Unsupported log level: {value}")
    return level


@dataclass
class RuntimeActivityReporter:
    command: str
    log_level: str = "info"
    log_file: Path | None = None
    progress_json: bool = False
    progress_heartbeat_seconds: float = 0.0
    _started_at: float = field(default_factory=time.monotonic, init=False)
    _last_progress_emit_at: float = field(default_factory=time.monotonic, init=False)
    _file_handle: TextIO | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        self.log_level = normalize_log_level(self.log_level)
        self.progress_heartbeat_seconds = max(0.0, float(self.progress_heartbeat_seconds or 0.0))
        if self.log_file is not None:
            path = Path(self.log_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._file_handle = path.open("a", encoding="utf-8")

    def close(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()
            self._file_handle = None

    def log(self, level: str, message: str) -> None:
        normalized = normalize_log_level(level)
        if LOG_LEVEL_ORDER[normalized] < LOG_LEVEL_ORDER[self.log_level]:
            return
        line = f"[{normalized}] {message}"
        click.echo(line, err=True)
        self._write_file_line(line)

    def progress(
        self,
        *,
        event: str,
        phase: str,
        counters: dict[str, object] | None = None,
        detail: object | None = None,
    ) -> None:
        if not self.progress_json:
            return
        payload: dict[str, object] = {
            "event": event,
            "phase": phase,
            "elapsed_seconds": round(max(0.0, time.monotonic() - self._started_at), 3),
            "counters": counters or {},
            "detail": detail,
        }
        line = json.dumps(payload, sort_keys=True)
        click.echo(line, err=True)
        self._write_file_line(line)
        self._last_progress_emit_at = time.monotonic()

    def maybe_heartbeat(
        self,
        *,
        phase: str,
        counters: dict[str, object] | None = None,
        detail: object | None = None,
    ) -> None:
        if not self.progress_json or self.progress_heartbeat_seconds <= 0:
            return
        now = time.monotonic()
        if (now - self._last_progress_emit_at) < self.progress_heartbeat_seconds:
            return
        self.progress(event="heartbeat", phase=phase, counters=counters, detail=detail)

    def _write_file_line(self, line: str) -> None:
        if self._file_handle is None:
            return
        self._file_handle.write(line + "\n")
        self._file_handle.flush()

