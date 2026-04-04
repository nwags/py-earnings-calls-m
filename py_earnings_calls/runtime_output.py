from __future__ import annotations


def render_summary_block(title: str, values: dict[str, object], *, mode: str = "default") -> str:
    if mode == "quiet":
        return _render_quiet_summary_block(title, values)
    if mode == "verbose":
        return _render_verbose_summary_block(title, values)
    return _render_default_summary_block(title, values)


def _render_default_summary_block(title: str, values: dict[str, object]) -> str:
    lines = [title]
    ordered_keys = _ordered_keys(values)
    for key in ordered_keys:
        value = values.get(key)
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _render_quiet_summary_block(title: str, values: dict[str, object]) -> str:
    lines = [title]
    quiet_keys = [
        "status",
        "requested_count",
        "fetched_count",
        "failed_count",
        "snapshot_count",
        "point_count",
        "document_count",
        "targets_considered",
        "actions_taken",
        "success",
        "reason_code",
    ]
    for key in quiet_keys:
        if key not in values:
            continue
        value = values.get(key)
        if _is_nested(value):
            continue
        lines.append(f"- {key}: {value}")
    if "next_step" in values and _is_incomplete_or_failed(values):
        lines.append(f"- next_step: {values.get('next_step')}")
    return "\n".join(lines)


def _render_verbose_summary_block(title: str, values: dict[str, object]) -> str:
    lines = [title]
    ordered_keys = _ordered_keys(values)
    for key in ordered_keys:
        value = values.get(key)
        lines.append(f"- {key}: {_format_verbose_value(value)}")
    return "\n".join(lines)


def _ordered_keys(values: dict[str, object]) -> list[str]:
    preferred_order = [
        "mode",
        "status",
        "requested_count",
        "fetched_count",
        "failed_count",
        "total_fetched_rows_considered",
        "rows_with_transcript_visible_datetime",
        "rows_with_transcript_structured_datetime",
        "rows_with_article_published_datetime",
        "rows_with_missing_datetime",
        "rows_with_legacy_unknown_source",
        "suspect_rows_count",
        "suspect_rows_sample",
        "manifest_written",
        "manifest_path",
        "snapshot_count",
        "point_count",
        "document_count",
        "artifact_count",
        "artifact_paths",
        "manifest_paths",
        "archive_manifest_path",
        "failures_path",
        "success",
        "reason_code",
        "next_step",
    ]
    seen = set()
    ordered_keys = [key for key in preferred_order if key in values]
    seen.update(ordered_keys)
    ordered_keys.extend([key for key in values.keys() if key not in seen])
    return ordered_keys


def _is_nested(value: object) -> bool:
    return isinstance(value, (dict, list, tuple, set))


def _is_incomplete_or_failed(values: dict[str, object]) -> bool:
    if values.get("success") is False:
        return True
    for key in ["failed_count", "failures", "skipped"]:
        value = values.get(key)
        if isinstance(value, int) and value > 0:
            return True
    requested = values.get("requested_count")
    fetched = values.get("fetched_count")
    if isinstance(requested, int) and isinstance(fetched, int) and fetched < requested:
        return True
    status = str(values.get("status") or "").strip().lower()
    if status in {"failed", "error", "partial", "incomplete"}:
        return True
    return False


def _format_verbose_value(value: object) -> object:
    if isinstance(value, str):
        return value if len(value) <= 200 else f"{value[:200]}... (len={len(value)})"
    if isinstance(value, list):
        if len(value) <= 5:
            return [_format_verbose_value(item) for item in value]
        head = [_format_verbose_value(item) for item in value[:3]]
        return f"{head} ... (len={len(value)})"
    if isinstance(value, tuple):
        as_list = list(value)
        return _format_verbose_value(as_list)
    if isinstance(value, dict):
        items = list(value.items())
        if len(items) <= 8:
            return {k: _format_verbose_value(v) for (k, v) in items}
        limited = {k: _format_verbose_value(v) for (k, v) in items[:6]}
        limited["..."] = f"{len(items) - 6} more fields"
        return limited
    return value
