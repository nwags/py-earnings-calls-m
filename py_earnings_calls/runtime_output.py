from __future__ import annotations


def render_summary_block(title: str, values: dict[str, object]) -> str:
    lines = [title]
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
    for key in ordered_keys:
        value = values.get(key)
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)
