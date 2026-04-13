from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

import click
import pandas as pd

from py_earnings_calls.augmentation_shared import (
    pack_events_view,
    pack_run_status_not_found,
    pack_run_status_view,
    parse_json_input_payload,
)
from py_earnings_calls.augmentation import (
    AUGMENTATION_TYPES,
    parse_transcript_call_id,
    read_augmentation_artifacts,
    read_augmentation_events,
    read_augmentation_runs,
    submit_producer_artifact,
    submit_producer_run,
    transcript_target_descriptor,
    transcript_augmentation_meta,
    transcript_canonical_key,
    transcript_source_text_version_from_path,
)
from py_earnings_calls.config import (
    AppConfig,
    MCacheEffectiveConfig,
    load_config_from_effective_config,
    load_effective_config,
)
from py_earnings_calls.pipelines.forecast_refresh import run_forecast_refresh
from py_earnings_calls.pipelines.refdata_refresh import run_refdata_refresh
from py_earnings_calls.pipelines.transcript_backfill import run_transcript_backfill
from py_earnings_calls.pipelines.transcript_datetime_audit import run_transcript_datetime_audit
from py_earnings_calls.pipelines.transcript_import import run_transcript_bulk_import
from py_earnings_calls.provider_registry import load_provider_registry, materialize_provider_registry
from py_earnings_calls.refdata import run_refdata_fetch_sec_sources
from py_earnings_calls.resolution import ResolutionMode, parse_resolution_mode
from py_earnings_calls.resolution_service import ProviderAwareResolutionService
from py_earnings_calls.runtime_activity import RuntimeActivityReporter
from py_earnings_calls.runtime_output import render_summary_block
from py_earnings_calls.storage.paths import normalized_path


@click.group()
@click.option("--config", "config_path", type=click.Path(path_type=Path), default=None, help="Path to canonical m-cache.toml config.")
@click.pass_context
def main(ctx: click.Context, config_path: Path | None) -> None:
    """Canonical m-cache command line interface."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.group("earnings")
def earnings_group() -> None:
    """Earnings domain commands."""


@earnings_group.group("refdata")
def earnings_refdata_group() -> None:
    """Refdata operations."""


@earnings_refdata_group.command("refresh")
@click.option("--universe", type=click.Path(path_type=Path), default=None)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_refdata_refresh(
    ctx: click.Context,
    universe: Path | None,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "refdata", "refresh"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        result = run_refdata_refresh(config, universe_path=str(universe) if universe else None)
        finished_at = _utc_now()
        _emit_result(
            title="Refdata refresh complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=False,
            persisted_locally=True,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "refdata refresh"})


@earnings_refdata_group.command("fetch-sec-sources")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_refdata_fetch_sec_sources(
    ctx: click.Context,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "refdata", "fetch-sec-sources"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        result = run_refdata_fetch_sec_sources(config)
        finished_at = _utc_now()
        _emit_result(
            title="Refdata SEC source fetch complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=True,
            persisted_locally=True,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "refdata fetch-sec-sources"})


@earnings_group.group("transcripts")
def earnings_transcripts_group() -> None:
    """Transcript operations."""


@earnings_transcripts_group.command("import-bulk")
@click.option("--dataset", required=True, type=click.Path(path_type=Path))
@click.option(
    "--adapter",
    default="kaggle_motley_fool",
    show_default=True,
    type=click.Choice(["kaggle_motley_fool", "local_tabular", "motley_fool_pickle"]),
)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_transcripts_import_bulk(
    ctx: click.Context,
    dataset: Path,
    adapter: str,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "transcripts", "import-bulk"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        result = run_transcript_bulk_import(config, str(dataset), adapter_name=adapter)
        finished_at = _utc_now()
        _emit_result(
            title="Transcript bulk import complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=False,
            persisted_locally=True,
            provider_requested=adapter,
            provider_used=adapter,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "transcripts import-bulk"})


@earnings_transcripts_group.command("backfill")
@click.option("--manifest", type=click.Path(path_type=Path), default=None)
@click.option("--url", "urls", multiple=True, required=False)
@click.option("--symbol", default=None)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_transcripts_backfill(
    ctx: click.Context,
    manifest: Path | None,
    urls: tuple[str, ...],
    symbol: str | None,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    if manifest is None and not urls:
        raise click.BadParameter("Provide either --manifest or at least one --url.")
    command_path = ["m-cache", "earnings", "transcripts", "backfill"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        reporter.progress(event="phase_start", phase="backfill", detail={"command": "transcripts backfill"})
        result = run_transcript_backfill(
            config,
            manifest_path=str(manifest) if manifest else None,
            urls=list(urls),
            symbol=symbol,
        )
        reporter.progress(event="phase_completed", phase="backfill", counters=_summary_counters(result), detail={"command": "transcripts backfill"})
        finished_at = _utc_now()
        _emit_result(
            title="Transcript backfill complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=True,
            persisted_locally=True,
            provider_requested="motley_fool",
            provider_used="motley_fool",
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "transcripts backfill"})


@earnings_transcripts_group.command("audit-datetime")
@click.option("--provider", default="motley_fool", show_default=True, type=click.Choice(["motley_fool"]))
@click.option("--limit", default=50, show_default=True, type=int)
@click.option("--write-manifest", type=click.Path(path_type=Path), default=None)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_transcripts_audit_datetime(
    ctx: click.Context,
    provider: str,
    limit: int,
    write_manifest: Path | None,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "transcripts", "audit-datetime"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = run_transcript_datetime_audit(
        config,
        provider=provider,
        limit=limit,
        write_manifest_path=str(write_manifest) if write_manifest else None,
    )
    finished_at = _utc_now()
    _emit_result(
        title="Transcript datetime audit complete.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
        provider_requested=provider,
        provider_used=provider,
    )


@earnings_group.group("forecasts")
def earnings_forecasts_group() -> None:
    """Forecast operations."""


@earnings_forecasts_group.command("refresh-daily")
@click.option("--provider-mode", type=click.Choice(["single", "fallback"]), default="single", show_default=True)
@click.option("--provider", type=click.Choice(["finnhub", "fmp"]), default="finnhub", show_default=True)
@click.option("--provider-priority", type=click.Choice(["finnhub", "fmp"]), multiple=True)
@click.option("--date", "as_of_date", required=True)
@click.option("--symbol", "symbols", multiple=True, required=True)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_forecasts_refresh_daily(
    ctx: click.Context,
    provider_mode: str,
    provider: str,
    provider_priority: tuple[str, ...],
    as_of_date: str,
    symbols: tuple[str, ...],
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "forecasts", "refresh-daily"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        reporter.progress(event="phase_start", phase="refresh", detail={"command": "forecasts refresh-daily"})
        result = run_forecast_refresh(
            config,
            symbols=[symbol.upper() for symbol in symbols],
            as_of_date=date.fromisoformat(as_of_date),
            provider=provider,
            provider_mode=provider_mode,
            provider_priority=list(provider_priority),
        )
        reporter.progress(event="phase_completed", phase="refresh", counters=_summary_counters(result), detail={"command": "forecasts refresh-daily"})
        finished_at = _utc_now()
        _emit_result(
            title="Forecast refresh complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=True,
            persisted_locally=True,
            provider_requested=provider,
            provider_used=provider,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "forecasts refresh-daily"})


@earnings_group.group("providers")
def earnings_providers_group() -> None:
    """Provider registry inspection."""


@earnings_providers_group.command("list")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--content-domain", type=click.Choice(["transcript", "forecast"]), default=None)
@click.option("--active-only", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_providers_list(
    ctx: click.Context,
    summary_json: bool,
    content_domain: str | None,
    active_only: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "providers", "list"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    path = materialize_provider_registry(config)
    df = load_provider_registry(config)
    if content_domain:
        df = df[df["content_domain"].astype(str).str.lower() == content_domain]
    if active_only:
        df = df[df["is_active"]]
    providers = df[
        [
            "provider_id",
            "domain",
            "content_domain",
            "display_name",
            "provider_type",
            "is_active",
            "fallback_priority",
            "rate_limit_policy",
            "direct_resolution_allowed",
            "supports_direct_resolution",
            "supports_incremental_refresh",
        ]
    ].to_dict(orient="records")
    result = {
        "provider_count": int(len(df.index)),
        "artifact_path": str(path),
        "providers": providers,
    }
    finished_at = _utc_now()
    _emit_result(
        title="Providers list.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_providers_group.command("show")
@click.option("--provider", "provider_id", required=True)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_providers_show(
    ctx: click.Context,
    provider_id: str,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "providers", "show"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    materialize_provider_registry(config)
    registry = load_provider_registry(config)
    normalized_provider = str(provider_id).strip().lower()
    matches = registry[registry["provider_id"].astype(str).str.lower() == normalized_provider]
    if matches.empty:
        raise click.ClickException(f"Provider not found: {provider_id}")
    row = matches.iloc[-1].to_dict()
    result = _provider_show_payload(row=row, effective=effective)
    finished_at = _utc_now()
    _emit_result(
        title=f"Provider detail for {normalized_provider}.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
        provider_requested=normalized_provider,
        provider_used=normalized_provider,
    )


@earnings_group.group("resolve")
def earnings_resolve_group() -> None:
    """Explicit provider-aware resolution operations."""


@earnings_resolve_group.command("transcript")
@click.option("--call-id", required=True)
@click.option(
    "--resolution-mode",
    default=ResolutionMode.LOCAL_ONLY.value,
    show_default=True,
    type=click.Choice([item.value for item in ResolutionMode]),
)
@click.option("--admin", is_flag=True, default=False, help="Enable operator-gated modes.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_resolve_transcript(
    ctx: click.Context,
    call_id: str,
    resolution_mode: str,
    admin: bool,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "resolve", "transcript"]
    effective, config = _load_runtime(ctx=ctx)
    mode = parse_resolution_mode(resolution_mode)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        reporter.progress(event="phase_start", phase="resolve", detail={"content_domain": "transcript", "call_id": call_id})
        service = ProviderAwareResolutionService(config)
        resolved = service.resolve_transcript_if_missing(
            call_id=call_id,
            resolution_mode=mode,
            allow_admin=admin,
            public_surface=False,
        )
        result = {
            "call_id": call_id,
            "found": resolved.found,
            "served_from": resolved.served_from,
            "resolution_mode": resolved.resolution_mode,
            "provider_requested": resolved.provider_requested,
            "provider_used": resolved.provider_used,
            "method_used": resolved.method_used,
            "success": resolved.success,
            "reason_code": resolved.reason_code,
            "persisted_locally": resolved.persisted_locally,
            "rate_limited": resolved.rate_limited,
            "retry_count": resolved.retry_count,
            "deferred_until": resolved.deferred_until,
            "selection_outcome": resolved.selection_outcome,
            "provider_skip_reasons": resolved.provider_skip_reasons or [],
        }
        if resolved.deferred_until:
            reporter.progress(
                event="deferred",
                phase="resolve",
                detail={"deferred_until": resolved.deferred_until, "provider": resolved.provider_used or resolved.provider_requested},
            )
        reporter.progress(event="phase_completed", phase="resolve", counters=_summary_counters(result), detail={"content_domain": "transcript"})
        finished_at = _utc_now()
        _emit_result(
            title="Transcript resolution complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=mode != ResolutionMode.LOCAL_ONLY,
            persisted_locally=resolved.persisted_locally,
            provider_requested=resolved.provider_requested,
            provider_used=resolved.provider_used,
            rate_limited=resolved.rate_limited,
            retry_count=resolved.retry_count,
            deferred_until=resolved.deferred_until,
            provider_skip_reasons=resolved.provider_skip_reasons or [],
            selection_outcome=resolved.selection_outcome,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "resolve transcript"})


@earnings_resolve_group.command("forecast-snapshot")
@click.option("--provider", required=True, type=click.Choice(["finnhub", "fmp"]))
@click.option("--symbol", required=True)
@click.option("--date", "as_of_date", required=True, help="Snapshot date in YYYY-MM-DD format.")
@click.option(
    "--resolution-mode",
    default=ResolutionMode.LOCAL_ONLY.value,
    show_default=True,
    type=click.Choice([item.value for item in ResolutionMode]),
)
@click.option("--admin", is_flag=True, default=False, help="Enable operator-gated modes.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--progress-json", is_flag=True, default=False)
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None)
@click.pass_context
def earnings_resolve_forecast_snapshot(
    ctx: click.Context,
    provider: str,
    symbol: str,
    as_of_date: str,
    resolution_mode: str,
    admin: bool,
    summary_json: bool,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
) -> None:
    command_path = ["m-cache", "earnings", "resolve", "forecast-snapshot"]
    effective, config = _load_runtime(ctx=ctx)
    mode = parse_resolution_mode(resolution_mode)
    started_at = _utc_now()
    with _runtime_reporter(
        command_path=command_path,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
    ) as reporter:
        reporter.progress(
            event="phase_start",
            phase="resolve",
            detail={"content_domain": "forecast", "provider": provider, "symbol": symbol.upper(), "as_of_date": as_of_date},
        )
        service = ProviderAwareResolutionService(config)
        resolved = service.resolve_forecast_snapshot_if_missing(
            provider=provider,
            symbol=symbol,
            as_of_date=date.fromisoformat(as_of_date),
            resolution_mode=mode,
            allow_admin=admin,
            public_surface=False,
        )
        result = {
            "provider": provider.lower(),
            "symbol": symbol.upper(),
            "as_of_date": as_of_date,
            "found": resolved.found,
            "served_from": resolved.served_from,
            "resolution_mode": resolved.resolution_mode,
            "provider_requested": resolved.provider_requested,
            "provider_used": resolved.provider_used,
            "method_used": resolved.method_used,
            "success": resolved.success,
            "reason_code": resolved.reason_code,
            "persisted_locally": resolved.persisted_locally,
            "rate_limited": resolved.rate_limited,
            "retry_count": resolved.retry_count,
            "deferred_until": resolved.deferred_until,
            "selection_outcome": resolved.selection_outcome,
            "provider_skip_reasons": resolved.provider_skip_reasons or [],
        }
        if resolved.deferred_until:
            reporter.progress(
                event="deferred",
                phase="resolve",
                detail={"deferred_until": resolved.deferred_until, "provider": resolved.provider_used or resolved.provider_requested},
            )
        reporter.progress(event="phase_completed", phase="resolve", counters=_summary_counters(result), detail={"content_domain": "forecast"})
        finished_at = _utc_now()
        _emit_result(
            title="Forecast snapshot resolution complete.",
            result=result,
            summary_json=summary_json,
            quiet=quiet,
            verbose=verbose,
            output_schema=output_schema,
            command_path=command_path,
            started_at=started_at,
            finished_at=finished_at,
            effective=effective,
            remote_attempted=mode != ResolutionMode.LOCAL_ONLY,
            persisted_locally=resolved.persisted_locally,
            provider_requested=resolved.provider_requested,
            provider_used=resolved.provider_used,
            rate_limited=resolved.rate_limited,
            retry_count=resolved.retry_count,
            deferred_until=resolved.deferred_until,
            provider_skip_reasons=resolved.provider_skip_reasons or [],
            selection_outcome=resolved.selection_outcome,
        )
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "resolve forecast-snapshot"})


@earnings_group.group("aug")
def earnings_aug_group() -> None:
    """Transcript augmentation planning and inspection surfaces."""


@earnings_aug_group.command("list-types")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_list_types(
    ctx: click.Context,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "list-types"]
    effective, _config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = {
        "domain": "earnings",
        "resource_family": "transcripts",
        "augmentation_types": list(AUGMENTATION_TYPES),
    }
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation types.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("inspect-target")
@click.option("--resource-family", type=click.Choice(["transcripts", "forecasts"]), default="transcripts", show_default=True)
@click.option("--call-id", default=None, help="Transcript call_id for transcript augmentation targets.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_inspect_target(
    ctx: click.Context,
    resource_family: str,
    call_id: str | None,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "inspect-target"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()

    normalized_family = str(resource_family).strip().lower()
    if normalized_family == "forecasts":
        result = {
            "domain": "earnings",
            "resource_family": normalized_family,
            "text_bearing": False,
            "augmentation_applicable": False,
            "reason_code": "NUMERIC_ONLY_RESOURCE_FAMILY",
            "message": "Forecast snapshots/points are numeric-only and excluded from augmentation.",
            "canonical_key": None,
            "source_text_version": None,
        }
    else:
        if not str(call_id or "").strip():
            raise click.BadParameter("--call-id is required for transcripts augmentation target inspection.")
        normalized_call_id = str(call_id).strip()
        descriptor = transcript_target_descriptor(config, call_id=normalized_call_id)
        transcript_path = descriptor["text_source"]
        source_text_version = transcript_source_text_version_from_path(transcript_path)
        result = {
            "domain": "earnings",
            "resource_family": normalized_family,
            "text_bearing": True,
            "augmentation_applicable": True,
            "reason_code": "AUGMENTATION_ELIGIBLE",
            "message": "Transcript text resources are augmentation-eligible.",
            "canonical_key": transcript_canonical_key(normalized_call_id),
            "call_id": normalized_call_id,
            "transcript_path": transcript_path,
            "source_text_version": source_text_version,
            "target_descriptor": descriptor,
            "augmentation_meta": transcript_augmentation_meta(
                config,
                call_id=normalized_call_id,
                source_text_version=source_text_version,
            ),
        }
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation target inspection.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("target-descriptor")
@click.option("--call-id", required=True, help="Transcript call_id.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_target_descriptor(
    ctx: click.Context,
    call_id: str,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    """Compatibility alias for producer target descriptor reads."""
    command_path = ["m-cache", "earnings", "aug", "target-descriptor"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = transcript_target_descriptor(config, call_id=str(call_id).strip())
    finished_at = _utc_now()
    _emit_result(
        title="Producer target descriptor.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("status")
@click.option("--run-id", default=None, help="Primary run selector.")
@click.option("--idempotency-key", default=None, help="Optional artifact idempotency selector.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_status(
    ctx: click.Context,
    run_id: str | None,
    idempotency_key: str | None,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "status"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    normalized_run_id = str(run_id or "").strip()
    normalized_idempotency_key = str(idempotency_key or "").strip()
    if not normalized_run_id and not normalized_idempotency_key:
        raise click.BadParameter("Provide --run-id (primary) or --idempotency-key.")
    result = _inspect_augmentation_status(
        config=config,
        run_id=normalized_run_id or None,
        idempotency_key=normalized_idempotency_key or None,
    )
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation run status.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("events")
@click.option("--resource-family", type=click.Choice(["transcripts", "forecasts"]), default="transcripts", show_default=True)
@click.option("--call-id", default=None)
@click.option("--run-id", default=None)
@click.option("--limit", default=50, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_events(
    ctx: click.Context,
    resource_family: str,
    call_id: str | None,
    run_id: str | None,
    limit: int,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "events"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = _inspect_augmentation_records(
        config=config,
        resource_family=resource_family,
        call_id=call_id,
        run_id=run_id,
        limit=limit,
        kind="events",
    )
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation event timeline.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("submit-run")
@click.option("--input-json", "input_path", required=True, type=click.Path(path_type=Path))
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_submit_run(
    ctx: click.Context,
    input_path: Path,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "submit-run"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    payload = _read_json_object(input_path)
    try:
        result = submit_producer_run(config, payload)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    finished_at = _utc_now()
    _emit_result(
        title="Producer run submission accepted.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=True,
    )


@earnings_aug_group.command("submit-artifact")
@click.option("--input-json", "input_path", required=True, type=click.Path(path_type=Path))
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_submit_artifact(
    ctx: click.Context,
    input_path: Path,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    command_path = ["m-cache", "earnings", "aug", "submit-artifact"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    payload = _read_json_object(input_path)
    try:
        result = submit_producer_artifact(config, payload)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    finished_at = _utc_now()
    _emit_result(
        title="Producer artifact submission accepted.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=True,
    )


@earnings_aug_group.command("inspect-runs")
@click.option("--resource-family", type=click.Choice(["transcripts", "forecasts"]), default="transcripts", show_default=True)
@click.option("--call-id", default=None)
@click.option("--limit", default=50, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_inspect_runs(
    ctx: click.Context,
    resource_family: str,
    call_id: str | None,
    limit: int,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    """Compatibility alias for detailed run inspection reads."""
    command_path = ["m-cache", "earnings", "aug", "inspect-runs"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = _inspect_augmentation_records(
        config=config,
        resource_family=resource_family,
        call_id=call_id,
        run_id=None,
        limit=limit,
        kind="runs",
    )
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation runs inspection.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


@earnings_aug_group.command("inspect-artifacts")
@click.option("--resource-family", type=click.Choice(["transcripts", "forecasts"]), default="transcripts", show_default=True)
@click.option("--call-id", default=None)
@click.option("--limit", default=50, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--output-schema", type=click.Choice(["legacy", "canonical"]), default="canonical", show_default=True)
@click.option("--quiet", is_flag=True, default=False)
@click.option("--verbose", is_flag=True, default=False)
@click.pass_context
def earnings_aug_inspect_artifacts(
    ctx: click.Context,
    resource_family: str,
    call_id: str | None,
    limit: int,
    summary_json: bool,
    output_schema: str,
    quiet: bool,
    verbose: bool,
) -> None:
    """Compatibility alias for artifact inspection reads."""
    command_path = ["m-cache", "earnings", "aug", "inspect-artifacts"]
    effective, config = _load_runtime(ctx=ctx)
    started_at = _utc_now()
    result = _inspect_augmentation_records(
        config=config,
        resource_family=resource_family,
        call_id=call_id,
        run_id=None,
        limit=limit,
        kind="artifacts",
    )
    finished_at = _utc_now()
    _emit_result(
        title="Augmentation artifacts inspection.",
        result=result,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
        output_schema=output_schema,
        command_path=command_path,
        started_at=started_at,
        finished_at=finished_at,
        effective=effective,
        remote_attempted=False,
        persisted_locally=False,
    )


def _load_runtime(*, ctx: click.Context) -> tuple[MCacheEffectiveConfig, AppConfig]:
    config_path = ctx.obj.get("config_path") if isinstance(ctx.obj, dict) else None
    effective = load_effective_config(config_path=config_path)
    return effective, load_config_from_effective_config(effective)


@contextmanager
def _runtime_reporter(
    *,
    command_path: list[str],
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
    output_schema: str,
):
    reporter = RuntimeActivityReporter(
        command=" ".join(command_path),
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
        output_schema=output_schema,
        domain="earnings",
        command_path=command_path,
    )
    reporter.progress(event="started", phase="command", detail={"command_path": command_path})
    try:
        yield reporter
    except KeyboardInterrupt:
        reporter.progress(event="interrupted", phase="command", detail={"command_path": command_path})
        raise
    except Exception as exc:
        reporter.progress(event="failed", phase="command", detail={"command_path": command_path, "error": str(exc)})
        raise
    finally:
        reporter.close()


def _emit_result(
    *,
    title: str,
    result: dict[str, Any],
    summary_json: bool,
    quiet: bool,
    verbose: bool,
    output_schema: str,
    command_path: list[str],
    started_at: str,
    finished_at: str,
    effective: MCacheEffectiveConfig,
    remote_attempted: bool,
    persisted_locally: bool | None,
    provider_requested: str | None = None,
    provider_used: str | None = None,
    rate_limited: bool = False,
    retry_count: int = 0,
    deferred_until: str | None = None,
    provider_skip_reasons: list[dict[str, str]] | None = None,
    selection_outcome: str | None = None,
) -> None:
    if summary_json:
        if output_schema == "canonical":
            payload = _canonical_summary(
                result=result,
                command_path=command_path,
                started_at=started_at,
                finished_at=finished_at,
                remote_attempted=remote_attempted,
                persisted_locally=persisted_locally,
                provider_requested=provider_requested,
                provider_used=provider_used,
                rate_limited=rate_limited,
                retry_count=retry_count,
                deferred_until=deferred_until,
                provider_skip_reasons=provider_skip_reasons or [],
                selection_outcome=selection_outcome,
                effective=effective,
            )
            click.echo(json.dumps(payload, sort_keys=True))
        else:
            click.echo(json.dumps(result, sort_keys=True))
        return
    mode = _resolve_human_output_mode(quiet=quiet, verbose=verbose, summary_json=summary_json)
    click.echo(render_summary_block(title, result, mode=mode))


def _canonical_summary(
    *,
    result: dict[str, Any],
    command_path: list[str],
    started_at: str,
    finished_at: str,
    remote_attempted: bool,
    persisted_locally: bool | None,
    provider_requested: str | None,
    provider_used: str | None,
    rate_limited: bool,
    retry_count: int,
    deferred_until: str | None,
    provider_skip_reasons: list[dict[str, str]],
    selection_outcome: str | None,
    effective: MCacheEffectiveConfig,
) -> dict[str, Any]:
    elapsed_seconds = max(0.0, (_parse_utc(finished_at) - _parse_utc(started_at)).total_seconds())
    failed = int(result.get("failed_count", result.get("failures", 0)) or 0)
    status = "ok"
    if failed > 0:
        status = "partial"
    errors: list[str] = []
    warnings: list[str] = []
    if failed > 0:
        warnings.append("Command completed with failures.")
    deferred = deferred_until is not None
    defer_reason = "quota_limited" if deferred and rate_limited else None
    return {
        "status": status,
        "domain": "earnings",
        "command_path": command_path,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "resolution_mode": None,
        "remote_attempted": bool(remote_attempted),
        "provider_requested": provider_requested,
        "provider_used": provider_used,
        "selection_outcome": selection_outcome,
        "rate_limited": bool(rate_limited),
        "retry_count": max(0, int(retry_count)),
        "deferred": deferred,
        "deferred_until": deferred_until,
        "defer_reason": defer_reason,
        "provider_skip_reasons": provider_skip_reasons,
        "persisted_locally": persisted_locally,
        "counters": _summary_counters(result),
        "warnings": warnings,
        "errors": errors,
        "effective_config": effective.to_dict(),
        "result": result,
    }


def _provider_show_payload(*, row: dict[str, Any], effective: MCacheEffectiveConfig) -> dict[str, Any]:
    provider_id = str(row.get("provider_id") or "").strip().lower()
    providers = dict(effective.domains.get("earnings", {})).get("providers", {})
    provider_config = dict(providers.get(provider_id, {})) if isinstance(providers, dict) else {}
    auth_env_var = str(provider_config.get("auth_env_var") or row.get("auth_env_var") or "").strip()
    auth_type = str(provider_config.get("auth_type") or row.get("auth_type") or "").strip().lower()
    effective_enabled = bool(provider_config.get("enabled", row.get("is_active", True)))
    effective_auth_present = True
    if auth_type != "none" and auth_env_var:
        effective_auth_present = bool(os.getenv(auth_env_var, "").strip())

    payload = dict(row)
    payload["effective_auth_present"] = effective_auth_present
    payload["effective_enabled"] = effective_enabled
    return payload


def _inspect_augmentation_records(
    *,
    config: AppConfig,
    resource_family: str,
    call_id: str | None,
    run_id: str | None,
    limit: int,
    kind: str,
) -> dict[str, Any]:
    normalized_family = str(resource_family).strip().lower()
    if normalized_family == "forecasts":
        return pack_events_view(
            domain="earnings",
            resource_family=normalized_family,
            augmentation_applicable=False,
            reason_code="NUMERIC_ONLY_RESOURCE_FAMILY",
            message="Forecast snapshots/points are numeric-only and excluded from augmentation.",
            records=[],
        )

    if kind == "runs":
        frame = read_augmentation_runs(config)
    elif kind == "artifacts":
        frame = read_augmentation_artifacts(config)
    elif kind == "events":
        frame = read_augmentation_events(config)
    else:
        raise ValueError(f"unsupported augmentation record kind: {kind}")

    if frame.empty:
        return pack_events_view(
            domain="earnings",
            resource_family=normalized_family,
            augmentation_applicable=True,
            reason_code=None,
            message=None,
            records=[],
        )

    view = frame[
        (frame["domain"].astype(str).str.lower() == "earnings")
        & (frame["resource_family"].astype(str).str.lower() == normalized_family)
    ]
    normalized_call_id = str(call_id or "").strip()
    if normalized_call_id:
        view = view[view["canonical_key"].astype(str) == transcript_canonical_key(normalized_call_id)]
    elif "canonical_key" in view.columns:
        view = view[view["canonical_key"].map(lambda value: parse_transcript_call_id(str(value or "")) is not None)]
    normalized_run_id = str(run_id or "").strip()
    if normalized_run_id and "run_id" in view.columns:
        view = view[view["run_id"].astype(str) == normalized_run_id]
    if "event_at" in view.columns:
        view = view.sort_values(["event_at"], ascending=[False], na_position="last")
    capped = view.head(max(0, int(limit)))
    return pack_events_view(
        domain="earnings",
        resource_family=normalized_family,
        augmentation_applicable=True,
        reason_code=None,
        message=None,
        records=capped.to_dict(orient="records"),
    )


def _inspect_augmentation_status(
    *,
    config: AppConfig,
    run_id: str | None,
    idempotency_key: str | None,
) -> dict[str, Any]:
    runs = read_augmentation_runs(config)
    artifacts = read_augmentation_artifacts(config)
    if not runs.empty:
        runs = runs[
            (runs["domain"].astype(str).str.lower() == "earnings")
            & (runs["resource_family"].astype(str).str.lower() == "transcripts")
        ]
    if not artifacts.empty:
        artifacts = artifacts[
            (artifacts["domain"].astype(str).str.lower() == "earnings")
            & (artifacts["resource_family"].astype(str).str.lower() == "transcripts")
        ]

    normalized_run_id = str(run_id or "").strip()
    normalized_idempotency_key = str(idempotency_key or "").strip()
    run_row: dict[str, Any] | None = None
    artifact_row: dict[str, Any] | None = None

    if normalized_run_id:
        candidate = runs[runs["run_id"].astype(str) == normalized_run_id]
        if not candidate.empty:
            if "event_at" in candidate.columns:
                candidate = candidate.sort_values(["event_at"], ascending=[False], na_position="last")
            run_row = candidate.iloc[0].to_dict()

    if run_row is None and normalized_idempotency_key:
        artifact_match = artifacts[artifacts["idempotency_key"].astype(str) == normalized_idempotency_key]
        if not artifact_match.empty:
            if "event_at" in artifact_match.columns:
                artifact_match = artifact_match.sort_values(["event_at"], ascending=[False], na_position="last")
            artifact_row = artifact_match.iloc[0].to_dict()
            matched_runs = runs[
                (runs["canonical_key"].astype(str) == str(artifact_row.get("canonical_key") or ""))
                & (runs["augmentation_type"].astype(str) == str(artifact_row.get("augmentation_type") or ""))
                & (runs["source_text_version"].astype(str) == str(artifact_row.get("source_text_version") or ""))
                & (runs["producer_name"].astype(str) == str(artifact_row.get("producer_name") or ""))
                & (runs["producer_version"].astype(str) == str(artifact_row.get("producer_version") or ""))
            ]
            if not matched_runs.empty:
                if "event_at" in matched_runs.columns:
                    matched_runs = matched_runs.sort_values(["event_at"], ascending=[False], na_position="last")
                run_row = matched_runs.iloc[0].to_dict()

    if run_row is not None and artifact_row is None:
        artifact_match = artifacts[
            (artifacts["canonical_key"].astype(str) == str(run_row.get("canonical_key") or ""))
            & (artifacts["augmentation_type"].astype(str) == str(run_row.get("augmentation_type") or ""))
            & (artifacts["source_text_version"].astype(str) == str(run_row.get("source_text_version") or ""))
            & (artifacts["producer_name"].astype(str) == str(run_row.get("producer_name") or ""))
            & (artifacts["producer_version"].astype(str) == str(run_row.get("producer_version") or ""))
        ]
        if not artifact_match.empty:
            if "event_at" in artifact_match.columns:
                artifact_match = artifact_match.sort_values(["event_at"], ascending=[False], na_position="last")
            artifact_row = artifact_match.iloc[0].to_dict()

    if run_row is None:
        reason_code = "RUN_NOT_FOUND"
        if normalized_idempotency_key and artifact_row is not None:
            reason_code = "RUN_NOT_FOUND_FOR_IDEMPOTENCY_KEY"
        return pack_run_status_not_found(
            domain="earnings",
            resource_family="transcripts",
            run_id=normalized_run_id or None,
            idempotency_key=normalized_idempotency_key or None,
            reason_code=reason_code,
        )

    canonical_key = str(run_row.get("canonical_key") or "").strip()
    source_text_version = str(run_row.get("source_text_version") or "").strip() or None
    call_id = parse_transcript_call_id(canonical_key)
    augmentation_stale: bool | None = None
    if call_id is not None:
        meta = transcript_augmentation_meta(
            config,
            call_id=call_id,
            source_text_version=source_text_version,
        )
        stale_value = meta.get("augmentation_stale")
        augmentation_stale = bool(stale_value) if isinstance(stale_value, bool) else None
    return pack_run_status_view(
        domain="earnings",
        resource_family="transcripts",
        run_id=str(run_row.get("run_id") or "").strip() or None,
        idempotency_key=str((artifact_row or {}).get("idempotency_key") or "").strip() or (normalized_idempotency_key or None),
        canonical_key=canonical_key,
        augmentation_type=str(run_row.get("augmentation_type") or "").strip() or None,
        source_text_version=source_text_version,
        producer_name=str(run_row.get("producer_name") or "").strip() or None,
        producer_version=str(run_row.get("producer_version") or "").strip() or None,
        status=str(run_row.get("status") or "").strip() or None,
        success=bool(run_row.get("success")) if run_row.get("success") is not None else None,
        reason_code=str(run_row.get("reason_code") or "").strip() or None,
        persisted_locally=bool(run_row.get("persisted_locally")) if run_row.get("persisted_locally") is not None else None,
        augmentation_stale=augmentation_stale,
        last_updated_at=str(run_row.get("event_at") or "").strip() or None,
    )


def _summary_counters(result: dict[str, object]) -> dict[str, int | float]:
    known = {
        "candidate_count": result.get("requested_count"),
        "attempted_count": result.get("requested_count"),
        "succeeded_count": result.get("fetched_count"),
        "failed_count": result.get("failed_count", result.get("failures")),
        "skipped_count": result.get("skipped"),
        "persisted_count": result.get("fetched_count"),
        "lookup_refreshed_count": len(result.get("lookup_updates", [])) if isinstance(result.get("lookup_updates"), list) else None,
        "discrepancy_count": result.get("discrepancy_count"),
    }
    out: dict[str, int | float] = {}
    for key, value in known.items():
        if isinstance(value, (int, float)):
            out[key] = value
    return out


def _resolve_human_output_mode(*, quiet: bool, verbose: bool, summary_json: bool) -> str:
    if summary_json:
        return "default"
    if quiet and verbose:
        raise click.BadParameter("`--quiet` and `--verbose` cannot be used together.")
    if quiet:
        return "quiet"
    if verbose:
        return "verbose"
    return "default"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        return parse_json_input_payload(path)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
