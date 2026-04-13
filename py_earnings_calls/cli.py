from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path

import click

from py_earnings_calls.config import load_config
from py_earnings_calls.lookup import load_lookup_dataframe, query_forecasts, query_transcripts
from py_earnings_calls.monitoring import run_monitor_poll
from py_earnings_calls.pipelines.forecast_refresh import run_forecast_refresh
from py_earnings_calls.pipelines.lookup_refresh import run_lookup_refresh
from py_earnings_calls.pipelines.refdata_refresh import run_refdata_refresh
from py_earnings_calls.pipelines.storage_layout import (
    run_storage_cleanup_legacy,
    run_storage_migrate_layout,
    run_storage_verify_layout,
)
from py_earnings_calls.pipelines.transcript_datetime_audit import run_transcript_datetime_audit
from py_earnings_calls.refdata import run_refdata_fetch_sec_sources
from py_earnings_calls.pipelines.transcript_backfill import run_transcript_backfill
from py_earnings_calls.pipelines.transcript_import import run_transcript_bulk_import
from py_earnings_calls.reconciliation import run_reconciliation
from py_earnings_calls.resolution import ResolutionMode, parse_resolution_mode
from py_earnings_calls.resolution_service import ProviderAwareResolutionService
from py_earnings_calls.runtime_activity import RuntimeActivityReporter
from py_earnings_calls.runtime_output import render_summary_block


@click.group()
def main() -> None:
    """py-earnings-calls command line interface."""


@main.group("refdata")
def refdata_group() -> None:
    """Reference-data operations."""


@refdata_group.command("refresh")
@click.option(
    "--universe",
    type=click.Path(path_type=Path),
    default=None,
    help="Optional CSV/parquet issuer universe. When provided, it is the sole issuer input for this run.",
)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
def refdata_refresh(universe: Path | None, quiet: bool, verbose: bool, log_level: str, log_file: Path | None) -> None:
    config = load_config()
    with _runtime_reporter(command="refdata refresh", log_level=log_level, log_file=log_file) as reporter:
        result = run_refdata_refresh(config, universe_path=str(universe) if universe else None)
        display = dict(result)
        display.setdefault("next_step", "Run `py-earnings-calls transcripts import-bulk ...` or `forecasts refresh-daily ...`.")
        _emit_result_summary("Refdata refresh complete.", display, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(display), detail={"command": "refdata refresh"})


@refdata_group.command("fetch-sec-sources")
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
def refdata_fetch_sec_sources(quiet: bool, verbose: bool, log_level: str, log_file: Path | None) -> None:
    """Fetch bounded SEC issuer reference files into refdata/sec_sources/."""
    config = load_config()
    with _runtime_reporter(command="refdata fetch-sec-sources", log_level=log_level, log_file=log_file) as reporter:
        result = run_refdata_fetch_sec_sources(config)
        _emit_result_summary("Refdata SEC source fetch complete.", result, quiet=quiet, verbose=verbose)
        reporter.progress(
            event="completed",
            phase="command",
            counters=_summary_counters(result),
            detail={"command": "refdata fetch-sec-sources"},
        )


@main.group("transcripts")
def transcripts_group() -> None:
    """Transcript operations."""


@transcripts_group.command("import-bulk")
@click.option("--dataset", required=True, type=click.Path(path_type=Path))
@click.option(
    "--adapter",
    default="kaggle_motley_fool",
    show_default=True,
    type=click.Choice(["kaggle_motley_fool", "local_tabular", "motley_fool_pickle"]),
    help="Bulk import adapter. Use motley_fool_pickle only for known local DataFrame pickle datasets.",
)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
def transcripts_import_bulk(dataset: Path, adapter: str, quiet: bool, verbose: bool, log_level: str, log_file: Path | None) -> None:
    config = load_config()
    with _runtime_reporter(command="transcripts import-bulk", log_level=log_level, log_file=log_file) as reporter:
        result = run_transcript_bulk_import(config, str(dataset), adapter_name=adapter)
        display = dict(result)
        display.setdefault("next_step", "Run `py-earnings-calls lookup refresh` or `transcripts backfill ...`.")
        _emit_result_summary("Transcript bulk import complete.", display, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(display), detail={"command": "transcripts import-bulk"})


@transcripts_group.command("backfill")
@click.option("--manifest", type=click.Path(path_type=Path), default=None, help="Backfill manifest (CSV required; JSONL optional).")
@click.option("--url", "urls", multiple=True, required=False, help="Repeatable transcript page URL (compatibility path).")
@click.option("--symbol", default=None, help="Optional symbol override.")
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def transcripts_backfill(
    manifest: Path | None,
    urls: tuple[str, ...],
    symbol: str | None,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    if manifest is None and not urls:
        raise click.BadParameter("Provide either --manifest or at least one --url.")
    config = load_config()
    with _runtime_reporter(
        command="transcripts backfill",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    ) as reporter:
        reporter.progress(event="phase_start", phase="backfill", detail={"command": "transcripts backfill"})
        result = run_transcript_backfill(
            config,
            manifest_path=str(manifest) if manifest else None,
            urls=list(urls),
            symbol=symbol,
        )
        reporter.progress(event="phase_completed", phase="backfill", counters=_summary_counters(result), detail={"command": "transcripts backfill"})
        display = dict(result)
        display.setdefault("next_step", "Run `py-earnings-calls lookup refresh` to refresh local query artifacts.")
        _emit_result_summary("Transcript backfill complete.", display, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(display), detail={"command": "transcripts backfill"})


@transcripts_group.command("audit-datetime")
@click.option("--provider", default="motley_fool", show_default=True, type=click.Choice(["motley_fool"]))
@click.option("--limit", default=50, show_default=True, type=int, help="Max suspect rows in sample/export output.")
@click.option("--write-manifest", type=click.Path(path_type=Path), default=None, help="Optional CSV path for suspect re-backfill manifest.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def transcripts_audit_datetime(provider: str, limit: int, write_manifest: Path | None, summary_json: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    result = run_transcript_datetime_audit(
        config,
        provider=provider,
        limit=limit,
        write_manifest_path=str(write_manifest) if write_manifest else None,
    )

    display = dict(result)
    if int(result.get("suspect_rows_count", 0)) == 0:
        display.setdefault("status", "no_suspects")
        display.setdefault("next_step", "No datetime correction re-backfill is needed right now.")
    elif result.get("manifest_written"):
        display.setdefault(
            "next_step",
            "Run `py-earnings-calls transcripts backfill --manifest <path>` then `py-earnings-calls lookup refresh`.",
        )
    else:
        display.setdefault(
            "next_step",
            "Run again with `--write-manifest <path>`, then backfill that manifest and refresh lookup.",
        )
    _emit_result_summary(
        "Transcript datetime audit complete.",
        display,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
    )


@main.group("forecasts")
def forecasts_group() -> None:
    """Forecast operations."""


@forecasts_group.command("refresh-daily")
@click.option("--provider-mode", type=click.Choice(["single", "fallback"]), default="single", show_default=True)
@click.option("--provider", type=click.Choice(["finnhub", "fmp"]), default="finnhub", show_default=True)
@click.option(
    "--provider-priority",
    type=click.Choice(["finnhub", "fmp"]),
    multiple=True,
    help="Ordered provider priority list used in fallback mode.",
)
@click.option("--date", "as_of_date", required=True, help="Snapshot date in YYYY-MM-DD format.")
@click.option("--symbol", "symbols", multiple=True, required=True, help="Repeatable symbol.")
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def forecasts_refresh_daily(
    provider_mode: str,
    provider: str,
    provider_priority: tuple[str, ...],
    as_of_date: str,
    symbols: tuple[str, ...],
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    with _runtime_reporter(
        command="forecasts refresh-daily",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
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
        display = dict(result)
        display.setdefault("next_step", "Run `py-earnings-calls lookup refresh` to refresh local query artifacts.")
        _emit_result_summary("Forecast refresh complete.", display, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(display), detail={"command": "forecasts refresh-daily"})


@main.group("lookup")
def lookup_group() -> None:
    """Lookup operations."""


@lookup_group.command("refresh")
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def lookup_refresh(
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    with _runtime_reporter(
        command="lookup refresh",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    ) as reporter:
        reporter.progress(event="phase_start", phase="refresh", detail={"command": "lookup refresh"})
        result = run_lookup_refresh(config)
        reporter.progress(event="phase_completed", phase="refresh", counters=_summary_counters(result), detail={"command": "lookup refresh"})
        display = dict(result)
        display.setdefault("next_step", "Use API reads (local-first) or `lookup query` for local inspection.")
        _emit_result_summary("Lookup refresh complete.", display, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(display), detail={"command": "lookup refresh"})


@lookup_group.command("query")
@click.option("--scope", type=click.Choice(["transcripts", "forecasts"]), required=True)
@click.option("--symbol", default=None)
@click.option("--call-id", default=None)
@click.option("--json", "as_json", is_flag=True, default=False)
@click.option(
    "--compact-json",
    is_flag=True,
    default=False,
    help="When used with --json, omit large transcript payload fields for operator readability.",
)
def lookup_query(scope: str, symbol: str | None, call_id: str | None, as_json: bool, compact_json: bool) -> None:
    config = load_config()
    df = load_lookup_dataframe(config, scope=scope)
    if scope == "transcripts":
        filtered = query_transcripts(df, symbol=symbol, call_id=call_id)
    else:
        filtered = query_forecasts(df, symbol=symbol)

    if as_json:
        payload = filtered.to_dict(orient="records")
        if compact_json and scope == "transcripts":
            payload = _compact_transcript_json_rows(payload)
        click.echo(json.dumps(payload, sort_keys=True))
    elif filtered.empty:
        click.echo("No rows matched.")
    else:
        click.echo(filtered.to_string(index=False))


@main.group("monitor")
def monitor_group() -> None:
    """Monitor operations."""


@monitor_group.command("poll")
@click.option("--date", "target_date", required=True)
@click.option("--warm", is_flag=True, default=False, help="Enable bounded warm actions for retryable/missing targets.")
@click.option("--symbol", "symbols", multiple=True, help="Optional symbol targets for forecast checks.")
@click.option("--max-symbols", default=200, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def monitor_poll(
    target_date: str,
    warm: bool,
    symbols: tuple[str, ...],
    max_symbols: int,
    summary_json: bool,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    with _runtime_reporter(
        command="monitor poll",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    ) as reporter:
        reporter.progress(event="phase_start", phase="poll", detail={"command": "monitor poll"})
        result = run_monitor_poll(
            config,
            target_date=date.fromisoformat(target_date),
            warm=warm,
            symbols=list(symbols),
            max_symbols=max_symbols,
        )
        reporter.progress(event="phase_completed", phase="poll", counters=_summary_counters(result), detail={"command": "monitor poll"})
        _emit_operator_summary("Monitor poll result.", result, summary_json=summary_json, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "monitor poll"})


@monitor_group.command("loop")
@click.option("--date", "target_date", required=True)
@click.option("--interval-seconds", default=30.0, show_default=True, type=float)
@click.option("--max-iterations", default=10, show_default=True, type=int)
@click.option("--warm", is_flag=True, default=False, help="Enable bounded warm actions for retryable/missing targets.")
@click.option("--symbol", "symbols", multiple=True, help="Optional symbol targets for forecast checks.")
@click.option("--max-symbols", default=200, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def monitor_loop(
    target_date: str,
    interval_seconds: float,
    max_iterations: int,
    warm: bool,
    symbols: tuple[str, ...],
    max_symbols: int,
    summary_json: bool,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    from py_earnings_calls.monitoring import run_monitor_loop

    with _runtime_reporter(
        command="monitor loop",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    ) as reporter:
        reporter.progress(event="phase_start", phase="loop", detail={"command": "monitor loop"})
        result = run_monitor_loop(
            config,
            target_date=date.fromisoformat(target_date),
            interval_seconds=interval_seconds,
            max_iterations=max_iterations,
            warm=warm,
            symbols=list(symbols),
            max_symbols=max_symbols,
            iteration_progress_callback=lambda event, counters: reporter.progress(
                event=event,
                phase="monitor_iteration",
                counters=counters,
                detail={"command": "monitor loop"},
            ),
            heartbeat_callback=lambda: reporter.maybe_heartbeat(
                phase="monitor_loop_wait",
                detail={"command": "monitor loop"},
            ),
        )
        reporter.progress(event="phase_completed", phase="loop", counters=_summary_counters(result), detail={"command": "monitor loop"})
        _emit_operator_summary("Monitor loop result.", result, summary_json=summary_json, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "monitor loop"})


@main.group("reconcile")
def reconcile_group() -> None:
    """Reconciliation operations."""


@reconcile_group.command("run")
@click.option("--date", "target_date", required=True)
@click.option("--symbol", "symbols", multiple=True, help="Optional symbols for forecast reconciliation.")
@click.option("--max-symbols", default=200, show_default=True, type=int)
@click.option("--catch-up-warm", is_flag=True, default=False, help="Optionally run bounded warm actions for discrepancies.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def reconcile_run(
    target_date: str,
    symbols: tuple[str, ...],
    max_symbols: int,
    catch_up_warm: bool,
    summary_json: bool,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    with _runtime_reporter(
        command="reconcile run",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    ) as reporter:
        reporter.progress(event="phase_start", phase="reconcile", detail={"command": "reconcile run"})
        result = run_reconciliation(
            config,
            target_date=date.fromisoformat(target_date),
            symbols=list(symbols),
            max_symbols=max_symbols,
            catch_up_warm=catch_up_warm,
        )
        reporter.progress(event="phase_completed", phase="reconcile", counters=_summary_counters(result), detail={"command": "reconcile run"})
        _emit_operator_summary("Reconciliation result.", result, summary_json=summary_json, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "reconcile run"})


@main.group("resolve")
def resolve_group() -> None:
    """Explicit provider-aware resolution operations."""


@resolve_group.command("transcript")
@click.option("--call-id", required=True)
@click.option(
    "--resolution-mode",
    default=ResolutionMode.LOCAL_ONLY.value,
    show_default=True,
    type=click.Choice([item.value for item in ResolutionMode]),
)
@click.option("--admin", is_flag=True, default=False, help="Enable operator-gated modes.")
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def resolve_transcript(call_id: str, resolution_mode: str, admin: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    service = ProviderAwareResolutionService(config)
    mode = parse_resolution_mode(resolution_mode)
    result = service.resolve_transcript_if_missing(
        call_id=call_id,
        resolution_mode=mode,
        allow_admin=admin,
        public_surface=False,
    )
    _emit_result_summary("Transcript resolution complete.", {
        "call_id": call_id,
        "found": result.found,
        "served_from": result.served_from,
        "resolution_mode": result.resolution_mode,
        "provider_requested": result.provider_requested,
        "provider_used": result.provider_used,
        "method_used": result.method_used,
        "success": result.success,
        "reason_code": result.reason_code,
        "persisted_locally": result.persisted_locally,
    }, quiet=quiet, verbose=verbose)


@resolve_group.command("forecast-snapshot")
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
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def resolve_forecast_snapshot(provider: str, symbol: str, as_of_date: str, resolution_mode: str, admin: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    service = ProviderAwareResolutionService(config)
    mode = parse_resolution_mode(resolution_mode)
    result = service.resolve_forecast_snapshot_if_missing(
        provider=provider,
        symbol=symbol,
        as_of_date=date.fromisoformat(as_of_date),
        resolution_mode=mode,
        allow_admin=admin,
        public_surface=False,
    )
    _emit_result_summary("Forecast snapshot resolution complete.", {
        "provider": provider,
        "symbol": symbol.upper(),
        "as_of_date": as_of_date,
        "found": result.found,
        "served_from": result.served_from,
        "resolution_mode": result.resolution_mode,
        "provider_requested": result.provider_requested,
        "provider_used": result.provider_used,
        "method_used": result.method_used,
        "success": result.success,
        "reason_code": result.reason_code,
        "persisted_locally": result.persisted_locally,
    }, quiet=quiet, verbose=verbose)


@main.group("storage")
def storage_group() -> None:
    """Archive layout migration and verification operations."""


@storage_group.command("migrate-layout")
@click.option("--dry-run", is_flag=True, default=False, help="Plan migration without writing files or updating parquet.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def storage_migrate_layout(dry_run: bool, summary_json: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    result = run_storage_migrate_layout(config, dry_run=dry_run)
    display = dict(result)
    display.setdefault("next_step", "Run `py-earnings-calls storage cleanup-legacy` after verifying results.")
    _emit_result_summary(
        "Storage layout migration complete.",
        display,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
    )


@storage_group.command("verify-layout")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def storage_verify_layout(summary_json: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    result = run_storage_verify_layout(config)
    display = dict(result)
    display.setdefault("next_step", "If legacy roots remain, run `py-earnings-calls storage cleanup-legacy`.")
    _emit_result_summary(
        "Storage layout verification complete.",
        display,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
    )


@storage_group.command("cleanup-legacy")
@click.option("--dry-run", is_flag=True, default=False, help="Plan verified legacy cleanup without deleting files.")
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
def storage_cleanup_legacy(dry_run: bool, summary_json: bool, quiet: bool, verbose: bool) -> None:
    config = load_config()
    result = run_storage_cleanup_legacy(config, dry_run=dry_run)
    display = dict(result)
    display.setdefault("next_step", "Run `py-earnings-calls storage verify-layout` to confirm post-cleanup state.")
    _emit_result_summary(
        "Legacy storage cleanup complete.",
        display,
        summary_json=summary_json,
        quiet=quiet,
        verbose=verbose,
    )


def _emit_operator_summary(
    title: str,
    result: dict[str, object],
    *,
    summary_json: bool,
    quiet: bool = False,
    verbose: bool = False,
) -> None:
    stable = {
        "mode": result.get("mode"),
        "iterations": result.get("iterations"),
        "targets_considered": result.get("targets_considered"),
        "actions_taken": result.get("actions_taken"),
        "skipped": result.get("skipped"),
        "failures": result.get("failures"),
        "lookup_updates": result.get("lookup_updates"),
        "artifacts_written": result.get("artifacts_written"),
    }
    _emit_result_summary(title, stable, summary_json=summary_json, quiet=quiet, verbose=verbose)


def _emit_result_summary(
    title: str,
    result: dict[str, object],
    *,
    summary_json: bool = False,
    quiet: bool = False,
    verbose: bool = False,
) -> None:
    if summary_json:
        click.echo(json.dumps(result, sort_keys=True))
        return
    mode = _resolve_human_output_mode(quiet=quiet, verbose=verbose, summary_json=summary_json)
    click.echo(render_summary_block(title, result, mode=mode))


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


@contextmanager
def _runtime_reporter(
    *,
    command: str,
    log_level: str,
    log_file: Path | None,
    progress_json: bool = False,
    progress_heartbeat_seconds: float = 0.0,
):
    reporter = RuntimeActivityReporter(
        command=command,
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    )
    reporter.progress(event="started", phase="command", detail={"command": command})
    try:
        yield reporter
    except KeyboardInterrupt:
        reporter.progress(event="interrupted", phase="command", detail={"command": command})
        raise
    except Exception as exc:
        reporter.log("error", f"{command} failed: {exc}")
        reporter.progress(event="failed", phase="command", detail={"command": command, "error": str(exc)})
        raise
    finally:
        reporter.close()


def _summary_counters(result: dict[str, object]) -> dict[str, object]:
    keys = [
        "requested_count",
        "fetched_count",
        "failed_count",
        "snapshot_count",
        "point_count",
        "targets_considered",
        "actions_taken",
        "skipped",
        "failures",
        "iterations",
    ]
    out: dict[str, object] = {}
    for key in keys:
        if key in result:
            out[key] = result.get(key)
    return out


def _compact_transcript_json_rows(rows: list[dict]) -> list[dict]:
    compacted: list[dict] = []
    for row in rows:
        trimmed = dict(row)
        trimmed.pop("raw_html", None)
        trimmed.pop("transcript_text", None)
        compacted.append(trimmed)
    return compacted


if __name__ == "__main__":
    main()
