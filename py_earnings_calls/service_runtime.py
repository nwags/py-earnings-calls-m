from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import click
import uvicorn

from py_earnings_calls.api.app import create_app
from py_earnings_calls.config import load_config
from py_earnings_calls.monitoring import run_monitor_loop, run_monitor_poll
from py_earnings_calls.runtime_activity import RuntimeActivityReporter
from py_earnings_calls.runtime_output import render_summary_block


@click.group()
def main() -> None:
    """Runtime wrapper."""


@main.command("api")
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8000, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False, help="Emit machine-readable startup summary.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress non-essential startup output.")
@click.option("--verbose", is_flag=True, default=False, help="Include additional startup detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def api(
    host: str,
    port: int,
    summary_json: bool,
    quiet: bool,
    verbose: bool,
    log_level: str,
    log_file: Path | None,
    progress_json: bool,
    progress_heartbeat_seconds: float,
) -> None:
    config = load_config()
    reporter = RuntimeActivityReporter(
        command="service_runtime api",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    )
    startup = {
        "service": "api",
        "host": host,
        "port": port,
        "project_root": str(config.project_root),
    }
    uvicorn_log_level = _effective_uvicorn_log_level(log_level=log_level, quiet=quiet, summary_json=summary_json)
    reporter.progress(event="started", phase="command", detail={"command": "service_runtime api"})
    reporter.progress(event="phase_start", phase="startup", detail={"command": "service_runtime api"})
    try:
        if summary_json:
            click.echo(json.dumps(startup, sort_keys=True))
        else:
            if quiet and verbose:
                raise click.BadParameter("`--quiet` and `--verbose` cannot be used together.")
            if not quiet:
                click.echo(render_summary_block("Service runtime startup.", startup, mode="verbose" if verbose else "default"))
        reporter.progress(event="phase_completed", phase="startup", detail={"command": "service_runtime api"})
        uvicorn.run(create_app(config), host=host, port=port, log_level=uvicorn_log_level)
    except KeyboardInterrupt:
        reporter.progress(event="interrupted", phase="command", detail={"command": "service_runtime api"})
        raise
    except Exception as exc:
        reporter.log("error", f"service_runtime api failed: {exc}")
        reporter.progress(event="failed", phase="command", detail={"command": "service_runtime api", "error": str(exc)})
        raise
    finally:
        reporter.close()


@main.command("monitor-once")
@click.option("--date", "target_date", required=True)
@click.option("--warm", is_flag=True, default=False, help="Enable bounded warm actions.")
@click.option("--symbol", "symbols", multiple=True, help="Optional symbol targets for forecast checks.")
@click.option("--max-symbols", default=200, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def monitor_once(
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
    reporter = RuntimeActivityReporter(
        command="service_runtime monitor-once",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    )
    reporter.progress(event="started", phase="command", detail={"command": "service_runtime monitor-once"})
    reporter.progress(event="phase_start", phase="poll", detail={"command": "service_runtime monitor-once"})
    try:
        result = run_monitor_poll(
            config,
            target_date=date.fromisoformat(target_date),
            warm=warm,
            symbols=list(symbols),
            max_symbols=max_symbols,
        )
        reporter.progress(event="phase_completed", phase="poll", counters=_summary_counters(result), detail={"command": "service_runtime monitor-once"})
        _emit_service_summary("Service runtime monitor-once result.", result, summary_json=summary_json, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "service_runtime monitor-once"})
    except KeyboardInterrupt:
        reporter.progress(event="interrupted", phase="command", detail={"command": "service_runtime monitor-once"})
        raise
    except Exception as exc:
        reporter.log("error", f"service_runtime monitor-once failed: {exc}")
        reporter.progress(event="failed", phase="command", detail={"command": "service_runtime monitor-once", "error": str(exc)})
        raise
    finally:
        reporter.close()


@main.command("monitor-loop")
@click.option("--date", "target_date", required=True)
@click.option("--interval-seconds", default=30.0, show_default=True, type=float)
@click.option("--max-iterations", default=10, show_default=True, type=int)
@click.option("--warm", is_flag=True, default=False, help="Enable bounded warm actions.")
@click.option("--symbol", "symbols", multiple=True, help="Optional symbol targets for forecast checks.")
@click.option("--max-symbols", default=200, show_default=True, type=int)
@click.option("--summary-json", is_flag=True, default=False)
@click.option("--quiet", is_flag=True, default=False, help="Print a minimal human summary.")
@click.option("--verbose", is_flag=True, default=False, help="Print additional bounded detail.")
@click.option("--log-level", default="info", show_default=True, type=click.Choice(["debug", "info", "warning", "error"]))
@click.option("--log-file", type=click.Path(path_type=Path), default=None, help="Optional runtime log file path.")
@click.option("--progress-json", is_flag=True, default=False, help="Emit compact NDJSON progress events on stderr.")
@click.option("--progress-heartbeat-seconds", default=0.0, show_default=True, type=float, help="Heartbeat interval for progress-json idle periods; 0 disables heartbeat.")
def monitor_loop_cmd(
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
    reporter = RuntimeActivityReporter(
        command="service_runtime monitor-loop",
        log_level=log_level,
        log_file=log_file,
        progress_json=progress_json,
        progress_heartbeat_seconds=progress_heartbeat_seconds,
    )
    reporter.progress(event="started", phase="command", detail={"command": "service_runtime monitor-loop"})
    reporter.progress(event="phase_start", phase="loop", detail={"command": "service_runtime monitor-loop"})
    try:
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
                detail={"command": "service_runtime monitor-loop"},
            ),
            heartbeat_callback=lambda: reporter.maybe_heartbeat(
                phase="monitor_loop_wait",
                detail={"command": "service_runtime monitor-loop"},
            ),
        )
        reporter.progress(event="phase_completed", phase="loop", counters=_summary_counters(result), detail={"command": "service_runtime monitor-loop"})
        _emit_service_summary("Service runtime monitor-loop result.", result, summary_json=summary_json, quiet=quiet, verbose=verbose)
        reporter.progress(event="completed", phase="command", counters=_summary_counters(result), detail={"command": "service_runtime monitor-loop"})
    except KeyboardInterrupt:
        reporter.progress(event="interrupted", phase="command", detail={"command": "service_runtime monitor-loop"})
        raise
    except Exception as exc:
        reporter.log("error", f"service_runtime monitor-loop failed: {exc}")
        reporter.progress(event="failed", phase="command", detail={"command": "service_runtime monitor-loop", "error": str(exc)})
        raise
    finally:
        reporter.close()


def _emit_service_summary(
    title: str,
    result: dict[str, object],
    *,
    summary_json: bool,
    quiet: bool,
    verbose: bool,
) -> None:
    if summary_json:
        click.echo(json.dumps(result, sort_keys=True))
        return
    if quiet and verbose:
        raise click.BadParameter("`--quiet` and `--verbose` cannot be used together.")
    mode = "default"
    if quiet:
        mode = "quiet"
    elif verbose:
        mode = "verbose"
    click.echo(render_summary_block(title, result, mode=mode))


def _summary_counters(result: dict[str, object]) -> dict[str, object]:
    keys = ["targets_considered", "actions_taken", "skipped", "failures", "iterations"]
    return {key: result.get(key) for key in keys if key in result}


def _effective_uvicorn_log_level(*, log_level: str, quiet: bool, summary_json: bool) -> str:
    # Keep quiet/summary-json startup runs from being noisy unless explicitly strict.
    if quiet or summary_json:
        if log_level in {"debug", "info"}:
            return "warning"
    return log_level


if __name__ == "__main__":
    main()
