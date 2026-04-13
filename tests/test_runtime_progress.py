from __future__ import annotations

import json

from click.testing import CliRunner

from py_earnings_calls import cli, service_runtime
from py_earnings_calls.runtime_activity import RuntimeActivityReporter


def test_runtime_reporter_progress_schema_and_err_stream(monkeypatch):
    emitted: list[tuple[str, bool]] = []

    def _fake_echo(message=None, file=None, nl=True, err=False, color=None):  # pragma: no cover - signature shim
        emitted.append((str(message), bool(err)))

    monkeypatch.setattr("py_earnings_calls.runtime_activity.click.echo", _fake_echo)
    reporter = RuntimeActivityReporter(command="test", progress_json=True)
    reporter.progress(event="started", phase="command", detail={"command": "x"})
    reporter.progress(event="completed", phase="command", counters={"requested_count": 1}, detail={"command": "x"})
    reporter.close()

    assert len(emitted) == 2
    for line, err in emitted:
        assert err is True
        payload = json.loads(line)
        assert sorted(payload.keys()) == ["counters", "detail", "elapsed_seconds", "event", "phase"]


def test_cli_summary_json_and_progress_route_to_separate_streams(monkeypatch):
    runner = CliRunner()
    echoes: list[tuple[str, bool]] = []

    def _fake_echo(message=None, file=None, nl=True, err=False, color=None):  # pragma: no cover - signature shim
        echoes.append((str(message), bool(err)))

    monkeypatch.setattr("py_earnings_calls.runtime_activity.click.echo", _fake_echo)
    monkeypatch.setattr("py_earnings_calls.cli.click.echo", _fake_echo)
    monkeypatch.setattr(
        cli,
        "run_monitor_poll",
        lambda config, target_date, warm, symbols, max_symbols: {
            "mode": "poll",
            "iterations": 1,
            "targets_considered": 2,
            "actions_taken": 1,
            "skipped": 1,
            "failures": 0,
            "lookup_updates": [],
            "artifacts_written": ["/tmp/a"],
        },
    )
    result = runner.invoke(cli.main, ["monitor", "poll", "--date", "2026-03-27", "--summary-json", "--progress-json"])
    assert result.exit_code == 0, result.output
    stdout_lines = [line for line, err in echoes if not err]
    stderr_lines = [line for line, err in echoes if err]
    assert len(stdout_lines) == 1
    assert json.loads(stdout_lines[0])["mode"] == "poll"
    assert stderr_lines
    for line in stderr_lines:
        payload = json.loads(line)
        assert sorted(payload.keys()) == ["counters", "detail", "elapsed_seconds", "event", "phase"]


def test_legacy_cli_summary_json_default_shape_is_preserved(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "run_monitor_poll",
        lambda config, target_date, warm, symbols, max_symbols: {
            "mode": "poll",
            "iterations": 1,
            "targets_considered": 2,
            "actions_taken": 1,
            "skipped": 1,
            "failures": 0,
            "lookup_updates": [],
            "artifacts_written": ["/tmp/a"],
        },
    )
    result = runner.invoke(cli.main, ["monitor", "poll", "--date", "2026-03-27", "--summary-json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["mode"] == "poll"
    assert "domain" not in payload


def test_progress_json_can_be_written_to_log_file(monkeypatch, tmp_path):
    runner = CliRunner()
    log_path = tmp_path / "runtime.log"
    monkeypatch.setattr(
        cli,
        "run_forecast_refresh",
        lambda config, symbols, as_of_date, provider, provider_mode, provider_priority: {
            "snapshot_count": 1,
            "point_count": 3,
        },
    )
    result = runner.invoke(
        cli.main,
        [
            "forecasts",
            "refresh-daily",
            "--provider-mode",
            "single",
            "--provider",
            "finnhub",
            "--date",
            "2026-03-27",
            "--symbol",
            "AAPL",
            "--progress-json",
            "--log-file",
            str(log_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert log_path.exists()
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["event"] == "started"
    assert parsed[-1]["event"] == "completed"


def test_service_runtime_monitor_once_summary_json_and_progress(monkeypatch):
    runner = CliRunner()
    echoes: list[tuple[str, bool]] = []

    def _fake_echo(message=None, file=None, nl=True, err=False, color=None):  # pragma: no cover - signature shim
        echoes.append((str(message), bool(err)))

    monkeypatch.setattr("py_earnings_calls.runtime_activity.click.echo", _fake_echo)
    monkeypatch.setattr("py_earnings_calls.service_runtime.click.echo", _fake_echo)
    monkeypatch.setattr(
        service_runtime,
        "run_monitor_poll",
        lambda config, target_date, warm, symbols, max_symbols: {
            "mode": "poll",
            "iterations": 1,
            "targets_considered": 1,
            "actions_taken": 0,
            "skipped": 1,
            "failures": 0,
            "lookup_updates": [],
            "artifacts_written": [],
        },
    )
    result = runner.invoke(service_runtime.main, ["monitor-once", "--date", "2026-03-27", "--summary-json", "--progress-json"])
    assert result.exit_code == 0, result.output
    stdout_lines = [line for line, err in echoes if not err]
    stderr_lines = [line for line, err in echoes if err]
    assert json.loads(stdout_lines[0])["mode"] == "poll"
    assert stderr_lines
    assert json.loads(stderr_lines[0])["event"] == "started"


def test_service_runtime_monitor_loop_emits_iteration_events(monkeypatch):
    runner = CliRunner()
    echoes: list[tuple[str, bool]] = []

    def _fake_echo(message=None, file=None, nl=True, err=False, color=None):  # pragma: no cover - signature shim
        echoes.append((str(message), bool(err)))

    def _fake_loop(
        config,
        target_date,
        interval_seconds,
        max_iterations,
        warm,
        symbols,
        max_symbols,
        iteration_progress_callback,
        heartbeat_callback,
    ):
        iteration_progress_callback("iteration_start", {"iteration": 1, "max_iterations": max_iterations})
        heartbeat_callback()
        iteration_progress_callback("iteration_end", {"iteration": 1, "max_iterations": max_iterations, "targets_considered": 2})
        return {
            "mode": "loop",
            "iterations": 1,
            "targets_considered": 2,
            "actions_taken": 1,
            "skipped": 1,
            "failures": 0,
            "lookup_updates": [],
            "artifacts_written": [],
        }

    monkeypatch.setattr("py_earnings_calls.runtime_activity.click.echo", _fake_echo)
    monkeypatch.setattr("py_earnings_calls.service_runtime.click.echo", _fake_echo)
    monkeypatch.setattr(service_runtime, "run_monitor_loop", _fake_loop)
    result = runner.invoke(
        service_runtime.main,
        [
            "monitor-loop",
            "--date",
            "2026-03-27",
            "--interval-seconds",
            "0.0",
            "--max-iterations",
            "1",
            "--progress-json",
            "--progress-heartbeat-seconds",
            "0.001",
        ],
    )
    assert result.exit_code == 0, result.output
    stderr_events = [json.loads(line)["event"] for line, err in echoes if err]
    assert "iteration_start" in stderr_events
    assert "iteration_end" in stderr_events
