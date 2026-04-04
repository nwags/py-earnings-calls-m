from __future__ import annotations

import json

from click.testing import CliRunner

from py_earnings_calls import service_runtime


def test_service_runtime_summary_json_precedence(monkeypatch):
    calls = []

    def _fake_run(app, host, port, log_level):
        calls.append({"host": host, "port": port, "log_level": log_level})

    monkeypatch.setattr(service_runtime.uvicorn, "run", _fake_run)
    runner = CliRunner()
    result = runner.invoke(
        service_runtime.main,
        ["api", "--summary-json", "--quiet", "--verbose", "--host", "127.0.0.1", "--port", "9000"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["service"] == "api"
    assert payload["host"] == "127.0.0.1"
    assert payload["port"] == 9000
    assert calls == [{"host": "127.0.0.1", "port": 9000, "log_level": "warning"}]


def test_service_runtime_quiet_suppresses_human_banner(monkeypatch):
    monkeypatch.setattr(service_runtime.uvicorn, "run", lambda app, host, port, log_level: None)
    runner = CliRunner()
    result = runner.invoke(service_runtime.main, ["api", "--quiet"])
    assert result.exit_code == 0, result.output
    assert result.output.strip() == ""


def test_service_runtime_verbose_prints_extra_detail(monkeypatch):
    monkeypatch.setattr(service_runtime.uvicorn, "run", lambda app, host, port, log_level: None)
    runner = CliRunner()
    result = runner.invoke(service_runtime.main, ["api", "--verbose", "--host", "127.0.0.1", "--port", "9000"])
    assert result.exit_code == 0, result.output
    assert "Service runtime startup." in result.output
    assert "host: 127.0.0.1" in result.output
    assert "port: 9000" in result.output
    assert "project_root:" in result.output


def test_service_runtime_api_log_level_controls_uvicorn(monkeypatch):
    calls = []

    def _fake_run(app, host, port, log_level):
        calls.append(log_level)

    monkeypatch.setattr(service_runtime.uvicorn, "run", _fake_run)
    runner = CliRunner()
    result = runner.invoke(service_runtime.main, ["api", "--quiet", "--log-level", "error"])
    assert result.exit_code == 0, result.output
    assert calls == ["error"]
