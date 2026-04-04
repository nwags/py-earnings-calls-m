from __future__ import annotations

import json

from click.testing import CliRunner

from py_earnings_calls import cli


def test_summary_json_takes_precedence_over_quiet(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(cli, "run_storage_verify_layout", lambda config: {"mode": "verify", "status": "ok"})

    result = runner.invoke(cli.main, ["storage", "verify-layout", "--summary-json", "--quiet"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["mode"] == "verify"
    assert "Storage layout verification complete." not in result.output


def test_quiet_mode_is_minimal_and_hides_next_step_on_success(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "run_lookup_refresh",
        lambda config: {
            "transcript_row_count": 10,
            "forecast_row_count": 20,
            "artifact_paths": ["/x", "/y", "/z"],
            "status": "ok",
            "requested_count": 5,
            "fetched_count": 5,
            "failed_count": 0,
            "next_step": "Run something else",
        },
    )

    result = runner.invoke(cli.main, ["lookup", "refresh", "--quiet"])
    assert result.exit_code == 0, result.output
    output = result.output
    assert "Lookup refresh complete." in output
    assert "- requested_count: 5" in output
    assert "- fetched_count: 5" in output
    assert "- failed_count: 0" in output
    assert "artifact_paths" not in output
    assert "next_step" not in output


def test_verbose_mode_is_bounded_for_large_lists(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(
        cli,
        "run_lookup_refresh",
        lambda config: {
            "requested_count": 10,
            "fetched_count": 7,
            "failed_count": 3,
            "artifact_paths": [f"/path/{i}" for i in range(20)],
            "next_step": "Run lookup query",
        },
    )

    result = runner.invoke(cli.main, ["lookup", "refresh", "--verbose"])
    assert result.exit_code == 0, result.output
    output = result.output
    assert "Lookup refresh complete." in output
    assert "artifact_paths" in output
    assert "len=20" in output


def test_quiet_and_verbose_conflict_without_summary_json(monkeypatch):
    runner = CliRunner()
    monkeypatch.setattr(cli, "run_lookup_refresh", lambda config: {"status": "ok"})
    result = runner.invoke(cli.main, ["lookup", "refresh", "--quiet", "--verbose"])
    assert result.exit_code != 0
    assert "--quiet" in result.output and "--verbose" in result.output
