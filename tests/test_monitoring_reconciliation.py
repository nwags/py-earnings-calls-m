from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.monitoring import run_monitor_loop, run_monitor_poll
from py_earnings_calls.reconciliation import run_reconciliation
from py_earnings_calls.storage.paths import legacy_transcript_text_path, normalized_path
from py_earnings_calls.storage.writes import write_text


def test_monitor_poll_conservative_retry_and_summary_shape(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    failures_path = normalized_path(config, "transcript_backfill_failures")
    pd.DataFrame(
        [
            {"provider": "motley_fool", "url": "https://x/1", "symbol": "AAPL", "failure_reason": "HTTP_ERROR"},
            {"provider": "motley_fool", "url": "https://x/2", "symbol": "MSFT", "failure_reason": "NON_TRANSCRIPT_PAGE"},
        ]
    ).to_parquet(failures_path, index=False)

    calls: list[str] = []

    def _fake_backfill(cfg, *, manifest_path=None, urls=None, symbol=None, http_client=None):
        calls.extend(urls or [])
        return {"fetched_count": 1, "failed_count": 0}

    monkeypatch.setattr("py_earnings_calls.monitoring.run_transcript_backfill", _fake_backfill)
    result = run_monitor_poll(config, target_date=date(2026, 3, 27), warm=True, symbols=[], max_symbols=10)

    assert calls == ["https://x/1"]
    assert sorted(result.keys()) == [
        "actions_taken",
        "artifacts_written",
        "failures",
        "iterations",
        "lookup_updates",
        "mode",
        "skipped",
        "targets_considered",
    ]
    assert result["mode"] == "poll"
    assert result["targets_considered"] >= 2


def test_monitor_poll_self_heals_when_seen_artifacts_missing(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame(
        [
            {"provider": "motley_fool", "url": "https://x/3", "symbol": "AAPL", "failure_reason": "NON_TRANSCRIPT_PAGE"},
        ]
    ).to_parquet(normalized_path(config, "transcript_backfill_failures"), index=False)
    pd.DataFrame(
        [
            {
                "seen_key": "transcript|motley_fool|https://x/3",
                "target_type": "transcript",
                "provider": "motley_fool",
                "url": "https://x/3",
                "symbol": "AAPL",
                "last_status": "present",
                "expected_raw_path": "/tmp/does-not-exist.html",
                "expected_parsed_path": "/tmp/does-not-exist.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "monitor_seen_keys"), index=False)

    called = {"count": 0}

    def _fake_backfill(cfg, *, manifest_path=None, urls=None, symbol=None, http_client=None):
        called["count"] += 1
        return {"fetched_count": 0, "failed_count": 1}

    monkeypatch.setattr("py_earnings_calls.monitoring.run_transcript_backfill", _fake_backfill)
    run_monitor_poll(config, target_date=date(2026, 3, 27), warm=True, symbols=[], max_symbols=10)
    assert called["count"] == 1


def test_monitor_loop_is_bounded(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    def _fake_poll(*args, **kwargs):
        return {
            "mode": "poll",
            "iterations": 1,
            "targets_considered": 2,
            "actions_taken": 1,
            "skipped": 1,
            "failures": 0,
            "lookup_updates": [],
            "artifacts_written": ["/tmp/x"],
        }

    monkeypatch.setattr("py_earnings_calls.monitoring.run_monitor_poll", _fake_poll)
    result = run_monitor_loop(
        config,
        target_date=date(2026, 3, 27),
        interval_seconds=0.0,
        max_iterations=3,
        warm=False,
        symbols=[],
        max_symbols=10,
    )
    assert result["mode"] == "loop"
    assert result["iterations"] == 3
    assert result["targets_considered"] == 6
    assert result["actions_taken"] == 3


def test_monitor_lookup_fallback_when_incremental_fails(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    pd.DataFrame([{"provider": "motley_fool", "url": "https://x/1", "symbol": "AAPL", "failure_reason": "HTTP_ERROR"}]).to_parquet(
        normalized_path(config, "transcript_backfill_failures"),
        index=False,
    )

    def _fake_backfill(cfg, *, manifest_path=None, urls=None, symbol=None, http_client=None):
        return {"fetched_count": 1, "failed_count": 0}

    monkeypatch.setattr("py_earnings_calls.monitoring.run_transcript_backfill", _fake_backfill)

    def _raise_scoped(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("py_earnings_calls.monitoring.run_lookup_refresh_scoped", _raise_scoped)
    monkeypatch.setattr("py_earnings_calls.monitoring.run_lookup_refresh", lambda cfg: {"artifact_paths": ["a", "b"]})

    result = run_monitor_poll(config, target_date=date(2026, 3, 27), warm=True, symbols=[], max_symbols=10)
    assert any(update.get("mode") == "full_fallback" for update in result["lookup_updates"])


def test_reconciliation_persists_discrepancies_and_events(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    Path("/tmp/missing-parsed.txt")
    pd.DataFrame(
        [
            {
                "seen_key": "transcript|motley_fool|https://x/1",
                "target_type": "transcript",
                "provider": "motley_fool",
                "url": "https://x/1",
                "symbol": "AAPL",
                "last_status": "present",
                "expected_raw_path": "/tmp/missing-raw.html",
                "expected_parsed_path": "/tmp/missing-parsed.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "monitor_seen_keys"), index=False)
    pd.DataFrame([{"provider": "motley_fool", "url": "https://x/1", "symbol": "AAPL", "failure_reason": "HTTP_ERROR"}]).to_parquet(
        normalized_path(config, "transcript_backfill_failures"),
        index=False,
    )
    pd.DataFrame([{"symbol": "AAPL"}]).to_parquet(normalized_path(config, "issuers"), index=False)

    result = run_reconciliation(config, target_date=date(2026, 3, 27), symbols=["AAPL"], max_symbols=10, catch_up_warm=False)
    assert result["mode"] == "reconcile"
    discrepancies = pd.read_parquet(normalized_path(config, "reconciliation_discrepancies"))
    assert "domain" in discrepancies.columns
    assert set(discrepancies["domain"].astype(str).tolist()) == {"earnings"}
    codes = set(discrepancies["discrepancy_code"].astype(str).tolist())
    assert "missing_transcript_raw" in codes
    assert "missing_transcript_parsed" in codes
    assert "missing_forecast_snapshot" in codes
    assert "missing_forecast_points" in codes
    assert "retryable_transcript_failure" in codes
    events = pd.read_parquet(normalized_path(config, "reconciliation_events"))
    assert not events.empty
    assert "domain" in events.columns
    assert set(events["domain"].astype(str).tolist()) == {"earnings"}


def test_monitoring_legacy_path_fallback_prevents_false_missing(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    call_date = date(2026, 3, 27)
    legacy_path = legacy_transcript_text_path(
        config,
        provider="motley_fool",
        symbol="AAPL",
        call_date=call_date,
        call_id="clegacy",
    )
    write_text(legacy_path, "legacy transcript")

    pd.DataFrame(
        [
            {
                "provider": "motley_fool",
                "url": "https://x/legacy",
                "symbol": "AAPL",
                "failure_reason": "NON_TRANSCRIPT_PAGE",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_backfill_failures"), index=False)
    pd.DataFrame(
        [
            {
                "call_id": "clegacy",
                "provider": "motley_fool",
                "symbol": "AAPL",
                "source_url": "https://x/legacy",
                "call_datetime": "2026-03-27T09:00:00",
                "transcript_path": "/tmp/does-not-exist-new-layout.txt",
            }
        ]
    ).to_parquet(normalized_path(config, "transcript_calls"), index=False)

    result = run_monitor_poll(config, target_date=date(2026, 3, 27), warm=False, symbols=[], max_symbols=10)
    assert result["targets_considered"] >= 1
