from __future__ import annotations

import json
from hashlib import sha256

from click.testing import CliRunner
import pandas as pd

from py_earnings_calls import m_cache_cli
from py_earnings_calls.config import AppConfig, load_effective_config
from py_earnings_calls.storage.paths import normalized_path
from py_earnings_calls.storage.writes import write_text


def _write_config(tmp_path):
    path = tmp_path / "m-cache.toml"
    path.write_text(
        """
[global]
app_root = "."
log_level = "INFO"
default_summary_json = false
default_progress_json = false

[domains.earnings]
enabled = true
cache_root = "./.earnings_cache"
normalized_refdata_root = "./refdata/normalized"
lookup_root = "./refdata/normalized"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    return path


def test_m_cache_summary_json_is_canonical_by_default(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)

    monkeypatch.setattr(
        m_cache_cli,
        "run_forecast_refresh",
        lambda config, symbols, as_of_date, provider, provider_mode, provider_priority: {
            "requested_count": 2,
            "fetched_count": 2,
            "failed_count": 0,
            "snapshot_count": 2,
            "point_count": 8,
        },
    )

    result = runner.invoke(
        m_cache_cli.main,
        [
            "--config",
            str(config_path),
            "earnings",
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
            "--symbol",
            "MSFT",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["domain"] == "earnings"
    assert payload["command_path"] == ["m-cache", "earnings", "forecasts", "refresh-daily"]
    assert payload["provider_requested"] == "finnhub"
    assert payload["provider_used"] == "finnhub"
    assert payload["remote_attempted"] is True
    assert payload["persisted_locally"] is True
    assert isinstance(payload["counters"], dict)


def test_m_cache_progress_json_uses_canonical_shape(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    echoes: list[tuple[str, bool]] = []

    def _fake_echo(message=None, file=None, nl=True, err=False, color=None):  # pragma: no cover
        echoes.append((str(message), bool(err)))

    monkeypatch.setattr("py_earnings_calls.runtime_activity.click.echo", _fake_echo)
    monkeypatch.setattr("py_earnings_calls.m_cache_cli.click.echo", _fake_echo)
    monkeypatch.setattr(
        m_cache_cli,
        "run_transcript_backfill",
        lambda config, manifest_path=None, urls=None, symbol=None: {
            "requested_count": 1,
            "fetched_count": 1,
            "failed_count": 0,
        },
    )

    result = runner.invoke(
        m_cache_cli.main,
        [
            "--config",
            str(config_path),
            "earnings",
            "transcripts",
            "backfill",
            "--url",
            "https://example.com/transcript",
            "--progress-json",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    stderr_lines = [line for line, err in echoes if err]
    assert stderr_lines
    first = json.loads(stderr_lines[0])
    assert first["domain"] == "earnings"
    assert first["command_path"] == ["m-cache", "earnings", "transcripts", "backfill"]
    assert sorted(first.keys()) == ["command_path", "counters", "detail", "domain", "elapsed_seconds", "event", "phase"]


def test_m_cache_output_schema_legacy_emits_legacy_summary(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)

    monkeypatch.setattr(
        m_cache_cli,
        "run_refdata_refresh",
        lambda config, universe_path=None: {
            "artifact_count": 2,
            "issuer_count": 1,
        },
    )

    result = runner.invoke(
        m_cache_cli.main,
        [
            "--config",
            str(config_path),
            "earnings",
            "refdata",
            "refresh",
            "--summary-json",
            "--output-schema",
            "legacy",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert "domain" not in payload
    assert payload["artifact_count"] == 2


def test_m_cache_providers_show_exposes_effective_policy(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("FINNHUB_API_KEY", "test-token")

    result = runner.invoke(
        m_cache_cli.main,
        [
            "--config",
            str(config_path),
            "earnings",
            "providers",
            "show",
            "--provider",
            "finnhub",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    detail = payload["result"]
    assert detail["provider_id"] == "finnhub"
    assert detail["effective_auth_present"] is True
    assert detail["effective_enabled"] is True
    assert "quota_window_seconds" in detail
    assert "expected_error_modes" in detail


def test_m_cache_resolve_forecast_summary_includes_wave2_metadata(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)

    class _FakeResult:
        found = True
        served_from = "resolved_remote"
        resolution_mode = "resolve_if_missing"
        provider_requested = "finnhub"
        provider_used = "finnhub"
        method_used = "api"
        success = True
        reason_code = "RESOLVED"
        persisted_locally = True
        rate_limited = False
        retry_count = 1
        deferred_until = None
        selection_outcome = "used_requested_provider"
        provider_skip_reasons = []

    class _FakeService:
        def __init__(self, config):
            self.config = config

        def resolve_forecast_snapshot_if_missing(self, **kwargs):
            return _FakeResult()

    monkeypatch.setattr(m_cache_cli, "ProviderAwareResolutionService", _FakeService)

    result = runner.invoke(
        m_cache_cli.main,
        [
            "--config",
            str(config_path),
            "earnings",
            "resolve",
            "forecast-snapshot",
            "--provider",
            "finnhub",
            "--symbol",
            "AAPL",
            "--date",
            "2026-03-27",
            "--resolution-mode",
            "resolve_if_missing",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["rate_limited"] is False
    assert payload["retry_count"] == 1
    assert payload["selection_outcome"] == "used_requested_provider"


def test_m_cache_aug_list_types_is_read_only_and_canonical(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    result = runner.invoke(
        m_cache_cli.main,
        [
            "earnings",
            "aug",
            "list-types",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["command_path"] == ["m-cache", "earnings", "aug", "list-types"]
    assert payload["result"]["augmentation_types"] == ["entity_tagging", "temporal_expression_tagging"]


def test_m_cache_aug_inspect_target_excludes_numeric_forecasts(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    result = runner.invoke(
        m_cache_cli.main,
        [
            "earnings",
            "aug",
            "inspect-target",
            "--resource-family",
            "forecasts",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    assert payload["result"]["augmentation_applicable"] is False
    assert payload["result"]["reason_code"] == "NUMERIC_ONLY_RESOURCE_FAMILY"


def test_m_cache_aug_inspect_target_transcript_is_call_id_scoped_with_text_version(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    transcript_text_path = tmp_path / "c1.txt"
    transcript_text = "Apple earnings call transcript text."
    write_text(transcript_text_path, transcript_text)
    source_text_version = f"sha256:{sha256(transcript_text.encode('utf-8')).hexdigest()}"

    pd.DataFrame(
        [
            {
                "call_id": "c1",
                "transcript_path": str(transcript_text_path),
            }
        ]
    ).to_parquet(normalized_path(app_config, "local_lookup_transcripts"), index=False)
    pd.DataFrame(
        [
            {
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "artifact_locator": "refdata/normalized/augments/transcript_c1_entity_tagging.json",
                "source_text_version": source_text_version,
                "producer_name": "entity-tagger-v1",
                "event_at": "2026-04-09T12:00:00Z",
                "success": True,
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_artifacts"), index=False)

    result = runner.invoke(
        m_cache_cli.main,
        [
            "earnings",
            "aug",
            "inspect-target",
            "--resource-family",
            "transcripts",
            "--call-id",
            "c1",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    inspected = payload["result"]
    assert inspected["augmentation_applicable"] is True
    assert inspected["canonical_key"] == "transcript:c1"
    assert inspected["source_text_version"] == source_text_version
    assert inspected["augmentation_meta"]["augmentation_available"] is True
    assert inspected["augmentation_meta"]["augmentation_stale"] is False


def test_m_cache_aug_target_descriptor_returns_wave4_fields(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    transcript_text_path = tmp_path / "c42.txt"
    transcript_text = "Transcript text for target descriptor."
    write_text(transcript_text_path, transcript_text)
    source_text_version = f"sha256:{sha256(transcript_text.encode('utf-8')).hexdigest()}"
    pd.DataFrame([{"call_id": "c42", "transcript_path": str(transcript_text_path)}]).to_parquet(
        normalized_path(app_config, "local_lookup_transcripts"),
        index=False,
    )

    result = runner.invoke(
        m_cache_cli.main,
        [
            "earnings",
            "aug",
            "target-descriptor",
            "--call-id",
            "c42",
            "--summary-json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    descriptor = payload["result"]
    assert descriptor["resource_family"] == "transcripts"
    assert descriptor["canonical_key"] == "transcript:c42"
    assert descriptor["source_text_version"] == source_text_version
    assert descriptor["producer_hints"]["api_content_path"] == "/transcripts/c42/content"


def test_m_cache_aug_submit_run_is_idempotent(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    payload_path = tmp_path / "run.json"
    payload_path.write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    first = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "submit-run", "--input-json", str(payload_path), "--summary-json"],
    )
    assert first.exit_code == 0, first.output
    first_payload = json.loads(first.output.strip())
    assert first_payload["result"]["idempotent_replay"] is False

    second = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "submit-run", "--input-json", str(payload_path), "--summary-json"],
    )
    assert second.exit_code == 0, second.output
    second_payload = json.loads(second.output.strip())
    assert second_payload["result"]["idempotent_replay"] is True


def test_m_cache_aug_status_prefers_run_id_and_returns_concise_status(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    pd.DataFrame(
        [
            {
                "run_id": "run-1",
                "event_at": "2026-04-10T11:00:00Z",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
                "persisted_locally": True,
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_runs"), index=False)
    pd.DataFrame(
        [
            {
                "idempotency_key": "idem-1",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "artifact_locator": "s3://bucket/artifact.json",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "source_text_version": "sha256:abc",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "event_at": "2026-04-10T11:00:01Z",
                "success": True,
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_artifacts"), index=False)

    result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "status", "--run-id", "run-1", "--summary-json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    status = payload["result"]
    assert payload["command_path"] == ["m-cache", "earnings", "aug", "status"]
    assert status["found"] is True
    assert status["run_id"] == "run-1"
    assert status["idempotency_key"] == "idem-1"
    assert status["status"] == "completed"
    assert status["reason_code"] == "OK"
    assert "records" not in status


def test_m_cache_aug_status_supports_idempotency_key_lookup(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    pd.DataFrame(
        [
            {
                "run_id": "run-idem",
                "event_at": "2026-04-10T10:30:00Z",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c7",
                "augmentation_type": "temporal_expression_tagging",
                "source_text_version": "sha256:def",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "temporal.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_runs"), index=False)
    pd.DataFrame(
        [
            {
                "idempotency_key": "idem-lookup",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c7",
                "augmentation_type": "temporal_expression_tagging",
                "artifact_locator": "s3://bucket/timex.json",
                "payload_schema_name": "temporal.v1",
                "payload_schema_version": "1.0.0",
                "source_text_version": "sha256:def",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "event_at": "2026-04-10T10:30:01Z",
                "success": True,
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_artifacts"), index=False)

    result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "status", "--idempotency-key", "idem-lookup", "--summary-json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip())
    status = payload["result"]
    assert status["found"] is True
    assert status["run_id"] == "run-idem"
    assert status["idempotency_key"] == "idem-lookup"
    assert status["augmentation_type"] == "temporal_expression_tagging"


def test_m_cache_aug_events_is_timeline_and_alias_inspection_surfaces_remain(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    pd.DataFrame(
        [
            {
                "event_id": "e1",
                "run_id": "run-1",
                "event_at": "2026-04-10T10:00:00Z",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "status": "completed",
                "success": True,
            },
            {
                "event_id": "e2",
                "run_id": "run-2",
                "event_at": "2026-04-10T11:00:00Z",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c2",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:def",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "status": "completed",
                "success": True,
            },
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_events"), index=False)
    pd.DataFrame(
        [
            {
                "run_id": "run-1",
                "event_at": "2026-04-10T10:00:00Z",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_runs"), index=False)
    pd.DataFrame(
        [
            {
                "idempotency_key": "idem-compat",
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "artifact_locator": "s3://bucket/entity.json",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "source_text_version": "sha256:abc",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "event_at": "2026-04-10T10:00:01Z",
                "success": True,
            }
        ]
    ).to_parquet(normalized_path(app_config, "augmentation_artifacts"), index=False)

    events_result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "events", "--run-id", "run-2", "--summary-json"],
    )
    assert events_result.exit_code == 0, events_result.output
    events_payload = json.loads(events_result.output.strip())
    assert events_payload["command_path"] == ["m-cache", "earnings", "aug", "events"]
    assert events_payload["result"]["record_count"] == 1
    assert events_payload["result"]["records"][0]["run_id"] == "run-2"

    inspect_runs = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "inspect-runs", "--summary-json"],
    )
    assert inspect_runs.exit_code == 0, inspect_runs.output

    inspect_artifacts = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "inspect-artifacts", "--summary-json"],
    )
    assert inspect_artifacts.exit_code == 0, inspect_artifacts.output


def test_m_cache_aug_submit_artifact_supports_locator_and_inline_payload(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    locator_payload_path = tmp_path / "artifact_locator.json"
    locator_payload_path.write_text(
        json.dumps(
            {
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "artifact_locator": "s3://bucket/payload.json",
                "success": True,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    locator_result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "submit-artifact", "--input-json", str(locator_payload_path), "--summary-json"],
    )
    assert locator_result.exit_code == 0, locator_result.output
    locator_summary = json.loads(locator_result.output.strip())
    assert locator_summary["result"]["artifact_locator"] == "s3://bucket/payload.json"

    inline_payload_path = tmp_path / "artifact_inline.json"
    inline_payload_path.write_text(
        json.dumps(
            {
                "domain": "earnings",
                "resource_family": "transcripts",
                "canonical_key": "transcript:c1",
                "augmentation_type": "temporal_expression_tagging",
                "source_text_version": "sha256:def",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "temporal.v1",
                "payload_schema_version": "1.0.0",
                "payload": {"items": [{"span_start": 0, "span_end": 3, "text": "Q1"}]},
                "success": True,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    inline_result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "submit-artifact", "--input-json", str(inline_payload_path), "--summary-json"],
    )
    assert inline_result.exit_code == 0, inline_result.output
    inline_summary = json.loads(inline_result.output.strip())
    assert "augmentation_payloads" in inline_summary["result"]["artifact_locator"]


def test_m_cache_aug_submit_run_rejects_non_transcript_resource_family(monkeypatch, tmp_path):
    runner = CliRunner()
    config_path = _write_config(tmp_path)
    app_config = AppConfig.from_project_root(tmp_path)
    app_config.ensure_runtime_dirs()
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    monkeypatch.setattr(m_cache_cli, "_load_runtime", lambda ctx: (effective, app_config))

    payload_path = tmp_path / "run_bad.json"
    payload_path.write_text(
        json.dumps(
            {
                "run_id": "run-bad",
                "domain": "earnings",
                "resource_family": "forecasts",
                "canonical_key": "forecast:aapl:2026-03-27",
                "augmentation_type": "entity_tagging",
                "source_text_version": "sha256:abc",
                "producer_kind": "rules",
                "producer_name": "tagger",
                "producer_version": "1.0.0",
                "payload_schema_name": "entity.v1",
                "payload_schema_version": "1.0.0",
                "status": "completed",
                "success": True,
                "reason_code": "OK",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        m_cache_cli.main,
        ["earnings", "aug", "submit-run", "--input-json", str(payload_path), "--summary-json"],
    )
    assert result.exit_code != 0
    assert "resource_family must be 'transcripts'" in result.output
