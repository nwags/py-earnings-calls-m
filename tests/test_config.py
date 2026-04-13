from py_earnings_calls.config import (
    AppConfig,
    load_config_from_effective_config,
    load_effective_config,
)


def test_config_from_project_root(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    assert config.project_root == tmp_path.resolve()
    assert config.transcripts_root.name == "transcripts"
    assert config.transcripts_data_root.name == "data"
    assert config.transcripts_full_index_root.name == "full-index"
    assert config.forecasts_data_root.name == "data"
    assert config.forecasts_full_index_root.name == "full-index"
    assert config.legacy_transcript_raw_root.name == "raw"
    assert config.legacy_forecast_raw_root.name == "raw"
    assert config.sec_sources_root.name == "sec_sources"
    assert config.refdata_inputs_root.name == "inputs"


def test_ensure_runtime_dirs_does_not_recreate_legacy_shallow_roots(tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    assert config.transcripts_data_root.exists()
    assert config.transcripts_full_index_root.exists()
    assert config.forecasts_data_root.exists()
    assert config.forecasts_full_index_root.exists()
    assert not config.legacy_transcript_raw_root.exists()
    assert not config.legacy_transcript_parsed_root.exists()
    assert not config.legacy_forecast_raw_root.exists()


def test_effective_config_prefers_explicit_path_over_env(monkeypatch, tmp_path):
    explicit = tmp_path / "explicit.toml"
    explicit.write_text(
        """
[global]
app_root = "."
log_level = "WARNING"
default_summary_json = true
default_progress_json = false

[domains.earnings]
enabled = true
cache_root = "./cache_explicit"
normalized_refdata_root = "./norm_explicit"
lookup_root = "./norm_explicit"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    from_env = tmp_path / "from_env.toml"
    from_env.write_text(
        """
[global]
app_root = "."
log_level = "ERROR"
default_summary_json = false
default_progress_json = false

[domains.earnings]
enabled = true
cache_root = "./cache_env"
normalized_refdata_root = "./norm_env"
lookup_root = "./norm_env"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("M_CACHE_CONFIG", str(from_env))
    effective = load_effective_config(project_root=tmp_path, config_path=explicit)
    assert effective.source == "--config"
    assert effective.global_config["log_level"] == "WARNING"
    assert effective.domains["earnings"]["cache_root"] == str((tmp_path / "cache_explicit").resolve())


def test_effective_config_maps_to_app_config_roots(tmp_path):
    config_path = tmp_path / "m-cache.toml"
    config_path.write_text(
        """
[global]
app_root = "."
log_level = "INFO"
default_summary_json = false
default_progress_json = false

[domains.earnings]
enabled = true
cache_root = "./custom_cache"
normalized_refdata_root = "./custom_normalized"
lookup_root = "./custom_normalized"
default_resolution_mode = "local_only"
""".strip(),
        encoding="utf-8",
    )
    effective = load_effective_config(project_root=tmp_path, config_path=config_path)
    app_config = load_config_from_effective_config(effective)
    assert app_config.cache_root == (tmp_path / "custom_cache").resolve()
    assert app_config.normalized_refdata_root == (tmp_path / "custom_normalized").resolve()
