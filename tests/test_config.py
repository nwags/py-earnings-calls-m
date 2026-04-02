from py_earnings_calls.config import AppConfig


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
