from __future__ import annotations

import importlib

import pytest


def test_shared_facade_auto_falls_back_to_local_when_external_missing(monkeypatch) -> None:
    monkeypatch.delenv("M_CACHE_SHARED_SOURCE", raising=False)
    monkeypatch.delenv("PY_EARNINGS_CALLS_SHARED_SOURCE", raising=False)
    monkeypatch.setenv("M_CACHE_SHARED_EXTERNAL_ROOT", "missing_ext_root_for_test")
    module = importlib.import_module("py_earnings_calls.augmentation_shared")
    module = importlib.reload(module)
    assert module.shared_surface_source() == "local_fallback"


def test_shared_facade_external_mode_fails_when_external_missing(monkeypatch) -> None:
    monkeypatch.setenv("M_CACHE_SHARED_SOURCE", "external")
    monkeypatch.setenv("M_CACHE_SHARED_EXTERNAL_ROOT", "missing_ext_root_for_test")
    with pytest.raises(ImportError):
        module = importlib.import_module("py_earnings_calls.augmentation_shared")
        importlib.reload(module)


def test_shared_facade_local_mode_bypasses_external(monkeypatch) -> None:
    monkeypatch.setenv("M_CACHE_SHARED_SOURCE", "local")
    monkeypatch.setenv("M_CACHE_SHARED_EXTERNAL_ROOT", "missing_ext_root_for_test")
    module = importlib.import_module("py_earnings_calls.augmentation_shared")
    module = importlib.reload(module)
    assert module.shared_surface_source() == "local"


def test_shared_facade_compat_source_alias_retained_one_cycle(monkeypatch) -> None:
    monkeypatch.delenv("M_CACHE_SHARED_SOURCE", raising=False)
    monkeypatch.setenv("PY_EARNINGS_CALLS_SHARED_SOURCE", "local")
    module = importlib.import_module("py_earnings_calls.augmentation_shared")
    module = importlib.reload(module)
    assert module.shared_surface_source() == "local"


def test_shared_facade_canonical_source_wins_over_compat_alias(monkeypatch) -> None:
    monkeypatch.setenv("M_CACHE_SHARED_SOURCE", "local")
    monkeypatch.setenv("PY_EARNINGS_CALLS_SHARED_SOURCE", "external")
    module = importlib.import_module("py_earnings_calls.augmentation_shared")
    module = importlib.reload(module)
    assert module.shared_surface_source() == "local"


def test_shared_facade_pin_metadata_is_explicit_and_centralized() -> None:
    module = importlib.import_module("py_earnings_calls.augmentation_shared")
    pin = module.shared_pin_metadata()
    assert pin["pin_file"] == "requirements/m_cache_shared_external.txt"
    assert pin["distribution"] == "m-cache-shared-ext"
    assert pin["git_url"] == "https://github.com/m-cache/m_cache_shared_ext.git"
    assert pin["git_tag"] == "v0.1.0-rc9"
    assert pin["default_external_root"] == "m_cache_shared_ext.augmentation"
    assert pin["source_mode_env_var"] == "M_CACHE_SHARED_SOURCE"
    assert pin["external_root_env_var"] == "M_CACHE_SHARED_EXTERNAL_ROOT"
    assert pin["compat_source_env_var"] == "PY_EARNINGS_CALLS_SHARED_SOURCE"
