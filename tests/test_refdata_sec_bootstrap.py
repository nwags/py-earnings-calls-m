from __future__ import annotations

import json

from py_earnings_calls.config import AppConfig
from py_earnings_calls.refdata.sec_bootstrap import KNOWN_SEC_SOURCE_URLS, run_refdata_fetch_sec_sources


def test_refdata_fetch_sec_sources_overwrites_existing_files(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()
    existing = config.sec_sources_root / "ticker.txt"
    existing.write_text("OLD\n", encoding="utf-8")

    def _fake_request_text(self, url, *, params=None, max_attempts=3):
        return f"text:{url}"

    def _fake_request_json(self, url, *, params=None, max_attempts=3):
        return {"url": url, "ok": True}

    monkeypatch.setattr("py_earnings_calls.http.HttpClient.request_text", _fake_request_text)
    monkeypatch.setattr("py_earnings_calls.http.HttpClient.request_json", _fake_request_json)

    result = run_refdata_fetch_sec_sources(config)
    assert result["if_exists"] == "overwrite"
    assert "ticker.txt" in result["replaced"]
    assert set(result["fetched"] + result["replaced"]) == set(KNOWN_SEC_SOURCE_URLS.keys())
    assert result["failed"] == {}
    assert existing.read_text(encoding="utf-8").startswith("text:")

    json_path = config.sec_sources_root / "company_tickers.json"
    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["ok"] is True


def test_refdata_fetch_sec_sources_reports_failures(monkeypatch, tmp_path):
    config = AppConfig.from_project_root(tmp_path)
    config.ensure_runtime_dirs()

    def _fake_request_text(self, url, *, params=None, max_attempts=3):
        if url.endswith("/ticker.txt"):
            raise RuntimeError("network down")
        return "ok"

    def _fake_request_json(self, url, *, params=None, max_attempts=3):
        return {"ok": True}

    monkeypatch.setattr("py_earnings_calls.http.HttpClient.request_text", _fake_request_text)
    monkeypatch.setattr("py_earnings_calls.http.HttpClient.request_json", _fake_request_json)

    result = run_refdata_fetch_sec_sources(config)
    assert "ticker.txt" in result["failed"]
    assert result["artifact_count"] == len(KNOWN_SEC_SOURCE_URLS) - 1
