from __future__ import annotations

import json

from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpClient


KNOWN_SEC_SOURCE_URLS = {
    "company_tickers.json": "https://www.sec.gov/files/company_tickers.json",
    "company_tickers_exchange.json": "https://www.sec.gov/files/company_tickers_exchange.json",
    "company_tickers_mf.json": "https://www.sec.gov/files/company_tickers_mf.json",
    "ticker.txt": "https://www.sec.gov/include/ticker.txt",
    "cik-lookup-data.txt": "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt",
}


def run_refdata_fetch_sec_sources(config: AppConfig) -> dict[str, object]:
    config.ensure_runtime_dirs()
    http_client = HttpClient(config)

    fetched_files: list[str] = []
    replaced_files: list[str] = []
    artifact_paths: list[str] = []
    failed_files: dict[str, str] = {}

    for filename, url in KNOWN_SEC_SOURCE_URLS.items():
        target = config.sec_sources_root / filename
        existed = target.exists()
        try:
            if filename.endswith(".json"):
                payload = http_client.request_json(url, max_attempts=3)
                rendered = json.dumps(payload, sort_keys=True, indent=2)
            else:
                rendered = http_client.request_text(url, max_attempts=3)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(rendered, encoding="utf-8")
            artifact_paths.append(str(target))
            if existed:
                replaced_files.append(filename)
            else:
                fetched_files.append(filename)
        except Exception as exc:  # pragma: no cover - exercised via tests with monkeypatch
            failed_files[filename] = str(exc)

    return {
        "if_exists": "overwrite",
        "fetched": fetched_files,
        "replaced": replaced_files,
        "failed": failed_files,
        "artifact_count": len(artifact_paths),
        "artifact_paths": artifact_paths,
    }

