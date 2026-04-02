from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv()


def _env_path(name: str) -> Path | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    return Path(value).expanduser().resolve()


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw.strip())


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw.strip())


@dataclass(frozen=True)
class AppConfig:
    project_root: Path
    cache_root: Path
    transcripts_root: Path
    transcripts_data_root: Path
    transcripts_full_index_root: Path
    forecasts_root: Path
    forecasts_data_root: Path
    forecasts_full_index_root: Path
    legacy_transcript_raw_root: Path
    legacy_transcript_parsed_root: Path
    legacy_forecast_raw_root: Path
    refdata_root: Path
    sec_sources_root: Path
    refdata_inputs_root: Path
    normalized_refdata_root: Path
    user_agent: str
    request_timeout_connect: float
    request_timeout_read: float
    max_requests_per_second: float
    download_workers: int
    parse_workers: int
    finnhub_api_key: str | None
    fmp_api_key: str | None

    @classmethod
    def from_project_root(cls, project_root: Path | str) -> "AppConfig":
        root = Path(project_root).resolve()
        cache_root = root / ".earnings_cache"
        transcripts_root = cache_root / "transcripts"
        transcripts_data_root = transcripts_root / "data"
        transcripts_full_index_root = transcripts_root / "full-index"
        forecasts_root = cache_root / "forecasts"
        forecasts_data_root = forecasts_root / "data"
        forecasts_full_index_root = forecasts_root / "full-index"
        legacy_transcript_raw_root = transcripts_root / "raw"
        legacy_transcript_parsed_root = transcripts_root / "parsed"
        legacy_forecast_raw_root = forecasts_root / "raw"
        refdata_root = root / "refdata"
        sec_sources_root = refdata_root / "sec_sources"
        refdata_inputs_root = refdata_root / "inputs"
        normalized_refdata_root = refdata_root / "normalized"

        user_agent = os.getenv(
            "PY_EARNINGS_CALLS_USER_AGENT",
            "py-earnings-calls-m/0.1.0 (replace-with-contact)",
        )

        return cls(
            project_root=root,
            cache_root=cache_root,
            transcripts_root=transcripts_root,
            transcripts_data_root=transcripts_data_root,
            transcripts_full_index_root=transcripts_full_index_root,
            forecasts_root=forecasts_root,
            forecasts_data_root=forecasts_data_root,
            forecasts_full_index_root=forecasts_full_index_root,
            legacy_transcript_raw_root=legacy_transcript_raw_root,
            legacy_transcript_parsed_root=legacy_transcript_parsed_root,
            legacy_forecast_raw_root=legacy_forecast_raw_root,
            refdata_root=refdata_root,
            sec_sources_root=sec_sources_root,
            refdata_inputs_root=refdata_inputs_root,
            normalized_refdata_root=normalized_refdata_root,
            user_agent=user_agent,
            request_timeout_connect=_env_float("PY_EARNINGS_CALLS_HTTP_CONNECT_TIMEOUT", 10.0),
            request_timeout_read=_env_float("PY_EARNINGS_CALLS_HTTP_READ_TIMEOUT", 30.0),
            max_requests_per_second=_env_float("PY_EARNINGS_CALLS_MAX_REQUESTS_PER_SECOND", 2.0),
            download_workers=max(1, _env_int("PY_EARNINGS_CALLS_DOWNLOAD_WORKERS", 4)),
            parse_workers=max(1, _env_int("PY_EARNINGS_CALLS_PARSE_WORKERS", 1)),
            finnhub_api_key=os.getenv("FINNHUB_API_KEY") or None,
            fmp_api_key=os.getenv("FMP_API_KEY") or None,
        )

    def ensure_runtime_dirs(self) -> None:
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.transcripts_root.mkdir(parents=True, exist_ok=True)
        self.transcripts_data_root.mkdir(parents=True, exist_ok=True)
        self.transcripts_full_index_root.mkdir(parents=True, exist_ok=True)
        self.forecasts_root.mkdir(parents=True, exist_ok=True)
        self.forecasts_data_root.mkdir(parents=True, exist_ok=True)
        self.forecasts_full_index_root.mkdir(parents=True, exist_ok=True)
        self.refdata_root.mkdir(parents=True, exist_ok=True)
        self.sec_sources_root.mkdir(parents=True, exist_ok=True)
        self.refdata_inputs_root.mkdir(parents=True, exist_ok=True)
        self.normalized_refdata_root.mkdir(parents=True, exist_ok=True)


def load_config(project_root: Path | str | None = None) -> AppConfig:
    root = project_root or _env_path("PY_EARNINGS_CALLS_PROJECT_ROOT") or Path(__file__).resolve().parents[1]
    return AppConfig.from_project_root(root)
