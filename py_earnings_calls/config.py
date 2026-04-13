from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib

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


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _first_path(*values: str | Path | None) -> Path | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, Path):
            return value.expanduser().resolve()
        text = str(value).strip()
        if text:
            return Path(text).expanduser().resolve()
    return None


def _resolve_path_value(value: object | None, *, base: Path) -> Path:
    if value is None:
        return base.resolve()
    text = str(value).strip()
    if not text:
        return base.resolve()
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = base / path
    return path.resolve()


def _merge_dict(base: dict, incoming: dict) -> dict:
    out = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge_dict(out[key], value)
        else:
            out[key] = value
    return out


@dataclass(frozen=True)
class MCacheEffectiveConfig:
    source: str
    global_config: dict[str, object]
    domains: dict[str, dict[str, object]]

    def to_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "global": dict(self.global_config),
            "domains": dict(self.domains),
        }


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
        normalized_refdata_root = root / "refdata" / "normalized"
        return cls.from_roots(
            project_root=root,
            cache_root=cache_root,
            normalized_refdata_root=normalized_refdata_root,
        )

    @classmethod
    def from_roots(
        cls,
        *,
        project_root: Path | str,
        cache_root: Path | str,
        normalized_refdata_root: Path | str,
    ) -> "AppConfig":
        root = Path(project_root).resolve()
        cache_root = Path(cache_root).resolve()
        normalized_refdata_root = Path(normalized_refdata_root).resolve()
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


def load_effective_config(
    *,
    project_root: Path | str | None = None,
    config_path: Path | str | None = None,
) -> MCacheEffectiveConfig:
    root = (
        _first_path(project_root)
        or _env_path("PY_EARNINGS_CALLS_PROJECT_ROOT")
        or Path(__file__).resolve().parents[1]
    )
    default_config = {
        "global": {
            "app_root": str(root),
            "log_level": "INFO",
            "default_summary_json": False,
            "default_progress_json": False,
        },
        "domains": {
            "earnings": {
                "enabled": True,
                "cache_root": str(root / ".earnings_cache"),
                "normalized_refdata_root": str(root / "refdata" / "normalized"),
                "lookup_root": str(root / "refdata" / "normalized"),
                "default_resolution_mode": "local_only",
                "providers": {
                    "motley_fool": {
                        "enabled": True,
                        "auth_type": "none",
                        "auth_env_var": "",
                        "rate_limit_policy": "unknown",
                        "direct_resolution_allowed": True,
                    },
                    "finnhub": {
                        "enabled": True,
                        "auth_type": "api_key_header",
                        "auth_env_var": "FINNHUB_API_KEY",
                        "rate_limit_policy": "per_minute",
                        "direct_resolution_allowed": True,
                    },
                    "fmp": {
                        "enabled": True,
                        "auth_type": "api_key_query",
                        "auth_env_var": "FMP_API_KEY",
                        "rate_limit_policy": "per_minute",
                        "direct_resolution_allowed": True,
                    },
                },
                "runtime": {
                    "output_schema": "canonical",
                },
            }
        },
    }

    source = "defaults+legacy_env"
    resolved_path = _first_path(config_path)
    if resolved_path is None:
        env_config = _env_path("M_CACHE_CONFIG")
        if env_config is not None:
            resolved_path = env_config
            source = "M_CACHE_CONFIG"
        else:
            local_default = root / "m-cache.toml"
            if local_default.exists():
                resolved_path = local_default
                source = "./m-cache.toml"
    else:
        source = "--config"

    config_data = dict(default_config)
    if resolved_path is not None and resolved_path.exists():
        with resolved_path.open("rb") as handle:
            parsed = tomllib.load(handle)
        if not isinstance(parsed, dict):
            raise ValueError("m-cache config must be a TOML object.")
        config_data = _merge_dict(config_data, parsed)
    elif resolved_path is not None and not resolved_path.exists():
        raise FileNotFoundError(f"Config file not found: {resolved_path}")

    # Legacy env mappings (Wave 1 additive compatibility).
    global_block = dict(config_data.get("global", {}))
    resolved_app_root = _resolve_path_value(global_block.get("app_root"), base=root)
    global_block["app_root"] = str(resolved_app_root)
    global_block["log_level"] = os.getenv("M_CACHE_LOG_LEVEL", str(global_block.get("log_level", "INFO")))
    global_block["default_summary_json"] = _env_bool("M_CACHE_DEFAULT_SUMMARY_JSON", bool(global_block.get("default_summary_json", False)))
    global_block["default_progress_json"] = _env_bool("M_CACHE_DEFAULT_PROGRESS_JSON", bool(global_block.get("default_progress_json", False)))
    config_data["global"] = global_block

    domains = dict(config_data.get("domains", {}))
    earnings = dict(domains.get("earnings", {}))
    if "cache_root" not in earnings:
        earnings["cache_root"] = str(resolved_app_root / ".earnings_cache")
    if "normalized_refdata_root" not in earnings:
        earnings["normalized_refdata_root"] = str(resolved_app_root / "refdata" / "normalized")
    if "lookup_root" not in earnings:
        earnings["lookup_root"] = str(resolved_app_root / "refdata" / "normalized")
    if "default_resolution_mode" not in earnings:
        earnings["default_resolution_mode"] = "local_only"
    if "enabled" not in earnings:
        earnings["enabled"] = True
    providers = dict(earnings.get("providers", {}))
    for provider_name, provider_block in providers.items():
        if not isinstance(provider_block, dict):
            raise ValueError(f"domains.earnings.providers.{provider_name} must be a table/object.")
        required = ["auth_type", "rate_limit_policy", "direct_resolution_allowed"]
        missing = [key for key in required if key not in provider_block]
        if missing:
            raise ValueError(
                f"domains.earnings.providers.{provider_name} missing required keys: {', '.join(missing)}"
            )
    earnings["providers"] = providers
    domains["earnings"] = earnings
    config_data["domains"] = domains

    if bool(earnings.get("enabled", True)):
        if not str(earnings.get("cache_root") or "").strip():
            raise ValueError("domains.earnings.cache_root is required when earnings domain is enabled.")
        if not str(earnings.get("normalized_refdata_root") or "").strip():
            raise ValueError("domains.earnings.normalized_refdata_root is required when earnings domain is enabled.")
    earnings["cache_root"] = str(_resolve_path_value(earnings.get("cache_root"), base=resolved_app_root))
    earnings["normalized_refdata_root"] = str(
        _resolve_path_value(earnings.get("normalized_refdata_root"), base=resolved_app_root)
    )
    earnings["lookup_root"] = str(_resolve_path_value(earnings.get("lookup_root"), base=resolved_app_root))

    return MCacheEffectiveConfig(
        source=source,
        global_config=global_block,
        domains=domains,
    )


def load_config_from_effective_config(effective: MCacheEffectiveConfig) -> AppConfig:
    domain = dict(effective.domains.get("earnings", {}))
    app_root = Path(str(effective.global_config.get("app_root") or ".")).expanduser().resolve()
    cache_root = Path(str(domain.get("cache_root") or (app_root / ".earnings_cache"))).expanduser().resolve()
    normalized_refdata_root = Path(
        str(domain.get("normalized_refdata_root") or (app_root / "refdata" / "normalized"))
    ).expanduser().resolve()
    return AppConfig.from_roots(
        project_root=app_root,
        cache_root=cache_root,
        normalized_refdata_root=normalized_refdata_root,
    )
