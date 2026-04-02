from __future__ import annotations

from datetime import date, datetime, timezone

from py_earnings_calls.adapters.forecasts_finnhub import FinnhubForecastAdapter
from py_earnings_calls.adapters.forecasts_fmp import FmpForecastAdapter
from py_earnings_calls.adapters.base import ForecastAdapter
from py_earnings_calls.config import AppConfig
from py_earnings_calls.http import HttpClient
from py_earnings_calls.identifiers import forecast_archive_accession_id
from py_earnings_calls.lookup import build_symbol_to_cik_map, load_issuers_dataframe
from py_earnings_calls.storage.archive_index import upsert_forecast_archive_manifest
from py_earnings_calls.storage.paths import forecast_bundle_root, forecast_raw_snapshot_path, normalized_path
from py_earnings_calls.storage.writes import upsert_parquet, write_json

SUPPORTED_FORECAST_PROVIDERS = ("finnhub", "fmp")
SUPPORTED_FORECAST_PROVIDER_MODES = ("single", "fallback")


def run_forecast_refresh(
    config: AppConfig,
    *,
    symbols: list[str],
    as_of_date: date,
    provider: str | None = None,
    provider_mode: str = "single",
    provider_priority: list[str] | None = None,
) -> dict[str, object]:
    config.ensure_runtime_dirs()
    http_client = HttpClient(config)
    issuers_df = load_issuers_dataframe(config)
    symbol_to_cik = build_symbol_to_cik_map(issuers_df)

    normalized_symbols = sorted({symbol.upper() for symbol in symbols if str(symbol).strip()})
    if not normalized_symbols:
        raise ValueError("At least one symbol is required.")

    provider_order = _resolve_provider_order(provider=provider, provider_mode=provider_mode, provider_priority=provider_priority)
    adapters, provider_unavailable = _build_available_adapters(provider_order, config, http_client)

    selected_snapshots = []
    selected_points = []
    selected_provider_by_symbol: dict[str, str] = {}
    provider_attempts: dict[str, int] = {name: 0 for name in provider_order}
    provider_failures: dict[str, int] = {name: 0 for name in provider_order}
    no_data_symbols: list[str] = []

    for symbol in normalized_symbols:
        symbol_selected = False
        for provider_name in provider_order:
            if provider_name not in adapters:
                continue
            provider_attempts[provider_name] += 1
            adapter = adapters[provider_name]
            try:
                snapshots, points = adapter.fetch_snapshots([symbol], as_of_date)
            except Exception:
                provider_failures[provider_name] += 1
                continue

            symbol_snapshot = next((item for item in snapshots if item.symbol.upper() == symbol), None)
            symbol_points = [point for point in points if point.symbol.upper() == symbol]
            if symbol_snapshot is None:
                continue
            if not symbol_points:
                # Explicit no-data/partial-data path: keep fallback chain moving.
                continue

            selected_snapshots.append(symbol_snapshot)
            selected_points.extend(symbol_points)
            selected_provider_by_symbol[symbol] = provider_name
            symbol_selected = True
            break

        if not symbol_selected:
            no_data_symbols.append(symbol)

    selected_snapshots = sorted(
        selected_snapshots,
        key=lambda snapshot: (snapshot.provider, snapshot.symbol, snapshot.as_of_date.isoformat(), snapshot.snapshot_id),
    )
    selected_points = sorted(
        selected_points,
        key=lambda point: (
            point.provider,
            point.symbol,
            point.as_of_date.isoformat(),
            str(point.fiscal_year),
            str(point.fiscal_period),
            point.metric_name,
            point.stat_name,
        ),
    )

    for snapshot in selected_snapshots:
        raw_path = forecast_raw_snapshot_path(
            config,
            provider=snapshot.provider,
            symbol=snapshot.symbol,
            as_of_date=snapshot.as_of_date,
        )
        write_json(raw_path, snapshot.raw_payload)

    snapshot_rows = []
    manifest_rows = []
    for snapshot in selected_snapshots:
        archive_accession_id = forecast_archive_accession_id(
            provider=snapshot.provider,
            symbol=snapshot.symbol,
            as_of_date=snapshot.as_of_date,
        )
        bundle_root = forecast_bundle_root(
            config,
            provider=snapshot.provider,
            symbol=snapshot.symbol,
            as_of_date=snapshot.as_of_date,
        )
        row = snapshot.to_record()
        row["cik"] = symbol_to_cik.get(snapshot.symbol.upper())
        row["raw_payload_path"] = str(
            forecast_raw_snapshot_path(
                config,
                provider=snapshot.provider,
                symbol=snapshot.symbol,
                as_of_date=snapshot.as_of_date,
            )
        )
        row["archive_accession_id"] = archive_accession_id
        row["archive_bundle_path"] = str(bundle_root)
        row["imported_at"] = datetime.now(timezone.utc).isoformat()
        snapshot_rows.append(row)
        manifest_rows.append(
            {
                "snapshot_id": snapshot.snapshot_id,
                "archive_accession_id": archive_accession_id,
                "provider": snapshot.provider,
                "symbol": snapshot.symbol,
                "as_of_date": snapshot.as_of_date.isoformat(),
                "raw_payload_path": row["raw_payload_path"],
                "raw_payload_exists": True,
                "updated_at": row["imported_at"],
            }
        )

    snapshot_path = normalized_path(config, "forecast_snapshots")
    points_path = normalized_path(config, "forecast_points")
    point_rows = []
    for point in selected_points:
        point_row = point.to_record()
        point_row["cik"] = symbol_to_cik.get(point.symbol.upper())
        point_rows.append(point_row)
    upsert_parquet(snapshot_path, snapshot_rows, dedupe_keys=["provider", "symbol", "as_of_date"])
    upsert_parquet(points_path, point_rows, dedupe_keys=[
        "provider",
        "symbol",
        "as_of_date",
        "fiscal_year",
        "fiscal_period",
        "metric_name",
        "stat_name",
    ])
    manifest_path = upsert_forecast_archive_manifest(config, manifest_rows)

    return {
        "provider_mode": provider_mode,
        "provider_order": provider_order,
        "provider_unavailable": provider_unavailable,
        "selected_provider_by_symbol": selected_provider_by_symbol,
        "provider_attempts": provider_attempts,
        "provider_failures": provider_failures,
        "no_data_symbols": no_data_symbols,
        "snapshot_count": len(snapshot_rows),
        "point_count": len(selected_points),
        "artifact_paths": [str(snapshot_path), str(points_path)],
        "archive_manifest_path": str(manifest_path),
    }


def _resolve_provider_order(*, provider: str | None, provider_mode: str, provider_priority: list[str] | None) -> list[str]:
    if provider_mode not in SUPPORTED_FORECAST_PROVIDER_MODES:
        raise ValueError(f"Unsupported provider mode: {provider_mode}")

    if provider_mode == "single":
        if provider is None:
            raise ValueError("provider is required for single mode.")
        if provider not in SUPPORTED_FORECAST_PROVIDERS:
            raise ValueError(f"Unsupported forecast provider: {provider}")
        return [provider]

    priority = provider_priority or []
    deduped: list[str] = []
    for name in priority:
        if name in SUPPORTED_FORECAST_PROVIDERS and name not in deduped:
            deduped.append(name)
    if not deduped:
        raise ValueError("provider_priority is required for fallback mode.")
    return deduped


def _build_adapter(name: str, config: AppConfig, http_client: HttpClient) -> ForecastAdapter:
    if name == "finnhub":
        return FinnhubForecastAdapter(config, http_client)
    if name == "fmp":
        return FmpForecastAdapter(config, http_client)
    raise ValueError(f"Unsupported forecast provider: {name}")


def _build_available_adapters(
    provider_order: list[str], config: AppConfig, http_client: HttpClient
) -> tuple[dict[str, ForecastAdapter], dict[str, dict[str, str]]]:
    adapters: dict[str, ForecastAdapter] = {}
    provider_unavailable: dict[str, dict[str, str]] = {}
    for provider_name in provider_order:
        try:
            adapters[provider_name] = _build_adapter(provider_name, config, http_client)
        except ValueError as exc:
            provider_unavailable[provider_name] = _provider_unavailable_reason(provider_name, exc)
    return adapters, provider_unavailable


def _provider_unavailable_reason(provider_name: str, exc: ValueError) -> dict[str, str]:
    message = str(exc)
    upper_message = message.upper()
    if "API_KEY" in upper_message and "REQUIRED" in upper_message:
        if "FMP_API_KEY" in upper_message:
            missing_key = "FMP_API_KEY"
        elif "FINNHUB_API_KEY" in upper_message:
            missing_key = "FINNHUB_API_KEY"
        else:
            missing_key = "API_KEY"
        return {
            "reason_code": "missing_api_key",
            "message": "Provider unavailable: missing required API key.",
            "missing_key": missing_key,
        }
    return {
        "reason_code": "missing_config",
        "message": "Provider unavailable: missing required configuration.",
        "missing_key": "",
    }
