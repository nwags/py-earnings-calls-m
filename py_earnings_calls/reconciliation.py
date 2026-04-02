from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.monitoring import RETRYABLE_TRANSCRIPT_FAILURES, run_monitor_poll
from py_earnings_calls.pipelines.lookup_refresh import run_lookup_refresh, run_lookup_refresh_scoped
from py_earnings_calls.storage.paths import legacy_transcript_html_path, legacy_transcript_text_path, normalized_path

DISCREPANCY_CODES = {
    "missing_transcript_parsed",
    "missing_transcript_raw",
    "missing_forecast_snapshot",
    "missing_forecast_points",
    "lookup_visibility_mismatch",
    "stale_forecast_snapshot",
    "retryable_transcript_failure",
}


def run_reconciliation(
    config: AppConfig,
    *,
    target_date: date,
    symbols: list[str] | None = None,
    max_symbols: int = 200,
    catch_up_warm: bool = False,
) -> dict[str, object]:
    config.ensure_runtime_dirs()
    now = datetime.now(timezone.utc).isoformat()

    discrepancies_path = normalized_path(config, "reconciliation_discrepancies")
    events_path = normalized_path(config, "reconciliation_events")

    transcript_calls = _read_parquet_or_empty(normalized_path(config, "transcript_calls"))
    transcript_artifacts = _read_parquet_or_empty(normalized_path(config, "transcript_artifacts"))
    transcript_failures = _read_parquet_or_empty(normalized_path(config, "transcript_backfill_failures"))
    forecast_snapshots = _read_parquet_or_empty(normalized_path(config, "forecast_snapshots"))
    forecast_points = _read_parquet_or_empty(normalized_path(config, "forecast_points"))
    lookup_transcripts = _read_parquet_or_empty(normalized_path(config, "local_lookup_transcripts"))
    lookup_forecasts = _read_parquet_or_empty(normalized_path(config, "local_lookup_forecasts"))
    seen = _read_parquet_or_empty(normalized_path(config, "monitor_seen_keys"))
    issuers = _read_parquet_or_empty(normalized_path(config, "issuers"))

    discrepancies: list[dict[str, object]] = []
    events: list[dict[str, object]] = []

    # Transcript artifact presence checks for seen successful transcript targets.
    if not seen.empty:
        seen_transcripts = seen[seen["target_type"].astype(str) == "transcript"] if "target_type" in seen.columns else pd.DataFrame()
    else:
        seen_transcripts = pd.DataFrame()
    for row in seen_transcripts.to_dict(orient="records"):
        if str(row.get("last_status") or "") != "present":
            continue
        key = str(row.get("seen_key") or "")
        raw_path = _to_str_or_none(row.get("expected_raw_path"))
        parsed_path = _to_str_or_none(row.get("expected_parsed_path"))
        if raw_path and not _path_exists_with_legacy(raw_path, transcript_calls, row, artifact_type="raw", config=config):
            discrepancies.append(
                _discrepancy_row(
                    now=now,
                    code="missing_transcript_raw",
                    key=f"missing_transcript_raw|{key}",
                    target_type="transcript",
                    seen_key=key,
                    symbol=_to_str_or_none(row.get("symbol")),
                    provider=_to_str_or_none(row.get("provider")),
                    target_date=None,
                    details={"expected_raw_path": raw_path},
                )
            )
        if parsed_path and not _path_exists_with_legacy(parsed_path, transcript_calls, row, artifact_type="parsed", config=config):
            discrepancies.append(
                _discrepancy_row(
                    now=now,
                    code="missing_transcript_parsed",
                    key=f"missing_transcript_parsed|{key}",
                    target_type="transcript",
                    seen_key=key,
                    symbol=_to_str_or_none(row.get("symbol")),
                    provider=_to_str_or_none(row.get("provider")),
                    target_date=None,
                    details={"expected_parsed_path": parsed_path},
                )
            )

    for row in transcript_failures.to_dict(orient="records"):
        reason = str(row.get("failure_reason") or "")
        if reason not in RETRYABLE_TRANSCRIPT_FAILURES:
            continue
        provider = str(row.get("provider") or "motley_fool")
        url = str(row.get("url") or "")
        discrepancies.append(
            _discrepancy_row(
                now=now,
                code="retryable_transcript_failure",
                key=f"retryable_transcript_failure|{provider}|{url}",
                target_type="transcript",
                seen_key=f"transcript|{provider}|{url}",
                symbol=_to_str_or_none(row.get("symbol")),
                provider=provider,
                target_date=None,
                details={"failure_reason": reason},
            )
        )

    target_symbols = _resolve_target_symbols(issuers, symbols, max_symbols=max_symbols)
    for symbol in target_symbols:
        has_snapshot = _has_forecast_row(forecast_snapshots, symbol=symbol, as_of=target_date)
        has_points = _has_forecast_row(forecast_points, symbol=symbol, as_of=target_date)
        if not has_snapshot:
            discrepancies.append(
                _discrepancy_row(
                    now=now,
                    code="missing_forecast_snapshot",
                    key=f"missing_forecast_snapshot|{symbol}|{target_date.isoformat()}",
                    target_type="forecast",
                    seen_key=f"forecast|{symbol}|{target_date.isoformat()}",
                    symbol=symbol,
                    provider=None,
                    target_date=target_date.isoformat(),
                    details={},
                )
            )
        if not has_points:
            discrepancies.append(
                _discrepancy_row(
                    now=now,
                    code="missing_forecast_points",
                    key=f"missing_forecast_points|{symbol}|{target_date.isoformat()}",
                    target_type="forecast",
                    seen_key=f"forecast|{symbol}|{target_date.isoformat()}",
                    symbol=symbol,
                    provider=None,
                    target_date=target_date.isoformat(),
                    details={},
                )
            )

        latest = _latest_snapshot_date_for_symbol(forecast_snapshots, symbol)
        if latest is not None and latest < target_date:
            discrepancies.append(
                _discrepancy_row(
                    now=now,
                    code="stale_forecast_snapshot",
                    key=f"stale_forecast_snapshot|{symbol}|{target_date.isoformat()}",
                    target_type="forecast",
                    seen_key=f"forecast|{symbol}|{target_date.isoformat()}",
                    symbol=symbol,
                    provider=None,
                    target_date=target_date.isoformat(),
                    details={"latest_snapshot_date": latest.isoformat()},
                )
            )

    # Lookup visibility mismatch checks
    if not transcript_calls.empty and not lookup_transcripts.empty and "call_id" in transcript_calls.columns and "call_id" in lookup_transcripts.columns:
        lookup_ids = set(lookup_transcripts["call_id"].astype(str).tolist())
        for call_id in transcript_calls["call_id"].astype(str).tolist():
            if call_id not in lookup_ids:
                discrepancies.append(
                    _discrepancy_row(
                        now=now,
                        code="lookup_visibility_mismatch",
                        key=f"lookup_visibility_mismatch|transcript|{call_id}",
                        target_type="lookup",
                        seen_key=f"transcript_call|{call_id}",
                        symbol=None,
                        provider=None,
                        target_date=None,
                        details={"scope": "transcripts"},
                    )
                )

    if not forecast_points.empty and not lookup_forecasts.empty and "symbol" in forecast_points.columns and "symbol" in lookup_forecasts.columns:
        lookup_symbols = set(lookup_forecasts["symbol"].astype(str).str.upper().tolist())
        for symbol in forecast_points["symbol"].astype(str).str.upper().tolist():
            if symbol not in lookup_symbols:
                discrepancies.append(
                    _discrepancy_row(
                        now=now,
                        code="lookup_visibility_mismatch",
                        key=f"lookup_visibility_mismatch|forecast|{symbol}",
                        target_type="lookup",
                        seen_key=f"forecast_symbol|{symbol}",
                        symbol=symbol,
                        provider=None,
                        target_date=None,
                        details={"scope": "forecasts"},
                    )
                )

    _write_discrepancies(discrepancies_path, discrepancies)
    events.append(
        {
            "event_at": now,
            "event_code": "reconcile_run_completed",
            "target_date": target_date.isoformat(),
            "discrepancy_count": len(discrepancies),
            "catch_up_warm": catch_up_warm,
        }
    )

    actions_taken = 0
    failures = 0
    skipped = 0
    lookup_updates: list[dict[str, object]] = []
    visibility_changed = {"transcripts": False, "forecasts": False}

    if catch_up_warm:
        # bounded catch-up via one-shot monitor warm path.
        poll_result = run_monitor_poll(
            config,
            target_date=target_date,
            warm=True,
            symbols=target_symbols,
            max_symbols=max_symbols,
        )
        actions_taken += int(poll_result.get("actions_taken", 0))
        failures += int(poll_result.get("failures", 0))
        skipped += int(poll_result.get("skipped", 0))
        updates = poll_result.get("lookup_updates", [])
        if isinstance(updates, list):
            lookup_updates.extend(updates)
    else:
        # no catch-up warm actions
        skipped += len(discrepancies)

    # Reconciliation can also drive lookup update when mismatches are present.
    mismatch_present = any(item["discrepancy_code"] == "lookup_visibility_mismatch" for item in discrepancies)
    if mismatch_present:
        visibility_changed = {"transcripts": True, "forecasts": True}
    lookup_result = _apply_lookup_updates(config, visibility_changed=visibility_changed)
    lookup_updates.extend(lookup_result["updates"])
    events.extend(lookup_result["events"])

    _append_events(events_path, events)
    return {
        "mode": "reconcile",
        "iterations": 1,
        "targets_considered": len(target_symbols) + len(transcript_failures.index),
        "actions_taken": actions_taken,
        "skipped": skipped,
        "failures": failures,
        "lookup_updates": lookup_updates,
        "artifacts_written": [str(discrepancies_path), str(events_path)],
    }


def _discrepancy_row(
    *,
    now: str,
    code: str,
    key: str,
    target_type: str,
    seen_key: str,
    symbol: str | None,
    provider: str | None,
    target_date: str | None,
    details: dict[str, object],
) -> dict[str, object]:
    if code not in DISCREPANCY_CODES:
        raise ValueError(f"Unsupported discrepancy code: {code}")
    return {
        "discrepancy_key": key,
        "discrepancy_code": code,
        "target_type": target_type,
        "seen_key": seen_key,
        "symbol": symbol,
        "provider": provider,
        "target_date": target_date,
        "details": str(details),
        "observed_at": now,
    }


def _write_discrepancies(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        if not path.exists():
            pd.DataFrame(
                columns=[
                    "discrepancy_key",
                    "discrepancy_code",
                    "target_type",
                    "seen_key",
                    "symbol",
                    "provider",
                    "target_date",
                    "details",
                    "observed_at",
                ]
            ).to_parquet(path, index=False)
        return
    incoming = pd.DataFrame(rows)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming
    combined = combined.drop_duplicates(subset=["discrepancy_key"], keep="last")
    combined.to_parquet(path, index=False)


def _append_events(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        if not path.exists():
            pd.DataFrame(columns=["event_at", "event_code", "target_date", "discrepancy_count", "catch_up_warm"]).to_parquet(path, index=False)
        return
    incoming = pd.DataFrame(rows)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming
    combined.to_parquet(path, index=False)


def _apply_lookup_updates(config: AppConfig, *, visibility_changed: dict[str, bool]) -> dict[str, object]:
    updates: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    now = datetime.now(timezone.utc).isoformat()

    include_transcripts = visibility_changed["transcripts"]
    include_forecasts = visibility_changed["forecasts"]
    if not include_transcripts and not include_forecasts:
        updates.append({"mode": "none", "reason": "no_visibility_change"})
        events.append(
            {
                "event_at": now,
                "event_code": "lookup_update_skipped",
                "target_date": None,
                "discrepancy_count": 0,
                "catch_up_warm": False,
            }
        )
        return {"updates": updates, "events": events}

    try:
        scoped = run_lookup_refresh_scoped(
            config,
            include_transcripts=include_transcripts,
            include_forecasts=include_forecasts,
        )
        updates.append({"mode": "incremental", "scope": scoped.get("scope"), "artifact_paths": scoped.get("artifact_paths", [])})
        events.append(
            {
                "event_at": now,
                "event_code": "lookup_update_incremental",
                "target_date": None,
                "discrepancy_count": 0,
                "catch_up_warm": False,
            }
        )
        return {"updates": updates, "events": events}
    except Exception as exc:
        full = run_lookup_refresh(config)
        updates.append(
            {
                "mode": "full_fallback",
                "reason": "incremental_failed",
                "error": str(exc),
                "artifact_paths": full.get("artifact_paths", []),
            }
        )
        events.append(
            {
                "event_at": now,
                "event_code": "lookup_update_full_refresh_fallback",
                "target_date": None,
                "discrepancy_count": 0,
                "catch_up_warm": False,
            }
        )
        return {"updates": updates, "events": events}


def _read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def _path_exists_with_legacy(
    path_value: str,
    transcript_calls: pd.DataFrame,
    seen_row: dict,
    *,
    artifact_type: str,
    config: AppConfig,
) -> bool:
    if Path(path_value).exists():
        return True
    url = str(seen_row.get("url") or "").strip()
    if not url or transcript_calls.empty or "source_url" not in transcript_calls.columns:
        return False
    matches = transcript_calls[transcript_calls["source_url"].astype(str) == url]
    if matches.empty:
        return False
    row = matches.iloc[0].to_dict()
    provider = str(row.get("provider") or "").strip()
    symbol = str(row.get("symbol") or "").strip()
    call_id = str(row.get("call_id") or "").strip()
    call_datetime = row.get("call_datetime")
    if not provider or not symbol or not call_id or not call_datetime:
        return False
    try:
        call_date = pd.to_datetime(call_datetime, errors="raise", utc=False).date()
    except Exception:
        return False
    if artifact_type == "raw":
        return legacy_transcript_html_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id).exists()
    if artifact_type == "parsed":
        return legacy_transcript_text_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id).exists()
    return False


def _resolve_target_symbols(issuers: pd.DataFrame, symbols: list[str] | None, *, max_symbols: int) -> list[str]:
    if symbols:
        return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})[:max_symbols]
    if issuers.empty or "symbol" not in issuers.columns:
        return []
    return sorted({str(item).strip().upper() for item in issuers["symbol"].tolist() if str(item).strip()})[:max_symbols]


def _has_forecast_row(df: pd.DataFrame, *, symbol: str, as_of: date) -> bool:
    if df.empty or "symbol" not in df.columns or "as_of_date" not in df.columns:
        return False
    mask = (df["symbol"].astype(str).str.upper() == symbol) & (df["as_of_date"].astype(str) == as_of.isoformat())
    return bool(mask.any())


def _latest_snapshot_date_for_symbol(df: pd.DataFrame, symbol: str) -> date | None:
    if df.empty or "symbol" not in df.columns or "as_of_date" not in df.columns:
        return None
    rows = df[df["symbol"].astype(str).str.upper() == symbol]
    if rows.empty:
        return None
    parsed = pd.to_datetime(rows["as_of_date"], errors="coerce").dropna()
    if parsed.empty:
        return None
    return parsed.max().date()


def _to_str_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
