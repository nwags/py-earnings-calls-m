from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import time

import pandas as pd

from py_earnings_calls.config import AppConfig
from py_earnings_calls.pipelines.forecast_refresh import run_forecast_refresh
from py_earnings_calls.pipelines.lookup_refresh import run_lookup_refresh, run_lookup_refresh_scoped
from py_earnings_calls.pipelines.transcript_backfill import run_transcript_backfill
from py_earnings_calls.storage.paths import (
    legacy_transcript_html_path,
    legacy_transcript_text_path,
    normalized_path,
    transcript_html_path,
    transcript_text_path,
)

RETRYABLE_TRANSCRIPT_FAILURES = {"HTTP_ERROR", "RETRY_EXHAUSTED", "PARSE_ERROR"}


@dataclass(frozen=True)
class TranscriptTarget:
    seen_key: str
    provider: str
    url: str
    symbol: str | None
    should_retry: bool
    reason_code: str
    expected_raw_path: str | None
    expected_parsed_path: str | None


@dataclass(frozen=True)
class ForecastTarget:
    seen_key: str
    symbol: str
    target_date: date
    has_snapshot: bool
    has_points: bool
    reason_code: str


def run_monitor_poll(
    config: AppConfig,
    *,
    target_date: date,
    warm: bool = False,
    symbols: list[str] | None = None,
    max_symbols: int = 200,
    provider_mode: str = "fallback",
    provider: str = "finnhub",
    provider_priority: list[str] | None = None,
) -> dict[str, object]:
    config.ensure_runtime_dirs()
    now = _utc_now()

    seen_path = normalized_path(config, "monitor_seen_keys")
    events_path = normalized_path(config, "monitor_events")
    transcript_calls_path = normalized_path(config, "transcript_calls")
    forecast_snapshots_path = normalized_path(config, "forecast_snapshots")
    forecast_points_path = normalized_path(config, "forecast_points")
    failures_path = normalized_path(config, "transcript_backfill_failures")

    seen_df = _read_parquet_or_empty(seen_path)
    transcript_calls = _read_parquet_or_empty(transcript_calls_path)
    forecast_snapshots = _read_parquet_or_empty(forecast_snapshots_path)
    forecast_points = _read_parquet_or_empty(forecast_points_path)
    failures_df = _read_parquet_or_empty(failures_path)

    transcript_targets = _build_transcript_targets(
        config=config,
        failures_df=failures_df,
        seen_df=seen_df,
        transcript_calls=transcript_calls,
    )
    forecast_targets = _build_forecast_targets(
        config=config,
        target_date=target_date,
        symbols=symbols,
        max_symbols=max_symbols,
        forecast_snapshots=forecast_snapshots,
        forecast_points=forecast_points,
    )

    events: list[dict[str, object]] = []
    seen_updates: list[dict[str, object]] = []
    actions_taken = 0
    skipped = 0
    failures = 0
    lookup_updates: list[dict[str, object]] = []
    visibility_changed = {"transcripts": False, "forecasts": False}

    for target in transcript_targets:
        action = "skipped"
        event_code = "transcript_retry_skipped"
        if warm and target.should_retry:
            action = "retry_backfill"
            event_code = "transcript_retry_attempted"
            try:
                backfill_result = run_transcript_backfill(config, urls=[target.url], symbol=target.symbol)
                fetched = int(backfill_result.get("fetched_count", 0))
                failed = int(backfill_result.get("failed_count", 0))
                if fetched > 0:
                    visibility_changed["transcripts"] = True
                    seen_updates.append(
                        {
                            "seen_key": target.seen_key,
                            "target_type": "transcript",
                            "provider": target.provider,
                            "url": target.url,
                            "symbol": target.symbol,
                            "target_date": None,
                            "last_status": "present",
                            "last_reason_code": "warm_success",
                            "last_action": action,
                            "last_seen_at": now,
                            "expected_raw_path": _latest_row_value_for_url(transcript_calls, target.url, "raw_html_path"),
                            "expected_parsed_path": _latest_row_value_for_url(transcript_calls, target.url, "transcript_path"),
                        }
                    )
                else:
                    seen_updates.append(
                        {
                            "seen_key": target.seen_key,
                            "target_type": "transcript",
                            "provider": target.provider,
                            "url": target.url,
                            "symbol": target.symbol,
                            "target_date": None,
                            "last_status": "missing",
                            "last_reason_code": "warm_no_fetch",
                            "last_action": action,
                            "last_seen_at": now,
                            "expected_raw_path": target.expected_raw_path,
                            "expected_parsed_path": target.expected_parsed_path,
                        }
                    )
                actions_taken += 1
                failures += failed
            except Exception as exc:
                failures += 1
                seen_updates.append(
                    {
                        "seen_key": target.seen_key,
                        "target_type": "transcript",
                        "provider": target.provider,
                        "url": target.url,
                        "symbol": target.symbol,
                        "target_date": None,
                        "last_status": "error",
                        "last_reason_code": "warm_error",
                        "last_action": action,
                        "last_seen_at": now,
                        "expected_raw_path": target.expected_raw_path,
                        "expected_parsed_path": target.expected_parsed_path,
                    }
                )
                events.append(
                    _event_row(
                        now=now,
                        mode="poll",
                        target_type="transcript",
                        seen_key=target.seen_key,
                        event_code="transcript_retry_error",
                        reason_code=target.reason_code,
                        action=action,
                        message=str(exc),
                        provider=target.provider,
                        url=target.url,
                        symbol=target.symbol,
                        target_date=None,
                    )
                )
                continue
        else:
            skipped += 1
            status = "missing" if target.should_retry else "present"
            seen_updates.append(
                {
                    "seen_key": target.seen_key,
                    "target_type": "transcript",
                    "provider": target.provider,
                    "url": target.url,
                    "symbol": target.symbol,
                    "target_date": None,
                    "last_status": status,
                    "last_reason_code": target.reason_code,
                    "last_action": action,
                    "last_seen_at": now,
                    "expected_raw_path": target.expected_raw_path,
                    "expected_parsed_path": target.expected_parsed_path,
                }
            )

        events.append(
            _event_row(
                now=now,
                mode="poll",
                target_type="transcript",
                seen_key=target.seen_key,
                event_code=event_code,
                reason_code=target.reason_code,
                action=action,
                message=None,
                provider=target.provider,
                url=target.url,
                symbol=target.symbol,
                target_date=None,
            )
        )

    for target in forecast_targets:
        action = "skipped"
        event_code = "forecast_refresh_skipped"
        should_refresh = (not target.has_snapshot) or (not target.has_points)
        if warm and should_refresh:
            action = "refresh_forecast"
            event_code = "forecast_refresh_attempted"
            try:
                refresh_result = run_forecast_refresh(
                    config,
                    symbols=[target.symbol],
                    as_of_date=target_date,
                    provider=provider,
                    provider_mode=provider_mode,
                    provider_priority=provider_priority or ["finnhub", "fmp"],
                )
                snapshot_count = int(refresh_result.get("snapshot_count", 0))
                point_count = int(refresh_result.get("point_count", 0))
                if snapshot_count > 0 and point_count > 0:
                    visibility_changed["forecasts"] = True
                    status = "present"
                    reason = "warm_success"
                else:
                    status = "missing"
                    reason = "warm_no_data"
                seen_updates.append(
                    {
                        "seen_key": target.seen_key,
                        "target_type": "forecast",
                        "provider": None,
                        "url": None,
                        "symbol": target.symbol,
                        "target_date": target.target_date.isoformat(),
                        "last_status": status,
                        "last_reason_code": reason,
                        "last_action": action,
                        "last_seen_at": now,
                        "expected_raw_path": None,
                        "expected_parsed_path": None,
                    }
                )
                actions_taken += 1
            except Exception as exc:
                failures += 1
                seen_updates.append(
                    {
                        "seen_key": target.seen_key,
                        "target_type": "forecast",
                        "provider": None,
                        "url": None,
                        "symbol": target.symbol,
                        "target_date": target.target_date.isoformat(),
                        "last_status": "error",
                        "last_reason_code": "warm_error",
                        "last_action": action,
                        "last_seen_at": now,
                        "expected_raw_path": None,
                        "expected_parsed_path": None,
                    }
                )
                events.append(
                    _event_row(
                        now=now,
                        mode="poll",
                        target_type="forecast",
                        seen_key=target.seen_key,
                        event_code="forecast_refresh_error",
                        reason_code=target.reason_code,
                        action=action,
                        message=str(exc),
                        provider=None,
                        url=None,
                        symbol=target.symbol,
                        target_date=target.target_date.isoformat(),
                    )
                )
                continue
        else:
            skipped += 1
            seen_updates.append(
                {
                    "seen_key": target.seen_key,
                    "target_type": "forecast",
                    "provider": None,
                    "url": None,
                    "symbol": target.symbol,
                    "target_date": target.target_date.isoformat(),
                    "last_status": "present" if not should_refresh else "missing",
                    "last_reason_code": target.reason_code,
                    "last_action": action,
                    "last_seen_at": now,
                    "expected_raw_path": None,
                    "expected_parsed_path": None,
                }
            )

        events.append(
            _event_row(
                now=now,
                mode="poll",
                target_type="forecast",
                seen_key=target.seen_key,
                event_code=event_code,
                reason_code=target.reason_code,
                action=action,
                message=None,
                provider=None,
                url=None,
                symbol=target.symbol,
                target_date=target.target_date.isoformat(),
            )
        )

    _write_seen_state(seen_path, seen_updates)
    _append_events(events_path, events)

    lookup_result = _apply_lookup_updates(config, visibility_changed=visibility_changed)
    lookup_updates.extend(lookup_result["updates"])
    if lookup_result["events"]:
        _append_events(events_path, lookup_result["events"])

    return {
        "mode": "poll",
        "iterations": 1,
        "targets_considered": len(transcript_targets) + len(forecast_targets),
        "actions_taken": actions_taken,
        "skipped": skipped,
        "failures": failures,
        "lookup_updates": lookup_updates,
        "artifacts_written": [str(seen_path), str(events_path)],
    }


def run_monitor_loop(
    config: AppConfig,
    *,
    target_date: date,
    interval_seconds: float,
    max_iterations: int,
    warm: bool = False,
    symbols: list[str] | None = None,
    max_symbols: int = 200,
    provider_mode: str = "fallback",
    provider: str = "finnhub",
    provider_priority: list[str] | None = None,
) -> dict[str, object]:
    iterations_run = 0
    total_targets = 0
    total_actions = 0
    total_skipped = 0
    total_failures = 0
    lookup_updates: list[dict[str, object]] = []
    artifacts_written: set[str] = set()

    for index in range(max_iterations):
        poll = run_monitor_poll(
            config,
            target_date=target_date,
            warm=warm,
            symbols=symbols,
            max_symbols=max_symbols,
            provider_mode=provider_mode,
            provider=provider,
            provider_priority=provider_priority,
        )
        iterations_run += 1
        total_targets += int(poll["targets_considered"])
        total_actions += int(poll["actions_taken"])
        total_skipped += int(poll["skipped"])
        total_failures += int(poll["failures"])
        lookup_updates.extend(list(poll.get("lookup_updates", [])))
        artifacts_written.update(list(poll.get("artifacts_written", [])))
        if index < max_iterations - 1 and interval_seconds > 0:
            time.sleep(interval_seconds)

    return {
        "mode": "loop",
        "iterations": iterations_run,
        "targets_considered": total_targets,
        "actions_taken": total_actions,
        "skipped": total_skipped,
        "failures": total_failures,
        "lookup_updates": lookup_updates,
        "artifacts_written": sorted(artifacts_written),
    }


def _build_transcript_targets(
    *, config: AppConfig, failures_df: pd.DataFrame, seen_df: pd.DataFrame, transcript_calls: pd.DataFrame
) -> list[TranscriptTarget]:
    targets: list[TranscriptTarget] = []
    if failures_df.empty:
        return targets
    seen_by_key = {}
    if not seen_df.empty:
        for row in seen_df.to_dict(orient="records"):
            seen_by_key[str(row.get("seen_key", ""))] = row

    for row in failures_df.to_dict(orient="records"):
        provider = str(row.get("provider") or "motley_fool")
        url = str(row.get("url") or "").strip()
        if not url:
            continue
        symbol = row.get("symbol")
        seen_key = f"transcript|{provider}|{url}"
        failure_reason = str(row.get("failure_reason") or "")
        retryable = failure_reason in RETRYABLE_TRANSCRIPT_FAILURES

        expected_raw = None
        expected_parsed = None
        if seen_key in seen_by_key:
            expected_raw = _to_str_or_none(seen_by_key[seen_key].get("expected_raw_path"))
            expected_parsed = _to_str_or_none(seen_by_key[seen_key].get("expected_parsed_path"))
        else:
            expected_raw = _latest_row_value_for_url(transcript_calls, url, "raw_html_path")
            expected_parsed = _latest_row_value_for_url(transcript_calls, url, "transcript_path")

        missing_expected_artifacts = (
            (expected_raw is not None and _is_missing_path_with_legacy(config, transcript_calls, url, expected_raw, artifact_type="raw"))
            or (expected_parsed is not None and _is_missing_path_with_legacy(config, transcript_calls, url, expected_parsed, artifact_type="parsed"))
        )
        should_retry = retryable or missing_expected_artifacts
        reason_code = "retryable_transcript_failure" if retryable else "skipped_non_retryable_failure"
        if missing_expected_artifacts:
            reason_code = "re_warm_due_to_missing_local_artifact"

        targets.append(
            TranscriptTarget(
                seen_key=seen_key,
                provider=provider,
                url=url,
                symbol=_to_str_or_none(symbol),
                should_retry=should_retry,
                reason_code=reason_code,
                expected_raw_path=expected_raw,
                expected_parsed_path=expected_parsed,
            )
        )
    return sorted(targets, key=lambda item: (item.provider, item.url))


def _build_forecast_targets(
    *,
    config: AppConfig,
    target_date: date,
    symbols: list[str] | None,
    max_symbols: int,
    forecast_snapshots: pd.DataFrame,
    forecast_points: pd.DataFrame,
) -> list[ForecastTarget]:
    if symbols:
        target_symbols = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})[:max_symbols]
    else:
        issuers_path = normalized_path(config, "issuers")
        if issuers_path.exists():
            issuers = pd.read_parquet(issuers_path)
            target_symbols = (
                sorted({str(row).strip().upper() for row in issuers.get("symbol", pd.Series(dtype="string")).tolist() if str(row).strip()})[
                    :max_symbols
                ]
            )
        else:
            target_symbols = []

    snapshot_matches = set()
    if not forecast_snapshots.empty:
        snapshot_rows = forecast_snapshots.to_dict(orient="records")
        for row in snapshot_rows:
            if str(row.get("as_of_date") or "") == target_date.isoformat():
                symbol = str(row.get("symbol") or "").strip().upper()
                if symbol:
                    snapshot_matches.add(symbol)

    point_matches = set()
    if not forecast_points.empty:
        point_rows = forecast_points.to_dict(orient="records")
        for row in point_rows:
            if str(row.get("as_of_date") or "") == target_date.isoformat():
                symbol = str(row.get("symbol") or "").strip().upper()
                if symbol:
                    point_matches.add(symbol)

    targets: list[ForecastTarget] = []
    for symbol in target_symbols:
        has_snapshot = symbol in snapshot_matches
        has_points = symbol in point_matches
        reason = "forecast_present"
        if not has_snapshot:
            reason = "missing_forecast_snapshot"
        elif not has_points:
            reason = "missing_forecast_points"
        targets.append(
            ForecastTarget(
                seen_key=f"forecast|{symbol}|{target_date.isoformat()}",
                symbol=symbol,
                target_date=target_date,
                has_snapshot=has_snapshot,
                has_points=has_points,
                reason_code=reason,
            )
        )
    return targets


def _apply_lookup_updates(config: AppConfig, *, visibility_changed: dict[str, bool]) -> dict[str, object]:
    updates: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    now = _utc_now()

    include_transcripts = visibility_changed["transcripts"]
    include_forecasts = visibility_changed["forecasts"]
    if not include_transcripts and not include_forecasts:
        updates.append({"mode": "none", "reason": "no_visibility_change"})
        events.append(
            _event_row(
                now=now,
                mode="poll",
                target_type="lookup",
                seen_key="lookup",
                event_code="lookup_update_skipped",
                reason_code="no_visibility_change",
                action="skipped",
                message=None,
                provider=None,
                url=None,
                symbol=None,
                target_date=None,
            )
        )
        return {"updates": updates, "events": events}

    try:
        scoped = run_lookup_refresh_scoped(
            config,
            include_transcripts=include_transcripts,
            include_forecasts=include_forecasts,
        )
        updates.append(
            {
                "mode": "incremental",
                "scope": scoped.get("scope"),
                "artifact_paths": scoped.get("artifact_paths", []),
            }
        )
        events.append(
            _event_row(
                now=now,
                mode="poll",
                target_type="lookup",
                seen_key="lookup",
                event_code="lookup_update_incremental",
                reason_code="visibility_changed",
                action="refresh_scoped",
                message=None,
                provider=None,
                url=None,
                symbol=None,
                target_date=None,
            )
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
            _event_row(
                now=now,
                mode="poll",
                target_type="lookup",
                seen_key="lookup",
                event_code="lookup_update_full_refresh_fallback",
                reason_code="incremental_failed",
                action="refresh_full",
                message=str(exc),
                provider=None,
                url=None,
                symbol=None,
                target_date=None,
            )
        )
        return {"updates": updates, "events": events}


def _event_row(
    *,
    now: str,
    mode: str,
    target_type: str,
    seen_key: str,
    event_code: str,
    reason_code: str,
    action: str,
    message: str | None,
    provider: str | None,
    url: str | None,
    symbol: str | None,
    target_date: str | None,
) -> dict[str, object]:
    return {
        "event_at": now,
        "mode": mode,
        "target_type": target_type,
        "seen_key": seen_key,
        "event_code": event_code,
        "reason_code": reason_code,
        "action": action,
        "message": message,
        "provider": provider,
        "url": url,
        "symbol": symbol,
        "target_date": target_date,
    }


def _write_seen_state(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        if not path.exists():
            pd.DataFrame(
                columns=[
                    "seen_key",
                    "target_type",
                    "provider",
                    "url",
                    "symbol",
                    "target_date",
                    "last_status",
                    "last_reason_code",
                    "last_action",
                    "last_seen_at",
                    "expected_raw_path",
                    "expected_parsed_path",
                ]
            ).to_parquet(path, index=False)
        return
    incoming = pd.DataFrame(rows)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming
    combined = combined.drop_duplicates(subset=["seen_key"], keep="last")
    combined.to_parquet(path, index=False)


def _append_events(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        if not path.exists():
            pd.DataFrame(
                columns=[
                    "event_at",
                    "mode",
                    "target_type",
                    "seen_key",
                    "event_code",
                    "reason_code",
                    "action",
                    "message",
                    "provider",
                    "url",
                    "symbol",
                    "target_date",
                ]
            ).to_parquet(path, index=False)
        return
    incoming = pd.DataFrame(rows)
    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming
    combined.to_parquet(path, index=False)


def _read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def _latest_row_value_for_url(df: pd.DataFrame, url: str, column: str) -> str | None:
    if df.empty or "source_url" not in df.columns or column not in df.columns:
        return None
    matches = df[df["source_url"].astype(str) == str(url)]
    if matches.empty:
        return None
    if "call_datetime" in matches.columns:
        matches = matches.assign(_dt=pd.to_datetime(matches["call_datetime"], errors="coerce", utc=False))
        matches = matches.sort_values("_dt", ascending=False, na_position="last")
    return _to_str_or_none(matches.iloc[0].get(column))


def _is_missing_path(value: str | None) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    return not Path(text).exists()


def _is_missing_path_with_legacy(
    config: AppConfig,
    transcript_calls: pd.DataFrame,
    url: str,
    value: str | None,
    *,
    artifact_type: str,
) -> bool:
    if not _is_missing_path(value):
        return False
    row = _latest_transcript_row_for_url(transcript_calls, url)
    if row is None:
        return True
    legacy_candidates = _legacy_candidates_from_call_row(config, row, artifact_type=artifact_type)
    return not any(path.exists() for path in legacy_candidates)


def _latest_transcript_row_for_url(df: pd.DataFrame, url: str) -> dict | None:
    if df.empty or "source_url" not in df.columns:
        return None
    matches = df[df["source_url"].astype(str) == str(url)]
    if matches.empty:
        return None
    if "call_datetime" in matches.columns:
        matches = matches.assign(_dt=pd.to_datetime(matches["call_datetime"], errors="coerce", utc=False))
        matches = matches.sort_values("_dt", ascending=False, na_position="last")
    return matches.iloc[0].to_dict()


def _legacy_candidates_from_call_row(config: AppConfig, row: dict, *, artifact_type: str) -> list[Path]:
    provider = str(row.get("provider") or "").strip()
    symbol = str(row.get("symbol") or "").strip()
    call_id = str(row.get("call_id") or "").strip()
    call_datetime = row.get("call_datetime")
    storage_cik = row.get("storage_cik")
    if not provider or not symbol or not call_id or not call_datetime:
        return []
    try:
        call_date = pd.to_datetime(call_datetime, errors="raise", utc=False).date()
    except Exception:
        return []
    candidates: list[Path] = []
    if artifact_type == "raw":
        candidates.append(legacy_transcript_html_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id))
        candidates.append(
            transcript_html_path(
                config,
                provider=provider,
                symbol=symbol,
                call_date=call_date,
                call_id=call_id,
                storage_cik=storage_cik,
            )
        )
    if artifact_type == "parsed":
        candidates.append(legacy_transcript_text_path(config, provider=provider, symbol=symbol, call_date=call_date, call_id=call_id))
        candidates.append(
            transcript_text_path(
                config,
                provider=provider,
                symbol=symbol,
                call_date=call_date,
                call_id=call_id,
                storage_cik=storage_cik,
            )
        )
    return candidates


def _to_str_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
