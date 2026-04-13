from __future__ import annotations

import importlib
import importlib.util
import inspect
import os
from pathlib import Path
import re
import sys
from types import ModuleType
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_LOCAL_SHARED_ROOT = (_REPO_ROOT / "m_cache_shared").resolve()
_PIN_FILE = _REPO_ROOT / "requirements" / "m_cache_shared_external.txt"
_CANONICAL_SOURCE_ENV = "M_CACHE_SHARED_SOURCE"
_CANONICAL_EXTERNAL_ROOT_ENV = "M_CACHE_SHARED_EXTERNAL_ROOT"
_COMPAT_SOURCE_ENV = "PY_EARNINGS_CALLS_SHARED_SOURCE"
_DEFAULT_EXTERNAL_ROOT = "m_cache_shared_ext.augmentation"
_SOURCE_MODE_AUTO = "auto"
_SOURCE_MODE_EXTERNAL = "external"
_SOURCE_MODE_LOCAL = "local"
_SOURCE_MODES = {_SOURCE_MODE_AUTO, _SOURCE_MODE_EXTERNAL, _SOURCE_MODE_LOCAL}
_REQUIRED_COMMON_SYMBOLS = (
    "ProducerTargetDescriptor",
    "ProducerRunSubmission",
    "ProducerArtifactSubmission",
    "RunStatusView",
    "EventsViewRow",
    "ApiAugmentationMeta",
    "AugmentationType",
    "ProducerKind",
    "RunStatus",
    "validate_producer_target_descriptor",
    "validate_producer_run_submission",
    "validate_producer_artifact_submission",
    "validate_run_submission_envelope",
    "validate_artifact_submission_envelope",
    "load_json_schema",
    "pack_run_status_view",
    "pack_events_view",
    "parse_json_input_payload",
)


def _load_external_augmentation_module(*, import_root: str) -> ModuleType:
    return importlib.import_module(import_root)


def _load_local_augmentation_module() -> ModuleType:
    try:
        return importlib.import_module("m_cache_shared.augmentation")
    except ModuleNotFoundError:
        package_init = _LOCAL_SHARED_ROOT / "__init__.py"
        package_spec = importlib.util.spec_from_file_location(
            "m_cache_shared",
            package_init,
            submodule_search_locations=[str(_LOCAL_SHARED_ROOT)],
        )
        if package_spec is None or package_spec.loader is None:
            raise
        package_module = importlib.util.module_from_spec(package_spec)
        sys.modules["m_cache_shared"] = package_module
        package_spec.loader.exec_module(package_module)
        return importlib.import_module("m_cache_shared.augmentation")


def _validate_common_surface(module: ModuleType) -> None:
    missing = [symbol for symbol in _REQUIRED_COMMON_SYMBOLS if not hasattr(module, symbol)]
    if missing:
        raise ImportError(f"External shared module is missing required symbols: {', '.join(missing)}")


def _canonical_source_mode() -> str:
    canonical_text = str(os.getenv(_CANONICAL_SOURCE_ENV, "")).strip().lower()
    compat_text = str(os.getenv(_COMPAT_SOURCE_ENV, "")).strip().lower()
    selected = canonical_text or compat_text or _SOURCE_MODE_AUTO
    if selected not in _SOURCE_MODES:
        raise ValueError(
            f"Unsupported {_CANONICAL_SOURCE_ENV} mode '{selected}'. "
            f"Expected one of: {', '.join(sorted(_SOURCE_MODES))}."
        )
    return selected


def _external_import_root() -> str:
    return str(os.getenv(_CANONICAL_EXTERNAL_ROOT_ENV, "")).strip() or _DEFAULT_EXTERNAL_ROOT


def _parse_pin_line() -> tuple[str | None, str | None]:
    if not _PIN_FILE.exists():
        return None, None
    pattern = re.compile(r"git\+(?P<url>[^@]+)@(?P<tag>\S+)")
    for raw in _PIN_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        matched = pattern.search(line)
        if matched:
            return matched.group("url"), matched.group("tag")
    return None, None


def _load_common_surface() -> tuple[str, ModuleType]:
    source_mode = _canonical_source_mode()
    if source_mode == _SOURCE_MODE_LOCAL:
        return "local", _load_local_augmentation_module()

    import_root = _external_import_root()
    if source_mode == _SOURCE_MODE_EXTERNAL:
        module = _load_external_augmentation_module(import_root=import_root)
        _validate_common_surface(module)
        return "external", module

    if source_mode == _SOURCE_MODE_AUTO:
        try:
            module = _load_external_augmentation_module(import_root=import_root)
            _validate_common_surface(module)
            return "external", module
        except Exception:
            return "local_fallback", _load_local_augmentation_module()
    raise ValueError(f"Unsupported source mode '{source_mode}'.")


_SHARED_SOURCE, _COMMON_MODULE = _load_common_surface()
_LOCAL_MODULE = _load_local_augmentation_module()
_LOCAL_PACKERS = importlib.import_module("m_cache_shared.augmentation.packers")
_LOCAL_HELPERS = importlib.import_module("m_cache_shared.augmentation.helpers")

# Strict first external common subset (Wave 6 boundary).
ProducerTargetDescriptor = getattr(_COMMON_MODULE, "ProducerTargetDescriptor")
ProducerRunSubmission = getattr(_COMMON_MODULE, "ProducerRunSubmission")
ProducerArtifactSubmission = getattr(_COMMON_MODULE, "ProducerArtifactSubmission")
RunStatusView = getattr(_COMMON_MODULE, "RunStatusView")
EventsViewRow = getattr(_COMMON_MODULE, "EventsViewRow")
ApiAugmentationMeta = getattr(_COMMON_MODULE, "ApiAugmentationMeta")
AugmentationType = getattr(_COMMON_MODULE, "AugmentationType")
ProducerKind = getattr(_COMMON_MODULE, "ProducerKind")
RunStatus = getattr(_COMMON_MODULE, "RunStatus")
validate_producer_target_descriptor = getattr(_COMMON_MODULE, "validate_producer_target_descriptor")
validate_producer_run_submission = getattr(_COMMON_MODULE, "validate_producer_run_submission")
validate_producer_artifact_submission = getattr(_COMMON_MODULE, "validate_producer_artifact_submission")
_COMMON_VALIDATE_RUN_SUBMISSION_ENVELOPE = getattr(_COMMON_MODULE, "validate_run_submission_envelope")
_COMMON_VALIDATE_ARTIFACT_SUBMISSION_ENVELOPE = getattr(_COMMON_MODULE, "validate_artifact_submission_envelope")
load_json_schema = getattr(_COMMON_MODULE, "load_json_schema")
_COMMON_PACK_RUN_STATUS_VIEW = getattr(_COMMON_MODULE, "pack_run_status_view")
_COMMON_PACK_EVENTS_VIEW = getattr(_COMMON_MODULE, "pack_events_view")
parse_json_input_payload = getattr(_COMMON_MODULE, "parse_json_input_payload")

# Compatibility value lists used by local wrappers.
AUGMENTATION_TYPES = getattr(_LOCAL_MODULE, "AUGMENTATION_TYPES")
PRODUCER_KINDS = getattr(_LOCAL_MODULE, "PRODUCER_KINDS")
PRODUCER_RUN_STATUSES = getattr(_LOCAL_MODULE, "PRODUCER_RUN_STATUSES")

# Intentionally local-only in first externalization cycle.
pack_additive_augmentation_meta = getattr(_LOCAL_MODULE, "pack_additive_augmentation_meta")
pack_run_status_not_found = getattr(_LOCAL_PACKERS, "pack_run_status_not_found")
pack_run_event_row = getattr(_LOCAL_PACKERS, "pack_run_event_row")
pack_artifact_event_row = getattr(_LOCAL_PACKERS, "pack_artifact_event_row")
build_artifact_idempotency_key = getattr(_LOCAL_PACKERS, "build_artifact_idempotency_key")
coerce_bool = getattr(_LOCAL_HELPERS, "coerce_bool")
max_nonempty_text = getattr(_LOCAL_HELPERS, "max_nonempty_text")
to_int_or_none = getattr(_LOCAL_HELPERS, "to_int_or_none")


def _validate_envelope_with_compat(
    func: Any,
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: Any,
    canonical_key_error: str,
) -> dict[str, Any]:
    params = inspect.signature(func).parameters
    if "expected_domain" in params:
        return func(
            envelope,
            expected_domain=expected_domain,
            expected_resource_family=expected_resource_family,
            canonical_key_validator=canonical_key_validator,
            canonical_key_error=canonical_key_error,
            resource_family_context="Wave 4 in earnings",
        )

    validated = func(envelope)
    domain = str(validated.get("domain") or "").strip().lower()
    if domain != str(expected_domain).strip().lower():
        raise ValueError(f"domain must be '{expected_domain}'")
    family = str(validated.get("resource_family") or "").strip().lower()
    if family != str(expected_resource_family).strip().lower():
        raise ValueError(f"resource_family must be '{expected_resource_family}'")
    canonical_key = str(validated.get("canonical_key") or "").strip()
    if not canonical_key_validator(canonical_key):
        raise ValueError(canonical_key_error)
    return validated


def validate_run_submission_envelope(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: Any,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    del resource_family_context
    return _validate_envelope_with_compat(
        _COMMON_VALIDATE_RUN_SUBMISSION_ENVELOPE,
        envelope,
        expected_domain=expected_domain,
        expected_resource_family=expected_resource_family,
        canonical_key_validator=canonical_key_validator,
        canonical_key_error=canonical_key_error,
    )


def validate_artifact_submission_envelope(
    envelope: dict[str, Any],
    *,
    expected_domain: str,
    expected_resource_family: str,
    canonical_key_validator: Any,
    canonical_key_error: str,
    resource_family_context: str,
) -> dict[str, Any]:
    del resource_family_context
    return _validate_envelope_with_compat(
        _COMMON_VALIDATE_ARTIFACT_SUBMISSION_ENVELOPE,
        envelope,
        expected_domain=expected_domain,
        expected_resource_family=expected_resource_family,
        canonical_key_validator=canonical_key_validator,
        canonical_key_error=canonical_key_error,
    )


def pack_run_status_view(**kwargs: Any) -> dict[str, Any]:
    payload = dict(_COMMON_PACK_RUN_STATUS_VIEW(**kwargs))
    if "found" not in payload:
        payload["found"] = True
    for key in (
        "domain",
        "resource_family",
        "run_id",
        "idempotency_key",
        "canonical_key",
        "augmentation_type",
        "source_text_version",
        "producer_name",
        "producer_version",
        "status",
        "success",
        "reason_code",
        "persisted_locally",
        "augmentation_stale",
        "last_updated_at",
    ):
        if key not in payload:
            payload[key] = kwargs.get(key)
    return payload


def pack_events_view(**kwargs: Any) -> dict[str, Any]:
    payload = dict(_COMMON_PACK_EVENTS_VIEW(**kwargs))
    records = kwargs.get("records")
    if "record_count" not in payload:
        payload["record_count"] = len(records) if isinstance(records, list) else 0
    for key in ("domain", "resource_family", "augmentation_applicable", "reason_code", "message", "records"):
        if key not in payload:
            payload[key] = kwargs.get(key)
    return payload


def shared_surface_source() -> str:
    return _SHARED_SOURCE


def shared_pin_metadata() -> dict[str, Any]:
    git_url, git_tag = _parse_pin_line()
    return {
        "git_url": git_url,
        "git_tag": git_tag,
        "pin_file": str(_PIN_FILE.relative_to(_REPO_ROOT)),
        "distribution": "m-cache-shared-ext",
        "default_external_root": _DEFAULT_EXTERNAL_ROOT,
        "source_mode_env_var": _CANONICAL_SOURCE_ENV,
        "external_root_env_var": _CANONICAL_EXTERNAL_ROOT_ENV,
        "compat_source_env_var": _COMPAT_SOURCE_ENV,
    }
