from __future__ import annotations

import json
from pathlib import Path


ARTIFACT_ROOT = Path("docs/standardization/wave7_repo_lifecycle")


def _load_json(name: str) -> dict:
    path = ARTIFACT_ROOT / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_wave7_governance_artifact_has_required_fields() -> None:
    payload = _load_json("governance_plan.py-earnings-calls-m.json")
    for key in ("repo", "external_repo_name", "public_api_policy", "required_release_artifacts"):
        assert key in payload
    assert payload["repo"] == "py-earnings-calls-m"
    assert payload["external_repo_name"] == "m-cache-shared-ext"
    assert "strict-common v1" in payload["public_api_policy"].lower()
    assert "does not broaden" in payload["public_api_policy"].lower()


def test_wave7_rc_workflow_artifact_has_required_fields_and_role_posture() -> None:
    payload = _load_json("rc_workflow_plan.py-earnings-calls-m.json")
    for key in ("repo", "facade_module", "rc_validation_role", "full_test_command"):
        assert key in payload
    assert payload["facade_module"] == "py_earnings_calls.augmentation_shared"
    role = payload["rc_validation_role"].lower()
    assert "pilot consumer-validator" in role
    assert "release blocker" in role
    assert "not" in role
    assert "public-api broadening authority" in role
    assert payload["full_test_command"] == "pytest -q"


def test_wave7_user_testing_policy_is_compat_impacting_only() -> None:
    payload = _load_json("user_testing_plan.py-earnings-calls-m.json")
    for key in ("repo", "user_testing_required", "key_user_flows"):
        assert key in payload
    assert payload["repo"] == "py-earnings-calls-m"
    assert payload["user_testing_required"] is True
    notes = str(payload.get("release_gate_notes") or "").lower()
    assert "compatibility-impacting stable releases only" in notes
    assert "not" in notes and "routine stable release" in notes
    assert "never a replacement" in notes


def test_wave7_shim_retirement_artifact_keeps_facade_and_defers_cleanup() -> None:
    payload = _load_json("shim_retirement_plan.py-earnings-calls-m.json")
    for key in ("repo", "local_shims_that_must_remain", "earliest_cleanup_condition"):
        assert key in payload
    shims = payload["local_shims_that_must_remain"].lower()
    assert "augmentation_shared" in shims
    assert "local fallback" in shims
    earliest = payload["earliest_cleanup_condition"].lower()
    assert "multiple successful stable cycles" in earliest
    assert "validation" in earliest
    assert "user-testing" in earliest
    assert "rollback confidence" in earliest


def test_wave7_lifecycle_docs_present() -> None:
    assert Path("docs/WAVE7_MIGRATION_NOTE.md").exists()
    assert (ARTIFACT_ROOT / "README.md").exists()
    assert (ARTIFACT_ROOT / "rc_stable_evidence_checklist.md").exists()
