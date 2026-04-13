from __future__ import annotations

import json
from pathlib import Path


ARTIFACT_ROOT = Path("docs/standardization/wave7_1_repo_release_lifecycle")


def _load_json(name: str) -> dict:
    path = ARTIFACT_ROOT / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_wave7_1_package_release_plan_is_obligation_only() -> None:
    payload = _load_json("package_release_plan.py-earnings-calls-m.json")
    for key in ("repo", "release_role", "facade_module", "required_validation"):
        assert key in payload
    assert payload["repo"] == "py-earnings-calls-m"
    assert payload["facade_module"] == "py_earnings_calls.augmentation_shared"
    role = payload["release_role"].lower()
    assert "pilot consumer-validator" in role
    assert "release blocker" in role


def test_wave7_1_promotion_plan_has_signoff_blockers_and_rollback() -> None:
    payload = _load_json("promotion_plan.py-earnings-calls-m.json")
    for key in ("repo", "required_signoff", "blocker_conditions", "rollback_path"):
        assert key in payload
    required_signoff = payload["required_signoff"].lower()
    assert "transcript write-path safety" in required_signoff
    blockers = payload["blocker_conditions"].lower()
    assert "role/applicability/authority drift" in blockers
    assert "cli/api semantic drift" in blockers
    rollback = payload["rollback_path"].lower()
    assert "repin" in rollback
    assert "m_cache_shared_external.txt" in rollback
    assert "m_cache_shared_source=local" in rollback


def test_wave7_1_user_testing_start_gate_is_explicit() -> None:
    payload = _load_json("user_testing_start_plan.py-earnings-calls-m.json")
    for key in ("repo", "requires_wave7_1_completion", "requires_shared_rc_validation", "key_start_gate_note"):
        assert key in payload
    assert payload["requires_wave7_1_completion"] is True
    assert payload["requires_shared_rc_validation"] is True
    note = payload["key_start_gate_note"].lower()
    assert "wave 7.1 implementation" in note
    assert "one shared rc fully validated across all repos" in note
    assert "evidence/signoff flow operational end-to-end" in note
    assert "rollback verified" in note
    assert "no open blocking lifecycle incident" in note


def test_wave7_1_cleanup_deferral_explicitly_blocks_removal_work() -> None:
    payload = _load_json("cleanup_deferral_plan.py-earnings-calls-m.json")
    for key in ("repo", "deferred_items", "earliest_entry_condition"):
        assert key in payload
    deferred = payload["deferred_items"].lower()
    assert "no public api broadening" in deferred
    assert "no shim/fallback removal" in deferred
    assert "no env alias removal" in deferred
    assert "no import-root collapse" in deferred
    assert "no local ownership reduction" in deferred


def test_wave7_1_docs_and_bundle_input_checklist_present() -> None:
    assert Path("docs/WAVE7_1_MIGRATION_NOTE.md").exists()
    assert (ARTIFACT_ROOT / "README.md").exists()
    checklist = (ARTIFACT_ROOT / "shared_release_bundle_inputs_checklist.md").read_text(encoding="utf-8").lower()
    assert "central package-side release evidence bundle" in checklist
    assert "does not define a separate release process" not in checklist  # checklist should stay operational, not policy-heavy
    assert "blocker taxonomy" in checklist
