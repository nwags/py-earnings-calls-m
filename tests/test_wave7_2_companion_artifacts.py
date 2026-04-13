from __future__ import annotations

import json
from pathlib import Path


ARTIFACT_ROOT = Path("docs/standardization/wave7_2_repo_companion")


def _load_json(name: str) -> dict:
    path = ARTIFACT_ROOT / name
    return json.loads(path.read_text(encoding="utf-8"))


def test_wave7_2_companion_plan_is_minimal_and_participation_only() -> None:
    payload = _load_json("consumer_companion_plan.py-earnings-calls-m.json")
    for key in ("repo", "release_role", "required_validation", "evidence_input", "blocker_scope"):
        assert key in payload
    assert payload["repo"] == "py-earnings-calls-m"
    role = payload["release_role"].lower()
    assert "pilot consumer-validator" in role
    assert "release blocker" in role
    required_validation = payload["required_validation"].lower()
    assert "no runtime" not in required_validation  # keep machine artifact operational, constraints are in migration note/tests
    assert "writable /tmp editable-install path" in required_validation
    assert "--no-build-isolation" in required_validation


def test_wave7_2_signoff_schema_matches_exact_package_side_contract() -> None:
    schema = _load_json("SIGNOFF.schema.json")
    assert schema["additionalProperties"] is False
    required = set(schema["required"])
    expected = {
        "candidate_tag",
        "repo",
        "release_role",
        "pin_confirmed",
        "validation_status",
        "signoff_state",
        "blockers",
        "warnings",
        "rollback_ready",
    }
    assert required == expected
    signoff_enum = schema["properties"]["signoff_state"]["enum"]
    assert signoff_enum == ["pass", "warn", "block"]


def test_wave7_2_signoff_template_uses_exact_fields_and_canonical_signoff_vocab() -> None:
    payload = _load_json("SIGNOFF.template.json")
    assert set(payload.keys()) == {
        "candidate_tag",
        "repo",
        "release_role",
        "pin_confirmed",
        "validation_status",
        "signoff_state",
        "blockers",
        "warnings",
        "rollback_ready",
    }
    assert payload["candidate_tag"] == "v0.1.0-rc9"
    assert payload["signoff_state"] in {"pass", "warn", "block"}


def test_wave7_2_minimum_steps_include_portable_rc_consumption_and_exact_commands() -> None:
    text = (ARTIFACT_ROOT / "RC_COMPANION_MINIMUM_STEPS.md").read_text(encoding="utf-8")
    lowered = text.lower()
    assert "requirements/m_cache_shared_external.txt" in lowered
    assert 'm_cache_shared_ext_local_repo="${m_cache_shared_ext_local_repo:-../m-cache-shared-ext}"' in lowered
    assert 'm_cache_shared_ext_local_copy="${m_cache_shared_ext_local_copy:-/tmp/m-cache-shared-ext-rc9}"' in lowered
    assert 'm_cache_shared_ext_repo_venv="${m_cache_shared_ext_repo_venv:-.venv}"' in lowered
    assert 'cp -r "$m_cache_shared_ext_local_repo" "$m_cache_shared_ext_local_copy"' in lowered
    assert '"$m_cache_shared_ext_repo_venv/bin/python" -m pip install --no-build-isolation -e "$m_cache_shared_ext_local_copy"' in lowered
    assert "pytest -q" in lowered
    assert "m_cache_shared_source=external" in lowered
    assert "signoff_state = pass | warn | block" in lowered


def test_wave7_2_docs_are_present_and_bundle_input_only_framed() -> None:
    assert Path("docs/WAVE7_2_COMPANION_MIGRATION_NOTE.md").exists()
    readme = (ARTIFACT_ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "central evidence bundle" in readme
    assert "do **not** define a separate earnings-local release process" in readme
