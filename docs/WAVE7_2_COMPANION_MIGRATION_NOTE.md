# Wave 7.2 Companion Migration Note (py-earnings-calls-m)

## Scope Freeze (Minimal Participation-Only)

Wave 7.2 companion work in `py-earnings-calls-m` is limited to first real shared RC participation.

This pass does **not**:

- change runtime behavior,
- change CLI/API semantics,
- broaden shared public API,
- perform cleanup/removal work.

## Companion Role (Unchanged)

This repo remains:

- pilot consumer-validator for transcript write-path safety,
- release blocker when transcript write-path safety drifts,
- central-bundle signoff/evidence contributor,
- not an owner of external package governance.

Applicability and local boundaries remain unchanged:

- transcript-only applicability remains unchanged,
- forecasts remain non-augmentation,
- `py_earnings_calls.augmentation_shared` remains first-hop facade,
- transcript identity/targeting/text retrieval/storage/API wire-shape/live write orchestration remain local.

## Exact RC Consumption Step (Portable Default)

Keep `requirements/m_cache_shared_external.txt` as canonical pin reference.

For first real local RC validation in the active virtualenv, default to sibling editable install using a portable repo-relative override:

```bash
export M_CACHE_SHARED_EXT_LOCAL_REPO="${M_CACHE_SHARED_EXT_LOCAL_REPO:-../m-cache-shared-ext}"
export M_CACHE_SHARED_EXT_LOCAL_COPY="${M_CACHE_SHARED_EXT_LOCAL_COPY:-/tmp/m-cache-shared-ext-rc2}"
export M_CACHE_SHARED_EXT_REPO_VENV="${M_CACHE_SHARED_EXT_REPO_VENV:-.venv}"
rm -rf "$M_CACHE_SHARED_EXT_LOCAL_COPY"
cp -R "$M_CACHE_SHARED_EXT_LOCAL_REPO" "$M_CACHE_SHARED_EXT_LOCAL_COPY"
"$M_CACHE_SHARED_EXT_REPO_VENV/bin/python" -m pip install --no-build-isolation -e "$M_CACHE_SHARED_EXT_LOCAL_COPY"
```

This keeps facade/shim contract unchanged while enabling local RC consumption for validation.

## Exact Validation Commands

```bash
# full non-regression baseline
pytest -q

# focused shared-surface + companion checks
pytest -q tests/test_augmentation_shared_facade.py tests/test_m_cache_shared.py tests/test_wave7_1_lifecycle_artifacts.py tests/test_wave7_2_companion_artifacts.py

# explicit facade external-mode check in active venv
M_CACHE_SHARED_SOURCE=external python -c "import py_earnings_calls.augmentation_shared as m; print(m.shared_surface_source())"
```

Expected external-mode check output: `external`.

## Exact Signoff Contract Fields (Central Bundle Input)

Machine-readable signoff input must align exactly to package-side `SIGNOFF.json` fields:

- `candidate_tag`
- `repo`
- `release_role`
- `pin_confirmed`
- `validation_status`
- `signoff_state`
- `blockers`
- `warnings`
- `rollback_ready`

Terminal decision vocabulary is canonical:

- `signoff_state = pass | warn | block`

No repo-local terminal alternatives (for example `ready`/`blocked` or `no-block`) are permitted in machine-readable outputs.

## Exact Signoff / Blocker Mapping

- `signoff_state = pass`:
  - required validations passed,
  - `blockers` is empty,
  - `rollback_ready = true`.
- `signoff_state = warn`:
  - validations completed with non-blocking concerns only,
  - `blockers` is empty,
  - warnings recorded in `warnings`,
  - `rollback_ready = true`.
- `signoff_state = block`:
  - any blocker present, or
  - validation failure preventing signoff, or
  - rollback readiness cannot be confirmed (`rollback_ready = false`).

## Exact Rollback-Readiness Evidence Fields

Rollback readiness for companion signoff is represented by:

- `pin_confirmed` (bool),
- `rollback_ready` (bool),
- `blockers` (list),
- `warnings` (list),
- `signoff_state` (must reflect blocker/rollback outcome using `pass|warn|block`).

Narrative detail may be kept in markdown, but machine-readable release ingestion relies on the fields above.

## Cleanup/Removal Deferral (Explicit)

Still deferred in this pass:

- public API broadening,
- shim/fallback removal,
- env alias removal,
- import-root collapse,
- local ownership reduction.
