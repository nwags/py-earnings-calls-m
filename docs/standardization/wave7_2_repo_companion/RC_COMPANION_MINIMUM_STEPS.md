# First Real Shared RC: Minimum Companion Steps (`py-earnings-calls-m`)

These steps are companion participation inputs to the central bundle only.

## 1. Confirm canonical pin reference

Use `requirements/m_cache_shared_external.txt` as the canonical RC pin reference for this repo.

## 2. Local RC consumption in active virtualenv (portable default)

```bash
export M_CACHE_SHARED_EXT_LOCAL_REPO="${M_CACHE_SHARED_EXT_LOCAL_REPO:-../m-cache-shared-ext}"
export M_CACHE_SHARED_EXT_LOCAL_COPY="${M_CACHE_SHARED_EXT_LOCAL_COPY:-/tmp/m-cache-shared-ext-rc9}"
export M_CACHE_SHARED_EXT_REPO_VENV="${M_CACHE_SHARED_EXT_REPO_VENV:-.venv}"
rm -rf "$M_CACHE_SHARED_EXT_LOCAL_COPY"
cp -R "$M_CACHE_SHARED_EXT_LOCAL_REPO" "$M_CACHE_SHARED_EXT_LOCAL_COPY"
"$M_CACHE_SHARED_EXT_REPO_VENV/bin/python" -m pip install --no-build-isolation -e "$M_CACHE_SHARED_EXT_LOCAL_COPY"
```

## 3. Run required validations

```bash
pytest -q
pytest -q tests/test_augmentation_shared_facade.py tests/test_m_cache_shared.py tests/test_wave7_1_lifecycle_artifacts.py tests/test_wave7_2_companion_artifacts.py
M_CACHE_SHARED_SOURCE=external python -c "import py_earnings_calls.augmentation_shared as m; print(m.shared_surface_source())"
```

## 4. Produce machine-readable signoff input (exact central fields)

`SIGNOFF.json` field set (exact):

- `candidate_tag`
- `repo`
- `release_role`
- `pin_confirmed`
- `validation_status`
- `signoff_state`
- `blockers`
- `warnings`
- `rollback_ready`

Terminal machine value:

- `signoff_state = pass | warn | block`

## 5. Exact signoff/blocker mapping

- `pass`: validations pass, blockers empty, rollback ready.
- `warn`: non-blocking warnings only, blockers empty, rollback ready.
- `block`: any blocker present, or validation failure preventing signoff, or rollback readiness not confirmed.

## 6. Exact rollback-readiness evidence fields

- `pin_confirmed`
- `rollback_ready`
- `blockers`
- `warnings`
- `signoff_state`
