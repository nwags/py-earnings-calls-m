# RC2 Companion Signoff Input (`py-earnings-calls-m`)

This file is a central-bundle input only for:

`evidence/candidates/v0.1.0-rc2/consumer/py-earnings-calls-m/`

## Candidate

- `candidate_tag`: `v0.1.0-rc2`
- `repo`: `py-earnings-calls-m`
- `release_role`: `pilot consumer-validator for transcript write-path safety`

## Pin Confirmation

- canonical pin reference: `requirements/m_cache_shared_external.txt`
- confirmed pin line:
  - `m-cache-shared-ext @ git+https://github.com/m-cache/m_cache_shared_ext.git@v0.1.0-rc2`

## Validation Commands

1. `pytest -q`
2. `pytest -q tests/test_augmentation_shared_facade.py tests/test_m_cache_shared.py tests/test_wave7_1_lifecycle_artifacts.py tests/test_wave7_2_companion_artifacts.py`
3. `M_CACHE_SHARED_SOURCE=external python -c "import py_earnings_calls.augmentation_shared as m; print(m.shared_surface_source())"`

## Validation Results

- full suite: pass (`163 passed`)
- focused companion suite: pass (`25 passed`)
- external shared-surface check output: `external`

## Blocker/Warn Summary

- blockers: `[]`
- warnings: `[]`

## Rollback Readiness

- `rollback_ready`: `true`
- rollback path maintained:
  1. repin `requirements/m_cache_shared_external.txt` to prior known-good stable tag,
  2. force `M_CACHE_SHARED_SOURCE=local` if isolation is needed,
  3. keep public CLI/API semantics unchanged during recovery.

## Machine Signoff

See sibling machine-readable artifact:

- `SIGNOFF.json`
