# py-earnings-calls-m User Testing Report (RC9 Remediation Pass)

Date: 2026-04-13 (America/New_York)
Mode: regular remediation pass (not planning, not architecture wave)

## Scope and intent

This pass applied minimum safe remediations to move from conditional-pass to signoff-ready:
- RC baseline alignment artifacts,
- operator entrypoint stability in active venv,
- resolve local-hit behavior correctness,
- optional lookup JSON ergonomics,
- re-run of the same user-testing slice.

## Baseline alignment (RC9)

### Baseline-alignment changes made
- Updated canonical shared pin to RC9:
  - `requirements/m_cache_shared_external.txt` -> `v0.1.0-rc9`
- Updated companion docs/templates to RC9 defaults:
  - `docs/WAVE7_2_COMPANION_MIGRATION_NOTE.md`
  - `docs/standardization/wave7_2_repo_companion/RC_COMPANION_MINIMUM_STEPS.md`
  - `docs/standardization/wave7_2_repo_companion/SIGNOFF.template.json`
- Updated assertions expecting RC2 -> RC9:
  - `tests/test_augmentation_shared_facade.py`
  - `tests/test_wave7_2_companion_artifacts.py`

### RC9 consumption step executed in active venv
- `rm -rf /tmp/m-cache-shared-ext-rc9`
- `cp -R /home/nick/Code/m-cache-shared-ext /tmp/m-cache-shared-ext-rc9`
- `source .venv/bin/activate && python -m pip install --no-build-isolation -e /tmp/m-cache-shared-ext-rc9`

## Console-script / entrypoint remediation

### Fix made
- Reinstalled this repo editable in active venv to ensure script entrypoints are materialized:
  - `source .venv/bin/activate && python -m pip install --no-build-isolation -e .`

### Verification commands
- `source .venv/bin/activate && py-earnings-calls --help`
- `source .venv/bin/activate && m-cache --help`

### Result
- `m-cache` now resolves as expected in active venv.

## Resolve-surface remediation

### Root cause observed
- `ProviderAwareResolutionService.resolve_*_if_missing` returned `LOCAL_ONLY_MISS` before checking local lookup artifacts.

### Fix made
- Added local-hit short-circuit in resolver for both domains:
  - transcript: check `local_lookup_transcripts` for call-id availability
  - forecast: check `local_lookup_forecasts` for provider+symbol+date presence
- Local hit now returns deterministic success metadata:
  - `found=true`, `served_from=local_hit`, `reason_code=LOCAL_HIT`, `method_used=local_lookup`

### Files changed for fix
- `py_earnings_calls/resolution_service.py`
- `tests/test_resolution_service.py` (new local-hit regression coverage)

### Verification commands
- `source .venv/bin/activate && py-earnings-calls resolve transcript --call-id 654f620f8cbe091c --resolution-mode local_only --verbose`
- `source .venv/bin/activate && py-earnings-calls resolve transcript --call-id 654f620f8cbe091c --resolution-mode resolve_if_missing --verbose`
- `source .venv/bin/activate && py-earnings-calls resolve forecast-snapshot --provider finnhub --symbol CTAS --date 2026-03-25 --resolution-mode local_only --verbose`
- `source .venv/bin/activate && py-earnings-calls resolve forecast-snapshot --provider finnhub --symbol CTAS --date 2026-03-25 --resolution-mode resolve_if_missing --verbose`
- `source .venv/bin/activate && python -m py_earnings_calls.m_cache_cli earnings resolve transcript --call-id 654f620f8cbe091c --resolution-mode local_only --summary-json`
- `source .venv/bin/activate && python -m py_earnings_calls.m_cache_cli earnings resolve forecast-snapshot --provider finnhub --symbol CTAS --date 2026-03-25 --resolution-mode local_only --summary-json`

### Result
- All above now report `LOCAL_HIT` with `found=true` for locally present records.

## Lookup JSON ergonomics remediation

### Fix made
- Added optional compact JSON mode for transcript lookup output:
  - `py-earnings-calls lookup query ... --json --compact-json`
  - omits `raw_html` and `transcript_text` fields (large payload fields)
- Kept existing `--json` semantics unchanged when `--compact-json` is not used.

### Files changed for fix
- `py_earnings_calls/cli.py`
- `tests/test_cli_output_modes.py`

### Verification command
- `source .venv/bin/activate && py-earnings-calls lookup query --scope transcripts --call-id 654f620f8cbe091c --json --compact-json`

## Shared-facade compatibility hardening

### Fix made
- Added compatibility fallback in shared packer wrappers to handle stricter external packer signatures and preserve operator surfaces.

### File changed
- `py_earnings_calls/augmentation_shared.py`

## Re-run of required user-testing slice

### CLI entrypoint sanity
- `source .venv/bin/activate && py-earnings-calls --help`
- `source .venv/bin/activate && m-cache --help`

### Transcript workflows
- `source .venv/bin/activate && py-earnings-calls lookup query --scope transcripts --symbol STC --json --compact-json`
- `source .venv/bin/activate && py-earnings-calls transcripts audit-datetime --provider motley_fool --limit 20 --summary-json`
- `source .venv/bin/activate && py-earnings-calls transcripts backfill --manifest data/motley_fool_resolution_probe.csv --progress-json --progress-heartbeat-seconds 1 --verbose`

### Forecast workflows
- `source .venv/bin/activate && py-earnings-calls lookup query --scope forecasts --symbol CTAS --json`
- `source .venv/bin/activate && py-earnings-calls forecasts refresh-daily --provider finnhub --date 2026-03-26 --symbol XOS --progress-json --progress-heartbeat-seconds 1 --verbose`

### Resolve paths
- transcript + forecast legacy resolve commands (`local_only` and `resolve_if_missing`) and canonical `m-cache` resolve commands (local_only) as listed above.

### Targeted smoke tests (same family as prior pass + remediation coverage)
- `source .venv/bin/activate && pytest -q tests/test_api_app.py tests/test_m_cache_cli.py tests/test_m_cache_shared.py`
- `source .venv/bin/activate && pytest -q tests/test_resolution_service.py tests/test_cli_output_modes.py tests/test_augmentation_shared_facade.py tests/test_wave7_2_companion_artifacts.py`

### Test results
- `66 passed in 1.49s` across targeted suite.

## API bind issue classification

### Check performed
- `source .venv/bin/activate && python -m py_earnings_calls.service_runtime api --host 127.0.0.1 --port 18011 --summary-json`

### Outcome
- Bind still fails in this sandbox with `could not bind on any address`.
- Classified as environment/test-harness limitation (`--unshare-net` sandbox), not product defect.

## Defect status after remediation

1. RC baseline traceability/pin mismatch
- Status: **Resolved at repo-local pin/documentation level** (RC9 artifacts now explicit).
- Ownership: shared-package + consumer companion artifacts.

2. Missing `m-cache` entrypoint in active venv
- Status: **Resolved in active venv** via editable reinstall.
- Ownership: environment install flow.

3. Resolve local-miss for locally present records
- Status: **Resolved**.
- Ownership: repo-local resolver behavior.

4. API bind failure in smoke
- Status: **Open advisory (environment-only)**.
- Ownership: test harness/environment.

5. Lookup JSON payload noise
- Status: **Improved** via optional `--compact-json`.
- Ownership: repo-local CLI ergonomics.

## Files changed in this remediation pass
- `requirements/m_cache_shared_external.txt`
- `docs/WAVE7_2_COMPANION_MIGRATION_NOTE.md`
- `docs/standardization/wave7_2_repo_companion/SIGNOFF.template.json`
- `docs/standardization/wave7_2_repo_companion/RC_COMPANION_MINIMUM_STEPS.md`
- `tests/test_augmentation_shared_facade.py`
- `tests/test_wave7_2_companion_artifacts.py`
- `py_earnings_calls/resolution_service.py`
- `py_earnings_calls/cli.py`
- `py_earnings_calls/augmentation_shared.py`
- `tests/test_resolution_service.py`
- `tests/test_cli_output_modes.py`
- `evidence/user-testing/rc9/py-earnings-calls-m_user-testing-report.md`

## Final status
**Status: PASS WITH LOW-SEVERITY ADVISORY.**
- Operator-surface correctness issues targeted in remediation are fixed.
- Repo-local RC9 alignment is explicit and test-validated.
- Remaining blocker from prior pass (resolve local miss) is cleared.
- Only remaining advisory is environment-only API bind limitation in this sandbox.
