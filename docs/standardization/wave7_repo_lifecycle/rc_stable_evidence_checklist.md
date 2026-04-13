# RC/Stable Evidence Checklist (`py-earnings-calls-m`)

Use this checklist for each `m-cache-shared-ext` RC/stable candidate validated in this repo.

## Required Evidence

- Candidate tag and pin recorded in `requirements/m_cache_shared_external.txt`.
- Facade path remains `py_earnings_calls.augmentation_shared`.
- Source-mode contract checked for `local`, `auto`, and `external`.
- Local fallback remains available.
- Strict-common v1 usage remains intact.
- Full repo suite passes (`pytest -q`).
- Transcript pilot write-path behavior remains unchanged.
- Transcript-only applicability remains unchanged; forecasts remain non-augmentation.
- CLI/API semantics remain unchanged.

## Blocker Conditions

- Transcript write-path regression.
- Role/applicability/authority drift.
- Source-mode/facade regression.
- Non-public external symbol dependency.
- CLI/API semantic drift.

## Rollback Steps

1. Repin `requirements/m_cache_shared_external.txt` to prior known-good tag.
2. Switch to `M_CACHE_SHARED_SOURCE=local` if immediate isolation is needed.
3. Keep public CLI/API behavior unchanged during recovery.
4. Record incident note and follow-up remediation task.

## Cleanup Deferral Criteria

Cleanup remains deferred in Wave 7. Re-evaluate only when:

- multiple stable cycles succeed,
- validation remains consistently green,
- compatibility-impacting user testing passes consistently,
- rollback confidence remains high.
