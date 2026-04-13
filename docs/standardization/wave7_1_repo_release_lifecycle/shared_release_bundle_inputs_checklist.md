# Shared Release Bundle Inputs Checklist (`py-earnings-calls-m`)

Use this checklist to prepare repo inputs for the central package-side release evidence bundle.

## Required Repo Inputs

- candidate tag/pin reference from `requirements/m_cache_shared_external.txt`,
- full repo validation result (`pytest -q`),
- facade path confirmation (`py_earnings_calls.augmentation_shared`),
- source-mode contract evidence (`local`, `auto`, `external`) including local fallback,
- transcript-only applicability confirmation (forecasts remain non-augmentation),
- pilot transcript write-path safety confirmation,
- unchanged compatibility/canonical surface semantics confirmation.

## Required Signoff Output

- explicit validator signoff: safe to include in shared promotion decision,
- explicit block/no-block statement tied to blocker taxonomy.

## Blocker Taxonomy (Repo)

- transcript write-path safety regression,
- role/applicability/authority drift,
- facade/source-mode contract regression,
- strict-common v1 boundary drift or non-public symbol dependency,
- CLI/API semantic drift,
- unresolved blocking lifecycle incident.

## Rollback Path (Repo)

1. repin to prior stable tag in `requirements/m_cache_shared_external.txt`,
2. force `M_CACHE_SHARED_SOURCE=local` if immediate isolation is needed,
3. preserve public CLI/API behavior during recovery,
4. attach incident/recovery note to central release bundle.

## User-Testing Start-Gate Prerequisite Echo

This repo marks comprehensive user testing as startable only after:

- Wave 7.1 implementation completion,
- one shared RC fully validated across all repos,
- evidence/signoff flow operational end-to-end,
- rollback verified,
- no open blocking lifecycle incident.
