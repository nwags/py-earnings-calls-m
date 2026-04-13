# Wave 1 Migration Note (py-earnings-calls-m)

This note documents the Wave 1 parallel standardization outcome for
`py-earnings-calls-m` only.

Scope reminder:

- this repo remains standalone,
- Wave 1 changes are additive and compatibility-first,
- transcript and forecast identities remain domain-specific and separate.

## What Is Canonical In Wave 1

- Canonical shared command surface is available as `m-cache earnings ...`.
- Canonical shared config file is `m-cache.toml`.
- Canonical provider registry authority is materialized at:
  - `refdata/normalized/provider_registry.parquet`
- Canonical shared runtime/event contracts are implemented for Wave 1:
  - summary/progress shape support,
  - resolution/reconciliation artifact field alignment.

## What Remains Aliased

- `py-earnings-calls ...` remains a supported operator compatibility surface.
- Existing earnings command families and semantics remain in place.
- Canonical `m-cache earnings ...` commands are additive wrappers over existing
  pipeline/service behavior.

## Legacy Behavior Preservation

- Legacy `py-earnings-calls ...` machine-output defaults are preserved.
- No Wave 1 migration step requires operators to switch existing scripts
  immediately.
- No transcript/forecast storage identity model was flattened or rewritten.

## m-cache earnings Behavior

- `m-cache earnings ...` is the canonical shared surface for Wave 1.
- It routes to existing repo functionality with additive command-model
  alignment.
- It does not replace or remove `py-earnings-calls ...`.

## Dual-Shape Output Rule

- Legacy CLI (`py-earnings-calls ...`) keeps legacy machine-output defaults.
- Canonical CLI (`m-cache earnings ...`) uses canonical Wave 1 output defaults.
- Explicit schema selection is additive:
  - `--output-schema legacy|canonical`
- This supports migration without silently breaking legacy operator scripts.

## Canonical Config Loading (Wave 1)

Canonical config resolution order:

1. explicit CLI `--config PATH`,
2. `M_CACHE_CONFIG`,
3. `./m-cache.toml`,
4. compatibility env/default mapping.

Wave 1 behavior is additive:

- canonical config is supported,
- existing env-based runtime behavior remains usable.

## Provider Registry Materialization

- `provider_registry.parquet` is materialized deterministically from
  code-defined provider specs.
- Local overrides are supported from:
  - `refdata/inputs/provider_registry_overrides.parquet`
  - `refdata/inputs/provider_registry_overrides.csv`
- Active remote providers and local/bulk adapters are represented additively.

## Resolution/Reconciliation Artifact Alignment

Wave 1 alignment is additive:

- canonical required fields are present on newly written rows,
- historical rows are not broadly rewritten/migrated in this wave.

Canonical artifacts:

- `refdata/normalized/resolution_events.parquet`
- `refdata/normalized/reconciliation_discrepancies.parquet`
- `refdata/normalized/reconciliation_events.parquet`

## Reserved For Later Waves

- cross-repo shared package extraction,
- deep command/pipeline refactors beyond Wave 1 wrappers,
- broad historical event backfill/migration,
- any transcript/forecast identity flattening.
