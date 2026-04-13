# Migration Checklist (Wave 1)

This checklist is for the first parallel implementation wave.

## Repository-level success criteria

Each repo should complete all items in this order.

### 1. Command model

- [ ] Add `m-cache <domain>` dispatch
- [ ] Preserve current base CLI as compatibility alias
- [ ] Reserve canonical shared family names
- [ ] Add `aug` alias in SEC while preserving `augmentations`
- [ ] Standardize global runtime flags where applicable

### 2. Config model

- [ ] Add canonical loader for `m-cache.toml`
- [ ] Map existing env vars into canonical config fields
- [ ] Emit one effective resolved config object in runtime startup summaries
- [ ] Validate domain/provider blocks consistently

### 3. Provider registry

- [ ] Materialize or reserve `refdata/normalized/provider_registry.parquet`
- [ ] Represent each effective upstream source as a provider row
- [ ] Add local override support from `refdata/inputs/provider_registry_overrides.*`
- [ ] Make direct-resolution and rate-limit policy inspectable from registry state

### 4. Runtime outputs/events

- [ ] Make `--summary-json` clean JSON on stdout
- [ ] Make `--progress-json` NDJSON on stderr
- [ ] Standardize top-level summary fields
- [ ] Standardize progress event fields
- [ ] Standardize resolution event fields
- [ ] Standardize reconcile discrepancy/event fields

### 5. Tests

- [ ] Add config-load tests
- [ ] Add registry-materialization tests
- [ ] Add summary-json shape tests
- [ ] Add progress-json stream tests
- [ ] Add resolution-event schema tests
- [ ] Add reconcile schema tests
- [ ] Add compatibility CLI alias tests

## Pause point before Wave 2

Pause after Wave 1 and compare all four repos side by side.

Do not begin deeper shared-code extraction until:

- all repos pass the checklist
- command names are aligned
- summary/event shapes are aligned
- provider registry materialization exists or is reserved in each repo
- no repo is silently downgrading shared resolution modes
