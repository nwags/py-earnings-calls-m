# Canonical Shared Models and Helpers

## Goal

Wave 4 should lock the stable shared model/helper layer before any broad extraction.

## Shared model families

### Provider / rate-limit / resolution
These are already strong candidates from Waves 1–2:
- provider detail / effective policy
- provider usage metadata
- rate-limit state
- resolution result metadata
- API resolution metadata

### Augmentation outer metadata
These are the new strong candidates from Wave 3:
- augmentation run metadata
- augmentation event metadata
- augmentation artifact metadata
- API augmentation metadata
- augmentation type enums
- augmentation status enums
- producer kinds

## Helper families

### Schema validation helpers
The shared package should provide helpers for validating:
- Wave 1 config/runtime/event shapes
- Wave 2 provider/resolution shapes
- Wave 3/4 augmentation metadata shapes

### Metadata packers
The shared package should provide packers/builders for:
- additive `resolution_meta`
- additive `augmentation_meta`
- provider/rate-limit metadata
- common machine-output envelopes where already aligned

### CLI helper plumbing
The shared package should provide helpers for:
- formatting shared read surfaces
- validating shared options
- packaging shared machine-output data
- keeping repo-local commands thin around shared helpers

## Important boundary rule

The shared package should own:
- types
- schemas/validators
- metadata packers
- shared helper plumbing

The repo should own:
- source record identity
- storage location derivation
- provider adapter internals
- execution strategy
- domain-local augmentation payload interpretation
