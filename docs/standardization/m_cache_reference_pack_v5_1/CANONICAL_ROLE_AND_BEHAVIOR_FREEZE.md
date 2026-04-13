# Canonical Role and Behavior Freeze

## Goal

Wave 5.1 must not change runtime behavior.

## Freeze rules

### Pilot repos
Pilot repos remain pilot repos:
- live write-path behavior stays live,
- storage/authority behavior stays local,
- CLI/API semantics stay unchanged.

### Non-pilot repos
Non-pilot repos remain non-pilot:
- validate-only/reserved write-path behavior stays unchanged,
- no live producer write-path enablement,
- no new authority artifacts,
- no role drift.

### SEC guardrail
No-dual-authority posture must remain intact.

### Fed guardrail
Strict applicability and reserved write-path behavior must remain intact.

### General rule
Wave 5.1 is allowed to change imports, package layout, exports, tests, and docs.
It is not allowed to change runtime role behavior or public/operator semantics.
