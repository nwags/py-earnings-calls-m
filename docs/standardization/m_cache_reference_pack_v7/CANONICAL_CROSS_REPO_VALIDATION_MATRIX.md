# Canonical Cross-Repo Validation Matrix

## Goal

Define the required repo validation for every RC.

## Shared validation expectations

Each repo should validate:
- canonical external pin,
- strict v1 public API use,
- facade/shim behavior,
- `local` mode,
- `auto` mode,
- `external` mode where applicable,
- unchanged CLI behavior,
- unchanged API behavior,
- unchanged role/applicability/authority behavior.

## Repo-specific emphasis

### `py-earnings-calls-m`
- pilot transcript workflows remain correct,
- forecast non-augmentation behavior remains unchanged.

### `py-news-m`
- pilot article workflows remain correct,
- article-only applicability remains unchanged.

### `py-fed-m`
- non-pilot behavior remains reserved/non-executing,
- strict applicability remains unchanged.

### `py-sec-edgar-m`
- non-pilot validate-only behavior remains unchanged,
- no-dual-authority posture remains intact.

## Required evidence

Wave 7 should define:
- test commands,
- expected pass criteria,
- evidence artifacts or signoff notes,
- what constitutes a blocker.
