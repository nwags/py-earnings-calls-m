# Canonical Cross-Repo Release Execution Matrix

## Goal

Define how the four repos participate in a shared package release cycle.

## Repo roles

### `py-earnings-calls-m`
- pilot consumer-validator,
- validates transcript write-path safety,
- blocks on transcript/pilot drift.

### `py-news-m`
- pilot consumer-validator,
- validates article/pilot behavior,
- blocks on article workflow drift.

### `py-fed-m`
- validator + signoff,
- validates non-pilot reserved behavior and strict applicability,
- blocks on fed-specific safety drift.

### `py-sec-edgar-m`
- conservative validator/signoff,
- validates non-pilot and no-dual-authority safety,
- blocks on SEC authority/safety drift.

## Matrix expectations

Wave 7.1 should define:
- required commands/tests,
- evidence artifact names/locations,
- required signoff outputs,
- which failures are blockers versus warnings.
