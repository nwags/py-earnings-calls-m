# Canonical External Package Strategy

## Goal

Define how `m_cache_shared` becomes a true shared package source of truth.

## Recommended posture

Start with a **single canonical external package source** that all repos eventually import, while still allowing each repo to keep a local facade/shim during rollout.

## Recommended package shape

m_cache_shared/
  augmentation/
    __init__.py
    enums.py
    models.py
    validators.py
    schema_loaders.py
    packers.py
    cli_helpers.py

## Recommended source-of-truth model

Wave 6 should plan for:
- one shared package codebase,
- explicit package versioning,
- repo-local shims/facades that import from that package,
- repo-local rollback ability if the external package version causes drift.

## Initial rollout posture

Do not require repos to import the external package directly everywhere on day one.
Prefer:
1. external package created,
2. local repo shim/facade imports switched to the external package,
3. internal direct-import cleanup only later.
