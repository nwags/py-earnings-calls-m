# Wave 5.1 Reference Pack Outline

## Purpose

Wave 5.1 is a **shared-package normalization pass**.

Wave 5 proved the extraction boundary: all four repos now have an in-repo `m_cache_shared` package and preserved role behavior.
But the package itself is still structurally inconsistent across repos.

Wave 5.1 should normalize:
- package layout,
- public exports,
- shared symbol naming,
- test naming,
- doc structure,

while preserving:
- pilot vs non-pilot behavior,
- local authority boundaries,
- public CLI/API semantics,
- local wrappers where still needed.

## Problem Wave 5.1 solves

Current state after Wave 5:
- some repos use a flat `m_cache_shared/` layout,
- some use nested `m_cache_shared/augmentation/`,
- shared symbols have different names across repos,
- shared-package tests are named differently,
- docs describe the same extracted layer with different structure.

That is good enough to prove the extraction boundary, but not good enough to externalize the package.

## Recommended canonical target

Normalize to:

m_cache_shared/
  __init__.py
  augmentation/
    __init__.py
    enums.py
    models.py
    validators.py
    schema_loaders.py
    packers.py
    cli_helpers.py

Compatibility alias modules may remain temporarily, but this should become the clearly documented canonical layout.

## Acceptance criteria

Wave 5.1 is complete when:
1. all four repos use the same package layout,
2. all four repos expose the same canonical `m_cache_shared.augmentation` public API,
3. all four repos use aligned shared-package test naming,
4. all four repos keep pilot vs non-pilot behavior unchanged,
5. no additional extraction scope is introduced,
6. full suites remain green.
