# Canonical Shared Package Layout

## Goal

Define the package layout all repos should converge to in Wave 5.1.

## Canonical layout

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

## Layout rules

### `m_cache_shared/__init__.py`
May be thin and may re-export a very small stable surface if desired, but should not become a dumping ground.

### `m_cache_shared/augmentation/__init__.py`
Should define the canonical public import surface for the augmentation shared layer.

### `enums.py`
Holds shared vocab/enums/value lists.

### `models.py`
Holds shared outer protocol models and shared view models.

### `validators.py`
Holds validator logic for shared envelopes/models.

### `schema_loaders.py`
Holds shared schema loader helpers.

### `packers.py`
Holds pure status/events/meta packers/builders.

### `cli_helpers.py`
Holds thin CLI helper utilities only.

## Rules for migration from flat layouts

Repos currently using a flat layout should:
- move shared code into the nested layout,
- preserve temporary compatibility imports if needed,
- keep runtime behavior unchanged,
- remove the flat layout only after tests/docs are aligned.
