# Canonical Shim and Fallback Contract

## Goal

Normalize how repos choose external vs local shared code.

## Canonical source-mode contract

### Environment variable
- `M_CACHE_SHARED_SOURCE`

### Allowed values
- `auto`
- `external`
- `local`

## Canonical semantics

### `auto`
- try external package first,
- verify strict-common required symbol set,
- fall back to local in-repo shared implementation if external is unavailable or incomplete.

### `external`
- require external package,
- fail loudly if the external package is unavailable, incomplete, or the configured root is wrong.

### `local`
- bypass external package and use local in-repo shared implementation.

## Canonical external-root override

### Environment variable
- `M_CACHE_SHARED_EXTERNAL_ROOT`

### Default value
- `m_cache_shared_ext.augmentation`

## Loading rules

1. No repo should rely on repo-root import precedence or `sys.path` tricks.
2. No repo should mix local and external symbol sources in one process.
3. Each repo should use one first-hop facade/shim module only.
4. Facade/shim modules may keep repo-local helpers/wrappers for non-v1 symbols and local behavior.

## Repo-local wrapper rule

Repos may keep their own facade names, but they should obey the same source-mode and fallback semantics.
