# Canonical External Package Identity

## Goal

Choose one operational identity for the external shared package.

## Canonical choice for Wave 6.1

### Distribution name
- `m-cache-shared-ext`

### Import root
- `m_cache_shared_ext.augmentation`

### Repository/source of truth
- one external repository for `m-cache-shared-ext`

### Pin file path in repos
- `requirements/m_cache_shared_external.txt`

## Why not reuse `m_cache_shared` as the external import root yet

Because repos still retain in-repo `m_cache_shared` packages during early external adoption.
Using the same root would continue to force origin-path checks or import-precedence logic.
Wave 6.1 should remove that ambiguity by converging on a distinct external root for the stabilization period.

## Future note

A later wave may decide whether:
- the local in-repo package can be removed entirely,
- the external import root can collapse back to `m_cache_shared`.

That is explicitly out of scope for Wave 6.1.
