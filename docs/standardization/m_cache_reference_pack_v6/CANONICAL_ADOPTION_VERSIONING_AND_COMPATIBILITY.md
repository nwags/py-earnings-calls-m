# Canonical Adoption, Versioning, and Compatibility

## Goal

Define how repos adopt the external shared package safely.

## Recommended adoption sequence

1. Create the external `m_cache_shared` source of truth.
2. Version it explicitly.
3. Keep repo-local shims/facades.
4. Point repo-local shims to external package imports.
5. Keep public CLI/API behavior unchanged.
6. Clean up internal direct imports only after stability is proven.

## Versioning expectations

Wave 6 should define:
- semantic versioning policy,
- minimum compatibility promises for the first public API,
- how repos pin package versions,
- how upgrades are coordinated across repos,
- what constitutes a breaking change.

## Compatibility policy

- Compatibility aliases remain local where possible.
- The external public API should stay small.
- Repos may keep local shim layers for at least one stabilization cycle.
- A repo should be able to pin and lag safely if another repo moves faster.

## Rollback rule

Rollback should mean:
- restoring repo-local shim imports or pinning back to an earlier package version,
- not redesigning repo logic or CLI/API surfaces.
