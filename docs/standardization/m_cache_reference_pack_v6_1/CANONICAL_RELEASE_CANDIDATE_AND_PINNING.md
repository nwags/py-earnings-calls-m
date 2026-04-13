# Canonical Release-Candidate and Pinning Strategy

## Goal

Normalize early external package versioning and validation.

## Canonical pin file path

- `requirements/m_cache_shared_external.txt`

## Canonical pin format

For the convergence pass, use one pinned Git tag to the same external repository across all repos.

Example shape:
- `m-cache-shared-ext @ git+https://github.com/m-cache/m_cache_shared_ext.git@v0.1.0-rc1`

## Why Git-tag pinning

Wave 6.1 is still a stabilization-stage convergence pass.
Git-tag pinning keeps:
- rollout simple,
- source explicit,
- rollback easy,
- RC coordination visible.

## Canonical release-candidate rule

All four repos should validate the **same** release-candidate tag in this pass.

## Rollback rule

Rollback should mean:
1. repin the centralized file to a prior tag, or
2. switch the local facade/shim to `local` mode,
without changing public CLI/API behavior.
