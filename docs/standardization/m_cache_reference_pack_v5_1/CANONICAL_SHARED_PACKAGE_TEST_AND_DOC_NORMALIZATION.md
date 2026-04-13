# Canonical Shared Package Test and Doc Normalization

## Goal

Normalize how the shared package is tested and documented across repos.

## Canonical test target

Prefer one obvious shared-package test module name across repos:
- `tests/test_m_cache_shared.py`

If a repo needs additional shared-package contract coverage, keep the base shared-package test naming obvious and parallel.

## Test coverage expectations

Shared-package tests should cover:
- model construction/serialization
- validator behavior
- schema-loader behavior
- packer behavior
- canonical export availability

Repo integration tests should continue to prove:
- CLI behavior unchanged
- API behavior unchanged
- role behavior unchanged
- applicability unchanged

## Documentation normalization

Each repo should document Wave 5.1 with the same sections:
1. what package layout became canonical,
2. what shared exports became canonical,
3. what remained local,
4. what role behavior stayed unchanged,
5. what compatibility shims remain.
