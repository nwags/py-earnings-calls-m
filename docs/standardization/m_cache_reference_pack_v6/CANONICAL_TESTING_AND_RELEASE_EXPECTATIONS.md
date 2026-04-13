# Canonical Testing and Release Expectations

## Goal

Define what must be tested before the external package can be adopted.

## Shared package tests

The external package should have:
- model construction/serialization tests
- validator tests
- schema-loader tests
- packer tests
- public export surface tests

## Repo integration tests

Each repo should prove:
- CLI behavior unchanged
- API behavior unchanged
- pilot/non-pilot behavior unchanged
- applicability unchanged
- authority behavior unchanged

## Cross-repo checks

Wave 6 should plan explicit checks that:
- all repos import the same public symbols from the external package,
- no repo depends on non-public external symbols,
- local wrappers still preserve repo-specific behavior.

## Release expectations

Wave 6 should define:
- how the package is built/released,
- how release candidates are validated against repos,
- how rollback is triggered if drift appears.
