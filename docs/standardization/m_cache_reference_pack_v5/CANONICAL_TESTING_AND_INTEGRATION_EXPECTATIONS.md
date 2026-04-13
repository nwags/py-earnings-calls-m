# Canonical Testing and Integration Expectations

## Goal

Wave 5 must prove that shared-package extraction reduces duplication without changing behavior.

## Shared package tests

The shared package should have:
- model serialization/validation tests
- schema compatibility tests
- packer/output-shape tests
- alias/canonical helper tests

## Repo integration tests

Each repo should prove:
- existing CLI tests remain green
- existing API tests remain green
- applicability boundaries remain unchanged
- pilot vs non-pilot behavior remains unchanged
- no payload-schema ownership drift
- no new authority side effects in SEC/Fed

## Cross-repo consistency checks

Wave 5 should explicitly compare:
- canonical command semantics
- status/event view semantics
- target/submission model semantics
- metadata field naming where intentionally shared

## Test philosophy

Use the shared package to remove duplicated implementation, not to silently redefine behavior.

## Acceptance criteria for this slice

Each repo plan should answer:
1. what shared-package tests are required,
2. what repo integration tests are required,
3. what cross-repo consistency checks must be run before closure.
