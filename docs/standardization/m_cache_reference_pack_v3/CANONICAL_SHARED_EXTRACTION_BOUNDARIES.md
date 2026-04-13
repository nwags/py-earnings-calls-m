# Canonical Shared Extraction Boundaries

## Goal

Wave 3 should determine what code is stable enough to share without prematurely flattening domain logic.

## Principle

Extract only what became stable through Waves 1 and 2.

Do **not** extract code just because it sounds similar. Extract code because:
- contracts are already aligned,
- tests already enforce those contracts,
- the behavior is not domain-identity-specific,
- divergence is now mostly accidental rather than fundamental.

## Strong extraction candidates

### 1. Shared model classes / typed contracts
Likely candidates:
- provider detail / effective policy model
- provider usage event model
- rate-limit state model
- resolution result metadata model
- API resolution metadata model
- augmentation request/result/event/artifact metadata models

### 2. Shared schema validation helpers
Likely candidates:
- Wave 1 schema validators
- Wave 2 schema validators
- Wave 3 augmentation schema validators
- common "effective config" inspection helpers

### 3. Shared CLI helper plumbing
Likely candidates:
- `providers list`
- `providers show`
- resolution-mode parsing / validation
- augmentation-mode / augmentation-type parsing
- common machine-output rendering helpers

### 4. Shared API metadata helpers
Likely candidates:
- packaging additive `resolution_meta`
- packaging additive provider / rate-limit metadata
- packaging additive augmentation metadata on text-bearing detail/content endpoints

## Weak / not-yet extraction candidates

Keep these in repo-local code for now:
- domain-specific canonical identities
- storage layout internals
- provider-specific adapters
- resource-specific selectors
- monitor/reconcile operational loops
- SEC augmentation/provenance internals beyond the shared contract layer
- News content extraction specifics
- Fed document/release/series resolver specifics
- Earnings transcript versus forecast resolution specifics

## Extraction boundary rule

The shared layer should own:
- typed contracts
- enums / vocabularies
- schema validation
- helper builders/adapters for those shared contracts
- generic CLI/API packaging helpers

The domain repo should continue to own:
- canonical identity logic
- storage paths
- provider adapters
- resource-specific resolution strategy
- text extraction / parsing details
- domain-specific augmentation extensions

## Suggested package shape

A future extracted internal package could eventually contain modules like:

- `m_cache_shared.models.provider`
- `m_cache_shared.models.rate_limit`
- `m_cache_shared.models.resolution`
- `m_cache_shared.models.augmentation`
- `m_cache_shared.schemas`
- `m_cache_shared.cli_helpers`
- `m_cache_shared.api_helpers`

But Wave 3 should only plan this boundary and identify the concrete repo-local code that would move.

## Acceptance criteria for this slice

Plans should answer:
1. what can be shared now,
2. what must remain local,
3. what tests prove the boundary is safe,
4. what sequence would extract shared code without breaking standalone repos.
