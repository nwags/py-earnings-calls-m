# Canonical Shared Package Boundary

## Goal

Wave 4 should define the minimum viable shared internal package that can be introduced without destabilizing the standalone repos.

## Core rule

Extract only what is already stable across repos through Waves 1–3.

Do not extract code merely because it looks similar. Extract code because:
- the contracts are already aligned,
- the tests already enforce them,
- the behavior is no longer domain-identity-specific,
- divergence is now mostly accidental rather than essential.

## Strong extraction candidates

### 1. Shared typed models
Likely candidates:
- provider detail / effective policy
- rate-limit state
- resolution result metadata
- API resolution metadata
- augmentation run metadata
- augmentation artifact metadata
- API augmentation metadata
- shared enums/vocabularies for:
  - resolution modes
  - rate-limit outcomes
  - graceful-degradation codes
  - augmentation types
  - augmentation run status
  - producer kinds

### 2. Shared schema helpers
Likely candidates:
- JSON schema loaders
- JSON schema validators
- compatibility-check helpers
- example/fixture validation helpers

### 3. Shared metadata packers/builders
Likely candidates:
- provider metadata packers
- resolution metadata packers
- augmentation metadata packers
- API additive metadata packers
- selected runtime output helpers where behavior is already aligned

### 4. Shared CLI/API helper plumbing
Likely candidates:
- helper builders for already-standardized canonical surfaces:
  - `providers list`
  - `providers show`
  - `aug list-types`
  - `aug inspect-target`
  - `aug inspect-runs`
  - `aug inspect-artifacts`
- shared parsing/validation for:
  - resolution modes
  - augmentation types
  - shared applicability vocabulary

## Explicit non-candidates for Wave 4 extraction

Keep these repo-local:
- canonical identity derivation
- provider adapter implementations
- storage layout/path derivation
- execution engines/orchestration
- parser/extractor internals
- monitor/reconcile internals
- SEC-specific overlay/lifecycle/governance/provenance richness
- Fed resolver and resource-family specifics
- Earnings transcript vs forecast internals
- News article/content resolution ordering and parsing specifics

## Extraction phases

### Phase A: local mirror modules
Each repo refactors local modules to match the intended shared-package seams.

### Phase B: shared package introduction
Extract only stable models/validators/packers/helpers.

### Phase C: repo adoption
Swap repos to shared imports behind existing public surfaces.

### Phase D: post-adoption review
Compare remaining drift and decide whether more extraction is justified.
