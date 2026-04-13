# Canonical Shared Package Boundary

## Goal

Define the minimum viable first version of the real shared package, tentatively named `m_cache_shared`.

## Core rule

Extract only what is already intentionally the same across repos through Waves 1–4.1.

Do not extract code because it merely looks similar. Extract code because:
- the contracts are already aligned,
- the tests already enforce them,
- the semantics are already intentionally shared,
- divergence is now accidental rather than domain-essential.

## What belongs in the first package

### Shared outer protocol models
Candidates:
- producer target descriptor
- producer run submission
- producer artifact submission
- run status view
- events view
- additive augmentation metadata view

### Shared enums / vocabularies
Candidates:
- augmentation types
- producer kinds
- run status values
- canonical command-family vocabulary
- reason-code categories that are already truly cross-repo stable

### Shared validators / schema loaders
Candidates:
- target descriptor validator
- run submission validator
- artifact submission validator
- status/event validators
- compatibility alias helpers for canonical field names

### Shared metadata packers / builders
Candidates:
- `status` view packer
- `events` view packer
- additive `augmentation_meta` packer
- producer/version/schema enrichment helpers
- stale/source-text-version packaging helpers

### Thin CLI/API helper plumbing
Candidates:
- common option parsing for canonical command surfaces
- canonical output-shape helpers
- alias-registration helpers
- read-only role-aware command wiring helpers

## What must remain repo-local

Keep out of the first package:
- canonical record identity logic
- provider adapters/fetch implementations
- storage/path derivation
- write orchestration and scheduling
- parser/extractor internals
- monitor/reconcile internals
- SEC authority/provenance internals
- Fed resolver/resource-family internals
- Earnings transcript/forecast internals
- News parsing/strategy specifics

## Minimum viable package principle

Wave 5 should extract the smallest package that materially reduces duplication.
It should not attempt a “complete” abstraction.

## Acceptance criteria for this slice

Each plan should identify:
1. exact files/classes/functions that can move now,
2. exact files/classes/functions that must remain local,
3. exact seams that must stay as repo-local wrappers.
