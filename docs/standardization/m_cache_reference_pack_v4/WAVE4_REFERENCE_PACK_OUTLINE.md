# Wave 4 Reference Pack Outline

## Purpose

Wave 4 begins the next controlled phase after Wave 3 closure.

Wave 1 standardized the shared command/config/runtime spine.
Wave 2 standardized provider/rate-limit/resolve/API transparency semantics.
Wave 3 standardized the textual augmentation plane at the **outer metadata contract** level while preserving repo-local execution internals.

**Wave 4** should now do two things in a bounded, compatibility-first way:

1. define and begin extracting a **shared internal package** for stable contracts/helpers,
2. define an explicit **external augmentation producer protocol** for text-bearing resources.

Wave 4 is still **not** a formal merge.

---

## Core Wave 4 Goal

Create a common internal layer for the parts that are now genuinely stable across repos, while keeping the repos:
- separately runnable,
- separately testable,
- separately deployable,
- free to keep domain-specific storage, identity, and adapter logic.

At the same time, define a producer protocol so that external augmentation services can:
- read canonical text-bearing resources,
- compute annotations,
- submit augmentation results back using shared outer metadata contracts,
- keep payload schemas producer-owned.

---

## Wave 4 Scope

### In scope
- shared internal package boundary and initial extraction plan
- shared typed models for stable Wave 1/2/3 contracts
- shared schema validation helpers
- shared metadata packers/builders
- shared CLI/API helper plumbing for already-standardized surfaces
- explicit producer protocol for augmentation submission/inspection
- stand-off annotation conventions for entity tagging and temporal expression tagging
- compatibility/versioning strategy for introducing shared internals gradually

### Out of scope
- formal repo merge
- extracting provider adapters
- extracting domain identity logic
- extracting storage/path derivation logic
- extracting augmentation execution engines
- redesigning monitor/reconcile systems
- forcing one universal augmentation payload schema
- broad historical artifact rewrites

---

## Foundational Wave 4 Principles

1. **Shared outer contracts, local domain internals**
   - Stable metadata contracts move into a shared layer.
   - Domain-specific identities, adapters, execution logic, and storage remain repo-local.

2. **Additive, compatibility-first rollout**
   - Legacy repo/operator surfaces remain intact.
   - Shared package adoption starts behind existing public surfaces.

3. **Producer-owned payload bodies**
   - The shared layer standardizes run/event/artifact/API metadata.
   - The external producer owns the schema of the annotation payload body.

4. **Stand-off annotations, not inline mutation**
   - Annotation services should target spans against source text versions rather than mutate source text in place.

5. **Multi-producer safe**
   - The system should support multiple augmentation producers without assuming a single winner by default.

6. **Idempotent and replay-safe**
   - Re-submission against the same `source_text_version` should be handled safely and transparently.

---

## Pillar A: Shared Internal Package Extraction

### Goal
Identify the minimum viable shared internal package that can be introduced without destabilizing the standalone repos.

### Proposed package role
A future internal package (name to be decided, e.g. `m_cache_shared`) should contain only stable shared layers.

### Likely shared modules

#### 1. Shared typed models
Candidates:
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

#### 2. Shared schema helpers
Candidates:
- JSON schema loaders/validators for Wave 1/2/3 contracts
- schema compatibility check helpers
- common example/fixture validators

#### 3. Shared metadata packers/builders
Candidates:
- runtime summary/progress packers where sufficiently aligned
- provider metadata packers
- resolution metadata packers
- augmentation metadata packers
- API additive metadata packers

#### 4. Shared CLI/API helper plumbing
Candidates:
- helper builders for already-standardized canonical surfaces:
  - `providers list`
  - `providers show`
  - `aug list-types`
  - `aug inspect-target`
  - `aug inspect-runs`
  - `aug inspect-artifacts`
- shared parsing/validation helpers for:
  - resolution modes
  - augmentation types
  - applicability checks at the shared vocabulary level

### Explicit non-candidates for extraction in Wave 4
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

### Extraction strategy

#### Phase A: local mirror modules
Each repo can first create or refactor local modules so their shape matches the intended shared package boundary.

#### Phase B: shared package introduction
Extract only the models/validators/metadata-packers/helpers that are already proven stable.

#### Phase C: repo adoption
Switch each repo to use the shared package behind existing public surfaces.

#### Phase D: convergence review
Compare divergence still remaining after adoption and decide whether more extraction is justified.

---

## Pillar B: External Augmentation Producer Protocol

### Goal
Define how an external augmentation service interacts with the repos in a shared, predictable way.

The producer protocol should allow an external service to:
1. discover or receive a target text-bearing resource,
2. retrieve the source text and version identity,
3. generate annotations,
4. submit augmentation results back using shared outer metadata,
5. keep payload bodies under producer control.

### Producer protocol principle

**The repo owns canonical identity and source text access.**
**The producer owns annotation payload generation.**
**The shared layer owns outer metadata contracts.**

---

## Text-bearing vs Numeric-only applicability rule

Wave 4 must preserve and operationalize the Wave 3 rule:

- text-bearing resources are augmentation-eligible,
- numeric-only resources are not.

### Applicability examples
- SEC filings/content -> eligible
- News article text/content -> eligible
- Fed documents/text-bearing releases -> eligible
- Fed numeric series -> not eligible
- Earnings transcripts -> eligible
- Earnings numeric forecasts -> not eligible unless a future persisted narrative text family exists

---

## Producer Protocol: Required Concepts

### 1. Target descriptor
The producer should receive or resolve a target descriptor containing at least:
- `domain`
- `resource_family`
- `canonical_key`
- `text_source`
- `source_text_version`
- optional `language`
- optional `document_time_reference`
- optional `producer_hints`

### 2. Source text retrieval
The producer should retrieve source text through:
- existing detail/content APIs, or
- canonical local text artifacts, or
- a bounded internal relay compatible with repo-local policy.

The source retrieval contract should include:
- exact text-bearing body
- source text version/hash
- identity fields
- any required content headers or metadata

### 3. Augmentation run submission
A producer submission should include:
- shared run metadata
- shared artifact metadata
- payload schema version
- payload body or payload locator per local policy

### 4. Inspection surfaces
The system should support read-only inspection of:
- augmentation runs
- augmentation artifacts
- augmentation availability/staleness
- producer identity/version

---

## Annotation Best Practices for Wave 4

### Stand-off annotation model
Use stand-off annotations rather than embedding markup into source text.

Each annotation should be tied to:
- canonical target identity
- `source_text_version`
- span selectors
- annotation type
- producer information

### Redundant span anchoring
Each annotation should ideally carry:
- start offset
- end offset
- selected text
- optional quote/checksum/snippet anchor
- optional token/span identifiers if the producer uses them

This improves replay safety and robustness to text changes.

### Multiple producer support
Do not assume one winning producer.
The metadata model should permit:
- multiple runs over the same target,
- multiple payload schemas,
- multiple augmentation types,
- later promotion/selection policies without overwriting raw producer outputs.

---

## Canonical Named Entity Tagging Function Expectations

Wave 4 should define a minimum producer-facing contract for entity tagging that supports:
- multi-token spans
- optional overlapping spans
- label type
- selected text
- confidence (optional)
- normalization/canonical-link fields (optional)
- domain-local extension fields

### Suggested minimum payload fields for an entity annotation
- `span_start`
- `span_end`
- `text`
- `label`
- `confidence` (optional)
- `normalized_text` (optional)
- `canonical_entity_id` (optional)
- `attributes` (optional open object)

### Shared layer responsibility
The shared package should not define a universal entity ontology beyond minimal shared augmentation-type classification.

### Repo-local responsibility
Each repo may extend entity types or normalization/linking behavior according to domain needs.

---

## Canonical Temporal Expression Tagging Function Expectations

Wave 4 should define a minimum producer-facing contract for temporal expression tagging that supports:
- span anchoring
- type classification
- normalized value
- optional anchoring to document/reference time
- optional ambiguity/confidence data

### Suggested minimum payload fields for a temporal annotation
- `span_start`
- `span_end`
- `text`
- `timex_type`
- `normalized_value`
- `document_time_reference` (optional)
- `anchor_time` (optional)
- `confidence` (optional)
- `attributes` (optional open object)

### Important principle
Temporal interpretation often depends on document time.
Wave 4 should therefore plan to pass document/reference time when available.

---

## Shared Outer Metadata vs Producer Payload

### Shared outer metadata should include
- `run_id`
- `domain`
- `resource_family`
- `canonical_key`
- `augmentation_type`
- `source_text_version`
- `producer_kind`
- `producer_name`
- `producer_version`
- `status`
- `success`
- `reason_code`
- `persisted_locally`
- `latency_ms`
- `rate_limited`
- `retry_count`
- `deferred_until`
- `payload_schema_name`
- `payload_schema_version`

### Producer-owned payload should include
- span-level annotations
- any domain-specific enrichment fields
- any normalization/linking extensions
- any confidence/disambiguation/internal rationale fields the producer wishes to expose

### Rule
The shared layer validates the outer metadata.
The producer owns payload schema and validation.

---

## Idempotency and Versioning Rules

Wave 4 should define:

### Idempotency
A producer submission should be safely identifiable by some combination of:
- `producer_name`
- `producer_version`
- `augmentation_type`
- `canonical_key`
- `source_text_version`
- optional producer-local run key

### Versioning
Keep separate versions for:
- shared outer contract version
- payload schema version
- producer version

### Replay handling
The system should support:
- re-reading the same source version,
- re-submitting compatible results,
- detecting stale augmentations when `source_text_version` changes.

---

## API Planning for Wave 4

Wave 4 should define a protocol-level expectation for additive augmentation API behavior.

### Existing detail/content endpoints should continue to expose
- `augmentation_meta`
- optional headers for content endpoints where already appropriate

### Future producer-facing surfaces to plan
Possible future bounded surfaces:
- read source text for augmentation
- submit augmentation run/artifact metadata
- inspect submitted augmentation state

But Wave 4 planning should not assume identical implementation across repos in the first pass.

---

## Shared Package Rollout Strategy

Wave 4 should remain phased.

### Phase 1: planning and boundary lock
- inventory actual extraction candidates in all repos
- map equivalent local modules
- lock what is safe to move

### Phase 2: shared models/validators package
- extract models
- extract validators
- extract metadata builders/packers
- keep helpers minimal

### Phase 3: adoption without public surface changes
- adopt shared internals behind existing public interfaces
- maintain standalone behavior in each repo

### Phase 4: producer protocol pilot
- pilot one external augmentation producer against one or two repos first
- confirm payload/metadata fit and idempotency behavior

### Phase 5: broader adoption review
- compare remaining repo-local divergences
- decide whether more extraction is warranted

---

## Testing Expectations for Wave 4 Planning

Plans should explicitly cover:
- shared package compatibility tests
- repo-local backward-compatibility tests
- schema compatibility tests for outer metadata
- producer payload round-trip tests
- stale/version mismatch tests
- idempotent resubmission tests
- multi-producer coexistence tests

---

## Deliverables the Wave 4 Plans Should Produce

Each repo plan should answer:
1. which modules can move to the shared package now,
2. which modules must remain local,
3. how the repo will expose or consume the external producer protocol,
4. which resource families are augmentation-eligible,
5. how payload-schema ownership remains external/service-owned,
6. what the phased implementation order should be,
7. what risks/rollback strategy apply.

---

## Pause Point Before Wave 4 Implementation

Do not approve Wave 4 implementation until:
- all four repo plans are compared side by side,
- the shared package contents are explicitly bounded,
- the producer protocol is clear enough to test across repos,
- text-bearing applicability remains consistent,
- no plan tries to extract domain-local adapters/identities/execution engines prematurely.
