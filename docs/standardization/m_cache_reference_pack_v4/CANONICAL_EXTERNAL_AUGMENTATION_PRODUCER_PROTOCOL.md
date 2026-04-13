# Canonical External Augmentation Producer Protocol

## Goal

Define how an external augmentation service interacts with the repos in a shared, predictable, replay-safe way.

## Core producer protocol principle

- The **repo** owns canonical identity and source text access.
- The **producer** owns payload generation and payload schema.
- The **shared layer** owns run/event/artifact/API outer metadata contracts.

## Producer flow

1. Receive or discover a target descriptor.
2. Retrieve source text and `source_text_version`.
3. Produce annotation payload(s).
4. Submit augmentation run metadata.
5. Submit augmentation artifact metadata plus payload or payload locator.
6. Allow read-only inspection of augmentation state.

## Required protocol concepts

### 1. Target descriptor
A producer-facing target descriptor should include at least:
- `domain`
- `resource_family`
- `canonical_key`
- `text_source`
- `source_text_version`
- optional `language`
- optional `document_time_reference`
- optional `producer_hints`

### 2. Source text retrieval
The producer should be able to retrieve text through:
- existing detail/content APIs,
- canonical local text artifacts,
- or a bounded internal relay consistent with repo policy.

### 3. Run submission
A producer run submission should include:
- shared run metadata
- payload schema name/version
- producer name/version
- source text version
- success/status and reason fields

### 4. Artifact submission
A producer artifact submission should include:
- shared artifact metadata
- payload schema name/version
- payload body or payload locator
- producer name/version
- target identity and source text version

### 5. Inspection surfaces
The system should support inspecting:
- runs
- artifacts
- augmentation availability/staleness
- producer identity/version

## Protocol-level rules

### Idempotency
Safe de-duplication should be possible using some combination of:
- `producer_name`
- `producer_version`
- `augmentation_type`
- `canonical_key`
- `source_text_version`
- optional producer-local run key

### Replay safety
The system should support:
- re-reading the same source version,
- re-submitting compatible results,
- detecting stale augmentation when source text version changes.

### Multi-producer support
Do not assume one winning producer.
The protocol must allow:
- multiple producers on the same target,
- multiple payload schemas,
- multiple augmentation types,
- later promotion/selection policies without overwriting raw producer outputs.

## Payload ownership rule

The shared layer validates only the outer metadata.
The producer owns payload schema and payload validation.
