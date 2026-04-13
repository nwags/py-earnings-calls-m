# Canonical Augmentation Artifacts and Events

## Goal

Define the shared artifact/event layer for textual augmentation.

## Canonical artifacts to plan

### Augmentation runs
Suggested canonical artifact:
- `refdata/normalized/augmentation_runs.parquet`

### Augmentation events
Suggested canonical artifact:
- `refdata/normalized/augmentation_events.parquet`

### Augmentation artifacts
Suggested canonical artifact family:
- domain/resource-specific structured artifacts plus shared metadata rows

Examples:
- SEC may keep filing-linked augmentation sidecars
- News may keep article-linked augmentation artifacts
- Earnings may keep transcript-linked augmentation artifacts
- Fed may keep document/release-linked augmentation artifacts

The shared layer should standardize metadata, not one universal payload body.

## Shared fields for augmentation run/event metadata

Recommended common fields:
- `run_id`
- `event_at`
- `domain`
- `resource_family`
- `canonical_key`
- `augmentation_type`
- `source_text_version`
- `producer_kind`
- `producer_name`
- `status`
- `success`
- `reason_code`
- `message`
- `persisted_locally`
- `latency_ms`
- `rate_limited`
- `retry_count`
- `deferred_until`

## Shared producer kinds

Initial shared values:
- `llm`
- `rules`
- `hybrid`
- `manual`

## Shared status values

Initial shared values:
- `queued`
- `running`
- `completed`
- `failed`
- `deferred`
- `skipped`

## Compatibility with existing provenance

SEC and other repos may keep richer companion provenance artifacts.
Wave 3 should preserve those while standardizing the shared outer event/run metadata.

## Acceptance criteria for this slice

Plans should answer:
1. what shared augmentation metadata artifacts should exist,
2. what stays repo-specific in the payload body,
3. how companion provenance artifacts coexist with the shared canonical outer event contract,
4. what minimal event vocabulary all repos can share.
