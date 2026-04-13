# Canonical Annotation Targeting and Payload Guidance

## Goal

Provide shared best practices for producer-generated annotations, especially:
- named entity tagging
- temporal expression tagging

## Stand-off annotation rule

Use stand-off annotations rather than mutating source text inline.

Every annotation should be anchored to:
- canonical target identity
- `source_text_version`
- span selectors
- annotation type
- producer information

## Redundant span anchoring

Each annotation should ideally carry:
- `span_start`
- `span_end`
- selected `text`
- optional quote/checksum/snippet anchor
- optional token/span identifiers if the producer uses them

This improves robustness when text or tokenization changes.

## Named entity tagging expectations

Suggested minimum entity payload fields:
- `span_start`
- `span_end`
- `text`
- `label`
- `confidence` (optional)
- `normalized_text` (optional)
- `canonical_entity_id` (optional)
- `attributes` (optional open object)

## Temporal expression tagging expectations

Suggested minimum temporal payload fields:
- `span_start`
- `span_end`
- `text`
- `timex_type`
- `normalized_value`
- `document_time_reference` (optional)
- `anchor_time` (optional)
- `confidence` (optional)
- `attributes` (optional open object)

## Shared outer metadata vs payload body

The shared layer standardizes:
- run/event/artifact/API metadata

The producer owns:
- span-level payload schema
- local ontology choices
- domain-specific extensions
- normalization/linking extras
