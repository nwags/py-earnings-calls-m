# Canonical Role and Authority Preservation

## Goal

Externalization must not flatten role differences or authority boundaries.

## Pilot repos

Pilot repos remain pilot repos:
- live `submit-run` / `submit-artifact` behavior stays local,
- write-path persistence stays local,
- role logic is not externalized.

## Non-pilot repos

Non-pilot repos remain non-pilot:
- validate-only or reserved write-path behavior stays local,
- no live write-path enablement is introduced by the shared package.

## SEC guardrail

SEC no-dual-authority posture remains local and unchanged.
The external package must not create or imply a second authority path.

## Fed guardrail

Fed strict applicability and reserved behavior remain local and unchanged.

## General rule

The external package provides:
- representation,
- validation,
- canonical shared packing,
- thin generic parsing helpers.

The repos continue to own:
- runtime role behavior,
- applicability enforcement,
- authority routing,
- storage and execution internals.
