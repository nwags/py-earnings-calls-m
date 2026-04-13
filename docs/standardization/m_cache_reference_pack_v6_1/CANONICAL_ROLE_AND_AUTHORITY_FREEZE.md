# Canonical Role and Authority Freeze

## Goal

Wave 6.1 must not change runtime behavior.

## Pilot repos
Pilot repos remain pilot repos:
- live write-path behavior remains local,
- persistence remains local,
- article/transcript role behavior remains unchanged.

## Non-pilot repos
Non-pilot repos remain non-pilot:
- validate-only or reserved behavior remains local,
- no live write-path enablement is introduced.

## SEC guardrail
- no-dual-authority posture remains unchanged,
- SEC sidecar/submission/provenance artifacts remain the sole operational authority.

## Fed guardrail
- strict applicability remains unchanged,
- documents/releases still require explicit persisted text-bearing representation,
- series remains `not_applicable`.

## General rule

Wave 6.1 may normalize:
- external package name,
- import root,
- pin file,
- source-mode contract,
- RC validation process.

It may not change:
- runtime role behavior,
- CLI/API semantics,
- applicability,
- authority routing,
- storage/execution logic.
