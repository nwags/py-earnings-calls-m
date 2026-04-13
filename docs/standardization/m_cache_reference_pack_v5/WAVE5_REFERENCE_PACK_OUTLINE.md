# Wave 5 Reference Pack Outline

## Purpose

Wave 5 is the first **real shared-package extraction wave**.

Waves 1–4.1 standardized:
- command/config/runtime structure,
- provider/rate-limit/resolve semantics,
- textual augmentation outer metadata contracts,
- producer-protocol concepts,
- canonical augmentation command family,
- cross-repo naming and workflow normalization.

Wave 5 should now extract the **first actual shared internal package** for the parts that are intentionally the same across repos.

This is still **not** a formal merge.

---

## Core Wave 5 Goal

Create a real shared package for the stable **outer protocol layer**, while keeping all domain-specific logic inside each repo.

The Wave 5 shared package should initially cover only:
- models
- enums / vocabularies
- schema validators / schema loaders
- metadata packers / builders
- thin CLI/API helper plumbing

It should **not** cover:
- provider adapters
- domain identities
- storage/path derivation
- execution/orchestration engines
- parser/extractor internals
- SEC authority/provenance internals
- Fed resolver/resource-family internals
- Earnings transcript/forecast internals
- News parsing/strategy specifics
