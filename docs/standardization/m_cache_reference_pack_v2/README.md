# m-cache Reference Pack v2

This bundle is the **second canonical Codex reference pack** for parallel standardization across:

- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

Wave 2 standardizes the **provider / rate-limit / resolution / API semantics plane** while preserving:

- each repo's current base CLI,
- Wave 1 compatibility guarantees,
- domain-specific storage layouts,
- domain-specific identity models,
- additive-only migration rules.

## Required references in this pack

- `CANONICAL_PROVIDER_OPERATIONS.md`
- `CANONICAL_RATE_LIMIT_AND_DEGRADATION.md`
- `CANONICAL_RESOLUTION_SEMANTICS.md`
- `CANONICAL_API_RESOLUTION_CONVENTIONS.md`
- `CANONICAL_PROVIDER_USAGE_EVENTS.md`
- `MIGRATION_CHECKLIST.md`
- `REFERENCE_MODELS.py`
- `schemas/*.json`
- `examples/*`

## How to use this pack

Use this pack for **Wave 2 planning first**, then implementation after all four repo plans are reviewed side by side.

Recommended sequence:

1. add this pack to each repo under `docs/standardization/m_cache_reference_pack_v2/`
2. run plan mode in all four repos
3. compare and tighten the four plans against this pack
4. approve implementation in all four repos in parallel
5. pause again before shared-package extraction

## Core Wave 2 questions every repo should answer consistently

1. Which providers exist and what are their effective policies?
2. Why was one provider selected over another?
3. Was remote work attempted?
4. Was the result served locally, fetched remotely, or deferred?
5. Was the operation affected by quota or policy?
6. Can an operator inspect all of the above without live network access?

## Explicitly out of scope for Wave 2

Do not do these in Wave 2:

- extract a shared Python package,
- formally merge repos,
- flatten domain identities,
- broadly rewrite historical artifacts,
- redesign `py-news-m` monitor/reconcile into a new operational system,
- redesign SEC augmentation architecture,
- standardize every storage subdirectory layout.
