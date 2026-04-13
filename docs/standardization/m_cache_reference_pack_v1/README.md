# m-cache Reference Pack v1

This bundle is the **first canonical Codex reference pack** for parallel standardization across:

- `py-sec-edgar-m`
- `py-earnings-calls-m`
- `py-fed-m`
- `py-news-m`

It is intentionally focused on the first shared slice:

1. command model
2. configuration model
3. provider registry model
4. runtime summary / progress / resolution / reconciliation event models

## Intended use

Use this pack as the **normative source of truth** when implementing the next wave in each repo **in parallel**.

Do not treat this pack as an instruction to immediately merge the codebases.
The intended sequence is:

1. standardize contracts in each repo
2. add compatibility shims
3. compare behavior across repos
4. only then decide which pieces become a true shared package
5. only after that, decide whether to formally merge repos

## Deliverables in this pack

- `CANONICAL_COMMAND_MODEL.md`
- `CANONICAL_CONFIG_SCHEMA.md`
- `CANONICAL_PROVIDER_REGISTRY.md`
- `CANONICAL_RUNTIME_OUTPUT_AND_EVENTS.md`
- `MIGRATION_CHECKLIST.md`
- `CODEX_REFERENCE_PROMPT.md`
- `REFERENCE_MODELS.py`
- `schemas/*.json`
- `examples/*`

## Scope boundaries

This pack standardizes **shared orchestration and contracts**.
It does **not** flatten domain-specific identities or storage semantics.

That means:

- SEC accession semantics remain SEC-specific
- earnings transcript/forecast identities remain earnings-specific
- Fed document/release/series identities remain Fed-specific
- news article storage and provider/content fetch behavior remain news-specific

The shared layer standardizes how those domain-specific behaviors are configured, surfaced, reported, and orchestrated.
