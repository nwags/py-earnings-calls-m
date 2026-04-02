
This project should remain local-first today, but preserve clear contracts for later scaling.

## Prefer

- deterministic path construction,
- append-friendly event recording,
- idempotent pipeline stages,
- rebuildable lookup artifacts,
- raw-versus-normalized separation.

## Avoid

- provider logic embedded in unrelated modules,
- hidden filesystem assumptions scattered across the codebase,
- mixing snapshot identity with archival/compression policy,
- irreversible storage choices in hot paths.

## Guiding statement

Build for today's single-node workflow, but preserve the contracts that make later distributed storage or background workers straightforward.
