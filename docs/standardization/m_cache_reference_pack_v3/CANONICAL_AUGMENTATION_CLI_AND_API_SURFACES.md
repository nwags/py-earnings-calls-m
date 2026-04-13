# Canonical Augmentation CLI and API Surfaces

## Goal

Plan one shared augmentation vocabulary across repos without forcing immediate identical implementation.

## Canonical CLI family

Wave 3 should plan a shared command family:

```bash
m-cache <domain> aug ...
```

SEC already has `aug` canonically; other repos should plan toward the same family for text-bearing resources.

## Planned CLI surfaces

### Shared read-only / planning-friendly surfaces
- `m-cache <domain> aug list-types`
- `m-cache <domain> aug inspect-target ...`
- `m-cache <domain> aug inspect-runs ...`
- `m-cache <domain> aug inspect-artifacts ...`

### Shared execution-oriented planned surfaces
- `m-cache <domain> aug submit ...`
- `m-cache <domain> aug status ...`
- `m-cache <domain> aug events ...`

Wave 3 does not require all repos to fully implement execution yet.
It should plan the shared surface and identify what can be shared versus what remains repo-specific.

## Canonical API planning

Text-bearing detail/content endpoints should eventually be able to expose additive augmentation metadata such as:
- whether augmentation artifacts exist,
- which augmentation types exist,
- when augmentation was last run,
- whether augmentation is stale relative to text version,
- where artifacts can be inspected.

Possible additive metadata container:
- `augmentation_meta`

Possible future read-only endpoints:
- `GET /.../{id}/augmentations`
- `GET /.../{id}/augmentations/{type}`
- `GET /augmentation-runs/{run_id}`

Wave 3 should plan, not broadly implement, these cross-repo conventions.

## Compatibility rule

Do not force non-SEC repos to fully copy SEC augmentation execution in Wave 3.
Wave 3 should define shared naming, target selection, artifact/event contracts, and extraction boundaries first.

## Acceptance criteria for this slice

Plans should answer:
1. what shared `aug` surfaces can be adopted immediately,
2. what remains planned/reserved,
3. how augmentation metadata will appear additively in existing detail/content APIs,
4. how non-text numeric resources are excluded from augmentation surfaces.
