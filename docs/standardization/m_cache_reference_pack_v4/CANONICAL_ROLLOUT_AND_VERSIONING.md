# Canonical Rollout and Versioning

## Goal

Introduce the shared internal package and producer protocol in a way that preserves standalone repo behavior.

## Versioning layers

Keep distinct versions for:
- shared outer-contract version
- shared package version
- producer payload schema version
- producer version

## Rollout phases

### Phase 1: boundary lock
- compare all four plans
- lock shared package contents
- lock producer protocol envelopes

### Phase 2: shared package skeleton
- create minimal internal package
- include models/validators/metadata packers/helpers only

### Phase 3: repo-local adoption
- adopt shared imports behind existing public surfaces
- keep repo behavior unchanged externally

### Phase 4: producer protocol pilot
- pilot one producer against one or two repos first
- verify payload round-trips and replay behavior

### Phase 5: broader rollout review
- compare drift
- decide whether additional extraction is warranted

## Compatibility rules

- repos remain separately runnable
- public surfaces remain stable
- local fallback remains possible if shared package changes must be rolled back
- repo-local tests remain first-class
