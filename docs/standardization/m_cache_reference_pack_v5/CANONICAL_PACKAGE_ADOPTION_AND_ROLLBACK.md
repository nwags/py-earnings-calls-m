# Canonical Package Adoption and Rollback

## Goal

Define how the first real shared package is introduced safely.

## Adoption strategy

### Phase A — boundary lock
- compare all four repo plans side by side
- lock exact first-package contents
- lock exact non-package local wrappers

### Phase B — create `m_cache_shared`
- create the package with only the agreed stable contents
- include models, enums, validators, packers, thin helpers
- do not include adapters or execution internals

### Phase C — repo-local adoption
- switch each repo to shared imports behind existing public/operator surfaces
- preserve pilot/non-pilot behavior
- preserve applicability boundaries
- preserve compatibility aliases

### Phase D — stability review
- evaluate remaining duplication
- decide whether a later extraction wave is justified

## Suggested adoption order

1. `py-earnings-calls-m`
2. `py-news-m`
3. `py-fed-m`
4. `py-sec-edgar-m`

## Versioning questions to answer

- where does `m_cache_shared` live first,
- how is it versioned,
- how do repos pin it,
- how do local development changes move together,
- how is rollback handled if drift appears.

## Rollback rule

Rollback to repo-local implementations must remain possible during early adoption.

## Acceptance criteria for this slice

Each repo plan should answer:
1. what import/adoption sequence it will follow,
2. what fallback path it will use if shared-package drift appears,
3. how rollout can stop safely after partial adoption.
