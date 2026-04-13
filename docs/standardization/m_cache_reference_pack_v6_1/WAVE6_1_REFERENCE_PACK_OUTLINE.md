# Wave 6.1 Reference Pack Outline

## Purpose

Wave 6.1 is the **external package convergence and release-candidate unification pass**.

Wave 6 proved that all four repos can adopt an external shared package safely through local facades/shims.
But the repos still diverge on:
- external distribution naming,
- external import root,
- Git-tag pin targets,
- shim/fallback controls,
- release-candidate validation mechanics.

Wave 6.1 should normalize those operational details.

## Problem Wave 6.1 solves

After Wave 6:
- the repos agree on the shared API boundary,
- but they do not yet agree on one external package identity and one adoption contract.

That means the “single source of truth” is conceptually present but not yet operationally uniform.

## Recommended canonical target

For the convergence pass, normalize to:
- distribution/package name: `m-cache-shared-ext`
- import root: `m_cache_shared_ext.augmentation`
- centralized pin file path: `requirements/m_cache_shared_external.txt`
- shared source-mode env var: `M_CACHE_SHARED_SOURCE={auto|external|local}`
- shared external-root env var: `M_CACHE_SHARED_EXTERNAL_ROOT=m_cache_shared_ext.augmentation`
- one shared release-candidate tag validated across all repos

## Why use a distinct external import root

Because local in-repo `m_cache_shared` packages still exist during early adoption.
A distinct external root avoids:
- repo-root precedence ambiguity,
- accidental shadowing,
- import-order hacks,
- mixed-source loading.

## Acceptance criteria

Wave 6.1 is complete when:
1. all repos target the same external distribution and import root,
2. all repos use the same pin-file convention,
3. all repos use the same source-mode contract,
4. one shared external release-candidate tag is validated across all four repos,
5. role/applicability/authority behavior remains unchanged.
