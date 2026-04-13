# Wave 7 Reference-Pack Outline

## Purpose

Wave 7 is the **external shared package source-of-truth realization and release-management hardening wave**.

Waves 5 and 5.1 established and normalized the in-repo shared package shape.
Wave 6 established first-pass external adoption scaffolding.
Wave 6.1 converged the external package identity, import root, pinning contract, and shim/fallback behavior across repos.

Wave 7 should now define how the external package itself is:
- maintained,
- validated,
- promoted,
- versioned,
- rolled back,
- and eventually allowed to replace more temporary local compatibility layers.

This wave shifts the center of gravity from **repo-side convergence** to **shared-package lifecycle management**.

## Core Wave 7 Goal

Make `m-cache-shared-ext` a real managed source of truth with:
- explicit governance,
- explicit RC/stable release workflow,
- explicit compatibility policy,
- explicit cross-repo validation matrix,
- explicit rollback policy,
- explicit conditions for retiring temporary local layers later.

Wave 7 should harden the lifecycle of the package before broadening the shared API.
