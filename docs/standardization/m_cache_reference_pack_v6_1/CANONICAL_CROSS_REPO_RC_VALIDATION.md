# Canonical Cross-Repo Release-Candidate Validation

## Goal

Define what it means for one external package release candidate to be accepted across all repos.

## Required checks for the shared RC tag

1. All repos pin the same external tag.
2. All repos target the same external distribution name.
3. All repos target the same external import root.
4. All repos use the same source-mode contract.
5. All repos keep local fallback available.
6. All full suites remain green.

## Repo validation expectations

### Shared checks
- facade/shim imports only strict-common v1 symbols
- no repo depends on non-public external symbols
- local fallback continues to work

### Repo integration checks
- CLI behavior unchanged
- API behavior unchanged
- pilot/non-pilot behavior unchanged
- applicability unchanged
- authority behavior unchanged

## Closure rule

Wave 6.1 is not complete until all four repos have validated the same RC tag successfully.
