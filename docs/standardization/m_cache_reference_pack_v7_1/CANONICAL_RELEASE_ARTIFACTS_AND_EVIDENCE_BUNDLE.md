# Canonical Release Artifacts and Evidence Bundle

## Goal

Define what artifacts must exist for each RC/stable release.

## Required release artifacts

Each RC/stable candidate should have:
- package tag,
- changelog entry,
- release notes,
- compatibility notes,
- rollback notes,
- explicit public API freeze / boundary statement,
- cross-repo validation evidence,
- repo signoff records,
- incident references if applicable.

## Evidence bundle rule

Wave 7.1 should define one standard bundle or folder structure that can hold:
- package-side test evidence,
- earnings evidence,
- news evidence,
- fed signoff evidence,
- SEC signoff evidence,
- promotion decision note.

## Output quality rule

Evidence should be reproducible and sufficient for a release decision without reinterpreting repo-local docs ad hoc.
