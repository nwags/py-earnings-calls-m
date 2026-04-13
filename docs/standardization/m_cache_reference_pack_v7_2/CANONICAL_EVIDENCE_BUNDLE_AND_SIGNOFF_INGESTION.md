# Canonical Evidence Bundle and Signoff Ingestion

## Goal

Define the central evidence structure for RC/stable candidates.

## Required bundle contents

For each candidate, the central bundle should contain:
- package candidate metadata,
- package-side test evidence,
- earnings validation/signoff evidence,
- news validation/signoff evidence,
- fed validation/signoff evidence,
- SEC validation/signoff evidence,
- promotion/rejection decision record,
- rollback or incident references if applicable.

## Ingestion rule

Consumer repos should contribute evidence as standardized bundle inputs.
Wave 7.2 should avoid making release decisions depend on ad hoc interpretation of repo-local docs.
