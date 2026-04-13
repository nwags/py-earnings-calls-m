# Canonical Rollback and Incident Policy

## Goal

Define how release failures are handled safely.

## Rollback guidance

Rollback should remain simple:
- repin to prior known-good tag, and/or
- force local facade mode where supported,
- without changing public CLI/API behavior.

## Incident policy guidance

Wave 7 should define:
- who may trigger rollback,
- what evidence is required,
- what incident note or changelog annotation is required,
- how repos recover from a bad RC or stable release,
- how post-incident follow-up is recorded.

## Separation of concerns

Rollback policy should not require redesigning repo logic.
It should work through pinning and controlled facade behavior first.
