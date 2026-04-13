# Canonical External Package Repo Implementation

## Goal

Define what must exist in the `m-cache-shared-ext` source-of-truth repo for Wave 7.2.

## Required package-repo artifacts

At minimum, the external package repo should contain:
- `README.md`
- `GOVERNANCE.md`
- `PUBLIC_API_BOUNDARY.md`
- `RELEASE_WORKFLOW.md`
- `CHANGELOG.md`
- `COMPATIBILITY_POLICY.md`
- `ROLLBACK_AND_INCIDENTS.md`
- `evidence/` directory or equivalent bundle root
- release decision templates
- signoff ingestion templates

## Public API freeze

The strict common v1 API remains frozen in Wave 7.2.
Do not broaden the public API in this wave.

## Package ownership posture

Wave 7.2 should clearly define:
- package maintainers / release managers,
- who may cut RCs,
- who may record stable-promotion decisions,
- who may declare rollback,
- what requires review and what requires signoff evidence.

## Consumer-repo relationship

Consumer repos remain downstream validators/signoff contributors.
They do not become co-owners of the package governance by participating in RC validation.
