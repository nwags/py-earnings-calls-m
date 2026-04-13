# Canonical First Real RC Execution

## Goal

Define the first actual shared release-candidate execution cycle.

## Required execution sequence

1. Package-side change lands in `m-cache-shared-ext`.
2. Package-side tests pass.
3. Real RC tag is cut.
4. Central evidence bundle is opened for that RC.
5. All four consumer repos pin and validate the same RC.
6. Repo-specific signoff/evidence is ingested into the central bundle.
7. Promotion/rejection decision is recorded.
8. If accepted, stable release may be cut.
9. If rejected, rollback/recovery and next-RC path are recorded.

## RC rule

Wave 7.2 should use one real shared RC tag for all four repos in the first execution cycle.

## Evidence rule

The first RC cycle should generate a complete end-to-end evidence bundle that proves the lifecycle is operational rather than merely documented.
