from __future__ import annotations

AUGMENTATION_TYPES = (
    "entity_tagging",
    "temporal_expression_tagging",
)

PRODUCER_KINDS = (
    "llm",
    "rules",
    "hybrid",
    "manual",
)

PRODUCER_RUN_STATUSES = (
    "queued",
    "running",
    "completed",
    "failed",
    "deferred",
    "skipped",
)

# Canonical export aliases for shared import-path normalization.
AugmentationType = AUGMENTATION_TYPES
ProducerKind = PRODUCER_KINDS
RunStatus = PRODUCER_RUN_STATUSES

CANONICAL_AUG_COMMAND_FAMILY = (
    "inspect-target",
    "submit-run",
    "submit-artifact",
    "status",
    "events",
)

CANONICAL_AUG_COMMAND_ALIASES = {
    "target-descriptor": "inspect-target",
    "inspect-runs": "status",
    "inspect-artifacts": "events",
}
