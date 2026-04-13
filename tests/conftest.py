from __future__ import annotations

# Ensure the local shared package fallback is initialized for test collection
# without relying on sys.path precedence tricks.
from py_earnings_calls import augmentation_shared as _augmentation_shared  # noqa: F401
