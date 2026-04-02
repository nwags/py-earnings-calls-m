from __future__ import annotations

import re


_NON_DIGIT_RE = re.compile(r"\D+")


def normalize_ticker(raw: object | None) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip().upper()
    return value or None


def normalize_cik(raw: object | None) -> str | None:
    if raw is None:
        return None
    digits = _NON_DIGIT_RE.sub("", str(raw))
    if not digits:
        return None
    if len(digits) > 10:
        digits = digits[-10:]
    return digits.zfill(10)

