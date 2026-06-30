from __future__ import annotations

import re
from typing import Any


COLUMN_MAPPING_VERSION = "bronze_column_sanitization_v1"
INVALID_COLUMN_CHARS_RE = re.compile(r"[ \t\n\r,;{}()=./\\`-]+")
INVALID_COLUMN_CHAR_RE = re.compile(r"[ \t\n\r,;{}()=./\\`-]")
MULTIPLE_UNDERSCORES_RE = re.compile(r"_+")


def sanitize_column_name(raw_name: Any) -> tuple[str, list[str]]:
    original = "" if raw_name is None else str(raw_name)
    reasons: list[str] = []

    sanitized = INVALID_COLUMN_CHARS_RE.sub("_", original)
    if sanitized != original:
        reasons.append("invalid_characters")

    if "invalid_characters" in reasons:
        collapsed = MULTIPLE_UNDERSCORES_RE.sub("_", sanitized)
        if collapsed != sanitized:
            reasons.append("collapsed_underscores")
        sanitized = collapsed

    trimmed = sanitized
    while trimmed.startswith("_") and original and INVALID_COLUMN_CHAR_RE.match(original[0]):
        trimmed = trimmed[1:]
    while trimmed.endswith("_") and original and INVALID_COLUMN_CHAR_RE.match(original[-1]):
        trimmed = trimmed[:-1]
    if trimmed != sanitized:
        reasons.append("trimmed_underscores")
    sanitized = trimmed

    if not sanitized:
        sanitized = "unnamed_field"
        reasons.append("empty_name")

    if sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
        reasons.append("starts_with_number")

    return sanitized, reasons


def unique_sanitized_names(raw_names: list[Any]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    for raw_name in raw_names:
        base_name, reasons = sanitize_column_name(raw_name)
        candidate = base_name
        suffix = 2
        while candidate.lower() in seen:
            candidate = f"{base_name}_{suffix}"
            suffix += 1

        if candidate != base_name:
            reasons = [*reasons, "duplicate_sanitized_name"]

        seen.add(candidate.lower())
        results.append(
            {
                "raw_name": "" if raw_name is None else str(raw_name),
                "bronze_name": candidate,
                "reasons": reasons,
                "reason": ",".join(dict.fromkeys(reasons)),
                "changed": candidate != ("" if raw_name is None else str(raw_name)),
            }
        )

    return results
