from __future__ import annotations

from typing import Any


def parse_swiss_float(value: Any) -> float | None:
    """Parse a Swiss-formatted number to float; None for empty. Handles ' and , separators."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).replace("'", "").replace(" ", "")
    if "," in cleaned and "." not in cleaned:
        cleaned = cleaned.replace(",", ".")
    return float(cleaned)


def parse_optional_text(value: Any) -> str | None:
    """Return stripped string, or None for empty / literal 'None' (UBS exports emit 'None')."""
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text in ("", "None"):
        return None
    return text
