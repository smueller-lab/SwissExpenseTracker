from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel
from pydantic import field_validator


class Trip(BaseModel):
    """Validated trip record for creation, rename, or year-edit operations."""

    name: str
    year: int

    @field_validator("name", mode="before")
    @classmethod
    def _validate_name(cls, value: object) -> str:
        """Strip surrounding whitespace; raise ValueError if blank."""
        stripped = str(value).strip()
        if not stripped:
            raise ValueError("Trip name must not be empty or whitespace-only.")
        return stripped

    @field_validator("year", mode="before")
    @classmethod
    def _validate_year(cls, value: int | str | float) -> int:
        """Reject years outside [1990, current_year + 5]."""
        year = int(value)
        current_year = datetime.now().year
        if not (1990 <= year <= current_year + 5):
            raise ValueError(
                f"Year must be between 1990 and {current_year + 5}, got {year}."
            )
        return year
