from __future__ import annotations

from pydantic import BaseModel
from pydantic import field_validator


class CategoryBudget(BaseModel):
    """Monthly budget allocation for a single expense category.

    budget_chf is always non-negative; blank/None coerces to 0.0 and negatives clamp to 0.0.
    """

    category: str
    budget_chf: float

    @field_validator("budget_chf", mode="before")
    @classmethod
    def _coerce_budget(cls, value: object) -> float:
        """Return a non-negative float; blank string / None → 0.0, negative values → 0.0."""
        if value is None or value == "":
            return 0.0
        coerced = float(value)  # type: ignore[arg-type]
        return max(0.0, coerced)
