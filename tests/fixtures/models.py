from __future__ import annotations

from pydantic import BaseModel
from pydantic import field_validator


class UseTransactionFixture(BaseModel):
    """Fixture model for one transactions_use row, keyed to insert_transactions_use params."""

    transaction_id: int
    source_type: str
    date: str | None = None
    amount: float
    transaction_type: str
    currency: str | None = None
    reference: str | None = None
    merchant: str
    category_main: str
    category_second: str | None = None
    city: str | None = None
    balance_chf: float | None = None

    @field_validator(
        "date", "currency", "reference", "category_second", "city", mode="before"
    )
    @classmethod
    def _nullable_str(cls, value: object) -> object:
        """Coerce empty strings to None for nullable text columns."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("balance_chf", mode="before")
    @classmethod
    def _nullable_float(cls, value: object) -> object:
        """Coerce empty string to None for balance_chf."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    def to_insert_dict(self) -> dict[str, object]:
        """Return model fields as a dict keyed to insert_transactions_use named params."""
        return self.model_dump()


class GroceryUseFixture(BaseModel):
    """Fixture model for one groceries_use row, keyed to insert_groceries_use params."""

    rfn_id: int
    date: str
    time: str
    location: str
    article: str
    unit: str
    quantity: float
    price_chf: float
    discount_chf: float
    category_main: str
    category_detail: str

    @field_validator("date", mode="before")
    @classmethod
    def _validate_date(cls, value: object) -> object:
        """Reject empty or non-string date values for the non-nullable date column."""
        if not isinstance(value, str) or not value.strip():
            msg = f"date must be a non-empty string, got {value!r}"
            raise ValueError(msg)
        return value

    def to_insert_dict(self) -> dict[str, object]:
        """Return model fields as a dict keyed to insert_groceries_use named params."""
        return self.model_dump()
