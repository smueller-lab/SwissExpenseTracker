from __future__ import annotations

import math

from datetime import date
from typing import Any
from typing import Self

from pydantic import BaseModel
from pydantic import field_validator
from pydantic import model_validator


class SwissquotePositionRaw(BaseModel):
    # ── in CCY (original currency) ────────────────────────────────────
    symbol: str
    asset_class: str
    quantity: float
    unit_cost: float | None
    total_value: float | None
    price: float
    currency: str
    # ── in CHF ────────────────────────────────────────────────────────
    pnl_nominal_chf: float | None
    pnl_pct_chf: float | None
    total_value_chf: float
    position_weight_pct: float | None
    # ── injected by parser ────────────────────────────────────────────
    snapshot_date: str
    account_id: str
    name: str | None = None

    @field_validator(
        "unit_cost",
        "total_value",
        "pnl_nominal_chf",
        "pnl_pct_chf",
        "position_weight_pct",
        mode="before",
    )
    @classmethod
    def _parse_optional_float(cls, value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            f = float(value)
        except (ValueError, TypeError):
            return None
        return None if math.isnan(f) else f

    @model_validator(mode="after")
    def _check_total_value_consistency(self) -> Self:
        if self.total_value is not None:
            expected = self.quantity * self.price
            if abs(expected - self.total_value) > 0.01:
                raise ValueError(
                    f"{self.symbol}: quantity x price ({expected:.4f}) does not match "
                    f"total_value ({self.total_value:.4f}) in {self.currency}"
                )
        return self


class SwissquotePosition(BaseModel):
    # ── in CCY (original currency) ────────────────────────────────────
    symbol: str
    asset_class: str
    quantity: float
    unit_cost: float | None
    total_value: float | None
    price: float
    currency: str
    # ── in CHF ────────────────────────────────────────────────────────
    pnl_nominal_chf: float | None
    pnl_pct_chf: float | None
    total_value_chf: float
    position_weight_pct: float | None
    # ── injected by parser ────────────────────────────────────────────
    snapshot_date: date
    account_id: str
    name: str | None = None

    @field_validator(
        "unit_cost",
        "total_value",
        "pnl_nominal_chf",
        "pnl_pct_chf",
        "position_weight_pct",
        mode="before",
    )
    @classmethod
    def _parse_optional_float(cls, value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            f = float(value)
        except (ValueError, TypeError):
            return None
        return None if math.isnan(f) else f

    @model_validator(mode="after")
    def _check_total_value_consistency(self) -> Self:
        if self.total_value is not None:
            expected = self.quantity * self.price
            if abs(expected - self.total_value) > 0.01:
                raise ValueError(
                    f"{self.symbol}: quantity x price ({expected:.4f}) does not match "
                    f"total_value ({self.total_value:.4f}) in {self.currency}"
                )
        return self
