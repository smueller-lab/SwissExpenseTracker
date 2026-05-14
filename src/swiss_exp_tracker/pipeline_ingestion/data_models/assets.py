from __future__ import annotations

from datetime import date
from enum import Enum
from typing import cast

from pydantic import BaseModel
from pydantic import Field

from swiss_exp_tracker.pipeline_ingestion.data_models.transaction import Currency


class AssetType(Enum):
    SHARE = "SHARE"
    ETF = "ETF"


class SwissquotePositionsRaw(BaseModel):
    snapshot_date: date
    asset_type: AssetType | None
    symbol: str | None
    quantity: float | None
    opening_price: float | None
    total_value: float | None
    day_change: float | None
    day_difference: float | None
    price: float | None
    currency: Currency | None
    win_loss_nominal_value: float | None
    win_loss_percentage: float | None
    total_value_chf: float | None
    position_percentage: float | None


class SwissquotePositions(BaseModel):
    source_file: str
    snapshot_date: date
    positions: list[SwissquotePositionsRaw] = Field(
        default_factory=lambda: cast("list[SwissquotePositionsRaw]", [])
    )
