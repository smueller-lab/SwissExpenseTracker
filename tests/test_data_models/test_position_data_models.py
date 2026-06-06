from __future__ import annotations

import math

from pathlib import Path
from typing import Any

import pytest
import xlrd

from swiss_exp_tracker.pipeline_ingestion.data_models.position import (
    SwissquotePositionRaw,
)

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"


def _load_positions_xls(file_name: str) -> list[dict[str, Any]]:
    """Extract position rows from a Swissquote XLS fixture using the same parsing logic as stage_01_landing."""
    path = TEST_DATA_DIR / file_name
    wb = xlrd.open_workbook(str(path))
    ws = wb.sheet_by_index(0)

    positions: list[dict[str, Any]] = []
    asset_class = ""

    for row_idx in range(1, ws.nrows):
        row = ws.row_values(row_idx)
        s0 = str(row[0]).strip() if row[0] not in (None, "") else ""
        s1 = str(row[1]).strip() if row[1] not in (None, "") else ""

        if s0 and not s1:
            asset_class = s0
            continue

        qty = row[2]
        is_numeric = isinstance(qty, (int, float)) and not (
            isinstance(qty, float) and math.isnan(qty)
        )
        if not (s1 and not s0 and is_numeric):
            continue

        def _opt(v: object) -> object:
            if v == "" or (isinstance(v, float) and math.isnan(v)):
                return None
            return v

        positions.append(
            {
                "symbol": s1,
                "asset_class": asset_class,
                "quantity": qty,
                "unit_cost": _opt(row[3]),
                "total_value": _opt(row[4]),
                "price": row[7],
                "currency": str(row[8]).strip(),
                "pnl_nominal_chf": _opt(row[9]),
                "pnl_pct_chf": _opt(row[10]),
                "total_value_chf": row[11],
                "position_weight_pct": _opt(row[12]),
                "snapshot_date": "2025-01-01",
                "account_id": "99999",
                "name": (
                    str(row[13]).strip()
                    if len(row) > 13 and row[13] not in (None, "")
                    else None
                ),
            }
        )

    return positions


@pytest.mark.parametrize("pos", _load_positions_xls("positions_test.xls"))
def test_positions_xls_rows_parse(pos: dict[str, Any]) -> None:
    model = SwissquotePositionRaw.model_validate(pos)
    assert model.symbol
    assert model.asset_class
    assert model.quantity > 0
    assert model.price > 0
    assert model.total_value_chf > 0
    assert model.currency


def test_positions_total_value_consistency() -> None:
    """All position rows with a total_value must satisfy quantity * price == total_value."""
    for pos in _load_positions_xls("positions_test.xls"):
        model = SwissquotePositionRaw.model_validate(pos)
        if model.total_value is not None:
            assert abs(model.quantity * model.price - model.total_value) <= 0.01, (
                f"{model.symbol}: qty={model.quantity} * price={model.price} "
                f"!= total_value={model.total_value}"
            )


def test_positions_weights_sum_to_one() -> None:
    """Position weights across all rows must sum to approximately 1.0."""
    positions = [
        SwissquotePositionRaw.model_validate(p)
        for p in _load_positions_xls("positions_test.xls")
    ]
    weights = [
        p.position_weight_pct for p in positions if p.position_weight_pct is not None
    ]
    assert weights, "No position weights found"
    assert abs(sum(weights) - 1.0) < 0.01, (
        f"Weights sum to {sum(weights):.4f}, expected 1.0"
    )
